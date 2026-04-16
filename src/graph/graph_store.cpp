#include "graph_store.h"
#include "graph_serializer.h"
#include "../base/thread_pool.h"

#include <algorithm>    // std::find, std::reverse
#include <future>       // std::future（线程池 submit 返回值）
#include <queue>        // std::queue（BFS 用）
#include <unordered_set>
#include <mutex>
#include <cstring>      // std::memcpy

namespace minkv {
namespace graph {

// ══════════════════════════════════════════════════════════════════════════════
// 构造函数
// ══════════════════════════════════════════════════════════════════════════════

GraphStore::GraphStore(std::shared_ptr<GraphKVStore> kv_store, size_t n_threads)
    : kv_(std::move(kv_store))
{
    // n_threads > 1 时创建线程池，否则串行（避免单核机器上的无谓开销）
    if (n_threads > 1) {
        thread_pool_ = std::make_unique<minkv::base::ThreadPool>(n_threads);
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// Key 构造辅助函数
//
// 为什么需要转义？
//   Key 格式是 "prefix:part1:part2:..."，用 ':' 分隔各段。
//   如果 node_id 本身含有 ':'，直接拼接会导致 Key 解析歧义。
//   解决方案：把 id 中的 ':' 替换为 '\:'，把 '\' 替换为 '\\'。
//
// 例：node_id = "user:123"
//   EscapeId  -> "user\:123"
//   NodeKey   -> "n:user\:123"   （只有一个 ':' 分隔符，不会被误分割）
// ══════════════════════════════════════════════════════════════════════════════

std::string GraphStore::EscapeId(const std::string& id) {
    std::string out;
    out.reserve(id.size());
    for (char c : id) {
        if      (c == ':')  { out += '\\'; out += ':'; }   // ':' -> '\:'
        else if (c == '\\') { out += '\\'; out += '\\'; }  // '\' -> '\\'
        else                { out += c; }
    }
    return out;
}

std::string GraphStore::NodeKey(const std::string& node_id) {
    return "n:" + EscapeId(node_id);
}

std::string GraphStore::EdgeKey(const std::string& src,
                                const std::string& dst,
                                const std::string& label) {
    // 格式：e:{src}:{dst}:{label}
    // 三段都需要转义，确保每个 ':' 分隔符都是真正的分隔符
    return "e:" + EscapeId(src) + ":" + EscapeId(dst) + ":" + EscapeId(label);
}

std::string GraphStore::AdjOutKey(const std::string& node_id) {
    return "adj:out:" + EscapeId(node_id);
}

std::string GraphStore::AdjInKey(const std::string& node_id) {
    return "adj:in:" + EscapeId(node_id);
}

std::string GraphStore::VecKey(const std::string& node_id) {
    return "vec:" + EscapeId(node_id);
}

// ══════════════════════════════════════════════════════════════════════════════
// 邻接表内部辅助函数
//
// 邻接表存储格式：JSON 数组字符串，例如 ["alice","bob"]
// 每次修改都是"读 -> 改 -> 写"三步（read-modify-write）。
// ══════════════════════════════════════════════════════════════════════════════

/** 从 KV 读取邻接表；Key 不存在时返回空列表，不报错 */
std::vector<std::string> GraphStore::LoadAdjList(const std::string& kv_key) const {
    auto val = kv_->get(kv_key);
    if (!val) return {};  // Key 不存在 -> 空邻接表
    return GraphSerializer::DeserializeAdjList(*val);
}

/** 将邻接表序列化后写回 KV */
void GraphStore::SaveAdjList(const std::string& kv_key,
                              const std::vector<std::string>& list) {
    kv_->put(kv_key, GraphSerializer::SerializeAdjList(list));
}

/**
 * 向邻接表追加一个 id（自动去重）
 * 流程：读取现有列表 -> 检查是否已存在 -> 追加 -> 写回
 */
void GraphStore::AdjListAdd(const std::string& kv_key, const std::string& id) {
    auto list = LoadAdjList(kv_key);
    // 去重：如果 id 已经在列表里，不重复添加
    if (std::find(list.begin(), list.end(), id) == list.end()) {
        list.push_back(id);
        SaveAdjList(kv_key, list);
    }
}

/**
 * 从邻接表移除一个 id
 * 如果移除后列表变空，直接删除整个 Key（节省存储空间）
 */
void GraphStore::AdjListRemove(const std::string& kv_key, const std::string& id) {
    auto list = LoadAdjList(kv_key);
    auto it = std::find(list.begin(), list.end(), id);
    if (it == list.end()) return;  // id 不在列表里，无需操作
    list.erase(it);
    if (list.empty()) {
        kv_->remove(kv_key);  // 空列表直接删 Key
    } else {
        SaveAdjList(kv_key, list);
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// Phase 1: Node CRUD
// ══════════════════════════════════════════════════════════════════════════════

/** 添加节点：序列化后写入 n:{node_id} */
void GraphStore::AddNode(const Node& node) {
    kv_->put(NodeKey(node.node_id), GraphSerializer::SerializeNode(node));
}

/** 查询节点：读取 n:{node_id} 后反序列化；不存在返回 nullopt */
std::optional<Node> GraphStore::GetNode(const std::string& node_id) const {
    auto val = kv_->get(NodeKey(node_id));
    if (!val) return std::nullopt;
    return GraphSerializer::DeserializeNode(*val);
}

/**
 * 更新节点属性
 * 只覆盖 n: Key，不碰 adj:out/adj:in（邻接表）和 vec:（embedding）。
 * 这样更新属性不会破坏图的拓扑结构。
 */
void GraphStore::UpdateNode(const Node& node) {
    kv_->put(NodeKey(node.node_id), GraphSerializer::SerializeNode(node));
}

/**
 * 删除节点（级联删除）
 *
 * 删除顺序：
 *   1. 删除节点数据 n:{node_id} 和 embedding vec:{node_id}
 *   2. 遍历出边邻居，从它们的 adj:in 中移除本节点
 *   3. 遍历入边前驱，从它们的 adj:out 中移除本节点
 *   4. 删除本节点的 adj:out 和 adj:in
 *   5. 扫描所有 e: Key，删除以本节点为 src 或 dst 的边数据
 *
 * 顺序说明：
 *   - 步骤 1 先行：逻辑上宣告节点不存在，并发读取立即返回 nullopt
 *   - 步骤 2、3 必须在步骤 4 之前：需要先读取邻接表才能知道去哪些邻居处清理
 *   - 步骤 5 代价 O(total_keys)，仅在删除节点时触发
 */
void GraphStore::DeleteNode(const std::string& node_id) {
    kv_->remove(NodeKey(node_id));
    kv_->remove(VecKey(node_id));

    // 从所有出边邻居的入边邻接表中移除本节点
    auto out_neighbors = LoadAdjList(AdjOutKey(node_id));
    for (const auto& nb : out_neighbors) {
        AdjListRemove(AdjInKey(nb), node_id);
    }

    // 从所有入边前驱的出边邻接表中移除本节点
    auto in_predecessors = LoadAdjList(AdjInKey(node_id));
    for (const auto& pred : in_predecessors) {
        AdjListRemove(AdjOutKey(pred), node_id);
    }

    // 删除本节点自己的邻接表
    kv_->remove(AdjOutKey(node_id));
    kv_->remove(AdjInKey(node_id));

    // 删除以本节点为端点的所有边数据（e: Key）
    // 扫描所有 KV 数据，过滤出 src 或 dst 等于 node_id 的边并删除
    // 代价：O(total_keys)，仅在删除节点时触发，可接受
    const std::string escaped = EscapeId(node_id);
    const std::string src_prefix = "e:" + escaped + ":";   // e:{node_id}:...
    const std::string dst_marker = ":" + escaped + ":";    // e:{src}:{node_id}:...
    auto all_data = kv_->export_all_data();
    for (const auto& [k, v] : all_data) {
        if (k.size() < 2 || k.substr(0, 2) != "e:") continue;
        // 以 node_id 为 src：Key 以 "e:{escaped_id}:" 开头
        if (k.substr(0, src_prefix.size()) == src_prefix) {
            kv_->remove(k);
            continue;
        }
        // 以 node_id 为 dst：Key 中包含 ":{escaped_id}:" 且位于第二段
        // 格式 e:{src}:{dst}:{label}，找第一个 ':' 后的位置
        auto first_colon = k.find(':', 2);  // 跳过 "e:" 前缀
        if (first_colon != std::string::npos) {
            auto second_colon = k.find(':', first_colon + 1);
            if (second_colon != std::string::npos) {
                std::string dst_part = k.substr(first_colon + 1, second_colon - first_colon - 1);
                if (dst_part == escaped) {
                    kv_->remove(k);
                }
            }
        }
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// Phase 1: Edge CRUD
// ══════════════════════════════════════════════════════════════════════════════

/**
 * 添加有向边
 *
 * Phase 1 只写边数据（Step 1）。
 * Phase 2 会补全 Step 2（更新 adj:out）和 Step 3（更新 adj:in）。
 *
 * 写入顺序设计：先写边数据，再写邻接表。
 * 这样即使进程在 Step 2/3 之前崩溃，边数据是完整的，
 * 邻接表只是缺了条目（可以用 RebuildAdjacencyList 修复），
 * 不会出现"邻接表有记录但边不存在"的更危险情况。
 */
void GraphStore::AddEdge(const Edge& edge) {
    // Step 1: 写边数据（先写边，再写邻接表，保证崩溃后边数据完整）
    kv_->put(EdgeKey(edge.src_id, edge.dst_id, edge.label),
             GraphSerializer::SerializeEdge(edge));
    // Step 2: 更新出边邻接表（AdjListAdd 内部去重，多条不同 label 的边只记录一次 dst）
    AdjListAdd(AdjOutKey(edge.src_id), edge.dst_id);
    // Step 3: 更新入边邻接表
    AdjListAdd(AdjInKey(edge.dst_id), edge.src_id);
}

/** 查询边：按三元组 Key 查找；不存在返回 nullopt */
std::optional<Edge> GraphStore::GetEdge(const std::string& src_id,
                                         const std::string& dst_id,
                                         const std::string& label) const {
    auto val = kv_->get(EdgeKey(src_id, dst_id, label));
    if (!val) return std::nullopt;
    return GraphSerializer::DeserializeEdge(*val);
}

/**
 * 删除边
 * Phase 1 只删边数据；Phase 2 补全邻接表清理。
 */
void GraphStore::DeleteEdge(const std::string& src_id,
                             const std::string& dst_id,
                             const std::string& label) {
    // Step 1: 删除边数据
    kv_->remove(EdgeKey(src_id, dst_id, label));

    // Step 2: 检查 (src, dst) 间是否还有其他 label 的边
    // 只有当该对节点间所有边都删完后，才从邻接表中移除
    // 策略：尝试用 export_all_data 扫描太重，改用"尝试读取任意其他边"的方式
    // 实际上邻接表存的是 dst，不区分 label，所以需要检查是否还有同 src->dst 的边
    // 简单做法：扫描 KV 中是否还有 e:{src}:{dst}: 前缀的 Key
    bool has_other_edge = false;
    const std::string edge_prefix = "e:" + EscapeId(src_id) + ":" + EscapeId(dst_id) + ":";
    auto all_data = kv_->export_all_data();
    for (const auto& [k, v] : all_data) {
        if (k.size() > edge_prefix.size() &&
            k.substr(0, edge_prefix.size()) == edge_prefix) {
            has_other_edge = true;
            break;
        }
    }

    if (!has_other_edge) {
        // (src, dst) 间无其他边，从邻接表中移除
        AdjListRemove(AdjOutKey(src_id), dst_id);
        AdjListRemove(AdjInKey(dst_id), src_id);
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// Phase 2: 邻接表查询（辅助方法已在上面实现，这里是公开接口）
// ══════════════════════════════════════════════════════════════════════════════

/** 返回 node_id 的所有出边邻居；节点无出边时返回空列表 */
std::vector<std::string> GraphStore::GetOutNeighbors(const std::string& node_id) const {
    return LoadAdjList(AdjOutKey(node_id));
}

/** 返回 node_id 的所有入边前驱；节点无入边时返回空列表 */
std::vector<std::string> GraphStore::GetInNeighbors(const std::string& node_id) const {
    return LoadAdjList(AdjInKey(node_id));
}

void GraphStore::RebuildAdjacencyList() {
    // Step 1: 导出所有 KV 数据，过滤出边数据（e: 前缀）和邻接表 Key（adj: 前缀）
    auto all_data = kv_->export_all_data();

    // Step 2: 清空所有现有邻接表（adj:out:* 和 adj:in:*）
    for (const auto& [k, v] : all_data) {
        if (k.size() > 4 && k.substr(0, 4) == "adj:") {
            kv_->remove(k);
        }
    }

    // Step 3: 遍历所有边，重新构建邻接表
    // 边 Key 格式：e:{src}:{dst}:{label}，Value 是序列化的 Edge 结构体
    for (const auto& [k, v] : all_data) {
        if (k.size() > 2 && k.substr(0, 2) == "e:") {
            try {
                Edge edge = GraphSerializer::DeserializeEdge(v);
                AdjListAdd(AdjOutKey(edge.src_id), edge.dst_id);
                AdjListAdd(AdjInKey(edge.dst_id), edge.src_id);
            } catch (const std::exception&) {
                // 跳过损坏的边数据，继续处理其他边
            }
        }
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// Phase 3: K-hop BFS 遍历
//
// 算法：标准 BFS，用 visited 集合防止重复访问和死循环（处理有环图）。
// 返回 {node_id -> hop_distance}，不含起始节点。
// ══════════════════════════════════════════════════════════════════════════════

std::unordered_map<std::string, int>
GraphStore::KHopNeighbors(const std::string& start_id, int k) const {
    std::unordered_map<std::string, int> result;
    if (k <= 0) return result;  // k=0 直接返回空

    std::unordered_set<std::string> visited;
    // queue 存 {节点ID, 当前跳数}
    std::queue<std::pair<std::string, int>> q;

    visited.insert(start_id);
    q.push({start_id, 0});

    while (!q.empty()) {
        auto [node, depth] = q.front();
        q.pop();

        if (depth >= k) continue;  // 已达到最大跳数，不再扩展

        for (const auto& nb : GetOutNeighbors(node)) {
            // visited.insert 返回 {iterator, bool}，bool=true 表示是新节点
            if (visited.insert(nb).second) {
                result[nb] = depth + 1;
                q.push({nb, depth + 1});
            }
        }
    }
    return result;
}

// ══════════════════════════════════════════════════════════════════════════════
// Phase 3: 最短路径查询（BFS）
//
// 用 parent map 记录每个节点的前驱，找到终点后回溯还原路径。
// ══════════════════════════════════════════════════════════════════════════════

std::vector<std::string>
GraphStore::FindPath(const std::string& src_id,
                     const std::string& dst_id,
                     int max_hops) const {
    // 特殊情况：起点等于终点
    if (src_id == dst_id) return {src_id};

    // parent[node] = 到达 node 的前驱节点，用于路径回溯
    std::unordered_map<std::string, std::string> parent;
    std::queue<std::string> q;
    std::unordered_set<std::string> visited;

    visited.insert(src_id);
    q.push(src_id);
    int hops = 0;

    while (!q.empty() && hops < max_hops) {
        // 按层处理，每层代表一跳
        int level_size = static_cast<int>(q.size());
        ++hops;

        for (int i = 0; i < level_size; ++i) {
            std::string cur = q.front();
            q.pop();

            for (const auto& nb : GetOutNeighbors(cur)) {
                if (visited.insert(nb).second) {
                    parent[nb] = cur;  // 记录前驱

                    if (nb == dst_id) {
                        // 找到终点，从 parent map 回溯路径
                        std::vector<std::string> path;
                        for (std::string n = dst_id; n != src_id; n = parent[n]) {
                            path.push_back(n);
                        }
                        path.push_back(src_id);
                        // 回溯得到的是逆序，翻转后得到正向路径
                        std::reverse(path.begin(), path.end());
                        return path;
                    }
                    q.push(nb);
                }
            }
        }
    }
    return {};  // 无路径或超出 max_hops
}

// ══════════════════════════════════════════════════════════════════════════════
// Phase 4: Embedding 存取
//
// 格式：float[] 的 raw bytes，直接用 memcpy 转换，与 VectorOps 兼容。
// ══════════════════════════════════════════════════════════════════════════════

/**
 * 存储 embedding：把 float[] 的内存字节直接写入 KV
 * 例：{1.0f, 2.0f} -> 8 字节的字符串（每个 float 4 字节）
 */
void GraphStore::SetNodeEmbedding(const std::string& node_id,
                                   const std::vector<float>& embedding) {
    std::string raw(reinterpret_cast<const char*>(embedding.data()),
                    embedding.size() * sizeof(float));
    kv_->put(VecKey(node_id), raw);
}

/**
 * 读取 embedding：把 KV 中的字节流还原为 float[]
 * 维度 = 字节数 / 4（每个 float 4 字节）
 */
std::vector<float> GraphStore::GetNodeEmbedding(const std::string& node_id) const {
    auto val = kv_->get(VecKey(node_id));
    if (!val || val->empty()) return {};
    size_t dim = val->size() / sizeof(float);
    std::vector<float> vec(dim);
    std::memcpy(vec.data(), val->data(), dim * sizeof(float));
    return vec;
}

/**
 * 向量相似度检索（Phase 4）
 *
 * 遍历所有 vec: 前缀的 Key，对每个 embedding 调用 CosineSimilarity_AVX2。
 * 用最小堆维护 top-k：堆顶是当前 top-k 中相似度最低的，
 * 新元素比堆顶大时才入堆，保证堆始终是最高的 k 个。
 * 最后将堆内容倒序输出（降序）。
 *
 * 维度不匹配的节点直接跳过，不中断查询。
 */
std::vector<std::pair<std::string, float>>
GraphStore::SearchSimilarNodes(const std::vector<float>& query_embedding,
                                int top_k) const {
    if (query_embedding.empty() || top_k <= 0) return {};

    const size_t query_dim = query_embedding.size();
    const float* query_ptr = query_embedding.data();

    // 最小堆：{similarity, node_id}，堆顶是相似度最小的元素
    // 用 pair<float, string> 默认按 float 比较，正好是最小堆语义
    using Entry = std::pair<float, std::string>;
    std::priority_queue<Entry, std::vector<Entry>, std::greater<Entry>> min_heap;

    auto all_data = kv_->export_all_data();
    static const std::string VEC_PREFIX = "vec:";

    for (const auto& [k, v] : all_data) {
        // 只处理 vec: 前缀的 Key
        if (k.size() <= VEC_PREFIX.size() ||
            k.substr(0, VEC_PREFIX.size()) != VEC_PREFIX) {
            continue;
        }

        // 还原 embedding 维度，跳过维度不匹配的节点
        if (v.size() % sizeof(float) != 0) continue;
        size_t node_dim = v.size() / sizeof(float);
        if (node_dim != query_dim) continue;

        const float* node_ptr = reinterpret_cast<const float*>(v.data());
        float sim = VectorOps::CosineSimilarity_AVX2(query_ptr, node_ptr, query_dim);

        // node_id 是 Key 去掉 "vec:" 前缀后的部分（已转义，但对外接口直接用原始 Key 后缀）
        // 注意：这里存的是转义后的 node_id，调用方通过 GetNode 时需要原始 id。
        // 为保持一致性，我们存储原始 node_id（从 VecKey 逆推不现实），
        // 改为直接从 Edge 反序列化获取。实际上 VecKey = "vec:" + EscapeId(node_id)，
        // 但我们在 SetNodeEmbedding 时没有存原始 id。
        // 解决方案：遍历时用 GetNode 验证，或者直接把 Key 后缀当 node_id 传给 GetNode。
        // 由于 EscapeId 是单射的，这里我们存储转义后的 id 作为 key，
        // 但 GetNode 接受的是原始 id。
        // 正确做法：在 SearchSimilarNodes 中，node_id 应该是原始 id。
        // 我们通过 export_all_data 拿到的 Key 是 "vec:{escaped_id}"，
        // 需要去掉前缀后 unescape 得到原始 id。
        // 简化：由于 EscapeId 只转义 ':' 和 '\'，我们实现 UnescapeId。
        std::string escaped_id = k.substr(VEC_PREFIX.size());
        // Unescape: '\:' -> ':', '\\' -> '\'
        std::string node_id;
        node_id.reserve(escaped_id.size());
        for (size_t i = 0; i < escaped_id.size(); ++i) {
            if (escaped_id[i] == '\\' && i + 1 < escaped_id.size()) {
                ++i;
                node_id += escaped_id[i];  // '\:' -> ':', '\\' -> '\'
            } else {
                node_id += escaped_id[i];
            }
        }

        if (static_cast<int>(min_heap.size()) < top_k) {
            min_heap.push({sim, node_id});
        } else if (sim > min_heap.top().first) {
            min_heap.pop();
            min_heap.push({sim, node_id});
        }
    }

    // 将堆内容转为降序结果
    std::vector<std::pair<std::string, float>> result;
    result.reserve(min_heap.size());
    while (!min_heap.empty()) {
        auto [s, id] = min_heap.top();
        min_heap.pop();
        result.push_back({id, s});
    }
    // 堆弹出是升序，翻转得到降序
    std::reverse(result.begin(), result.end());
    return result;
}

/**
 * GraphRAG 两阶段查询（Phase 4）
 *
 * Phase 1：SearchSimilarNodes 找到 vector_top_k 个入口节点
 * Phase 2：对每个入口节点并发做 KHopNeighbors(hop_depth) 扩展，合并去重
 *   - 每个入口节点的 BFS 用 std::async 独立启动
 *   - BFS 是纯读操作，ShardedCache 的 shared_lock 支持并发读，无锁竞争
 *   - 结果合并用 mutex 保护，合并开销远小于 BFS 本身
 * Phase 3：加载所有节点的完整属性，跳过不存在的节点
 *
 * 并发收益：top_k 个入口节点的 BFS 从串行变并行，
 * 在多核机器上延迟接近单次 BFS，而非 top_k 倍。
 */
std::vector<Node>
GraphStore::GraphRAGQuery(const std::vector<float>& query_embedding,
                           int vector_top_k,
                           int hop_depth) const {
    // Phase 1: 向量检索入口节点
    auto entries = SearchSimilarNodes(query_embedding, vector_top_k);
    if (entries.empty()) return {};

    // Phase 2: 并发 K-hop 扩展
    // 策略：有线程池 且 入口节点 > 1 且 hop_depth >= 2 时并发，否则串行
    // 线程池预创建线程，消除 std::async 每次创建线程的 50~200μs 开销
    const bool use_parallel = thread_pool_
                              && (static_cast<int>(entries.size()) > 1)
                              && (hop_depth >= 2);

    std::unordered_set<std::string> all_node_ids;
    for (const auto& [entry_id, score] : entries) {
        all_node_ids.insert(entry_id);
    }

    if (use_parallel) {
        // 并发路径：通过线程池提交 BFS 任务
        // BFS 是纯读操作，ShardedCache 的 shared_lock 支持并发读，无锁竞争
        std::vector<std::future<std::unordered_map<std::string, int>>> futures;
        futures.reserve(entries.size());
        for (const auto& [entry_id, score] : entries) {
            futures.push_back(
                thread_pool_->submit(
                    [this, entry_id, hop_depth]() {
                        return KHopNeighbors(entry_id, hop_depth);
                    }
                )
            );
        }
        for (auto& fut : futures) {
            for (const auto& [nb_id, dist] : fut.get()) {
                all_node_ids.insert(nb_id);
            }
        }
    } else {
        // 串行路径：单入口、浅 hop 或无线程池时使用
        for (const auto& [entry_id, score] : entries) {
            for (const auto& [nb_id, dist] : KHopNeighbors(entry_id, hop_depth)) {
                all_node_ids.insert(nb_id);
            }
        }
    }

    // Phase 3: 加载完整节点属性，跳过不存在的节点
    std::vector<Node> result;
    result.reserve(all_node_ids.size());
    for (const auto& node_id : all_node_ids) {
        auto node = GetNode(node_id);
        if (node) {
            result.push_back(std::move(*node));
        }
    }
    return result;
}

} // namespace graph
} // namespace minkv
