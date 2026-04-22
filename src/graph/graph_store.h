#pragma once

#include "graph_types.h"
#include "../core/sharded_cache.h"
#include "../base/thread_pool.h"

#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>
#include <unordered_map>
#include <climits>

namespace minkv {
namespace graph {

/**
 * GraphKVStore — 底层 KV 存储的类型别名
 *
 * 整个图（节点、边、邻接表、embedding）都存在同一个
 * ShardedCache<string, string> 实例里，通过 Key 前缀区分数据类型：
 *
 *   n:{node_id}              -> Node 二进制序列化
 *   e:{src}:{dst}:{label}    -> Edge 二进制序列化
 *   adj:out:{node_id}        -> 出边邻接表 JSON 数组
 *   adj:in:{node_id}         -> 入边邻接表 JSON 数组
 *   vec:{node_id}            -> embedding raw bytes (float[])
 */
using GraphKVStore = minkv::db::ShardedCache<std::string, std::string>;

/**
 * GraphStore — 图数据库的顶层接口
 *
 * 职责：
 *   1. 把图操作（AddNode、AddEdge、KHopNeighbors 等）翻译成 KV 的 put/get/remove
 *   2. 调用 GraphSerializer 完成结构体与字节流的互转
 *   3. 维护邻接表的一致性（AddEdge 时同步更新 adj:out / adj:in）
 *
 * 分阶段实现：
 *   Phase 1 — Node/Edge CRUD（本文件已完成）
 *   Phase 2 — 邻接表维护（AddEdge/DeleteEdge 补全邻接表更新）
 *   Phase 3 — K-hop BFS、路径查询
 *   Phase 4 — Embedding 存取、向量检索、GraphRAG
 */
class GraphStore {
public:
    /**
     * 构造函数：接管或共享一个已创建好的 KV 存储实例
     *
     * @param kv_store   底层 KV 存储
     * @param n_threads  并发 BFS 的线程池大小，默认 hardware_concurrency
     *                   传 0 或 1 时退化为串行（不启动线程池）
     */
    explicit GraphStore(std::shared_ptr<GraphKVStore> kv_store,
                        size_t n_threads = std::thread::hardware_concurrency());

    // ── Phase 1: Node CRUD ────────────────────────────────────────────────────

    /** 添加节点；若 node_id 已存在则覆盖 */
    void AddNode(const Node& node);

    /** 查询节点；不存在返回 std::nullopt */
    std::optional<Node> GetNode(const std::string& node_id) const;

    /**
     * 更新节点属性
     * 只覆盖 n:{node_id} 这个 Key，不会动邻接表（adj:）和 embedding（vec:）
     */
    void UpdateNode(const Node& node);

    /**
     * 删除节点
     * 同时删除：节点数据(n:)、embedding(vec:)、以及该节点在所有邻居邻接表中的条目
     */
    void DeleteNode(const std::string& node_id);

    // ── Phase 1: Edge CRUD ────────────────────────────────────────────────────

    /**
     * 添加有向边
     * Phase 1 只写边数据；Phase 2 补全邻接表更新（adj:out / adj:in）
     */
    void AddEdge(const Edge& edge);

    /** 查询边；不存在返回 std::nullopt */
    std::optional<Edge> GetEdge(const std::string& src_id,
                                const std::string& dst_id,
                                const std::string& label) const;

    /**
     * 删除边
     * Phase 1 只删边数据；Phase 2 补全邻接表清理
     */
    void DeleteEdge(const std::string& src_id,
                    const std::string& dst_id,
                    const std::string& label);

    // ── Phase 2: Adjacency ────────────────────────────────────────────────────

    /** 返回 node_id 的所有出边邻居（直接读 adj:out:{node_id}） */
    std::vector<std::string> GetOutNeighbors(const std::string& node_id) const;

    /** 返回 node_id 的所有入边前驱（直接读 adj:in:{node_id}） */
    std::vector<std::string> GetInNeighbors(const std::string& node_id) const;

    // ── Phase 3: Graph Query ──────────────────────────────────────────────────

    /**
     * K-hop BFS 遍历
     *
     * 从 start_id 出发，沿有向边最多走 k 步，返回所有可达节点及其跳数。
     * 返回值：{node_id -> hop_distance}，不含起始节点自身。
     * k=0 时返回空 map。
     */
    std::unordered_map<std::string, int>
        KHopNeighbors(const std::string& start_id, int k) const;

    /**
     * 最短路径查询（BFS）
     *
     * 返回从 src_id 到 dst_id 的节点序列（含首尾）。
     * src_id == dst_id 时返回 {src_id}。
     * 无路径或超出 max_hops 时返回空列表。
     */
    std::vector<std::string>
        FindPath(const std::string& src_id,
                 const std::string& dst_id,
                 int max_hops = INT_MAX) const;

    // ── Phase 4: Embedding & Vector Search ───────────────────────────────────

    /**
     * 存储节点的 embedding 向量
     * 格式：float[] 的 raw bytes，与 VectorOps::Serialize 兼容
     */
    void SetNodeEmbedding(const std::string& node_id,
                          const std::vector<float>& embedding);

    /**
     * 读取节点的 embedding 向量
     * 不存在时返回空 vector
     */
    std::vector<float> GetNodeEmbedding(const std::string& node_id) const;

    /**
     * 向量相似度检索（余弦相似度，AVX2 加速）
     *
     * 遍历所有 vec: 前缀的 Key，找出与 query_embedding 最相似的 top_k 个节点。
     * 返回值：{node_id, cosine_similarity}，按相似度降序排列。
     * 维度不匹配的节点会被跳过。
     */
    std::vector<std::pair<std::string, float>>
        SearchSimilarNodes(const std::vector<float>& query_embedding,
                           int top_k) const;

    // ── Phase 4: GraphRAG ─────────────────────────────────────────────────────

    /**
     * GraphRAG 两阶段查询
     *
     * Phase 1：SearchSimilarNodes 找到 vector_top_k 个入口节点
     * Phase 2：对每个入口节点做 KHopNeighbors(hop_depth) 扩展上下文
     * Phase 3：加载所有涉及节点的完整属性，去重后返回
     *
     * 用途：把图结构知识注入 LLM 的 prompt，提升回答质量
     */
    std::vector<Node>
        GraphRAGQuery(const std::vector<float>& query_embedding,
                      int vector_top_k,
                      int hop_depth) const;

   /**
    * GraphRAG 两阶段查询 (批量并发版)
    *
    * @param query_embeddings  批量查询向量，每个向量代表一个待检索的实体
    * @param vector_top_k      每个向量检索的入口节点数
    * @param hop_depth         图遍历深度
    * @return                  合并去重后的节点列表
    *
    * 核心流程:
    *  1. 并发向量检索: 对每个 query_embedding 并发调用 SearchSimilarNodes
    *  2. 结果合并: 使用 std::unordered_set 去重所有入口节点
    *  3. 并发图遍历: 对所有入口节点并发执行 KHopNeighbors
    */
   std::vector<Node>
       GraphRAGQuery(const std::vector<std::vector<float>>& query_embeddings,
                     int vector_top_k,
                     int hop_depth) const;

    // ── 一致性修复 ────────────────────────────────────────────────────────────

    /**
     * 重建邻接表（崩溃恢复用）
     *
     * 扫描所有 e: 前缀的 Key，从边数据重新构建 adj:out / adj:in。
     * 用于进程崩溃后邻接表与边数据不一致的修复场景。
     */
    void RebuildAdjacencyList();

private:
    std::shared_ptr<GraphKVStore> kv_;  // 底层 KV 存储，所有图数据都在这里

    // 并发 BFS 线程池（GraphRAGQuery Phase 2 使用）
    // n_threads <= 1 时为 nullptr，退化为串行
    std::unique_ptr<minkv::base::ThreadPool> thread_pool_;

    // ── Key 构造辅助函数 ──────────────────────────────────────────────────────
    //
    // 规则：node_id / label 中的 ':' 需要转义为 '\:'，防止 Key 解析歧义。
    // 例如 node_id="a:b" -> EscapeId 后变成 "a\:b"，拼出的 Key 不会被误分割。

    /** 对 id 中的 ':' 和 '\' 做转义 */
    static std::string EscapeId(const std::string& id);

    /** 节点数据 Key：n:{node_id} */
    static std::string NodeKey(const std::string& node_id);

    /** 边数据 Key：e:{src}:{dst}:{label} */
    static std::string EdgeKey(const std::string& src,
                               const std::string& dst,
                               const std::string& label);

    /** 出边邻接表 Key：adj:out:{node_id} */
    static std::string AdjOutKey(const std::string& node_id);

    /** 入边邻接表 Key：adj:in:{node_id} */
    static std::string AdjInKey(const std::string& node_id);

    /** Embedding Key：vec:{node_id} */
    static std::string VecKey(const std::string& node_id);

    // ── 邻接表内部辅助 ────────────────────────────────────────────────────────

    /** 从 KV 读取邻接表并反序列化；Key 不存在时返回空列表 */
    std::vector<std::string> LoadAdjList(const std::string& kv_key) const;

    /** 将邻接表序列化后写入 KV */
    void SaveAdjList(const std::string& kv_key,
                     const std::vector<std::string>& list);

    /** 向邻接表追加一个 id（自动去重） */
    void AdjListAdd(const std::string& kv_key, const std::string& id);

    /** 从邻接表移除一个 id；列表变空时删除整个 Key */
    void AdjListRemove(const std::string& kv_key, const std::string& id);
};

} // namespace graph
} // namespace minkv
