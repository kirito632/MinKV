/**
 * GraphRAG 功能 + 性能演示
 *
 * 测试场景：
 *   1. 功能测试 — 用"钢铁侠的航天梦"查询，验证能通过图遍历联想到 Starship
 *   2. 性能测试 — 对比 SearchSimilarNodes vs GraphRAGQuery 的延迟
 *   3. 一致性测试 — 并发写入后 RebuildAdjacencyList 能修复邻接表
 *
 * 编译：
 *   make demo_graphrag   （见 CMakeLists.txt）
 *
 * 运行：
 *   ./bin/demo_graphrag
 */

#include "core/sharded_cache.h"
#include "graph/graph_serializer.h"
#include "graph/graph_store.h"

#include <chrono>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <vector>

using namespace minkv::graph;
using GraphKVStore = minkv::db::ShardedCache<std::string, std::string>;

// ── 工具函数
// ──────────────────────────────────────────────────────────────────

static std::shared_ptr<GraphKVStore> make_kv() {
  return std::make_shared<GraphKVStore>(1024 * 64, 16);
}

// 生成 dim 维的随机单位向量（模拟 embedding）
static std::vector<float> random_unit_vec(size_t dim, unsigned seed = 42) {
  std::mt19937 rng(seed);
  std::normal_distribution<float> dist(0.0f, 1.0f);
  std::vector<float> v(dim);
  float norm = 0.0f;
  for (auto &x : v) {
    x = dist(rng);
    norm += x * x;
  }
  norm = std::sqrt(norm);
  for (auto &x : v)
    x /= norm;
  return v;
}

// 在 base 向量上加小扰动，模拟"语义相近"的 embedding
static std::vector<float> perturb(const std::vector<float> &base, float noise,
                                  unsigned seed) {
  std::mt19937 rng(seed);
  std::normal_distribution<float> dist(0.0f, noise);
  std::vector<float> v(base.size());
  float norm = 0.0f;
  for (size_t i = 0; i < base.size(); ++i) {
    v[i] = base[i] + dist(rng);
    norm += v[i] * v[i];
  }
  norm = std::sqrt(norm);
  for (auto &x : v)
    x /= norm;
  return v;
}

// 计时工具：返回 lambda 执行时间（微秒）
template <typename F> static double time_us(F &&fn) {
  auto t0 = std::chrono::high_resolution_clock::now();
  fn();
  auto t1 = std::chrono::high_resolution_clock::now();
  return std::chrono::duration<double, std::micro>(t1 - t0).count();
}

// ── 测试 1：功能测试（GraphRAG 联想能力）────────────────────────────────────

/**
 * 知识图谱：
 *   Elon_Musk --[founded]--> SpaceX --[product]--> Starship
 *
 * 查询向量：与 Elon_Musk 的 embedding 非常接近（模拟"钢铁侠的航天梦"）
 * 期望：1-hop 只能找到 SpaceX，2-hop 能找到 Starship
 */
static bool test_graphrag_association() {
  std::cout << "\n══════════════════════════════════════════\n";
  std::cout << "测试 1：GraphRAG 联想能力（功能验证）\n";
  std::cout << "══════════════════════════════════════════\n";

  constexpr size_t DIM = 8; // 演示用小维度，原理与 768 维完全相同

  auto kv = make_kv();
  GraphStore gs(kv);

  // 构造三个节点的 embedding
  // Elon_Musk 的 embedding 作为基准
  auto emb_musk = random_unit_vec(DIM, 1);
  // SpaceX 和 Starship 用不同方向（语义不同）
  auto emb_spacex = random_unit_vec(DIM, 2);
  auto emb_starship = random_unit_vec(DIM, 3);

  // 插入节点
  gs.AddNode(
      {"Elon_Musk", R"({"role":"CEO","desc":"Tesla & SpaceX founder"})"});
  gs.AddNode(
      {"SpaceX", R"({"type":"company","desc":"aerospace manufacturer"})"});
  gs.AddNode({"Starship",
              R"({"type":"rocket","desc":"fully reusable launch vehicle"})"});

  // 设置 embedding
  gs.SetNodeEmbedding("Elon_Musk", emb_musk);
  gs.SetNodeEmbedding("SpaceX", emb_spacex);
  gs.SetNodeEmbedding("Starship", emb_starship);

  // 插入边：Musk --founded--> SpaceX --product--> Starship
  gs.AddEdge({"Elon_Musk", "SpaceX", "founded", 1.0f, ""});
  gs.AddEdge({"SpaceX", "Starship", "product", 1.0f, ""});

  // 查询向量：与 Elon_Musk 非常接近（小扰动），模拟"钢铁侠的航天梦"
  auto query = perturb(emb_musk, 0.05f, 99);

  std::cout << "\n[图谱结构]\n";
  std::cout << "  Elon_Musk --[founded]--> SpaceX --[product]--> Starship\n";
  std::cout << "\n[查询向量] 与 Elon_Musk embedding "
               "高度相似（模拟'钢铁侠的航天梦'）\n";

  // ── 纯向量检索（无图）────────────────────────────────────────────────────
  auto vec_results = gs.SearchSimilarNodes(query, 1);
  std::cout << "\n[纯向量检索 top-1]\n";
  for (auto &[id, sim] : vec_results) {
    std::cout << "  → " << id << "  (cosine=" << std::fixed
              << std::setprecision(4) << sim << ")\n";
  }

  // ── 1-hop GraphRAG ────────────────────────────────────────────────────────
  auto rag1 = gs.GraphRAGQuery(query, 1, 1);
  std::cout << "\n[GraphRAG top_k=1, hop=1]\n";
  for (auto &n : rag1) {
    std::cout << "  → " << n.node_id << "  props=" << n.properties_json << "\n";
  }

  // ── 2-hop GraphRAG ────────────────────────────────────────────────────────
  auto rag2 = gs.GraphRAGQuery(query, 1, 2);
  std::cout << "\n[GraphRAG top_k=1, hop=2]\n";
  for (auto &n : rag2) {
    std::cout << "  → " << n.node_id << "  props=" << n.properties_json << "\n";
  }

  // 验证：2-hop 结果必须包含 Starship
  bool found_starship = false;
  for (auto &n : rag2) {
    if (n.node_id == "Starship") {
      found_starship = true;
      break;
    }
  }

  // 验证：纯向量检索应该找到 Elon_Musk（最相似）
  bool vec_found_musk =
      !vec_results.empty() && vec_results[0].first == "Elon_Musk";

  std::cout << "\n[验证结果]\n";
  std::cout << "  纯向量检索定位到 Elon_Musk: "
            << (vec_found_musk ? "✓ PASS" : "✗ FAIL") << "\n";
  std::cout << "  2-hop 联想到 Starship:       "
            << (found_starship ? "✓ PASS" : "✗ FAIL") << "\n";

  std::cout << "\n[面试亮点]\n";
  std::cout << "  传统 RAG：只能找到 Elon_Musk（向量相似）\n";
  std::cout << "  Graph-on-KV：顺着 founded→product 关系链，额外挖掘出 SpaceX "
               "和 Starship\n";
  std::cout << "  这就是 GraphRAG 的核心价值：用图结构扩展语义检索的上下文\n";

  return vec_found_musk && found_starship;
}

// ── 测试 2：性能测试（向量检索 vs GraphRAG 延迟对比）────────────────────────

static void test_performance() {
  std::cout << "\n══════════════════════════════════════════\n";
  std::cout << "测试 2：性能测试（延迟对比）\n";
  std::cout << "══════════════════════════════════════════\n";

  constexpr size_t DIM = 128;  // 128 维，接近轻量 embedding
  constexpr int N_NODES = 200; // 图中节点数
  constexpr int N_EDGES = 400; // 图中边数（平均出度 2）
  constexpr int N_RUNS = 100;  // 每项测试重复次数

  auto kv = make_kv();
  GraphStore gs(kv);

  std::mt19937 rng(42);

  // 插入节点 + embedding
  for (int i = 0; i < N_NODES; ++i) {
    std::string id = "node_" + std::to_string(i);
    gs.AddNode({id, R"({"idx":)" + std::to_string(i) + "}"});
    gs.SetNodeEmbedding(id, random_unit_vec(DIM, i));
  }

  // 插入随机边（构造稀疏图）
  std::uniform_int_distribution<int> node_dist(0, N_NODES - 1);
  for (int i = 0; i < N_EDGES; ++i) {
    int src = node_dist(rng);
    int dst = node_dist(rng);
    if (src != dst) {
      gs.AddEdge({"node_" + std::to_string(src), "node_" + std::to_string(dst),
                  "link", 1.0f, ""});
    }
  }

  auto query = random_unit_vec(DIM, 9999);

  // 预热
  for (int i = 0; i < 5; ++i) {
    gs.SearchSimilarNodes(query, 5);
    gs.GraphRAGQuery(query, 5, 1);
    gs.GraphRAGQuery(query, 5, 2);
  }

  // 测量 SearchSimilarNodes
  std::vector<double> vec_times(N_RUNS);
  for (int i = 0; i < N_RUNS; ++i) {
    vec_times[i] = time_us([&] { gs.SearchSimilarNodes(query, 5); });
  }

  // 测量 GraphRAGQuery 1-hop
  std::vector<double> rag1_times(N_RUNS);
  for (int i = 0; i < N_RUNS; ++i) {
    rag1_times[i] = time_us([&] { gs.GraphRAGQuery(query, 5, 1); });
  }

  // 测量 GraphRAGQuery 2-hop
  std::vector<double> rag2_times(N_RUNS);
  for (int i = 0; i < N_RUNS; ++i) {
    rag2_times[i] = time_us([&] { gs.GraphRAGQuery(query, 5, 2); });
  }

  auto stats = [](std::vector<double> &v) -> std::pair<double, double> {
    std::sort(v.begin(), v.end());
    double avg = std::accumulate(v.begin(), v.end(), 0.0) / v.size();
    double p99 = v[static_cast<size_t>(v.size() * 0.99)];
    return {avg, p99};
  };

  auto [vec_avg, vec_p99] = stats(vec_times);
  auto [rag1_avg, rag1_p99] = stats(rag1_times);
  auto [rag2_avg, rag2_p99] = stats(rag2_times);

  std::cout << "\n[测试配置] " << N_NODES << " 节点, " << N_EDGES << " 边, "
            << DIM << " 维 embedding, 重复 " << N_RUNS << " 次\n\n";
  std::cout << std::fixed << std::setprecision(1);
  std::cout << "  操作                      avg(μs)   P99(μs)\n";
  std::cout << "  ─────────────────────────────────────────────\n";
  std::cout << "  SearchSimilarNodes         " << std::setw(7) << vec_avg
            << "   " << std::setw(7) << vec_p99 << "\n";
  std::cout << "  GraphRAGQuery (1-hop)      " << std::setw(7) << rag1_avg
            << "   " << std::setw(7) << rag1_p99 << "\n";
  std::cout << "  GraphRAGQuery (2-hop)      " << std::setw(7) << rag2_avg
            << "   " << std::setw(7) << rag2_p99 << "\n";
  std::cout << "\n  1-hop 额外开销: +" << std::setprecision(1)
            << (rag1_avg - vec_avg) << " μs\n";
  std::cout << "  2-hop 额外开销: +" << (rag2_avg - vec_avg) << " μs\n";

  std::cout << "\n[优化说明]\n";
  std::cout << "  优化 1：邻接表二进制序列化（替换 JSON）\n";
  std::cout << "    - 序列化：直接 memcpy，无转义，无状态机\n";
  std::cout << "    - 反序列化：按偏移读取，无分支，cache line 友好\n";
  std::cout << "  优化 2：并发 BFS（std::async + shared_lock）\n";
  std::cout << "    - top_k 个入口节点的 BFS 并行执行\n";
  std::cout << "    - BFS 是纯读操作，ShardedCache shared_lock 无竞争\n";
  std::cout << "    - 多核机器上 GraphRAG 延迟接近单次 BFS，而非 top_k 倍\n";
}

// ── 测试 3：一致性测试（并发写入 + RebuildAdjacencyList）────────────────────

static bool test_consistency() {
  std::cout << "\n══════════════════════════════════════════\n";
  std::cout << "测试 3：一致性测试（并发写入 + 邻接表重建）\n";
  std::cout << "══════════════════════════════════════════\n";

  constexpr int N_THREADS = 8;
  constexpr int EDGES_PER_THREAD = 20;

  auto kv = make_kv();
  GraphStore gs(kv);

  // 预先插入节点
  for (int i = 0; i < 20; ++i) {
    gs.AddNode({"n" + std::to_string(i), "{}"});
  }

  // 8 个线程并发 AddEdge
  std::cout << "\n[并发写入] " << N_THREADS << " 线程 × " << EDGES_PER_THREAD
            << " 条边 = " << N_THREADS * EDGES_PER_THREAD << " 条边\n";

  std::vector<std::thread> threads;
  for (int t = 0; t < N_THREADS; ++t) {
    threads.emplace_back([&gs, t]() {
      std::mt19937 rng(t * 100);
      std::uniform_int_distribution<int> d(0, 19);
      for (int i = 0; i < EDGES_PER_THREAD; ++i) {
        int src = d(rng), dst = d(rng);
        if (src != dst) {
          gs.AddEdge({"n" + std::to_string(src), "n" + std::to_string(dst),
                      "t" + std::to_string(t), 1.0f, ""});
        }
      }
    });
  }
  for (auto &th : threads)
    th.join();
  std::cout << "  并发写入完成\n";

  // 统计写入后的边数和邻接表条目数
  auto all_data = kv->export_all_data();
  int edge_count = 0, adj_count = 0;
  for (auto &[k, v] : all_data) {
    if (k.size() > 2 && k.substr(0, 2) == "e:")
      ++edge_count;
    if (k.size() > 4 && k.substr(0, 4) == "adj:")
      ++adj_count;
  }
  std::cout << "  写入后：" << edge_count << " 条边，" << adj_count
            << " 个邻接表 Key\n";

  // 模拟"邻接表损坏"：手动删除所有 adj: Key
  std::cout << "\n[模拟损坏] 删除所有邻接表 Key（模拟崩溃后邻接表丢失）\n";
  for (auto &[k, v] : all_data) {
    if (k.size() > 4 && k.substr(0, 4) == "adj:") {
      kv->remove(k);
    }
  }

  // 验证损坏后邻接表为空
  auto sample_out = gs.GetOutNeighbors("n0");
  std::cout << "  损坏后 n0 的出边邻居数: " << sample_out.size()
            << " (期望 0)\n";

  // 执行 RebuildAdjacencyList
  std::cout << "\n[重建] 执行 RebuildAdjacencyList()...\n";
  double rebuild_us = time_us([&] { gs.RebuildAdjacencyList(); });
  std::cout << "  重建耗时: " << std::fixed << std::setprecision(1)
            << rebuild_us << " μs\n";

  // 验证一致性：对每条边，检查邻接表是否包含对应条目
  auto all_data2 = kv->export_all_data();
  int ok = 0, fail = 0;
  for (auto &[k, v] : all_data2) {
    if (k.size() <= 2 || k.substr(0, 2) != "e:")
      continue;
    try {
      Edge e = minkv::graph::GraphSerializer::DeserializeEdge(v);
      auto out = gs.GetOutNeighbors(e.src_id);
      auto in = gs.GetInNeighbors(e.dst_id);
      bool dst_in_out =
          std::find(out.begin(), out.end(), e.dst_id) != out.end();
      bool src_in_in = std::find(in.begin(), in.end(), e.src_id) != in.end();
      if (dst_in_out && src_in_in)
        ++ok;
      else
        ++fail;
    } catch (...) {
      ++fail;
    }
  }

  std::cout << "\n[一致性验证] 检查每条边的邻接表条目\n";
  std::cout << "  通过: " << ok << " 条边\n";
  std::cout << "  失败: " << fail << " 条边\n";

  bool passed = (fail == 0 && ok > 0);
  std::cout << "  结果: " << (passed ? "✓ PASS" : "✗ FAIL") << "\n";

  std::cout << "\n[面试战绩]\n";
  std::cout << "  8 线程并发写入，ShardedCache 分片锁保证单次 KV 操作安全\n";
  std::cout << "  RebuildAdjacencyList 扫描所有 e: Key 重建邻接表\n";
  std::cout << "  即使邻接表完全丢失，也能通过重建恢复图拓扑一致性\n";

  return passed;
}

// ── main
// ──────────────────────────────────────────────────────────────────────

int main() {
  std::cout << "╔══════════════════════════════════════════╗\n";
  std::cout << "║     Graph-on-KV  GraphRAG Demo           ║\n";
  std::cout << "╚══════════════════════════════════════════╝\n";

  bool t1 = test_graphrag_association();
  test_performance();
  bool t3 = test_consistency();

  std::cout << "\n══════════════════════════════════════════\n";
  std::cout << "最终结果\n";
  std::cout << "══════════════════════════════════════════\n";
  std::cout << "  功能测试（GraphRAG 联想）: " << (t1 ? "✓ PASS" : "✗ FAIL")
            << "\n";
  std::cout << "  一致性测试（重建邻接表）:  " << (t3 ? "✓ PASS" : "✗ FAIL")
            << "\n";

  return (t1 && t3) ? 0 : 1;
}
