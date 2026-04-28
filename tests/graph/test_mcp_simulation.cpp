/**
 * MCP Server 功能模拟测试
 *
 * 模拟 graph_add_node / graph_add_edge / graph_rag_query 三个 MCP 工具
 * 的完整数据流，不依赖 HTTP Server 和 OpenAI API。
 *
 * embedding 用随机向量代替真实的文本 embedding，验证图遍历逻辑正确。
 *
 * 编译：make test_mcp_simulation
 * 运行：./bin/test_mcp_simulation
 */

#include "core/sharded_cache.h"
#include "graph/graph_store.h"

#include <cmath>
#include <iostream>
#include <random>
#include <string>
#include <vector>

using namespace minkv::graph;
using GraphKVStore = minkv::db::ShardedCache<std::string, std::string>;

// ── 工具函数
// ──────────────────────────────────────────────────────────────────

// 生成归一化随机向量（模拟 embedding）
static std::vector<float> make_embedding(size_t dim, unsigned seed) {
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

// 在 base 向量上加小扰动（模拟语义相近的查询）
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

// ── 模拟 MCP 工具调用
// ─────────────────────────────────────────────────────────

// 模拟 graph_add_node 工具
static void tool_graph_add_node(GraphStore &gs, const std::string &node_id,
                                const std::string &props_json,
                                const std::vector<float> &embedding = {}) {
  Node n{node_id, props_json};
  gs.AddNode(n);
  if (!embedding.empty()) {
    gs.SetNodeEmbedding(node_id, embedding);
  }
  std::cout << "  [graph_add_node] node_id=" << node_id << "\n";
}

// 模拟 graph_add_edge 工具
static void tool_graph_add_edge(GraphStore &gs, const std::string &src,
                                const std::string &dst,
                                const std::string &label, float weight = 1.0f) {
  Edge e{src, dst, label, weight, "{}"};
  gs.AddEdge(e);
  std::cout << "  [graph_add_edge] " << src << " -[" << label << "]-> " << dst
            << "\n";
}

// 模拟 graph_rag_query 工具（不调用 OpenAI，直接用传入的向量）
static std::vector<Node>
tool_graph_rag_query(GraphStore &gs, const std::vector<float> &query_embedding,
                     int vector_top_k, int hop_depth) {
  std::cout << "  [graph_rag_query] top_k=" << vector_top_k
            << " hop_depth=" << hop_depth << "\n";
  return gs.GraphRAGQuery(query_embedding, vector_top_k, hop_depth);
}

// ── 测试主体
// ──────────────────────────────────────────────────────────────────

int main() {
  std::cout << "╔══════════════════════════════════════════╗\n";
  std::cout << "║   MCP Server 功能模拟测试                ║\n";
  std::cout << "╚══════════════════════════════════════════╝\n\n";

  constexpr size_t DIM = 8; // 小维度，原理与 768 维相同

  auto kv = std::make_shared<GraphKVStore>(1024 * 64, 16);
  GraphStore gs(kv);

  // ── Step 1: 构建知识图谱（模拟 graph_add_node + graph_add_edge）────────────
  std::cout << "[Step 1] 构建知识图谱\n";

  auto emb_musk = make_embedding(DIM, 1);
  auto emb_spacex = make_embedding(DIM, 2);
  // Starship 没有 embedding（测试图遍历能找到无 embedding 的节点）

  tool_graph_add_node(gs, "Elon_Musk",
                      R"({"role":"CEO","desc":"Tesla & SpaceX founder"})",
                      emb_musk);
  tool_graph_add_node(gs, "SpaceX",
                      R"({"type":"company","desc":"aerospace manufacturer"})",
                      emb_spacex);
  tool_graph_add_node(
      gs, "Starship",
      R"({"type":"rocket","desc":"fully reusable launch vehicle"})");

  tool_graph_add_edge(gs, "Elon_Musk", "SpaceX", "founded");
  tool_graph_add_edge(gs, "SpaceX", "Starship", "product");

  std::cout << "\n[图谱结构]\n";
  std::cout << "  Elon_Musk(有embedding) --[founded]--> SpaceX(有embedding) "
               "--[product]--> Starship(无embedding)\n\n";

  // ── Step 2: 模拟 graph_rag_query（用接近 Elon_Musk 的向量查询）────────────
  std::cout << "[Step 2] 执行 graph_rag_query\n";

  // 查询向量：与 Elon_Musk 高度相似（模拟"钢铁侠的航天梦"）
  auto query = perturb(emb_musk, 0.05f, 99);

  // 1-hop：应该找到 Elon_Musk + SpaceX
  auto result1 = tool_graph_rag_query(gs, query, 1, 1);
  std::cout << "  1-hop 结果 (" << result1.size() << " 个节点):\n";
  for (auto &n : result1) {
    std::cout << "    → " << n.node_id << "  " << n.properties_json << "\n";
  }

  // 2-hop：应该额外找到 Starship
  auto result2 = tool_graph_rag_query(gs, query, 1, 2);
  std::cout << "  2-hop 结果 (" << result2.size() << " 个节点):\n";
  for (auto &n : result2) {
    std::cout << "    → " << n.node_id << "  " << n.properties_json << "\n";
  }

  // ── Step 3: 验证 ──────────────────────────────────────────────────────────
  std::cout << "\n[Step 3] 验证结果\n";

  bool found_musk = false;
  bool found_spacex = false;
  bool found_starship = false;
  for (auto &n : result2) {
    if (n.node_id == "Elon_Musk")
      found_musk = true;
    if (n.node_id == "SpaceX")
      found_spacex = true;
    if (n.node_id == "Starship")
      found_starship = true;
  }

  auto check = [](bool ok, const std::string &msg) {
    std::cout << "  " << (ok ? "✓ PASS" : "✗ FAIL") << "  " << msg << "\n";
    return ok;
  };

  bool all_pass = true;
  all_pass &= check(found_musk, "2-hop 包含 Elon_Musk（向量检索入口节点）");
  all_pass &= check(found_spacex, "2-hop 包含 SpaceX（1-hop 邻居）");
  all_pass &= check(found_starship,
                    "2-hop 包含 Starship（2-hop 邻居，无 embedding 也能找到）");
  all_pass &=
      check(result1.size() < result2.size(), "2-hop 结果集 > 1-hop 结果集");

  std::cout << "\n[面试亮点]\n";
  std::cout << "  传统 RAG：只能找到 Elon_Musk（向量相似）\n";
  std::cout << "  Graph-on-KV：顺着 founded→product 关系链，联想到 Starship\n";
  std::cout << "  Starship 没有 embedding，纯向量检索永远找不到它\n";
  std::cout << "  这就是 GraphRAG 的核心价值：图结构扩展了语义检索的边界\n";

  std::cout << "\n══════════════════════════════════════════\n";
  std::cout << "结果: " << (all_pass ? "✓ 全部通过" : "✗ 有失败项") << "\n";

  return all_pass ? 0 : 1;
}
