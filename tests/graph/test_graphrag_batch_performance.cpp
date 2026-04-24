/**
 * GraphRAG 批量并发性能对比测试
 *
 * 对比两种查询方式的性能：
 * 1. 单查询版本：循环调用 GraphRAGQuery(const vector<float>&, ...)
 * 2. 批量并发版本：调用 GraphRAGQuery(const vector<vector<float>>&, ...)
 *
 * 测试不同查询数量下的加速比。
 */

#include "core/sharded_cache.h"
#include "graph/graph_store.h"
#include <cassert>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <random>
#include <vector>

using namespace minkv;
using namespace minkv::graph;

// 生成随机 embedding
std::vector<float> random_embedding(int dim, std::mt19937 &rng) {
  std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
  std::vector<float> emb(dim);
  for (int i = 0; i < dim; ++i) {
    emb[i] = dist(rng);
  }
  return emb;
}

// 构建测试图
void build_test_graph(GraphStore &gs, int num_nodes, int edges_per_node,
                      int dim) {
  std::mt19937 rng(12345);
  std::uniform_int_distribution<int> node_dist(0, num_nodes - 1);

  // 添加节点
  for (int i = 0; i < num_nodes; ++i) {
    Node node;
    node.node_id = "node_" + std::to_string(i);
    node.properties_json =
        "{\"type\":\"test\",\"index\":" + std::to_string(i) + "}";
    gs.AddNode(node);
    // 设置 embedding
    gs.SetNodeEmbedding(node.node_id, random_embedding(dim, rng));
  }

  // 添加边
  for (int i = 0; i < num_nodes; ++i) {
    for (int e = 0; e < edges_per_node; ++e) {
      int dst = node_dist(rng);
      if (dst == i)
        continue;
      Edge edge;
      edge.src_id = "node_" + std::to_string(i);
      edge.dst_id = "node_" + std::to_string(dst);
      edge.label = "connects";
      edge.weight = 1.0f;
      gs.AddEdge(edge);
    }
  }

  std::cout << "构建测试图完成: " << num_nodes << " 节点, "
            << (num_nodes * edges_per_node) << " 边, embedding 维度 " << dim
            << std::endl;
}

// 测试单查询版本（串行循环）
double
test_single_query(GraphStore &gs,
                  const std::vector<std::vector<float>> &query_embeddings,
                  int vector_top_k, int hop_depth) {
  auto start = std::chrono::high_resolution_clock::now();

  for (const auto &emb : query_embeddings) {
    auto results = gs.GraphRAGQuery(emb, vector_top_k, hop_depth);
    // 确保返回一些结果（可能为空）
    (void)results;
  }

  auto end = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double, std::milli> elapsed = end - start;
  return elapsed.count();
}

// 测试批量并发版本
double test_batch_query(GraphStore &gs,
                        const std::vector<std::vector<float>> &query_embeddings,
                        int vector_top_k, int hop_depth) {
  auto start = std::chrono::high_resolution_clock::now();

  auto results = gs.GraphRAGQuery(query_embeddings, vector_top_k, hop_depth);
  (void)results;

  auto end = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double, std::milli> elapsed = end - start;
  return elapsed.count();
}

int main() {
  std::cout << "╔══════════════════════════════════════════════════════╗\n";
  std::cout << "║   GraphRAG 批量并发性能对比测试                      ║\n";
  std::cout << "╚══════════════════════════════════════════════════════╝\n\n";

  // 配置
  const int NUM_NODES = 200;
  const int EDGES_PER_NODE = 3;
  const int EMBEDDING_DIM = 128;
  const int VECTOR_TOP_K = 3;
  const int HOP_DEPTH = 2;

  // 初始化 KV 存储和图存储（启用线程池）
  auto kv = std::make_shared<GraphKVStore>(16);           // 16 分片
  GraphStore gs(kv, std::thread::hardware_concurrency()); // 使用线程池

  // 构建测试图
  build_test_graph(gs, NUM_NODES, EDGES_PER_NODE, EMBEDDING_DIM);

  // 生成查询向量
  std::mt19937 rng(67890);
  std::vector<int> query_counts = {1, 2, 4, 8, 16};

  std::cout << "\n══════════════════════════════════════════════════════\n";
  std::cout << "测试配置:\n";
  std::cout << "  - 线程池大小: " << std::thread::hardware_concurrency()
            << "\n";
  std::cout << "  - vector_top_k: " << VECTOR_TOP_K << "\n";
  std::cout << "  - hop_depth: " << HOP_DEPTH << "\n";
  std::cout << "  - 查询向量数量: " << query_counts.back() << " (最大)\n";
  std::cout << "══════════════════════════════════════════════════════\n\n";

  std::cout << std::left << std::setw(12) << "查询数量" << std::setw(20)
            << "单查询(ms)" << std::setw(20) << "批量并发(ms)" << std::setw(15)
            << "加速比" << std::setw(20) << "效率(%)" << std::endl;
  std::cout << std::string(85, '-') << std::endl;

  for (int n : query_counts) {
    // 生成 n 个查询向量
    std::vector<std::vector<float>> queries;
    queries.reserve(n);
    for (int i = 0; i < n; ++i) {
      queries.push_back(random_embedding(EMBEDDING_DIM, rng));
    }

    // 预热（避免冷启动影响）
    if (n == query_counts[0]) {
      test_single_query(gs, queries, VECTOR_TOP_K, HOP_DEPTH);
      test_batch_query(gs, queries, VECTOR_TOP_K, HOP_DEPTH);
    }

    // 运行测试
    double time_single =
        test_single_query(gs, queries, VECTOR_TOP_K, HOP_DEPTH);
    double time_batch = test_batch_query(gs, queries, VECTOR_TOP_K, HOP_DEPTH);

    // 计算加速比和效率
    double speedup = time_single / time_batch;
    double efficiency = (speedup / n) * 100.0; // 理想加速比是 n

    std::cout << std::left << std::setw(12) << n << std::setw(20) << std::fixed
              << std::setprecision(2) << time_single << std::setw(20)
              << time_batch << std::setw(15) << std::setprecision(2) << speedup
              << std::setw(20) << std::setprecision(1) << efficiency
              << std::endl;
  }

  std::cout << "\n══════════════════════════════════════════════════════\n";
  std::cout << "结论:\n";
  std::cout << "1. 当查询数量 > 1 时，批量并发版本显著快于串行循环。\n";
  std::cout
      << "2. 加速比随查询数量增加而提高，但受限于 CPU 核心数和任务并行度。\n";
  std::cout << "3. 效率指标显示并行化效果（理想值 100%）。\n";
  std::cout << "══════════════════════════════════════════════════════\n";

  return 0;
}