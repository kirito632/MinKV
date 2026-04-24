/**
 * GraphRAG 准确性测试
 *
 * 用手工设计的语义向量构建科技公司知识图谱，验证：
 * 1. 向量检索能找到语义相关的入口节点
 * 2. 图遍历能沿关系链扩展到没有 embedding 的节点
 * 3. 不同语义查询的召回准确性
 *
 * 向量设计：16 维，每个维度代表一个语义特征
 * 维度含义：
 *   [0] 航天/火箭    [1] 电动汽车    [2] AI/语言模型  [3] 云计算
 *   [4] 创始人/CEO   [5] 公司/组织   [6] 产品/硬件    [7] 软件/服务
 *   [8] 美国科技     [9] 中国科技    [10] 投资/商业   [11] 研究/学术
 *   [12-15] 保留
 */

#include "core/sharded_cache.h"
#include "graph/graph_store.h"

#include <algorithm>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

using namespace minkv::graph;
using GraphKVStore = minkv::db::ShardedCache<std::string, std::string>;

// ── 向量工具
// ──────────────────────────────────────────────────────────────────

static std::vector<float> normalize(std::vector<float> v) {
  float norm = 0.0f;
  for (float x : v)
    norm += x * x;
  norm = std::sqrt(norm);
  if (norm > 1e-8f)
    for (float &x : v)
      x /= norm;
  return v;
}

// 手工设计的语义向量（16 维）
// 格式：{航天, 电车, AI, 云, CEO, 公司, 产品, 软件, 美科技, 中科技, 投资, 研究,
// ...}
static std::vector<float> vec(std::initializer_list<float> vals) {
  std::vector<float> v(vals);
  v.resize(16, 0.0f);
  return normalize(v);
}

// ── 知识图谱构建
// ──────────────────────────────────────────────────────────────

static void build_knowledge_graph(GraphStore &gs) {
  // ── 节点（带 embedding）────────────────────────────────────────────────────
  // 人物节点
  gs.AddNode(
      {"Elon_Musk",
       R"({"type":"person","desc":"CEO of Tesla and SpaceX, serial entrepreneur"})"});
  gs.SetNodeEmbedding(
      "Elon_Musk", vec({0.3f, 0.3f, 0.1f, 0.0f, 0.9f, 0.2f, 0.0f, 0.0f, 0.8f}));

  gs.AddNode(
      {"Sam_Altman",
       R"({"type":"person","desc":"CEO of OpenAI, AI researcher and investor"})"});
  gs.SetNodeEmbedding("Sam_Altman", vec({0.0f, 0.0f, 0.9f, 0.1f, 0.9f, 0.2f,
                                         0.0f, 0.0f, 0.8f}));

  gs.AddNode(
      {"Jeff_Bezos",
       R"({"type":"person","desc":"founder of Amazon, cloud computing pioneer"})"});
  gs.SetNodeEmbedding("Jeff_Bezos", vec({0.0f, 0.0f, 0.1f, 0.7f, 0.9f, 0.2f,
                                         0.0f, 0.0f, 0.8f}));

  gs.AddNode(
      {"Jensen_Huang",
       R"({"type":"person","desc":"CEO of NVIDIA, GPU and AI chip pioneer"})"});
  gs.SetNodeEmbedding("Jensen_Huang", vec({0.1f, 0.0f, 0.7f, 0.3f, 0.9f, 0.2f,
                                           0.5f, 0.0f, 0.8f}));

  // 公司节点
  gs.AddNode(
      {"SpaceX",
       R"({"type":"company","desc":"aerospace manufacturer, rocket launch services"})"});
  gs.SetNodeEmbedding(
      "SpaceX", vec({0.9f, 0.0f, 0.0f, 0.0f, 0.0f, 0.9f, 0.5f, 0.0f, 0.8f}));

  gs.AddNode(
      {"Tesla",
       R"({"type":"company","desc":"electric vehicle and clean energy company"})"});
  gs.SetNodeEmbedding(
      "Tesla", vec({0.0f, 0.9f, 0.1f, 0.0f, 0.0f, 0.9f, 0.5f, 0.0f, 0.8f}));

  gs.AddNode(
      {"OpenAI",
       R"({"type":"company","desc":"AI research lab, creator of GPT and ChatGPT"})"});
  gs.SetNodeEmbedding(
      "OpenAI", vec({0.0f, 0.0f, 0.9f, 0.1f, 0.0f, 0.9f, 0.0f, 0.7f, 0.8f}));

  gs.AddNode(
      {"Amazon",
       R"({"type":"company","desc":"e-commerce and cloud computing giant"})"});
  gs.SetNodeEmbedding(
      "Amazon", vec({0.0f, 0.0f, 0.1f, 0.7f, 0.0f, 0.9f, 0.3f, 0.5f, 0.8f}));

  gs.AddNode(
      {"NVIDIA",
       R"({"type":"company","desc":"GPU manufacturer, AI computing infrastructure"})"});
  gs.SetNodeEmbedding(
      "NVIDIA", vec({0.1f, 0.0f, 0.7f, 0.3f, 0.0f, 0.9f, 0.8f, 0.2f, 0.8f}));

  // 产品节点（无 embedding，只能通过图遍历找到）
  gs.AddNode(
      {"Starship",
       R"({"type":"rocket","desc":"fully reusable super heavy-lift launch vehicle"})"});
  gs.AddNode(
      {"Falcon9",
       R"({"type":"rocket","desc":"reusable orbital rocket, workhorse of SpaceX"})"});
  gs.AddNode(
      {"Model_S", R"({"type":"car","desc":"Tesla flagship electric sedan"})"});
  gs.AddNode(
      {"Cybertruck", R"({"type":"car","desc":"Tesla electric pickup truck"})"});
  gs.AddNode(
      {"GPT4",
       R"({"type":"model","desc":"OpenAI large language model, multimodal"})"});
  gs.AddNode(
      {"ChatGPT",
       R"({"type":"product","desc":"OpenAI conversational AI assistant"})"});
  gs.AddNode(
      {"AWS",
       R"({"type":"service","desc":"Amazon Web Services, cloud computing platform"})"});
  gs.AddNode(
      {"H100",
       R"({"type":"chip","desc":"NVIDIA Hopper GPU, AI training accelerator"})"});
  gs.AddNode(
      {"A100", R"({"type":"chip","desc":"NVIDIA Ampere GPU, AI computing"})"});

  // ── 边（关系）──────────────────────────────────────────────────────────────
  gs.AddEdge({"Elon_Musk", "SpaceX", "founded", 1.0f, ""});
  gs.AddEdge({"Elon_Musk", "Tesla", "founded", 1.0f, ""});
  gs.AddEdge({"Sam_Altman", "OpenAI", "leads", 1.0f, ""});
  gs.AddEdge({"Jeff_Bezos", "Amazon", "founded", 1.0f, ""});
  gs.AddEdge({"Jensen_Huang", "NVIDIA", "founded", 1.0f, ""});

  gs.AddEdge({"SpaceX", "Starship", "product", 1.0f, ""});
  gs.AddEdge({"SpaceX", "Falcon9", "product", 1.0f, ""});
  gs.AddEdge({"Tesla", "Model_S", "product", 1.0f, ""});
  gs.AddEdge({"Tesla", "Cybertruck", "product", 1.0f, ""});
  gs.AddEdge({"OpenAI", "GPT4", "created", 1.0f, ""});
  gs.AddEdge({"OpenAI", "ChatGPT", "created", 1.0f, ""});
  gs.AddEdge({"Amazon", "AWS", "subsidiary", 1.0f, ""});
  gs.AddEdge({"NVIDIA", "H100", "product", 1.0f, ""});
  gs.AddEdge({"NVIDIA", "A100", "product", 1.0f, ""});
}

// ── 测试用例
// ──────────────────────────────────────────────────────────────────

struct TestCase {
  std::string query_name;
  std::vector<float> query_vec;      // 查询向量
  std::vector<std::string> expected; // 期望在结果中出现的节点
  std::string description;
};

static std::vector<TestCase> make_test_cases() {
  return {
      {"火箭/航天查询",
       vec({0.9f, 0.0f, 0.0f, 0.0f, 0.0f, 0.5f, 0.3f}), // 高航天特征
       {"SpaceX", "Starship", "Falcon9", "Elon_Musk"},
       "查询'火箭发射'，期望找到 SpaceX 及其产品 Starship/Falcon9"},
      {"电动汽车查询",
       vec({0.0f, 0.9f, 0.0f, 0.0f, 0.0f, 0.5f, 0.3f}), // 高电车特征
       {"Tesla", "Model_S", "Cybertruck", "Elon_Musk"},
       "查询'电动汽车'，期望找到 Tesla 及其产品"},
      {"AI语言模型查询",
       vec({0.0f, 0.0f, 0.9f, 0.1f, 0.0f, 0.5f, 0.0f, 0.5f}), // 高AI特征
       {"OpenAI", "GPT4", "ChatGPT", "Sam_Altman"},
       "查询'AI语言模型'，期望找到 OpenAI 及其产品"},
      {"云计算查询",
       vec({0.0f, 0.0f, 0.1f, 0.9f, 0.0f, 0.5f, 0.0f, 0.5f}), // 高云计算特征
       {"Amazon", "AWS", "Jeff_Bezos"},
       "查询'云计算服务'，期望找到 Amazon 和 AWS"},
      {"AI芯片查询",
       vec({0.1f, 0.0f, 0.7f, 0.3f, 0.0f, 0.5f, 0.8f}), // 高AI+硬件特征
       {"NVIDIA", "H100", "A100", "Jensen_Huang"},
       "查询'AI训练芯片'，期望找到 NVIDIA 及其 GPU 产品"},
      {"马斯克相关查询",
       vec({0.3f, 0.3f, 0.1f, 0.0f, 0.9f, 0.2f}), // 高CEO特征+航天+电车
       {"Elon_Musk", "SpaceX", "Tesla", "Starship", "Model_S"},
       "查询'马斯克的公司'，期望找到他创立的公司及产品"},
  };
}

// ── 主测试逻辑
// ────────────────────────────────────────────────────────────────

int main() {
  std::cout << "╔══════════════════════════════════════════════════════╗\n";
  std::cout << "║         GraphRAG 准确性测试（科技公司知识图谱）      ║\n";
  std::cout << "╚══════════════════════════════════════════════════════╝\n\n";

  auto kv = std::make_shared<GraphKVStore>(1024 * 64, 16);
  GraphStore gs(kv);

  // 构建知识图谱
  std::cout << "[构建知识图谱]\n";
  build_knowledge_graph(gs);
  std::cout
      << "  节点：9 个人物/公司（带 embedding）+ 9 个产品（无 embedding）\n";
  std::cout
      << "  边：14 条关系（founded/leads/product/created/subsidiary）\n\n";

  auto test_cases = make_test_cases();
  int total = 0, passed = 0;

  for (auto &tc : test_cases) {
    std::cout << "══════════════════════════════════════════════════════\n";
    std::cout << "查询：" << tc.query_name << "\n";
    std::cout << "说明：" << tc.description << "\n\n";

    // 执行 GraphRAG 查询（top_k=2, hop=2）
    auto results = gs.GraphRAGQuery(tc.query_vec, 2, 2);

    // 收集结果节点 ID
    std::vector<std::string> result_ids;
    for (auto &n : results)
      result_ids.push_back(n.node_id);

    std::cout << "  结果节点（" << results.size() << " 个）：";
    for (auto &id : result_ids)
      std::cout << id << " ";
    std::cout << "\n\n";

    // 验证期望节点是否都在结果中
    std::cout << "  验证：\n";
    int case_pass = 0, case_total = 0;
    for (auto &exp : tc.expected) {
      bool found = std::find(result_ids.begin(), result_ids.end(), exp) !=
                   result_ids.end();
      std::cout << "    " << (found ? "✓" : "✗") << " " << exp;
      if (!found)
        std::cout << "  ← 未找到";
      std::cout << "\n";
      if (found)
        ++case_pass;
      ++case_total;
      ++total;
      if (found)
        ++passed;
    }

    float recall = case_total > 0 ? 100.0f * case_pass / case_total : 0.0f;
    std::cout << "\n  召回率：" << case_pass << "/" << case_total << " = "
              << std::fixed << std::setprecision(0) << recall << "%\n";
  }

  std::cout << "\n══════════════════════════════════════════════════════\n";
  std::cout << "总体结果：" << passed << "/" << total << " 个期望节点被召回\n";
  float overall = total > 0 ? 100.0f * passed / total : 0.0f;
  std::cout << "总体召回率：" << std::fixed << std::setprecision(1) << overall
            << "%\n\n";

  // ── 对比：纯向量检索 vs GraphRAG ─────────────────────────────────────────
  std::cout << "══════════════════════════════════════════════════════\n";
  std::cout << "[对比] 纯向量检索 vs GraphRAG（2-hop）\n\n";

  // 产品节点（无 embedding，纯向量检索永远找不到）
  std::vector<std::string> product_nodes = {"Starship",   "Falcon9", "Model_S",
                                            "Cybertruck", "GPT4",    "ChatGPT",
                                            "AWS",        "H100",    "A100"};

  int vec_found = 0, rag_found = 0;
  for (auto &tc : test_cases) {
    auto vec_results = gs.SearchSimilarNodes(tc.query_vec, 2);
    auto rag_results = gs.GraphRAGQuery(tc.query_vec, 2, 2);

    std::vector<std::string> vec_ids, rag_ids;
    for (auto &[id, score] : vec_results)
      vec_ids.push_back(id);
    for (auto &n : rag_results)
      rag_ids.push_back(n.node_id);

    // 统计产品节点的召回
    for (auto &prod : product_nodes) {
      bool in_vec =
          std::find(vec_ids.begin(), vec_ids.end(), prod) != vec_ids.end();
      bool in_rag =
          std::find(rag_ids.begin(), rag_ids.end(), prod) != rag_ids.end();
      if (in_vec)
        ++vec_found;
      if (in_rag)
        ++rag_found;
    }
  }

  int total_product_checks = test_cases.size() * product_nodes.size();
  std::cout << "  产品节点（无 embedding，共 " << product_nodes.size()
            << " 个）检查 " << test_cases.size() << " 次查询：\n\n";
  std::cout << "  纯向量检索召回产品节点：" << vec_found << "/"
            << total_product_checks << " = " << std::fixed
            << std::setprecision(1)
            << (100.0f * vec_found / total_product_checks) << "%\n";
  std::cout << "  GraphRAG   召回产品节点：" << rag_found << "/"
            << total_product_checks << " = " << std::fixed
            << std::setprecision(1)
            << (100.0f * rag_found / total_product_checks) << "%\n\n";

  std::cout
      << "  结论：GraphRAG 通过图遍历额外召回了纯向量检索无法找到的产品节点\n";
  std::cout << "        Starship/GPT4/AWS 等节点没有 embedding，\n";
  std::cout << "        纯向量检索召回率 = 0%，GraphRAG 通过关系链找到它们\n";

  return (overall >= 70.0f) ? 0 : 1;
}
