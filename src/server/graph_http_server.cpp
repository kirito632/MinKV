/**
 * Graph HTTP Server
 *
 * 轻量级 HTTP Server，只暴露 Graph-on-KV 接口。
 * 不依赖向量相关的 put_vector/search_vectors 等未实现方法。
 *
 * 接口：
 *   POST /graph/add_node
 * {"node_id":"...","properties_json":"...","embedding":[...]} POST
 * /graph/add_edge    {"src_id":"...","dst_id":"...","label":"...","weight":1.0}
 *   POST /graph/rag_query
 * {"query_embedding":[...],"vector_top_k":3,"hop_depth":2} GET  /health
 *
 * 编译：
 *   cmake --build MinKV/build --target graph_http_server
 *
 * 运行：
 *   ./MinKV/build/bin/graph_http_server [port]   默认 8081
 */

#include "../core/sharded_cache.h"
#include "../graph/graph_store.h"
#include "httplib.h"
#include <iostream>
#include <memory>
#include <nlohmann/json.hpp>
#include <signal.h>

using namespace minkv::graph;
using GraphKVStore = minkv::db::ShardedCache<std::string, std::string>;
using json = nlohmann::json;

static std::shared_ptr<GraphStore> g_gs;

// ── 辅助函数
// ──────────────────────────────────────────────────────────────────

static void send_ok(httplib::Response &res, const json &data) {
  res.status = 200;
  res.set_content(data.dump(), "application/json");
}

static void send_err(httplib::Response &res, int code, const std::string &msg) {
  res.status = code;
  res.set_content(json{{"success", false}, {"error", msg}}.dump(),
                  "application/json");
}

// ── 路由处理
// ──────────────────────────────────────────────────────────────────

static void handle_add_node(const httplib::Request &req,
                            httplib::Response &res) {
  try {
    auto body = json::parse(req.body);
    if (!body.contains("node_id")) {
      send_err(res, 400, "missing node_id");
      return;
    }

    Node n;
    n.node_id = body["node_id"];
    n.properties_json = body.value("properties_json", "{}");
    g_gs->AddNode(n);

    if (body.contains("embedding")) {
      std::vector<float> emb = body["embedding"].get<std::vector<float>>();
      g_gs->SetNodeEmbedding(n.node_id, emb);
    }

    send_ok(res, {{"success", true}, {"node_id", n.node_id}});
  } catch (const std::exception &e) {
    send_err(res, 500, e.what());
  }
}

static void handle_add_edge(const httplib::Request &req,
                            httplib::Response &res) {
  try {
    auto body = json::parse(req.body);
    if (!body.contains("src_id") || !body.contains("dst_id") ||
        !body.contains("label")) {
      send_err(res, 400, "missing src_id/dst_id/label");
      return;
    }

    Edge e;
    e.src_id = body["src_id"];
    e.dst_id = body["dst_id"];
    e.label = body["label"];
    e.weight = body.value("weight", 1.0f);
    e.properties_json = body.value("properties_json", "{}");
    g_gs->AddEdge(e);

    send_ok(res, {{"success", true},
                  {"src_id", e.src_id},
                  {"dst_id", e.dst_id},
                  {"label", e.label}});
  } catch (const std::exception &e) {
    send_err(res, 500, e.what());
  }
}

static void handle_rag_query(const httplib::Request &req,
                             httplib::Response &res) {
  try {
    auto body = json::parse(req.body);
    int vector_top_k = body.value("vector_top_k", 3);
    int hop_depth = body.value("hop_depth", 2);

    std::vector<Node> nodes;

    // 支持批量向量检索 (query_embeddings) 或 单向量检索 (query_embedding)
    if (body.contains("query_embeddings")) {
      // 批量模式
      auto query_embs =
          body["query_embeddings"].get<std::vector<std::vector<float>>>();
      nodes = g_gs->GraphRAGQuery(query_embs, vector_top_k, hop_depth);
    } else if (body.contains("query_embedding")) {
      // 单向量模式 (兼容旧版)
      std::vector<float> query_emb =
          body["query_embedding"].get<std::vector<float>>();
      nodes = g_gs->GraphRAGQuery(query_emb, vector_top_k, hop_depth);
    } else {
      send_err(res, 400, "missing query_embedding or query_embeddings");
      return;
    }

    json nodes_json = json::array();
    for (const auto &n : nodes) {
      nodes_json.push_back(
          {{"node_id", n.node_id}, {"properties_json", n.properties_json}});
    }

    send_ok(res, {{"success", true},
                  {"node_count", (int)nodes.size()},
                  {"vector_top_k", vector_top_k},
                  {"hop_depth", hop_depth},
                  {"nodes", nodes_json}});
  } catch (const std::exception &e) {
    send_err(res, 500, e.what());
  }
}

// ── main
// ──────────────────────────────────────────────────────────────────────

int main(int argc, char *argv[]) {
  int port = 8081;
  if (argc >= 2)
    port = std::atoi(argv[1]);

  // 初始化 GraphStore
  auto kv = std::make_shared<GraphKVStore>(65536, 16);
  g_gs = std::make_shared<GraphStore>(kv);

  httplib::Server svr;

  svr.Post("/graph/add_node", handle_add_node);
  svr.Post("/graph/add_edge", handle_add_edge);
  svr.Post("/graph/rag_query", handle_rag_query);
  svr.Get("/health", [](const httplib::Request &, httplib::Response &res) {
    res.set_content(R"({"status":"ok","service":"MinKV Graph HTTP Server"})",
                    "application/json");
  });

  signal(SIGINT, [](int) { exit(0); });
  signal(SIGTERM, [](int) { exit(0); });

  std::cout << "[GraphHTTPServer] listening on 0.0.0.0:" << port << "\n";
  std::cout << "  POST /graph/add_node\n";
  std::cout << "  POST /graph/add_edge\n";
  std::cout << "  POST /graph/rag_query\n";
  std::cout << "  GET  /health\n\n";

  svr.listen("0.0.0.0", port);
  return 0;
}
