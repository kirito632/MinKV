#include "http_server.h"
#include <iostream>
#include <sstream>

namespace minkv {
namespace server {

HttpServer::HttpServer(std::shared_ptr<MinKV<std::string, std::string>> kv,
                       std::shared_ptr<graph::GraphStore> graph_store,
                       const std::string &host, int port)
    : kv_(kv), graph_store_(graph_store), host_(host), port_(port),
      running_(false),
      server_(std::make_unique<httplib::Server>()) {
  setup_routes(); // 构造时完成路由注册，start() 前不发起监听
}

HttpServer::~HttpServer() {
  stop(); // [RAII] 析构时确保服务器已停止，防止后台线程悬空
}

// ==========================================
// 路由注册
// ==========================================

void HttpServer::setup_routes() {
  // [KV 基础接口] 工作记忆的快速读写
  server_->Post("/kv/set",
                [this](const httplib::Request &req, httplib::Response &res) {
                  handle_kv_set(req, res);
                });
  server_->Get("/kv/get",
               [this](const httplib::Request &req, httplib::Response &res) {
                 handle_kv_get(req, res);
               });
  server_->Delete("/kv/del",
                  [this](const httplib::Request &req, httplib::Response &res) {
                    handle_kv_delete(req, res);
                  });

  // [向量接口] 情景记忆的语义存取与相似度检索
  server_->Post("/vector/put",
                [this](const httplib::Request &req, httplib::Response &res) {
                  handle_vector_put(req, res);
                });
  server_->Post("/vector/search",
                [this](const httplib::Request &req, httplib::Response &res) {
                  handle_vector_search(req, res);
                });
  server_->Get("/vector/get",
               [this](const httplib::Request &req, httplib::Response &res) {
                 handle_vector_get(req, res);
               });
  server_->Delete("/vector/delete",
                  [this](const httplib::Request &req, httplib::Response &res) {
                    handle_vector_delete(req, res);
                  });

  // [健康检查] 供负载均衡器或监控系统探活
  server_->Get("/health", [](const httplib::Request &, httplib::Response &res) {
    json response = {{"status", "ok"}, {"service", "MinKV Vector Database"}};
    res.set_content(response.dump(), "application/json");
  });

  // [图接口] 仅在传入 graph_store_ 时注册，避免空指针访问
  if (graph_store_) {
    server_->Post("/graph/add_node",
                  [this](const httplib::Request &req, httplib::Response &res) {
                    handle_graph_add_node(req, res);
                  });
    server_->Post("/graph/add_edge",
                  [this](const httplib::Request &req, httplib::Response &res) {
                    handle_graph_add_edge(req, res);
                  });
    server_->Post("/graph/rag_query",
                  [this](const httplib::Request &req, httplib::Response &res) {
                    handle_graph_rag_query(req, res);
                  });
  }
}

// ==========================================
// 服务器生命周期管理
// ==========================================

bool HttpServer::start() {
  if (running_.load()) {
    std::cerr << "[HttpServer] 服务器已在运行中" << std::endl;
    return false;
  }
  std::cout << "[HttpServer] 启动服务器：" << host_ << ":" << port_
            << std::endl;
  running_.store(true);
  bool success =
      server_->listen(host_.c_str(), port_); // 阻塞，直到 stop() 被调用
  running_.store(false);
  return success;
}

bool HttpServer::start_async() {
  if (running_.load()) {
    std::cerr << "[HttpServer] 服务器已在运行中" << std::endl;
    return false;
  }
  running_.store(true);
  // [后台线程] 在独立线程中进入事件循环，调用方立即返回
  server_thread_ = std::make_unique<std::thread>([this]() {
    std::cout << "[HttpServer] 启动服务器（异步）：" << host_ << ":" << port_
              << std::endl;
    server_->listen(host_.c_str(), port_);
    running_.store(false);
  });
  // 等待 100ms 确保 httplib 完成端口绑定，避免调用方立即发请求时连接被拒
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  return true;
}

void HttpServer::stop() {
  if (!running_.load()) {
    return;
  }
  std::cout << "[HttpServer] 正在停止服务器..." << std::endl;
  server_->stop(); // 通知 httplib 事件循环退出
  if (server_thread_ && server_thread_->joinable()) {
    server_thread_->join(); // 等待后台线程完成，确保资源安全释放
  }
  running_.store(false);
  std::cout << "[HttpServer] 服务器已停止" << std::endl;
}

// ==========================================
// KV 基础接口处理器
// ==========================================

void HttpServer::handle_kv_set(const httplib::Request &req,
                               httplib::Response &res) {
  try {
    json body = json::parse(req.body);
    if (!body.contains("key") || !body.contains("value")) {
      send_error(res, 400, "缺少必填字段：key, value");
      return;
    }
    std::string key = body["key"];
    std::string value = body["value"];
    int64_t ttl_ms = body.value("ttl_ms", (int64_t)0); // 0 表示永不过期
    kv_->put(key, value, ttl_ms);
    send_success(res, {{"success", true}, {"key", key}});
  } catch (const json::exception &e) {
    send_error(res, 400, std::string("JSON 解析错误：") + e.what());
  } catch (const std::exception &e) {
    send_error(res, 500, std::string("内部错误：") + e.what());
  }
}

void HttpServer::handle_kv_get(const httplib::Request &req,
                               httplib::Response &res) {
  try {
    if (!req.has_param("key")) {
      send_error(res, 400, "缺少必填查询参数：key");
      return;
    }
    std::string key = req.get_param_value("key");
    auto result = kv_->get(key); // 返回 std::optional，未命中时为 nullopt
    if (!result) {
      send_error(res, 404, "Key 不存在");
      return;
    }
    send_success(res, {{"success", true}, {"key", key}, {"value", *result}});
  } catch (const std::exception &e) {
    send_error(res, 500, std::string("内部错误：") + e.what());
  }
}

void HttpServer::handle_kv_delete(const httplib::Request &req,
                                  httplib::Response &res) {
  try {
    if (!req.has_param("key")) {
      send_error(res, 400, "缺少必填查询参数：key");
      return;
    }
    std::string key = req.get_param_value("key");
    bool ok = kv_->remove(key); // key 不存在时返回 false
    if (ok) {
      send_success(res, {{"success", true}, {"key", key}});
    } else {
      send_error(res, 404, "Key 不存在");
    }
  } catch (const std::exception &e) {
    send_error(res, 500, std::string("内部错误：") + e.what());
  }
}

// ==========================================
// 向量接口处理器
// ==========================================

void HttpServer::handle_vector_put(const httplib::Request &req,
                                   httplib::Response &res) {
  try {
    json request_body = json::parse(req.body);

    // 校验必填字段
    if (!request_body.contains("key") || !request_body.contains("embedding")) {
      send_error(res, 400, "缺少必填字段：key, embedding");
      return;
    }

    std::string key = request_body["key"];
    std::vector<float> embedding = request_body["embedding"];

    // 校验向量非空且维度一致
    if (embedding.empty()) {
      send_error(res, 400, "向量不能为空");
      return;
    }

    // 可选字段：TTL，0 表示永不过期
    int64_t ttl_ms = 0;
    if (request_body.contains("ttl_ms")) {
      ttl_ms = request_body["ttl_ms"];
    }

    // [核心写入] 调用 MinKV 向量存储接口
    kv_->vectorPut(key, embedding, ttl_ms);

    send_success(res, {{"success", true},
                       {"message", "向量写入成功"},
                       {"key", key},
                       {"dimension", embedding.size()}});
  } catch (const json::exception &e) {
    send_error(res, 400, std::string("JSON 解析错误：") + e.what());
  } catch (const std::exception &e) {
    send_error(res, 500, std::string("内部错误：") + e.what());
  }
}

void HttpServer::handle_vector_search(const httplib::Request &req,
                                      httplib::Response &res) {
  try {
    json request_body = json::parse(req.body);

    // 校验必填字段
    if (!request_body.contains("query") || !request_body.contains("top_k")) {
      send_error(res, 400, "缺少必填字段：query, top_k");
      return;
    }

    std::vector<float> query = request_body["query"];
    size_t top_k = request_body["top_k"];

    // 校验查询向量非空
    if (query.empty()) {
      send_error(res, 400, "查询向量不能为空");
      return;
    }

    // [核心检索] 调用 MinKV L2 距离 Top-K 搜索（SIMD AVX2 加速）
    auto results = kv_->vectorSearch(query, static_cast<int>(top_k));

    // 构建响应：结果仅为 key 列表
    json results_json = json::array();
    for (const auto &key : results) {
      results_json.push_back(key);
    }

    send_success(res, {{"success", true},
                       {"query_dimension", query.size()},
                       {"top_k", top_k},
                       {"results_count", results.size()},
                       {"results", results_json}});
  } catch (const json::exception &e) {
    send_error(res, 400, std::string("JSON 解析错误：") + e.what());
  } catch (const std::exception &e) {
    send_error(res, 500, std::string("内部错误：") + e.what());
  }
}

void HttpServer::handle_vector_get(const httplib::Request &req,
                                   httplib::Response &res) {
  try {
    if (!req.has_param("key")) {
      send_error(res, 400, "缺少必填查询参数：key");
      return;
    }

    std::string key = req.get_param_value("key");

    // [核心读取] 调用 MinKV 向量读取接口
    auto embedding = kv_->vectorGet(key);

    if (embedding.empty()) {
      send_error(res, 404, "向量不存在");
      return;
    }

    json response = {{"success", true},
                     {"key", key},
                     {"embedding", embedding},
                     {"dimension", embedding.size()}};

    send_success(res, response);
  } catch (const std::exception &e) {
    send_error(res, 500, std::string("内部错误：") + e.what());
  }
}

void HttpServer::handle_vector_delete(const httplib::Request &req,
                                      httplib::Response &res) {
  try {
    if (!req.has_param("key")) {
      send_error(res, 400, "缺少必填查询参数：key");
      return;
    }

    std::string key = req.get_param_value("key");

    // [核心删除] 向量底层存储为 KV，调用 remove 即可删除
    bool success = kv_->remove(key);

    if (success) {
      send_success(
          res, {{"success", true}, {"message", "向量删除成功"}, {"key", key}});
    } else {
      send_error(res, 404, "向量不存在");
    }
  } catch (const std::exception &e) {
    send_error(res, 500, std::string("内部错误：") + e.what());
  }
}

// ==========================================
// 辅助方法
// ==========================================

DistanceMetric HttpServer::parse_metric(const std::string &metric_str) {
  if (metric_str == "cosine") {
    return DistanceMetric::COSINE; // 余弦相似度，预留待实现
  } else if (metric_str == "euclidean" || metric_str == "l2") {
    return DistanceMetric::L2; // 欧氏距离平方（当前唯一实现）
  } else if (metric_str == "dot_product" || metric_str == "dot") {
    return DistanceMetric::DOT_PRODUCT; // 点积，预留待实现
  } else {
    return DistanceMetric::L2; // 未识别的度量名称，降级为 L2
  }
}

void HttpServer::send_error(httplib::Response &res, int status_code,
                            const std::string &message) {
  // [统一错误格式] {"success": false, "error": "<message>"}
  json response = {{"success", false}, {"error", message}};
  res.status = status_code;
  res.set_content(response.dump(), "application/json");
}

void HttpServer::send_success(httplib::Response &res, const json &data) {
  // [统一成功格式] HTTP 200，Content-Type: application/json
  res.status = 200;
  res.set_content(data.dump(), "application/json");
}

// ==========================================
// 图接口处理器
// ==========================================

void HttpServer::handle_graph_add_node(const httplib::Request &req,
                                       httplib::Response &res) {
  try {
    json body = json::parse(req.body);
    if (!body.contains("node_id")) {
      send_error(res, 400, "缺少必填字段：node_id");
      return;
    }

    graph::Node node;
    node.node_id = body["node_id"];
    node.properties_json =
        body.value("properties_json", "{}"); // 节点属性，默认空对象
    graph_store_->AddNode(node);

    // 可选：写入 embedding，使该节点可被 GraphRAG 的向量检索阶段命中
    if (body.contains("embedding")) {
      std::vector<float> emb = body["embedding"];
      graph_store_->SetNodeEmbedding(node.node_id, emb);
    }

    send_success(res, {{"success", true}, {"node_id", node.node_id}});
  } catch (const json::exception &e) {
    send_error(res, 400, std::string("JSON 解析错误：") + e.what());
  } catch (const std::exception &e) {
    send_error(res, 500, std::string("内部错误：") + e.what());
  }
}

void HttpServer::handle_graph_add_edge(const httplib::Request &req,
                                       httplib::Response &res) {
  try {
    json body = json::parse(req.body);
    if (!body.contains("src_id") || !body.contains("dst_id") ||
        !body.contains("label")) {
      send_error(res, 400, "缺少必填字段：src_id, dst_id, label");
      return;
    }

    graph::Edge edge;
    edge.src_id = body["src_id"];
    edge.dst_id = body["dst_id"];
    edge.label = body["label"];
    edge.weight = body.value("weight", 1.0f); // 边权重，默认 1.0
    edge.properties_json =
        body.value("properties_json", "{}"); // 边属性，默认空对象
    graph_store_->AddEdge(edge);

    send_success(res, {{"success", true},
                       {"src_id", edge.src_id},
                       {"dst_id", edge.dst_id},
                       {"label", edge.label}});
  } catch (const json::exception &e) {
    send_error(res, 400, std::string("JSON 解析错误：") + e.what());
  } catch (const std::exception &e) {
    send_error(res, 500, std::string("内部错误：") + e.what());
  }
}

void HttpServer::handle_graph_rag_query(const httplib::Request &req,
                                        httplib::Response &res) {
  try {
    json body = json::parse(req.body);
    if (!body.contains("query_embedding")) {
      send_error(res, 400, "缺少必填字段：query_embedding");
      return;
    }

    std::vector<float> query_emb = body["query_embedding"];
    int vector_top_k =
        body.value("vector_top_k", 3); // 向量检索阶段返回的入口节点数
    int hop_depth = body.value("hop_depth", 2); // BFS 图遍历的最大跳数

    // [两阶段 GraphRAG]
    // 第一阶段：向量检索，找到语义最近的 vector_top_k 个入口节点
    // 第二阶段：从入口节点出发做 hop_depth 跳 BFS，收集所有可达节点
    auto nodes =
        graph_store_->GraphRAGQuery(query_emb, vector_top_k, hop_depth);

    // 将节点列表序列化为 JSON 数组
    json nodes_json = json::array();
    for (const auto &n : nodes) {
      nodes_json.push_back(
          {{"node_id", n.node_id}, {"properties_json", n.properties_json}});
    }

    send_success(res, {{"success", true},
                       {"node_count", nodes.size()},
                       {"vector_top_k", vector_top_k},
                       {"hop_depth", hop_depth},
                       {"nodes", nodes_json}});
  } catch (const json::exception &e) {
    send_error(res, 400, std::string("JSON 解析错误：") + e.what());
  } catch (const std::exception &e) {
    send_error(res, 500, std::string("内部错误：") + e.what());
  }
}

} // namespace server
} // namespace minkv
