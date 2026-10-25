/**
 * @file http_server_example.cpp
 * @brief MinKV HTTP 服务示例（KV + 向量 + 可选 HNSW 索引）
 *
 * 这个文件同时充当 http_server.cpp 的编译入口：CMakeLists 里的
 * `http_server_example` target 由 `if(EXISTS examples/http_server_example.cpp)`
 * 保护，之前该文件缺失，导致 `src/server/http_server.cpp` 实际上没有任何
 * target 编译它。
 *
 * 用法：
 *   ./http_server_example [维度] [端口]
 *   ./http_server_example 128 8080
 *
 * 启动后可用：
 *   curl -s localhost:8080/health
 *   curl -s -X POST localhost:8080/vector/put \
 *        -d '{"key":"v1","embedding":[0.1,0.2,0.3,0.4]}'
 *   curl -s -X POST localhost:8080/vector/search \
 *        -d '{"query":[0.1,0.2,0.3,0.4],"top_k":3}'
 */

#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>

#include "core/minkv.h"
#include "server/http_server.h"

int main(int argc, char **argv) {
  const size_t dim = argc > 1 ? static_cast<size_t>(std::stoul(argv[1])) : 128;
  const int port = argc > 2 ? std::stoi(argv[2]) : 8080;

  using Engine = minkv::MinKV<std::string, std::string>;
  std::shared_ptr<Engine> kv(Engine::create(200000, 16));

  if (kv->enableAnnIndex(dim)) {
    std::cout << "[example] HNSW index enabled (dim=" << dim << ")" << std::endl;
  } else {
    std::cout << "[example] HNSW index unavailable, exact search only" << std::endl;
  }

  // graph_store 传 nullptr：本示例只验证 KV 与向量接口，
  // 图接口的注册条件见 HttpServer::setup_routes。
  minkv::server::HttpServer server(kv, nullptr, "127.0.0.1", port);
  std::cout << "[example] listening on http://127.0.0.1:" << port << std::endl;
  server.start();
  return 0;
}
