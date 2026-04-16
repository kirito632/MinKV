#pragma once

#include "../core/minkv.h"
#include "../graph/graph_store.h"
#include "../vector/vector_value.h"
#include "../vector/vector_ops.h"
#include "httplib.h"
#include <nlohmann/json.hpp>
#include <memory>
#include <string>
#include <thread>
#include <atomic>

namespace minkv {
namespace server {

using json = nlohmann::json;

/**
 * @brief 向量距离度量类型
 *
 * [设计取舍] 当前 MinKV 仅实现 L2 距离（欧氏距离平方），
 * 保留此枚举用于未来扩展余弦相似度、点积等度量方式。
 */
enum class DistanceMetric {
    L2,           ///< 欧氏距离平方（当前唯一实现）
    COSINE,       ///< 余弦相似度（预留）
    DOT_PRODUCT   ///< 点积（预留）
};

/**
 * @brief MinKV HTTP 服务器
 *
 * [核心职责] 将 MinKV 的存储能力以 RESTful JSON API 的形式对外暴露，
 * 是 MCP Server、外部客户端与 C++ 存储引擎之间的通信桥梁。
 *
 * 提供三类 API 端点：
 * - KV 基础接口：工作记忆的快速读写，对应 Agent 的短期上下文存储
 *   POST   /kv/set      写入键值对（支持 TTL 过期）
 *   GET    /kv/get      按 key 精确读取
 *   DELETE /kv/del      删除指定 key
 * - 向量接口：情景记忆的语义存取，支持近似最近邻检索
 *   POST   /vector/put      插入向量及元数据
 *   POST   /vector/search   向量相似度搜索
 *   GET    /vector/get      按 key 获取向量
 *   DELETE /vector/delete   删除向量
 * - 图接口：知识图谱与 GraphRAG 多跳推理
 *   POST   /graph/add_node  添加图节点
 *   POST   /graph/add_edge  添加有向边
 *   POST   /graph/rag_query GraphRAG 查询（向量检索 + K 跳 BFS 展开）
 *
 * 设计原则：
 * - [RAII] 服务器生命周期由构造/析构函数管理，资源自动释放
 * - [线程安全] 所有端点均线程安全，running_ 使用原子变量保护
 * - [错误处理] 统一使用 send_error/send_success 封装 HTTP 状态码与 JSON 响应
 * - [可选图支持] graph_store_ 为 nullptr 时图端点不注册，向量端点仍正常工作
 *
 * @note 依赖 cpp-httplib（单头文件）和 nlohmann/json
 */
class HttpServer {
public:
    /**
     * @brief 构造 HTTP 服务器
     * @param kv          MinKV 存储引擎实例，提供 KV 与向量操作能力
     * @param graph_store 图存储实例（可为 nullptr，为空时不注册图端点）
     * @param host        监听地址，默认 "0.0.0.0" 监听所有网卡
     * @param port        监听端口，默认 8080
     *
     * [初始化流程] 构造时调用 setup_routes() 完成所有路由注册，
     * 不会立即启动监听，需手动调用 start() 或 start_async()。
     */
    explicit HttpServer(
        std::shared_ptr<MinKV<std::string, std::string>> kv,
        std::shared_ptr<graph::GraphStore> graph_store = nullptr,
        const std::string& host = "0.0.0.0",
        int port = 8080
    );

    /**
     * @brief 析构函数，自动停止服务器并回收线程资源
     *
     * [RAII] 若服务器处于运行状态，析构时会调用 stop()，
     * 等待后台线程 join 完成后再释放资源，防止悬空线程。
     */
    ~HttpServer();

    /**
     * @brief 阻塞式启动服务器
     * @return 启动成功返回 true，已在运行则返回 false
     *
     * [阻塞模式] 调用后当前线程进入 httplib 事件循环，
     * 直到调用 stop() 或进程退出才返回，适合在主线程中使用。
     */
    bool start();

    /**
     * @brief 异步启动服务器（后台线程）
     * @return 启动成功返回 true，已在运行则返回 false
     *
     * [非阻塞模式] 在独立线程中启动事件循环，调用方立即返回，
     * 适合需要在同一进程中并行处理其他逻辑的场景。
     * 启动后等待 100ms 以确认服务器已进入监听状态。
     */
    bool start_async();

    /**
     * @brief 停止服务器并等待后台线程退出
     *
     * [优雅关闭] 调用 httplib::Server::stop() 通知事件循环退出，
     * 随后 join 后台线程，确保所有进行中的请求处理完成后再返回。
     */
    void stop();

    /**
     * @brief 查询服务器是否正在运行
     * @return 运行中返回 true
     *
     * [原子读取] 通过 std::atomic<bool> 无锁查询运行状态，线程安全。
     */
    bool is_running() const { return running_.load(); }

    /**
     * @brief 获取监听地址
     * @return 服务器绑定的 host 字符串
     */
    const std::string& host() const { return host_; }

    /**
     * @brief 获取监听端口
     * @return 服务器绑定的端口号
     */
    int port() const { return port_; }

private:
    std::shared_ptr<MinKV<std::string, std::string>> kv_;   ///< MinKV 存储引擎，提供 KV、向量、持久化能力
    std::shared_ptr<graph::GraphStore> graph_store_;         ///< 图存储实例，为空时不注册图端点
    std::unique_ptr<httplib::Server> server_;                ///< cpp-httplib 服务器对象，管理路由与事件循环
    std::string host_;                                       ///< 监听地址
    int port_;                                               ///< 监听端口
    std::atomic<bool> running_;                              ///< 运行状态标志，原子操作保证可见性
    std::unique_ptr<std::thread> server_thread_;             ///< 异步模式下的后台线程

    /**
     * @brief 注册所有 HTTP 路由
     *
     * [路由表] 在构造函数中调用一次，按分组依次注册：
     * 1. KV 基础接口（/kv/*）
     * 2. 向量接口（/vector/*）
     * 3. 健康检查（/health）
     * 4. 图接口（/graph/*，仅当 graph_store_ 非空时注册）
     */
    void setup_routes();

    // ==========================================
    // KV 基础接口处理器
    // ==========================================

    /**
     * @brief POST /kv/set — 写入键值对到工作记忆
     *
     * [请求体]
     * {
     *   "key":    "agent:state",   // 必填，唯一键
     *   "value":  "...",           // 必填，字符串值（可为 JSON 序列化内容）
     *   "ttl_ms": 5000             // 可选，过期时间毫秒，0 表示永不过期
     * }
     *
     * [响应]
     * {"success": true, "key": "agent:state"}
     *
     * [应用场景] Agent 短期上下文、会话状态、临时计算结果缓存
     */
    void handle_kv_set(const httplib::Request& req, httplib::Response& res);

    /**
     * @brief GET /kv/get?key=k — 从工作记忆精确读取
     *
     * [查询参数] key=<目标键>
     *
     * [响应（命中）]  {"success": true,  "key": "k", "value": "v"}
     * [响应（未命中）] HTTP 404，{"success": false, "error": "Key not found"}
     *
     * [应用场景] Agent 读取已保存的上下文或状态
     */
    void handle_kv_get(const httplib::Request& req, httplib::Response& res);

    /**
     * @brief DELETE /kv/del?key=k — 删除工作记忆中的键
     *
     * [查询参数] key=<目标键>
     *
     * [响应（成功）] {"success": true,  "key": "k"}
     * [响应（未找到）] HTTP 404，{"success": false, "error": "Key not found"}
     *
     * [应用场景] 清理已消费的 Agent 上下文，释放工作记忆容量
     */
    void handle_kv_delete(const httplib::Request& req, httplib::Response& res);

    // ==========================================
    // 向量接口处理器
    // ==========================================

    /**
     * @brief POST /vector/put — 插入向量及元数据
     *
     * [请求体]
     * {
     *   "key":       "doc:001",          // 必填，向量唯一标识
     *   "embedding": [0.1, 0.2, ...],    // 必填，浮点数组，维度须一致
     *   "metadata":  {"title": "..."},   // 可选，任意 JSON 对象
     *   "ttl_ms":    0                   // 可选，向量过期时间
     * }
     *
     * [响应] {"success": true, "key": "doc:001", "dimension": 1536}
     *
     * [应用场景] 将文本/图像 embedding 写入向量索引，用于后续语义检索
     */
    void handle_vector_put(const httplib::Request& req, httplib::Response& res);

    /**
     * @brief POST /vector/search — 向量相似度搜索（L2 距离 Top-K）
     *
     * [请求体]
     * {
     *   "query": [0.1, 0.2, ...],  // 必填，查询向量
     *   "top_k": 10                 // 必填，返回最相似的 k 个结果
     * }
     *
     * [响应]
     * {
     *   "success": true,
     *   "query_dimension": 1536,
     *   "top_k": 10,
     *   "results": ["doc:001", "doc:002", ...]  // 仅返回 key 列表
     * }
     *
     * [性能] 底层使用 SIMD（AVX2）加速 L2 距离计算，微秒级延迟
     * @note 当前实现仅支持 L2 距离，返回结果按距离升序排列（最近邻在前）
     */
    void handle_vector_search(const httplib::Request& req, httplib::Response& res);

    /**
     * @brief GET /vector/get?key=vec1 — 按 key 获取向量详情
     *
     * [查询参数] key=<向量键>
     *
     * [响应]
     * {
     *   "success":   true,
     *   "key":       "vec1",
     *   "embedding": [0.1, 0.2, ...],
     *   "metadata":  {"title": "..."},
     *   "timestamp": 1712345678000,
     *   "dimension": 1536
     * }
     */
    void handle_vector_get(const httplib::Request& req, httplib::Response& res);

    /**
     * @brief DELETE /vector/delete?key=vec1 — 删除向量
     *
     * [查询参数] key=<向量键>
     *
     * [响应（成功）]  {"success": true,  "key": "vec1"}
     * [响应（未找到）] HTTP 404，{"success": false, "error": "Vector not found"}
     *
     * @note 底层调用 KV remove，向量与元数据一同删除
     */
    void handle_vector_delete(const httplib::Request& req, httplib::Response& res);

    // ==========================================
    // 图接口处理器
    // ==========================================

    /**
     * @brief POST /graph/add_node — 添加图节点
     *
     * [请求体]
     * {
     *   "node_id":         "alice",          // 必填，节点唯一 ID
     *   "properties_json": "{\"age\": 30}",  // 可选，节点属性（JSON 字符串）
     *   "embedding":       [0.1, 0.2, ...]   // 可选，节点向量，用于 GraphRAG 入口检索
     * }
     *
     * [响应] {"success": true, "node_id": "alice"}
     *
     * [应用场景] 构建知识图谱节点，embedding 字段使该节点可被向量检索命中
     */
    void handle_graph_add_node(const httplib::Request& req, httplib::Response& res);

    /**
     * @brief POST /graph/add_edge — 添加有向边
     *
     * [请求体]
     * {
     *   "src_id":          "alice",          // 必填，起始节点 ID
     *   "dst_id":          "bob",            // 必填，目标节点 ID
     *   "label":           "KNOWS",          // 必填，边类型标签
     *   "weight":          1.0,              // 可选，边权重，默认 1.0
     *   "properties_json": "{}"              // 可选，边属性（JSON 字符串）
     * }
     *
     * [响应] {"success": true, "src_id": "alice", "dst_id": "bob", "label": "KNOWS"}
     */
    void handle_graph_add_edge(const httplib::Request& req, httplib::Response& res);

    /**
     * @brief POST /graph/rag_query — GraphRAG 混合查询
     *
     * [原理] 两阶段检索：
     *   第一阶段：用 query_embedding 做向量检索，找到 vector_top_k 个语义最近节点作为入口
     *   第二阶段：从入口节点出发做 hop_depth 跳 BFS 图遍历，收集所有可达节点
     * 这样可以找到语义相关节点的周边关联实体，解决纯向量检索的"孤岛问题"。
     *
     * [请求体]
     * {
     *   "query_embedding": [0.1, 0.2, ...],  // 必填，查询向量
     *   "vector_top_k":    3,                // 可选，向量检索入口节点数，默认 3
     *   "hop_depth":       2                 // 可选，BFS 跳数，默认 2
     * }
     *
     * [响应]
     * {
     *   "success":    true,
     *   "node_count": 12,
     *   "nodes": [
     *     {"node_id": "alice", "properties_json": "{\"age\": 30}"},
     *     ...
     *   ]
     * }
     */
    void handle_graph_rag_query(const httplib::Request& req, httplib::Response& res);

    // ==========================================
    // 辅助方法
    // ==========================================

    /**
     * @brief 将字符串解析为距离度量枚举（预留接口）
     * @param metric_str 度量名称："l2" / "euclidean" / "cosine" / "dot"
     * @return 对应的 DistanceMetric 枚举值，未识别时默认返回 L2
     *
     * @note 当前仅 L2 距离有实际实现，其余度量预留待扩展
     */
    DistanceMetric parse_metric(const std::string& metric_str);

    /**
     * @brief 发送错误响应
     * @param res         httplib 响应对象
     * @param status_code HTTP 状态码（如 400、404、500）
     * @param message     错误描述信息
     *
     * [统一格式] 响应体为 {"success": false, "error": "<message>"}
     */
    void send_error(httplib::Response& res, int status_code, const std::string& message);

    /**
     * @brief 发送成功响应（HTTP 200）
     * @param res  httplib 响应对象
     * @param data 要序列化为 JSON 的响应数据
     *
     * [统一格式] Content-Type 固定为 application/json
     */
    void send_success(httplib::Response& res, const json& data);
};

} // namespace server
} // namespace minkv
