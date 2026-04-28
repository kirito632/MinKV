/**
 * @file main.go
 * @brief MinKV Vector MCP Server (Go实现) - 入口点
 *
 * [核心功能] 实现Model Context Protocol (MCP)标准服务端，通过stdio进行JSON-RPC 2.0通信
 * 支持AI Agent工具调用：向量检索、向量插入、GraphRAG查询、Agent记忆管理等
 *
 * 设计特点：
 * - 零外部依赖：仅使用Go标准库，无第三方依赖
 * - 多提供商支持：OpenAI / Azure OpenAI / Cohere Embedding API
 * - 9个MCP Tools：vector_search, vector_insert, graph_rag_query, kv_set, kv_get等
 * - 分层记忆架构：Working Memory (KV) + Episodic Memory (向量检索)
 *
 * 架构流程：
 * Cursor/Claude Desktop (MCP Client) -> stdio JSON-RPC -> 本Server -> HTTP -> MinKV Core
 *
 * @note 零外部依赖设计：使用Go标准库实现完整的MCP协议栈，便于部署和移植
 */

package main

import (
	"log"
	"os"
)

func main() {
	// 设置日志
	log.SetFlags(log.LstdFlags)
	log.SetOutput(os.Stderr) // [协议规范] MCP使用stdout传输JSON-RPC，日志输出到stderr避免污染协议流

	// 加载配置
	config := LoadConfig()

	// 创建并启动服务器
	server := NewServer(config)
	server.Run()
}
