/**
 * @file mcp_handler.go
 * @brief MCP协议处理：JSON-RPC解析和路由
 *
 * 职责：
 * 1. JSON-RPC 2.0协议解析
 * 2. 请求路由和方法分发
 * 3. 错误处理和响应格式化
 * 4. MCP协议规范实现
 *
 * 设计原则：
 * - 协议与业务逻辑分离
 * - 统一的错误处理
 * - 类型安全的请求/响应
 */

package main

import (
	"encoding/json"
)

// ─── MCP协议类型定义 ───────────────────────────────────────────────────────────

// Request JSON-RPC请求
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// Response JSON-RPC响应
type Response struct {
	JSONRPC string    `json:"jsonrpc"`
	ID      any       `json:"id"`
	Result  any       `json:"result,omitempty"`
	Error   *RPCError `json:"error,omitempty"`
}

// RPCError JSON-RPC错误
type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Tool MCP工具定义
type Tool struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}

// TextContent MCP文本内容
type TextContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// ListToolsResult 列出工具响应
type ListToolsResult struct {
	Tools []Tool `json:"tools"`
}

// CallToolParams 调用工具参数
type CallToolParams struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
}

// CallToolResult 调用工具结果
type CallToolResult struct {
	Content []TextContent `json:"content"`
}

// ─── MCP处理器 ─────────────────────────────────────────────────────────────────

// MCPHandler MCP协议处理器
type MCPHandler struct {
	toolManager ToolManager
}

// NewMCPHandler 创建MCP处理器
func NewMCPHandler(toolManager ToolManager) *MCPHandler {
	return &MCPHandler{
		toolManager: toolManager,
	}
}

// HandleRequest 处理JSON-RPC请求
func (h *MCPHandler) HandleRequest(req Request) Response {
	base := Response{JSONRPC: "2.0", ID: req.ID}

	switch req.Method {
	case "initialize":
		base.Result = map[string]any{
			"protocolVersion": "2024-11-05",
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo":      map[string]any{"name": "minkv-vector", "version": "1.0.0"},
		}

	case "tools/list":
		base.Result = ListToolsResult{Tools: h.toolManager.ListTools()}

	case "tools/call":
		var p CallToolParams
		if err := json.Unmarshal(req.Params, &p); err != nil {
			base.Error = &RPCError{Code: -32602, Message: "无效参数: " + err.Error()}
			return base
		}

		content, err := h.toolManager.ExecuteTool(p.Name, p.Arguments)
		if err != nil {
			// 工具执行错误，返回错误信息
			content = []TextContent{{Type: "text", Text: jsonStr(map[string]any{
				"error":   "execution_failed",
				"message": err.Error(),
			})}}
		}
		base.Result = CallToolResult{Content: content}

	default:
		base.Error = &RPCError{Code: -32601, Message: "方法未找到: " + req.Method}
	}

	return base
}

