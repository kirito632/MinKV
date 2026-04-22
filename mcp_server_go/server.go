/**
 * @file server.go
 * @brief 服务器核心：依赖管理和主循环
 *
 * 职责：
 * 1. 服务器配置和初始化
 * 2. 依赖注入和生命周期管理
 * 3. 主循环和请求处理
 * 4. 错误处理和优雅关闭
 *
 * 设计原则：
 * - 单一职责：只负责服务器生命周期
 * - 依赖注入：所有服务通过接口注入
 * - 可测试性：便于单元测试和集成测试
 */

package main

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
	"strings"
)

// ─── 服务器配置 ─────────────────────────────────────────────────────────────────

// Config 服务器配置
type Config struct {
	// MinKV配置
	MinKVHost string
	MinKVPort string
	
	// 嵌入服务配置
	EmbeddingProvider string
	OpenAIKey        string
	OpenAIBaseURL    string
	EmbeddingModel   string
	ChatModel        string
	AzureEndpoint    string
	AzureApiVersion  string
	AzureDeployment  string
	AzureApiKey      string
	CohereKey        string
	CohereBaseURL    string
}

// LoadConfig 从环境变量加载配置
func LoadConfig() Config {
	getEnv := func(key, fallback string) string {
		if v := os.Getenv(key); v != "" {
			return v
		}
		return fallback
	}

	return Config{
		MinKVHost:         getEnv("MINKV_HOST", "localhost"),
		MinKVPort:         getEnv("MINKV_PORT", "8080"),
		EmbeddingProvider: strings.ToLower(getEnv("EMBEDDING_PROVIDER", "openai")),
		OpenAIKey:         os.Getenv("OPENAI_API_KEY"),
		OpenAIBaseURL:     getEnv("OPENAI_BASE_URL", "https://api.openai.com/v1"),
		EmbeddingModel:    getEnv("EMBEDDING_MODEL", "text-embedding-3-small"),
		ChatModel:         getEnv("CHAT_MODEL", "gpt-4-turbo-preview"),
		AzureEndpoint:     os.Getenv("AZURE_OPENAI_ENDPOINT"),
		AzureApiVersion:   getEnv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview"),
		AzureDeployment:   os.Getenv("AZURE_OPENAI_EMBEDDINGS_DEPLOYMENT"),
		AzureApiKey:       os.Getenv("AZURE_OPENAI_API_KEY"),
		CohereKey:         os.Getenv("COHERE_API_KEY"),
		CohereBaseURL:     getEnv("COHERE_BASE_URL", "https://api.cohere.ai/v1"),
	}
}

// ─── 服务器核心 ─────────────────────────────────────────────────────────────────

// Server 服务器结构体
type Server struct {
	config         Config
	embeddingSvc   EmbeddingService
	minkvClient    MinKVClient
	toolManager    ToolManager
	mcpHandler     *MCPHandler
}

// NewServer 创建服务器实例
func NewServer(config Config) *Server {
	// 创建嵌入服务
	embeddingConfig := EmbeddingConfig{
		Provider:         config.EmbeddingProvider,
		OpenAIKey:        config.OpenAIKey,
		OpenAIBaseURL:    config.OpenAIBaseURL,
		EmbeddingModel:   config.EmbeddingModel,
		ChatModel:        config.ChatModel,
		AzureEndpoint:    config.AzureEndpoint,
		AzureApiVersion:  config.AzureApiVersion,
		AzureDeployment:  config.AzureDeployment,
		AzureApiKey:      config.AzureApiKey,
		CohereKey:        config.CohereKey,
		CohereBaseURL:    config.CohereBaseURL,
	}
	embeddingSvc := NewEmbeddingService(embeddingConfig)

	// 创建MinKV客户端
	minkvConfig := MinKVConfig{
		Host: config.MinKVHost,
		Port: config.MinKVPort,
	}
	minkvClient := NewMinKVClient(minkvConfig)

	// 创建工具管理器
	toolManager := NewToolManager(embeddingSvc, minkvClient)

	// 创建MCP处理器
	mcpHandler := NewMCPHandler(toolManager)

	return &Server{
		config:       config,
		embeddingSvc: embeddingSvc,
		minkvClient:  minkvClient,
		toolManager:  toolManager,
		mcpHandler:   mcpHandler,
	}
}

// Run 启动服务器主循环
func (s *Server) Run() {
	// 验证配置
	s.validateConfig()

	// 设置日志
	log.SetFlags(log.LstdFlags)
	log.SetOutput(os.Stderr) // MCP协议使用stdout传输JSON-RPC，日志输出到stderr避免污染

	log.Println("=== MinKV MCP Server 启动 ===")
	log.Printf("MinKV服务地址: %s:%s", s.config.MinKVHost, s.config.MinKVPort)
	log.Printf("嵌入服务提供商: %s", s.config.EmbeddingProvider)
	
	if s.config.EmbeddingProvider == "openai" || s.config.EmbeddingProvider == "openrouter" {
		if s.config.OpenAIKey == "" {
			log.Println("[警告] OPENAI_API_KEY未设置")
		}
	}

	// 主循环：从stdin读取JSON-RPC请求，向stdout写入响应
	scanner := bufio.NewScanner(os.Stdin)
	encoder := json.NewEncoder(os.Stdout)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		// 解析JSON-RPC请求
		var req Request
		if err := json.Unmarshal(line, &req); err != nil {
			log.Printf("[错误] 解析请求失败: %v", err)
			_ = encoder.Encode(Response{
				JSONRPC: "2.0",
				Error:   &RPCError{Code: -32700, Message: "解析错误"},
			})
			continue
		}

		// 处理请求
		resp := s.mcpHandler.HandleRequest(req)
		
		// 发送响应
		if err := encoder.Encode(resp); err != nil {
			log.Printf("[错误] 编码响应失败: %v", err)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("[致命错误] stdin读取错误: %v", err)
	}
}

// validateConfig 验证配置
func (s *Server) validateConfig() {
	// 验证MinKV配置
	if s.config.MinKVHost == "" || s.config.MinKVPort == "" {
		log.Fatal("[配置错误] MinKV主机或端口未设置")
	}

	// 验证嵌入服务配置
	switch s.config.EmbeddingProvider {
	case "openai", "openrouter":
		if s.config.OpenAIKey == "" {
			log.Println("[警告] OPENAI_API_KEY未设置，嵌入服务可能无法使用")
		}
	case "azure":
		if s.config.AzureEndpoint == "" || s.config.AzureApiKey == "" || 
		   s.config.AzureDeployment == "" || s.config.AzureApiVersion == "" {
			log.Println("[警告] Azure OpenAI配置不完整，嵌入服务可能无法使用")
		}
	case "cohere":
		if s.config.CohereKey == "" {
			log.Println("[警告] COHERE_API_KEY未设置，嵌入服务可能无法使用")
		}
	default:
		log.Printf("[警告] 不支持的嵌入服务提供商: %s", s.config.EmbeddingProvider)
	}
}
