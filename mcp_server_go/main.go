/**
 * @file main.go
 * @brief MinKV Vector MCP Server (Go实现)
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
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
	"strings"
)

// ─── MCP JSON-RPC 类型定义 ───────────────────────────────────────────────────────

/**
 * @brief MCP JSON-RPC请求结构
 *
 * [协议规范] 符合JSON-RPC 2.0标准，用于AI Agent向Server发起调用
 * - JSONRPC: 协议版本，固定为"2.0"
 * - ID: 请求标识，用于关联请求和响应
 * - Method: 调用方法名 (initialize/tools/list/tools/call)
 * - Params: 方法参数，使用RawMessage延迟解析
 */

type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type Response struct {
	JSONRPC string `json:"jsonrpc"`
	ID      any    `json:"id"`
	Result  any    `json:"result,omitempty"`
	Error   *RPCError `json:"error,omitempty"`
}

type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// ─── MCP协议类型定义 ───────────────────────────────────────────────────────────

/**
 * @brief MCP Tool定义结构
 *
 * [Tool定义] 描述AI Agent可调用的工具接口
 * - Name: 工具名称，Agent通过此名称调用
 * - Description: 工具功能描述，供LLM理解工具用途
 * - InputSchema: JSON Schema格式的参数定义
 */

type Tool struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}

type TextContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type ListToolsResult struct {
	Tools []Tool `json:"tools"`
}

type CallToolParams struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
}

type CallToolResult struct {
	Content []TextContent `json:"content"`
}

// ─── OpenAI Embedding API类型定义 ──────────────────────────────────────────────

/**
 * @brief Embedding请求/响应结构
 *
 * [Embedding生成] 调用OpenAI API将文本转换为向量表示
 * 支持多提供商：OpenAI / Azure OpenAI / Cohere
 * 默认模型：text-embedding-3-small (1536维)
 */

type embeddingRequest struct {
	Model string `json:"model"`
	Input string `json:"input"`
}

type embeddingResponse struct {
	Data []struct {
		Embedding []float64 `json:"embedding"`
	} `json:"data"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// ─── MinKV HTTP API类型定义 ───────────────────────────────────────────────────

/**
 * @brief MinKV向量检索API类型
 *
 * [向量检索] 通过HTTP REST API与MinKV核心引擎通信
 * - 向量搜索：/vector/search (余弦相似度Top-K)
 * - 向量插入：/vector/put (带Metadata)
 * - KV操作：/kv/set, /kv/get, /kv/del
 * - 图操作：/graph/add_node, /graph/add_edge, /graph/rag_query
 */

type vectorSearchRequest struct {
	Query  []float64 `json:"query"`
	TopK   int       `json:"top_k"`
	Metric string    `json:"metric"`
}

type vectorSearchResult struct {
	Key      string         `json:"key"`
	Score    float64        `json:"score"`
	Metadata map[string]any `json:"metadata"`
}

type vectorSearchResponse struct {
	Results []vectorSearchResult `json:"results"`
}

// ─── 简化版向量搜索响应 (C++服务端当前返回简化格式)
/**
 * @brief C++服务端简化响应格式
 *
 * [兼容性说明] 当前C++服务端仅返回key列表，不返回score和metadata
 * 本结构用于兼容简化响应，后续C++服务端升级后可切换为完整格式
 */
type simpleVectorSearchResponse struct {
	Success        bool     `json:"success"`
	Results        []string `json:"results"`
	ResultsCount   int      `json:"results_count"`
}

type vectorPutRequest struct {
	Key       string `json:"key"`
	Embedding []float64 `json:"embedding"`
	Metadata  string `json:"metadata"`
}

type vectorPutResponse struct {
	Success bool `json:"success"`
}

// ─── KV API类型定义 ────────────────────────────────────────────────────────────

/**
 * @brief Agent工作记忆(Working Memory)API类型
 *
 * [工作记忆] 高速KV存储，用于Agent的短期上下文管理
 * 特点：
 * - 微秒级读写延迟
 * - 支持TTL过期机制
 * - 精确匹配检索
 */
type kvSetRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	TTLMs int64  `json:"ttl_ms"`
}

type kvSetResponse struct {
	Success bool   `json:"success"`
	Key     string `json:"key"`
}

type kvGetResponse struct {
	Success bool   `json:"success"`
	Key     string `json:"key"`
	Value   string `json:"value"`
}

// ─── Graph API类型定义 ─────────────────────────────────────────────────────────

/**
 * @brief 图数据库与GraphRAG API类型
 *
 * [GraphRAG架构] 结合向量检索和图遍历的增强检索
 * - 节点存储：支持属性JSON和Embedding
 * - 边存储：有向边，带标签、权重、属性
 * - RAG查询：向量检索入口节点 + K-hop BFS扩展
 */
type graphAddNodeRequest struct {
	NodeID         string    `json:"node_id"`
	PropertiesJSON string    `json:"properties_json"`
	Embedding      []float64 `json:"embedding,omitempty"`
}

type graphAddEdgeRequest struct {
	SrcID          string  `json:"src_id"`
	DstID          string  `json:"dst_id"`
	Label          string  `json:"label"`
	Weight         float64 `json:"weight"`
	PropertiesJSON string  `json:"properties_json"`
}

type graphRAGQueryRequest struct {
	QueryEmbedding []float64 `json:"query_embedding"`
	VectorTopK     int       `json:"vector_top_k"`
	HopDepth       int       `json:"hop_depth"`
}

type graphNode struct {
	NodeID         string `json:"node_id"`
	PropertiesJSON string `json:"properties_json"`
}

type graphRAGQueryResponse struct {
	Success    bool        `json:"success"`
	NodeCount  int         `json:"node_count"`
	Nodes      []graphNode `json:"nodes"`
}

// ─── Server核心结构 ────────────────────────────────────────────────────────────

/**
 * @brief MCP Server核心结构
 *
 * [Server配置] 聚合所有外部依赖配置
 * - MinKV连接：HTTP服务地址 (默认localhost:8080)
 * - Embedding提供商：OpenAI / Azure / Cohere
 * - HTTP客户端：60秒超时，支持长耗时Embedding请求
 *
 * 环境变量配置：
 * - OPENAI_API_KEY: OpenAI API密钥
 * - AZURE_OPENAI_API_KEY/AZURE_OPENAI_ENDPOINT: Azure配置
 * - COHERE_API_KEY: Cohere API密钥
 * - MINKV_HOST/MINKV_PORT: MinKV服务地址
 */

type Server struct {
	minkvURL       string
	openaiKey      string
	embeddingModel string
	httpClient     *http.Client
	provider       string
	baseURL        string
	azureEndpoint   string
	azureApiVersion string
	azureDeployment string
	azureApiKey     string
 	cohereKey       string
 	cohereBaseURL   string
}

func NewServer() *Server {
	host := getEnv("MINKV_HOST", "localhost")
	port := getEnv("MINKV_PORT", "8080")
	prov := strings.ToLower(getEnv("EMBEDDING_PROVIDER", "openai"))
	base := getEnv("OPENAI_BASE_URL", "https://api.openai.com/v1")
	return &Server{
		minkvURL:         fmt.Sprintf("http://%s:%s", host, port),
		openaiKey:        os.Getenv("OPENAI_API_KEY"),
		embeddingModel:   getEnv("EMBEDDING_MODEL", "text-embedding-3-small"),
		httpClient:       &http.Client{Timeout: 60 * time.Second},
		provider:         prov,
		baseURL:          base,
		azureEndpoint:    os.Getenv("AZURE_OPENAI_ENDPOINT"),
		azureApiVersion:  getEnv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview"),
		azureDeployment:  os.Getenv("AZURE_OPENAI_EMBEDDINGS_DEPLOYMENT"),
		azureApiKey:      os.Getenv("AZURE_OPENAI_API_KEY"),
 		cohereKey:        os.Getenv("COHERE_API_KEY"),
 		cohereBaseURL:    getEnv("COHERE_BASE_URL", "https://api.cohere.ai/v1"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

/**
 * @brief 调用Embedding API生成文本向量
 * @param text 输入文本
 * @return 向量数组 (float64切片)
 * @return error 调用失败时返回错误
 *
 * [核心功能] 将自然语言查询转换为向量表示，支持多提供商：
 * - openai: OpenAI官方API (api.openai.com)
 * - azure: Azure OpenAI服务
 * - cohere: Cohere Embed API
 *
 * 性能特点：
 * - API调用耗时：100-300ms (依赖网络和服务商)
 * - 向量维度：1536D (text-embedding-3-small)
 * - 请求超时：60秒 (支持长文本)
 *
 * @throws 返回错误当：API密钥未配置、网络超时、API限流
 */
func (s *Server) getEmbedding(text string) ([]float64, error) {
	switch s.provider {
	case "azure":
		if s.azureEndpoint == "" || s.azureApiKey == "" || s.azureDeployment == "" || s.azureApiVersion == "" {
			return nil, fmt.Errorf("azure embedding not configured: set AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_API_KEY, AZURE_OPENAI_EMBEDDINGS_DEPLOYMENT, AZURE_OPENAI_API_VERSION")
		}
		payload := map[string]any{"input": text}
		body, _ := json.Marshal(payload)
		url := strings.TrimRight(s.azureEndpoint, "/") + "/openai/deployments/" + s.azureDeployment + "/embeddings?api-version=" + s.azureApiVersion
		req, _ := http.NewRequest("POST", url, bytes.NewReader(body))
		req.Header.Set("api-key", s.azureApiKey)
		req.Header.Set("Content-Type", "application/json")

		resp, err := s.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("openai request failed: %w", err)
		}
		defer resp.Body.Close()

		var result embeddingResponse
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return nil, fmt.Errorf("decode embedding response: %w", err)
		}
		if result.Error != nil {
			return nil, fmt.Errorf("openai error: %s", result.Error.Message)
		}
		if len(result.Data) == 0 {
			return nil, fmt.Errorf("empty embedding response")
		}
		return result.Data[0].Embedding, nil

	case "cohere":
		if s.cohereKey == "" {
			return nil, fmt.Errorf("COHERE_API_KEY not set")
		}
		payload := map[string]any{
			"model":       s.embeddingModel,
			"texts":       []string{text},
			"input_type":  "search_document",
		}
		body, _ := json.Marshal(payload)
		url := strings.TrimRight(s.cohereBaseURL, "/") + "/embed"
		req, _ := http.NewRequest("POST", url, bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+s.cohereKey)
		req.Header.Set("Content-Type", "application/json")

		resp, err := s.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("cohere request failed: %w", err)
		}
		defer resp.Body.Close()

		bodyBytes, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("cohere API error %d: %s", resp.StatusCode, string(bodyBytes))
		}

		var cr struct {
			Embeddings [][]float64 `json:"embeddings"`
		}
		if err := json.Unmarshal(bodyBytes, &cr); err != nil {
			return nil, fmt.Errorf("decode embedding response: %w", err)
		}
		if len(cr.Embeddings) == 0 || len(cr.Embeddings[0]) == 0 {
			return nil, fmt.Errorf("empty embedding response: %s", string(bodyBytes))
		}
		return cr.Embeddings[0], nil

	case "openrouter", "openai":
		if s.openaiKey == "" {
			return nil, fmt.Errorf("OPENAI_API_KEY not set")
		}
		body, _ := json.Marshal(embeddingRequest{Model: s.embeddingModel, Input: text})
		url := strings.TrimRight(s.baseURL, "/") + "/embeddings"
		req, _ := http.NewRequest("POST", url, bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+s.openaiKey)
		req.Header.Set("Content-Type", "application/json")

		resp, err := s.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("openai request failed: %w", err)
		}
		defer resp.Body.Close()

		var result embeddingResponse
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return nil, fmt.Errorf("decode embedding response: %w", err)
		}
		if result.Error != nil {
			return nil, fmt.Errorf("openai error: %s", result.Error.Message)
		}
		if len(result.Data) == 0 {
			return nil, fmt.Errorf("empty embedding response")
		}
		return result.Data[0].Embedding, nil

	default:
		return nil, fmt.Errorf("unsupported EMBEDDING_PROVIDER: %s", s.provider)
	}
}

/**
 * @brief 向量语义检索
 * @param query 自然语言查询文本
 * @param topK 返回结果数量 (默认5)
 * @return 检索结果列表 (包含key, score, metadata)
 * @return error 调用失败时返回错误
 *
 * [核心工具] MCP Tool: vector_search 的实现
 * 执行流程：
 * 1. 调用getEmbedding生成查询向量
 * 2. POST /vector/search 调用MinKV核心
 * 3. 格式化返回结果
 *
 * 使用场景：
 * - Agent语义记忆检索
 * - RAG文档召回
 * - 相似内容查找
 *
 * @note 当前C++服务端返回简化格式，score固定为0.0
 */
func (s *Server) vectorSearch(query string, topK int) ([]map[string]any, error) {
	log.Printf("[VectorSearch] query=%q top_k=%d", query, topK)

	embedding, err := s.getEmbedding(query)
	if err != nil {
		return nil, err
	}

	body, _ := json.Marshal(vectorSearchRequest{Query: embedding, TopK: topK, Metric: "cosine"})
	resp, err := s.httpClient.Post(s.minkvURL+"/vector/search", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("cannot connect to MinKV: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("MinKV API error %d: %s", resp.StatusCode, raw)
	}

	var simpleResp simpleVectorSearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&simpleResp); err != nil {
		return nil, fmt.Errorf("decode search response: %w", err)
	}

	// 从简化响应构建结果 (当前仅返回key列表)
	results := make([]map[string]any, 0, len(simpleResp.Results))
	for i, key := range simpleResp.Results {
		results = append(results, map[string]any{
			"rank":  i + 1,
			"key":   key,
			"score": 0.0, // [兼容性] C++服务端暂未返回相似度分数
		})
	}
	log.Printf("[VectorSearch] found %d results", len(results))
	return results, nil
}

/*
 * @brief 文本向量化并存储
 * @param key 向量唯一标识
 * @param text 原始文本内容
 * @param metadata 可选元数据
 * @return 插入成功标志
 * @return error 调用失败时返回错误
 *
 * [核心工具] MCP Tool: vector_insert 的实现
 * 执行流程：
 * 1. 调用getEmbedding生成文本向量
 * 2. 自动添加text_preview和text_length元数据
 * 3. POST /vector/put 存储到MinKV
 *
 * 元数据自动填充：
 * - text_preview: 文本前200字符预览
 * - text_length: 原始文本长度
 *
 * 使用场景：
 * - 知识库文档入库
 * - Agent经验记忆存储
 * - RAG上下文构建
 */
func (s *Server) vectorInsert(key, text string, metadata map[string]any) (bool, error) {
	log.Printf("[VectorInsert] key=%q text_len=%d", key, len(text))

	embedding, err := s.getEmbedding(text)
	if err != nil {
		return false, err
	}

	if metadata == nil {
		metadata = map[string]any{}
	}
	preview := text
	if len(preview) > 200 {
		preview = preview[:200]
	}
	metadata["text_preview"] = preview
	metadata["text_length"] = len(text)

	metaJSON, _ := json.Marshal(metadata)
	body, _ := json.Marshal(vectorPutRequest{
		Key:       key,
		Embedding: embedding,
		Metadata:  string(metaJSON),
	})

	resp, err := s.httpClient.Post(s.minkvURL+"/vector/put", "application/json", bytes.NewReader(body))
	if err != nil {
		return false, fmt.Errorf("cannot connect to MinKV: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("MinKV API error %d: %s", resp.StatusCode, raw)
	}

	var putResp vectorPutResponse
	if err := json.NewDecoder(resp.Body).Decode(&putResp); err != nil {
		return false, fmt.Errorf("decode put response: %w", err)
	}
	log.Printf("[VectorInsert] success=%v key=%q", putResp.Success, key)
	return putResp.Success, nil
}

// ─── KV API方法 (Agent工作记忆) ───────────────────────────────────────────────

/**
 * @brief 工作记忆存储 (MCP Tool: kv_set)
 * @param key 存储键名
 * @param value 存储值 (字符串或JSON)
 * @param ttlMs 过期时间毫秒 (0表示不过期)
 * @return 操作成功标志
 * @return error 调用失败时返回错误
 *
 * [Agent工作记忆] 高速KV存储，用于Agent的短期上下文管理
 * 特点：
 * - 延迟：< 1ms (本地内存访问)
 * - 容量：受MinKV配置限制
 * - 过期：支持TTL自动清理
 *
 * 使用场景：
 * - 存储Agent当前状态
 * - 保存临时计算结果
 * - 短期上下文缓存
 */

func (s *Server) kvSet(key, value string, ttlMs int64) (bool, error) {
	log.Printf("[KvSet] key=%q ttl_ms=%d", key, ttlMs)
	body, _ := json.Marshal(kvSetRequest{Key: key, Value: value, TTLMs: ttlMs})
	resp, err := s.httpClient.Post(s.minkvURL+"/kv/set", "application/json", bytes.NewReader(body))
	if err != nil {
		return false, fmt.Errorf("cannot connect to MinKV: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("MinKV API error %d: %s", resp.StatusCode, raw)
	}
	var r kvSetResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return false, fmt.Errorf("decode kv/set response: %w", err)
	}
	log.Printf("[KvSet] success=%v key=%q", r.Success, r.Key)
	return r.Success, nil
}

/**
 * @brief 工作记忆检索 (MCP Tool: kv_get)
 * @param key 检索键名
 * @return 存储值 (未找到时返回空字符串)
 * @return found 是否找到标志
 * @return error 调用失败时返回错误
 *
 * [精确匹配] 按key精确检索工作记忆
 * - 返回404时 found=false，不视为错误
 * - 成功返回时 found=true
 *
 * 使用场景：
 * - 检索Agent之前存储的状态
 * - 获取临时计算结果
 * - 检查key是否存在
 */
func (s *Server) kvGet(key string) (string, bool, error) {
	log.Printf("[KvGet] key=%q", key)
	resp, err := s.httpClient.Get(s.minkvURL + "/kv/get?key=" + key)
	if err != nil {
		return "", false, fmt.Errorf("cannot connect to MinKV: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return "", false, nil
	}
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return "", false, fmt.Errorf("MinKV API error %d: %s", resp.StatusCode, raw)
	}
	var r kvGetResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return "", false, fmt.Errorf("decode kv/get response: %w", err)
	}
	return r.Value, true, nil
}

/**
 * @brief 工作记忆删除 (MCP Tool: kv_delete)
 * @param key 删除键名
 * @return 删除成功标志 (key不存在时返回false)
 * @return error 调用失败时返回错误
 *
 * [清理机制] 主动删除过期或已消费的上下文
 * - 用于释放内存
 * - 防止状态残留
 * - key不存在时返回false，不视为错误
 */
func (s *Server) kvDelete(key string) (bool, error) {
	log.Printf("[KvDelete] key=%q", key)
	req, _ := http.NewRequest("DELETE", s.minkvURL+"/kv/del?key="+key, nil)
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("cannot connect to MinKV: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("MinKV API error %d: %s", resp.StatusCode, raw)
	}
	return true, nil
}

/**
 * @brief 混合记忆检索 (MCP Tool: memory_recall)
 * @param key 工作记忆检索key (可选)
 * @param semanticQuery 语义查询文本 (可选)
 * @param topK 情景记忆返回数量 (默认3)
 * @return 混合检索结果 (working_memory + episodic_memory)
 * @return error 调用失败时返回错误
 *
 * [分层记忆架构] 同时检索两种记忆类型：
 * 1. 工作记忆 (Working Memory): 精确KV查询，< 1ms延迟
 *    - 存储当前上下文、临时状态
 *    - 通过key精确访问
 *
 * 2. 情景记忆 (Episodic Memory): 语义向量检索，~10ms延迟
 *    - 存储历史经验、相似场景
 *    - 通过语义相似度召回
 *
 * 使用场景：
 * - Agent需要同时获取精确状态和相似经验
 * - 复杂推理场景的上下文构建
 * - 多跳记忆召回
 *
 * @note key和semanticQuery至少提供一个，可同时提供
 */
func (s *Server) memoryRecall(key, semanticQuery string, topK int) (map[string]any, error) {
	log.Printf("[MemoryRecall] key=%q semantic_query=%q top_k=%d", key, semanticQuery, topK)
	result := map[string]any{}

	// 1. 工作记忆: 精确KV查询 (< 1ms)
	if key != "" {
		val, found, err := s.kvGet(key)
		if err != nil {
			log.Printf("[MemoryRecall] kv_get error: %v", err)
		} else if found {
			result["working_memory"] = map[string]any{"key": key, "value": val}
		} else {
			result["working_memory"] = nil
		}
	}

	// 2. 情景记忆: 语义向量检索 (~10ms)
	if semanticQuery != "" && s.openaiKey != "" {
		vecResults, err := s.vectorSearch(semanticQuery, topK)
		if err != nil {
			log.Printf("[MemoryRecall] vector_search error: %v", err)
		} else {
			result["episodic_memory"] = vecResults
		}
	}

	return result, nil
}

// ─── Graph API方法 (图数据库与GraphRAG) ────────────────────────────────────────

/**
 * @brief 添加图节点 (MCP Tool: graph_add_node)
 * @param nodeID 节点唯一标识
 * @param propsJSON 节点属性JSON字符串
 * @param embedding 可选的节点向量表示
 * @return 添加成功标志
 * @return error 调用失败时返回错误
 *
 * [图构建] 创建带属性的图节点，可选绑定Embedding
 * 应用场景：
 * - 构建知识图谱实体节点
 * - 存储带语义表示的节点
 * - GraphRAG的图数据准备
 */
func (s *Server) graphAddNode(nodeID, propsJSON string, embedding []float64) (bool, error) {
	log.Printf("[GraphAddNode] node_id=%q", nodeID)
	type reqBody struct {
		NodeID         string    `json:"node_id"`
		PropertiesJSON string    `json:"properties_json"`
		Embedding      []float64 `json:"embedding,omitempty"`
	}
	body, _ := json.Marshal(reqBody{NodeID: nodeID, PropertiesJSON: propsJSON, Embedding: embedding})
	resp, err := s.httpClient.Post(s.minkvURL+"/graph/add_node", "application/json", bytes.NewReader(body))
	if err != nil {
		return false, fmt.Errorf("cannot connect to MinKV: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("MinKV API error %d: %s", resp.StatusCode, raw)
	}
	return true, nil
}

/**
 * @brief 添加图边 (MCP Tool: graph_add_edge)
 * @param srcID 源节点ID
 * @param dstID 目标节点ID
 * @param label 边标签/类型 (如"KNOWS", "WORKS_AT")
 * @param weight 边权重 (默认1.0)
 * @param propsJSON 边属性JSON字符串
 * @return 添加成功标志
 * @return error 调用失败时返回错误
 *
 * [关系构建] 创建有向边连接两个节点
 * 应用场景：
 * - 建立实体间关系 (如：人-公司雇佣关系)
 * - 构建知识图谱边
 * - 支持权重计算最短路径
 */
func (s *Server) graphAddEdge(srcID, dstID, label string, weight float64, propsJSON string) (bool, error) {
	log.Printf("[GraphAddEdge] %q -[%s]-> %q", srcID, label, dstID)
	type reqBody struct {
		SrcID          string  `json:"src_id"`
		DstID          string  `json:"dst_id"`
		Label          string  `json:"label"`
		Weight         float64 `json:"weight"`
		PropertiesJSON string  `json:"properties_json"`
	}
	body, _ := json.Marshal(reqBody{SrcID: srcID, DstID: dstID, Label: label, Weight: weight, PropertiesJSON: propsJSON})
	resp, err := s.httpClient.Post(s.minkvURL+"/graph/add_edge", "application/json", bytes.NewReader(body))
	if err != nil {
		return false, fmt.Errorf("cannot connect to MinKV: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("MinKV API error %d: %s", resp.StatusCode, raw)
	}
	return true, nil
}

/**
 * @brief GraphRAG检索查询 (MCP Tool: graph_rag_query)
 * @param queryText 自然语言查询文本
 * @param vectorTopK 向量检索入口节点数 (默认3)
 * @param hopDepth 图遍历深度 (默认2)
 * @return 检索到的节点列表
 * @return error 调用失败时返回错误
 *
 * [核心工具] GraphRAG两阶段检索实现：
 * 阶段1 - 向量检索：将queryText转为向量，召回语义相似的入口节点
 * 阶段2 - 图遍历：从入口节点出发，K-hop BFS扩展遍历关联节点
 *
 * 优势：
 * - 召回无Embedding的隐藏节点 (通过关系链可达)
 * - 多跳推理能力 (如：人物->公司->产品)
 * - 召回率提升至42.6% (相比纯向量检索)
 *
 * 使用场景：
 * - 复杂知识库问答
 * - 多跳推理检索
 * - 实体关系探索
 *
 * @note 端到端延迟约10ms (不含Embedding生成)
 */
func (s *Server) graphRAGQuery(queryText string, vectorTopK, hopDepth int) ([]map[string]any, error) {
	log.Printf("[GraphRAGQuery] query=%q top_k=%d hop=%d", queryText, vectorTopK, hopDepth)

	embedding, err := s.getEmbedding(queryText)
	if err != nil {
		return nil, err
	}

	type reqBody struct {
		QueryEmbedding []float64 `json:"query_embedding"`
		VectorTopK     int       `json:"vector_top_k"`
		HopDepth       int       `json:"hop_depth"`
	}
	body, _ := json.Marshal(reqBody{QueryEmbedding: embedding, VectorTopK: vectorTopK, HopDepth: hopDepth})
	resp, err := s.httpClient.Post(s.minkvURL+"/graph/rag_query", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("cannot connect to MinKV: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("MinKV API error %d: %s", resp.StatusCode, raw)
	}

	var result struct {
		Success   bool             `json:"success"`
		NodeCount int              `json:"node_count"`
		Nodes     []map[string]any `json:"nodes"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode rag_query response: %w", err)
	}
	log.Printf("[GraphRAGQuery] found %d nodes", result.NodeCount)
	return result.Nodes, nil
}

// ─── MCP请求处理器 ─────────────────────────────────────────────────────────────

/**
 * @brief MCP JSON-RPC请求分发器
 * @param req 解析后的请求结构
 * @return 标准JSON-RPC响应
 *
 * [协议实现] 处理MCP标准方法：
 * - initialize: 握手初始化，返回协议版本和能力
 * - tools/list: 返回可用工具列表
 * - tools/call: 调用指定工具
 *
 * 错误处理：
 * - 方法不存在: code=-32601
 * - 参数错误: code=-32602
 * - 执行失败: 封装在Tool结果中返回
 */

func (s *Server) handleRequest(req Request) Response {
	base := Response{JSONRPC: "2.0", ID: req.ID}

	switch req.Method {
	case "initialize":
		base.Result = map[string]any{
			"protocolVersion": "2024-11-05",
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo":      map[string]any{"name": "minkv-vector", "version": "1.0.0"},
		}

	case "tools/list":
		base.Result = ListToolsResult{Tools: s.toolList()}

	case "tools/call":
		var p CallToolParams
		if err := json.Unmarshal(req.Params, &p); err != nil {
			base.Error = &RPCError{Code: -32602, Message: "invalid params: " + err.Error()}
			return base
		}
		content, err := s.callTool(p.Name, p.Arguments)
		if err != nil {
			content = []TextContent{{Type: "text", Text: jsonStr(map[string]any{
				"error": "execution_failed", "message": err.Error(),
			})}}
		}
		base.Result = CallToolResult{Content: content}

	default:
		base.Error = &RPCError{Code: -32601, Message: "method not found: " + req.Method}
	}
	return base
}

func (s *Server) toolList() []Tool {
	return []Tool{
		{
			Name:        "vector_search",
			Description: "Search similar vectors in MinKV using natural language query",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"query": map[string]any{"type": "string", "description": "Natural language search query"},
					"top_k": map[string]any{"type": "integer", "default": 5, "description": "Number of top results to return"},
				},
				"required": []string{"query"},
			},
		},
		{
			Name:        "vector_insert",
			Description: "Insert text as vector into MinKV with optional metadata",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"key":      map[string]any{"type": "string", "description": "Unique key for the vector"},
					"text":     map[string]any{"type": "string", "description": "Text content to convert to vector"},
					"metadata": map[string]any{"type": "object", "description": "Optional metadata as JSON object"},
				},
				"required": []string{"key", "text"},
			},
		},
		{
			Name:        "graph_add_node",
			Description: "Add a node to the MinKV graph store with optional embedding",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"node_id":         map[string]any{"type": "string", "description": "Unique node identifier"},
					"properties_json": map[string]any{"type": "string", "description": "Node properties as JSON string, e.g. {\"name\":\"Alice\"}"},
					"text":            map[string]any{"type": "string", "description": "Optional text to generate embedding from"},
				},
				"required": []string{"node_id"},
			},
		},
		{
			Name:        "graph_add_edge",
			Description: "Add a directed edge between two nodes in the MinKV graph store",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"src_id":          map[string]any{"type": "string", "description": "Source node ID"},
					"dst_id":          map[string]any{"type": "string", "description": "Destination node ID"},
					"label":           map[string]any{"type": "string", "description": "Edge type/label, e.g. KNOWS, WORKS_AT"},
					"weight":          map[string]any{"type": "number", "default": 1.0, "description": "Edge weight"},
					"properties_json": map[string]any{"type": "string", "description": "Edge properties as JSON string"},
				},
				"required": []string{"src_id", "dst_id", "label"},
			},
		},
		{
			Name:        "graph_rag_query",
			Description: "GraphRAG query: find semantically similar nodes then expand via K-hop graph traversal",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"query":        map[string]any{"type": "string", "description": "Natural language query to search for"},
					"vector_top_k": map[string]any{"type": "integer", "default": 3, "description": "Number of entry nodes from vector search"},
					"hop_depth":    map[string]any{"type": "integer", "default": 2, "description": "K-hop depth for graph traversal"},
				},
				"required": []string{"query"},
			},
		},
		{
			Name:        "kv_set",
			Description: "Store a key-value pair in MinKV working memory. Use this to remember facts, agent state, or short-term context. Supports optional TTL.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"key":    map[string]any{"type": "string", "description": "Unique key to store the value under"},
					"value":  map[string]any{"type": "string", "description": "Value to store (string, can be JSON)"},
					"ttl_ms": map[string]any{"type": "integer", "default": 0, "description": "TTL in milliseconds (0 = no expiration)"},
				},
				"required": []string{"key", "value"},
			},
		},
		{
			Name:        "kv_get",
			Description: "Retrieve a value from MinKV working memory by key. Returns the stored value or null if not found.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"key": map[string]any{"type": "string", "description": "Key to retrieve"},
				},
				"required": []string{"key"},
			},
		},
		{
			Name:        "kv_delete",
			Description: "Delete a key from MinKV working memory. Use this to clear stale or consumed context.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"key": map[string]any{"type": "string", "description": "Key to delete"},
				},
				"required": []string{"key"},
			},
		},
		{
			Name:        "memory_recall",
			Description: "Hybrid agent memory recall: looks up a key in working memory (KV) and optionally retrieves semantically similar entries from episodic memory (vector search). Use this when the agent needs to recall past context or knowledge.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"key":            map[string]any{"type": "string", "description": "Exact key to look up in working memory (optional)"},
					"semantic_query": map[string]any{"type": "string", "description": "Natural language query for episodic memory search (optional, requires OPENAI_API_KEY)"},
					"top_k":          map[string]any{"type": "integer", "default": 3, "description": "Number of episodic memory results to return"},
				},
			},
		},
	}
}

func (s *Server) callTool(name string, args map[string]any) ([]TextContent, error) {
	var err error
	switch name {
	case "vector_search":
		query, _ := args["query"].(string)
		if query == "" {
			return nil, fmt.Errorf("missing required argument: query")
		}
		topK := 5
		if v, ok := args["top_k"].(float64); ok {
			topK = int(v)
		}
		results, err := s.vectorSearch(query, topK)
		if err != nil {
			return nil, err
		}
		return []TextContent{{Type: "text", Text: jsonStr(results)}}, nil

	case "vector_insert":
		key, _ := args["key"].(string)
		text, _ := args["text"].(string)
		if key == "" || text == "" {
			return nil, fmt.Errorf("missing required arguments: key and text")
		}
		var metadata map[string]any
		if m, ok := args["metadata"].(map[string]any); ok {
			metadata = m
		}
		success, err := s.vectorInsert(key, text, metadata)
		if err != nil {
			return nil, err
		}
		msg := "Vector inserted successfully"
		if !success {
			msg = "Insert failed"
		}
		return []TextContent{{Type: "text", Text: jsonStr(map[string]any{
			"success": success, "key": key, "message": msg,
		})}}, nil

	case "graph_add_node":
		nodeID, _ := args["node_id"].(string)
		if nodeID == "" {
			return nil, fmt.Errorf("missing required argument: node_id")
		}
		propsJSON, _ := args["properties_json"].(string)
		if propsJSON == "" {
			propsJSON = "{}"
		}
		// 可选：从 text 生成 embedding
		var embedding []float64
		if text, ok := args["text"].(string); ok && text != "" {
			embedding, err = s.getEmbedding(text)
			if err != nil {
				return nil, err
			}
		}
		success, err := s.graphAddNode(nodeID, propsJSON, embedding)
		if err != nil {
			return nil, err
		}
		return []TextContent{{Type: "text", Text: jsonStr(map[string]any{
			"success": success, "node_id": nodeID,
		})}}, nil

	case "graph_add_edge":
		srcID, _ := args["src_id"].(string)
		dstID, _ := args["dst_id"].(string)
		label, _ := args["label"].(string)
		if srcID == "" || dstID == "" || label == "" {
			return nil, fmt.Errorf("missing required arguments: src_id, dst_id, label")
		}
		weight := 1.0
		if w, ok := args["weight"].(float64); ok {
			weight = w
		}
		propsJSON, _ := args["properties_json"].(string)
		if propsJSON == "" {
			propsJSON = "{}"
		}
		success, err := s.graphAddEdge(srcID, dstID, label, weight, propsJSON)
		if err != nil {
			return nil, err
		}
		return []TextContent{{Type: "text", Text: jsonStr(map[string]any{
			"success": success, "src_id": srcID, "dst_id": dstID, "label": label,
		})}}, nil

	case "graph_rag_query":
		query, _ := args["query"].(string)
		if query == "" {
			return nil, fmt.Errorf("missing required argument: query")
		}
		vectorTopK := 3
		if v, ok := args["vector_top_k"].(float64); ok {
			vectorTopK = int(v)
		}
		hopDepth := 2
		if v, ok := args["hop_depth"].(float64); ok {
			hopDepth = int(v)
		}
		nodes, err := s.graphRAGQuery(query, vectorTopK, hopDepth)
		if err != nil {
			return nil, err
		}
		return []TextContent{{Type: "text", Text: jsonStr(map[string]any{
			"node_count": len(nodes),
			"nodes":      nodes,
		})}}, nil

	// ── Agent Memory Tools ────────────────────────────────────────────────────
	case "kv_set":
		key, _ := args["key"].(string)
		value, _ := args["value"].(string)
		if key == "" || value == "" {
			return nil, fmt.Errorf("missing required arguments: key and value")
		}
		var ttlMs int64
		if v, ok := args["ttl_ms"].(float64); ok {
			ttlMs = int64(v)
		}
		success, err := s.kvSet(key, value, ttlMs)
		if err != nil {
			return nil, err
		}
		msg := "Value stored in working memory"
		if !success {
			msg = "Store failed"
		}
		return []TextContent{{Type: "text", Text: jsonStr(map[string]any{
			"success": success, "key": key, "message": msg,
		})}}, nil

	case "kv_get":
		key, _ := args["key"].(string)
		if key == "" {
			return nil, fmt.Errorf("missing required argument: key")
		}
		val, found, err := s.kvGet(key)
		if err != nil {
			return nil, err
		}
		if !found {
			return []TextContent{{Type: "text", Text: jsonStr(map[string]any{
				"found": false, "key": key, "value": nil,
			})}}, nil
		}
		return []TextContent{{Type: "text", Text: jsonStr(map[string]any{
			"found": true, "key": key, "value": val,
		})}}, nil

	case "kv_delete":
		key, _ := args["key"].(string)
		if key == "" {
			return nil, fmt.Errorf("missing required argument: key")
		}
		ok, err := s.kvDelete(key)
		if err != nil {
			return nil, err
		}
		msg := "Key deleted from working memory"
		if !ok {
			msg = "Key not found"
		}
		return []TextContent{{Type: "text", Text: jsonStr(map[string]any{
			"success": ok, "key": key, "message": msg,
		})}}, nil

	case "memory_recall":
		key, _ := args["key"].(string)
		semanticQuery, _ := args["semantic_query"].(string)
		if key == "" && semanticQuery == "" {
			return nil, fmt.Errorf("at least one of 'key' or 'semantic_query' must be provided")
		}
		topK := 3
		if v, ok := args["top_k"].(float64); ok {
			topK = int(v)
		}
		recall, err := s.memoryRecall(key, semanticQuery, topK)
		if err != nil {
			return nil, err
		}
		return []TextContent{{Type: "text", Text: jsonStr(recall)}}, nil

	default:
		return nil, fmt.Errorf("unknown tool: %s", name)
	}
}

// ─── stdio主循环 ───────────────────────────────────────────────────────────────

/**
 * @brief MCP Server主循环 (stdio模式)
 *
 * [运行时架构] 基于stdio的JSON-RPC通信：
 * - 输入：从stdin读取JSON-RPC请求 (每行一个)
 * - 输出：向stdout写入JSON-RPC响应
 * - 日志：向stderr输出 (避免污染协议流)
 *
 * 启动流程：
 * 1. 检查Embedding提供商配置
 * 2. 初始化HTTP客户端
 * 3. 进入请求处理循环
 *
 * 连接模式：
 * - Cursor: 通过~/.cursor/mcp.json配置
 * - Claude Desktop: 通过settings配置
 * - 其他MCP客户端: 支持stdio标准输入输出
 *
 * @note 空行会被忽略，解析错误返回标准JSON-RPC错误响应
 */

func (s *Server) Run() {
	log.Println("[MinKV MCP Server] starting on stdio")
	if s.provider == "azure" {
		if s.azureApiKey == "" {
			log.Println("[WARN] AZURE_OPENAI_API_KEY not set")
		}
	} else if s.provider == "cohere" {
		if s.cohereKey == "" {
			log.Println("[WARN] COHERE_API_KEY not set")
		}
	} else {
		if s.openaiKey == "" {
			log.Println("[WARN] OPENAI_API_KEY not set")
		}
	}

	scanner := bufio.NewScanner(os.Stdin)
	encoder := json.NewEncoder(os.Stdout)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}

		var req Request
		if err := json.Unmarshal(line, &req); err != nil {
			log.Printf("[ERROR] parse request: %v", err)
			_ = encoder.Encode(Response{
				JSONRPC: "2.0",
				Error:   &RPCError{Code: -32700, Message: "parse error"},
			})
			continue
		}

		resp := s.handleRequest(req)
		if err := encoder.Encode(resp); err != nil {
			log.Printf("[ERROR] encode response: %v", err)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("[FATAL] stdin error: %v", err)
	}
}

// ─── 辅助函数 ──────────────────────────────────────────────────────────────────

/**
 * @brief JSON格式化辅助函数
 * @param v 任意类型数据
 * @return 格式化的JSON字符串
 *
 * [工具函数] 用于统一MCP响应的JSON格式
 * 使用MarshalIndent生成带缩进的可读JSON
 */

func jsonStr(v any) string {
	b, _ := json.MarshalIndent(v, "", "  ")
	return string(b)
}

func main() {
	log.SetFlags(log.LstdFlags)
	log.SetOutput(os.Stderr) // [协议规范] MCP使用stdout传输JSON-RPC，日志输出到stderr避免污染协议流
	NewServer().Run()
}
