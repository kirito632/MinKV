/**
 * @file services.go
 * @brief 外部服务封装：嵌入服务和MinKV HTTP客户端
 *
 * 职责：
 * 1. 嵌入服务抽象：支持OpenAI/Azure/Cohere多种提供商
 * 2. MinKV HTTP客户端：与MinKV核心服务通信
 * 3. 统一的错误处理和配置管理
 */

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// ─── 嵌入服务 ──────────────────────────────────────────────────────────────────

// EmbeddingConfig 嵌入服务配置
type EmbeddingConfig struct {
	Provider         string // "openai", "azure", "cohere", "openrouter"
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

// EmbeddingService 嵌入服务接口
type EmbeddingService interface {
	GetEmbedding(text string) ([]float64, error)
}

// embeddingServiceImpl 嵌入服务实现
type embeddingServiceImpl struct {
	config     EmbeddingConfig
	httpClient *http.Client
}

// NewEmbeddingService 创建嵌入服务实例
func NewEmbeddingService(config EmbeddingConfig) EmbeddingService {
	return &embeddingServiceImpl{
		config: config,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
}

// GetEmbedding 生成文本向量
func (s *embeddingServiceImpl) GetEmbedding(text string) ([]float64, error) {
	switch s.config.Provider {
	case "azure":
		return s.getAzureEmbedding(text)
	case "cohere":
		return s.getCohereEmbedding(text)
	case "openrouter", "openai":
		return s.getOpenAIEmbedding(text)
	default:
		return nil, fmt.Errorf("不支持的嵌入服务提供商: %s", s.config.Provider)
	}
}

// getAzureEmbedding Azure OpenAI嵌入实现
func (s *embeddingServiceImpl) getAzureEmbedding(text string) ([]float64, error) {
	if s.config.AzureEndpoint == "" || s.config.AzureApiKey == "" || 
	   s.config.AzureDeployment == "" || s.config.AzureApiVersion == "" {
		return nil, fmt.Errorf("Azure配置不完整")
	}

	payload := map[string]any{"input": text}
	body, _ := json.Marshal(payload)
	url := strings.TrimRight(s.config.AzureEndpoint, "/") + "/openai/deployments/" + 
		s.config.AzureDeployment + "/embeddings?api-version=" + s.config.AzureApiVersion
	
	req, _ := http.NewRequest("POST", url, bytes.NewReader(body))
	req.Header.Set("api-key", s.config.AzureApiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Azure OpenAI请求失败: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		Data []struct {
			Embedding []float64 `json:"embedding"`
		} `json:"data"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error,omitempty"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("解析嵌入响应失败: %w", err)
	}
	if result.Error != nil {
		return nil, fmt.Errorf("Azure OpenAI错误: %s", result.Error.Message)
	}
	if len(result.Data) == 0 {
		return nil, fmt.Errorf("空的嵌入响应")
	}
	return result.Data[0].Embedding, nil
}

// getCohereEmbedding Cohere嵌入实现
func (s *embeddingServiceImpl) getCohereEmbedding(text string) ([]float64, error) {
	if s.config.CohereKey == "" {
		return nil, fmt.Errorf("COHERE_API_KEY未设置")
	}

	payload := map[string]any{
		"model":       s.config.EmbeddingModel,
		"texts":       []string{text},
		"input_type":  "search_document",
	}
	body, _ := json.Marshal(payload)
	url := strings.TrimRight(s.config.CohereBaseURL, "/") + "/embed"
	
	req, _ := http.NewRequest("POST", url, bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+s.config.CohereKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Cohere请求失败: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Cohere API错误 %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var cr struct {
		Embeddings [][]float64 `json:"embeddings"`
	}
	if err := json.Unmarshal(bodyBytes, &cr); err != nil {
		return nil, fmt.Errorf("解析嵌入响应失败: %w", err)
	}
	if len(cr.Embeddings) == 0 || len(cr.Embeddings[0]) == 0 {
		return nil, fmt.Errorf("空的嵌入响应")
	}
	return cr.Embeddings[0], nil
}

// getOpenAIEmbedding OpenAI/OpenRouter嵌入实现
func (s *embeddingServiceImpl) getOpenAIEmbedding(text string) ([]float64, error) {
	if s.config.OpenAIKey == "" {
		return nil, fmt.Errorf("OPENAI_API_KEY未设置")
	}

	payload := struct {
		Model string `json:"model"`
		Input string `json:"input"`
	}{
		Model: s.config.EmbeddingModel,
		Input: text,
	}
	
	body, _ := json.Marshal(payload)
	url := strings.TrimRight(s.config.OpenAIBaseURL, "/") + "/embeddings"
	
	req, _ := http.NewRequest("POST", url, bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+s.config.OpenAIKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("OpenAI请求失败: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		Data []struct {
			Embedding []float64 `json:"embedding"`
		} `json:"data"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error,omitempty"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("解析嵌入响应失败: %w", err)
	}
	if result.Error != nil {
		return nil, fmt.Errorf("OpenAI错误: %s", result.Error.Message)
	}
	if len(result.Data) == 0 {
		return nil, fmt.Errorf("空的嵌入响应")
	}
	return result.Data[0].Embedding, nil
}

// ─── MinKV HTTP客户端 ──────────────────────────────────────────────────────────

// MinKVConfig MinKV客户端配置
type MinKVConfig struct {
	Host string
	Port string
}

// MinKVClient MinKV客户端接口
type MinKVClient interface {
	VectorSearch(query []float64, topK int, metric string) ([]VectorSearchResult, error)
	VectorInsert(key string, embedding []float64, metadata string) (bool, error)
	KVSet(key, value string, ttlMs int64) (bool, error)
	KVGet(key string) (string, bool, error)
	KVDelete(key string) (bool, error)
	GraphAddNode(nodeID, propertiesJSON string, embedding []float64) (bool, error)
	GraphAddEdge(srcID, dstID, label string, weight float64, propertiesJSON string) (bool, error)
	GraphRAGQuery(queryEmbedding []float64, vectorTopK, hopDepth int) ([]GraphNode, error)
}

// minKVClientImpl MinKV客户端实现
type minKVClientImpl struct {
	baseURL    string
	httpClient *http.Client
}

// NewMinKVClient 创建MinKV客户端实例
func NewMinKVClient(config MinKVConfig) MinKVClient {
	return &minKVClientImpl{
		baseURL: fmt.Sprintf("http://%s:%s", config.Host, config.Port),
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// 向量操作相关类型
type VectorSearchRequest struct {
	Query  []float64 `json:"query"`
	TopK   int       `json:"top_k"`
	Metric string    `json:"metric"`
}

type VectorSearchResult struct {
	Key      string         `json:"key"`
	Score    float64        `json:"score"`
	Metadata map[string]any `json:"metadata"`
}

type SimpleVectorSearchResponse struct {
	Success      bool     `json:"success"`
	Results      []string `json:"results"`
	ResultsCount int      `json:"results_count"`
}

type VectorPutRequest struct {
	Key       string    `json:"key"`
	Embedding []float64 `json:"embedding"`
	Metadata  string    `json:"metadata"`
}

type VectorPutResponse struct {
	Success bool `json:"success"`
}

// VectorSearch 向量语义检索
func (c *minKVClientImpl) VectorSearch(query []float64, topK int, metric string) ([]VectorSearchResult, error) {
	body, _ := json.Marshal(VectorSearchRequest{
		Query:  query,
		TopK:   topK,
		Metric: metric,
	})
	
	resp, err := c.httpClient.Post(c.baseURL+"/vector/search", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("无法连接到MinKV: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("MinKV API错误 %d: %s", resp.StatusCode, raw)
	}

	var simpleResp SimpleVectorSearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&simpleResp); err != nil {
		return nil, fmt.Errorf("解析搜索响应失败: %w", err)
	}

	results := make([]VectorSearchResult, 0, len(simpleResp.Results))
	for _, key := range simpleResp.Results {
		results = append(results, VectorSearchResult{
			Key:   key,
			Score: 0.0,
		})
	}
	return results, nil
}

// VectorInsert 向量插入
func (c *minKVClientImpl) VectorInsert(key string, embedding []float64, metadata string) (bool, error) {
	body, _ := json.Marshal(VectorPutRequest{
		Key:       key,
		Embedding: embedding,
		Metadata:  metadata,
	})
	
	resp, err := c.httpClient.Post(c.baseURL+"/vector/put", "application/json", bytes.NewReader(body))
	if err != nil {
		return false, fmt.Errorf("无法连接到MinKV: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("MinKV API错误 %d: %s", resp.StatusCode, raw)
	}

	var putResp VectorPutResponse
	if err := json.NewDecoder(resp.Body).Decode(&putResp); err != nil {
		return false, fmt.Errorf("解析插入响应失败: %w", err)
	}
	return putResp.Success, nil
}

// KV操作相关类型
type KVSetRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	TTLMs int64  `json:"ttl_ms"`
}

type KVSetResponse struct {
	Success bool   `json:"success"`
	Key     string `json:"key"`
}

type KVGetResponse struct {
	Success bool   `json:"success"`
	Key     string `json:"key"`
	Value   string `json:"value"`
}

type KVDeleteResponse struct {
	Success bool `json:"success"`
}

// KVSet 工作记忆存储
func (c *minKVClientImpl) KVSet(key, value string, ttlMs int64) (bool, error) {
	body, _ := json.Marshal(KVSetRequest{
		Key:   key,
		Value: value,
		TTLMs: ttlMs,
	})
	
	resp, err := c.httpClient.Post(c.baseURL+"/kv/set", "application/json", bytes.NewReader(body))
	if err != nil {
		return false, fmt.Errorf("无法连接到MinKV: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("MinKV API错误 %d: %s", resp.StatusCode, raw)
	}

	var r KVSetResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return false, fmt.Errorf("解析KV设置响应失败: %w", err)
	}
	return r.Success, nil
}

// KVGet 工作记忆读取
func (c *minKVClientImpl) KVGet(key string) (string, bool, error) {
	resp, err := c.httpClient.Get(fmt.Sprintf("%s/kv/get?key=%s", c.baseURL, key))
	if err != nil {
		return "", false, fmt.Errorf("无法连接到MinKV: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusNotFound {
			return "", false, nil
		}
		raw, _ := io.ReadAll(resp.Body)
		return "", false, fmt.Errorf("MinKV API错误 %d: %s", resp.StatusCode, raw)
	}

	var r KVGetResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return "", false, fmt.Errorf("解析KV获取响应失败: %w", err)
	}
	if !r.Success {
		return "", false, nil
	}
	return r.Value, true, nil
}

// KVDelete 工作记忆删除
func (c *minKVClientImpl) KVDelete(key string) (bool, error) {
	req, _ := http.NewRequest("DELETE", fmt.Sprintf("%s/kv/delete?key=%s", c.baseURL, key), nil)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("无法连接到MinKV: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("MinKV API错误 %d: %s", resp.StatusCode, raw)
	}

	var r KVDeleteResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return false, fmt.Errorf("解析KV删除响应失败: %w", err)
	}
	return r.Success, nil
}

// 图操作相关类型
type GraphAddNodeRequest struct {
	NodeID         string    `json:"node_id"`
	PropertiesJSON string    `json:"properties_json"`
	Embedding      []float64 `json:"embedding,omitempty"`
}

type GraphAddEdgeRequest struct {
	SrcID          string  `json:"src_id"`
	DstID          string  `json:"dst_id"`
	Label          string  `json:"label"`
	Weight         float64 `json:"weight"`
	PropertiesJSON string  `json:"properties_json"`
}

type GraphRAGQueryRequest struct {
	QueryEmbedding []float64 `json:"query_embedding"`
	VectorTopK     int       `json:"vector_top_k"`
	HopDepth       int       `json:"hop_depth"`
}

type GraphNode struct {
	NodeID         string `json:"node_id"`
	PropertiesJSON string `json:"properties_json"`
}

type GraphRAGQueryResponse struct {
	Success   bool        `json:"success"`
	NodeCount int         `json:"node_count"`
	Nodes     []GraphNode `json:"nodes"`
}

// GraphAddNode 添加图节点
func (c *minKVClientImpl) GraphAddNode(nodeID, propertiesJSON string, embedding []float64) (bool, error) {
	body, _ := json.Marshal(GraphAddNodeRequest{
		NodeID:         nodeID,
		PropertiesJSON: propertiesJSON,
		Embedding:      embedding,
	})
	
	resp, err := c.httpClient.Post(c.baseURL+"/graph/add_node", "application/json", bytes.NewReader(body))
	if err != nil {
		return false, fmt.Errorf("无法连接到MinKV: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("MinKV API错误 %d: %s", resp.StatusCode, raw)
	}

	var respStruct struct {
		Success bool `json:"success"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&respStruct); err != nil {
		return false, fmt.Errorf("解析图节点添加响应失败: %w", err)
	}
	return respStruct.Success, nil
}

// GraphAddEdge 添加图边
func (c *minKVClientImpl) GraphAddEdge(srcID, dstID, label string, weight float64, propertiesJSON string) (bool, error) {
	body, _ := json.Marshal(GraphAddEdgeRequest{
		SrcID:          srcID,
		DstID:          dstID,
		Label:          label,
		Weight:         weight,
		PropertiesJSON: propertiesJSON,
	})
	
	resp, err := c.httpClient.Post(c.baseURL+"/graph/add_edge", "application/json", bytes.NewReader(body))
	if err != nil {
		return false, fmt.Errorf("无法连接到MinKV: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("MinKV API错误 %d: %s", resp.StatusCode, raw)
	}

	var respStruct struct {
		Success bool `json:"success"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&respStruct); err != nil {
		return false, fmt.Errorf("解析图边添加响应失败: %w", err)
	}
	return respStruct.Success, nil
}

// GraphRAGQuery GraphRAG查询
func (c *minKVClientImpl) GraphRAGQuery(queryEmbedding []float64, vectorTopK, hopDepth int) ([]GraphNode, error) {
	body, _ := json.Marshal(GraphRAGQueryRequest{
		QueryEmbedding: queryEmbedding,
		VectorTopK:     vectorTopK,
		HopDepth:       hopDepth,
	})
	
	resp, err := c.httpClient.Post(c.baseURL+"/graph/rag_query", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("无法连接到MinKV: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("MinKV API错误 %d: %s", resp.StatusCode, raw)
	}

	var ragResp GraphRAGQueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&ragResp); err != nil {
		return nil, fmt.Errorf("解析GraphRAG查询响应失败: %w", err)
	}
	if !ragResp.Success {
		return nil, fmt.Errorf("GraphRAG查询失败")
	}
	return ragResp.Nodes, nil
}