/**
 * @file tools.go
 * @brief MCP工具实现：向量、图、KV和记忆工具
 *
 * 职责：
 * 1. 实现所有MCP工具的业务逻辑
 * 2. 封装工具参数解析和验证
 * 3. 调用外部服务（嵌入服务、MinKV客户端）
 * 4. 格式化工具响应
 *
 * 设计原则：
 * - 每个工具独立实现，便于测试和维护
 * - 统一的错误处理和日志记录
 * - 类型安全的参数解析
 */

package main

import (
	"encoding/json"
	"fmt"
	"log"
)

// ─── 工具管理器 ─────────────────────────────────────────────────────────────────

// ToolManager 工具管理器接口
type ToolManager interface {
	ExecuteTool(name string, args map[string]any) ([]TextContent, error)
	ListTools() []Tool
}

// toolManagerImpl 工具管理器实现
type toolManagerImpl struct {
	embeddingService EmbeddingService
	minkvClient      MinKVClient
}

// NewToolManager 创建工具管理器
func NewToolManager(embeddingService EmbeddingService, minkvClient MinKVClient) ToolManager {
	return &toolManagerImpl{
		embeddingService: embeddingService,
		minkvClient:      minkvClient,
	}
}

// ExecuteTool 执行工具
func (tm *toolManagerImpl) ExecuteTool(name string, args map[string]any) ([]TextContent, error) {
	switch name {
	case "vector_search":
		return tm.vectorSearch(args)
	case "vector_insert":
		return tm.vectorInsert(args)
	case "graph_add_node":
		return tm.graphAddNode(args)
	case "graph_add_edge":
		return tm.graphAddEdge(args)
	case "graph_rag_query":
		return tm.graphRAGQuery(args)
	case "kv_set":
		return tm.kvSet(args)
	case "kv_get":
		return tm.kvGet(args)
	case "kv_delete":
		return tm.kvDelete(args)
	case "memory_recall":
		return tm.memoryRecall(args)
	default:
		return nil, fmt.Errorf("未知的工具: %s", name)
	}
}

// ListTools 列出所有可用工具
func (tm *toolManagerImpl) ListTools() []Tool {
	return []Tool{
		{
			Name:        "vector_search",
			Description: "语义搜索 - 将查询文本转换为向量，返回最相似的top-k个向量",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"query": map[string]any{"type": "string", "description": "搜索查询文本"},
					"top_k": map[string]any{"type": "integer", "description": "返回结果数量", "default": 5},
				},
				"required": []string{"query"},
			},
		},
		{
			Name:        "vector_insert",
			Description: "嵌入文本并存储到MinKV",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"key":      map[string]any{"type": "string", "description": "向量唯一标识"},
					"text":     map[string]any{"type": "string", "description": "原始文本内容"},
					"metadata": map[string]any{"type": "object", "description": "可选元数据"},
				},
				"required": []string{"key", "text"},
			},
		},
		{
			Name:        "graph_add_node",
			Description: "添加图节点；可选从文本生成嵌入",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"node_id":         map[string]any{"type": "string", "description": "节点ID"},
					"properties_json": map[string]any{"type": "string", "description": "节点属性JSON"},
					"text":            map[string]any{"type": "string", "description": "用于生成嵌入的文本"},
				},
				"required": []string{"node_id"},
			},
		},
		{
			Name:        "graph_add_edge",
			Description: "添加有向边",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"src_id":          map[string]any{"type": "string", "description": "源节点ID"},
					"dst_id":          map[string]any{"type": "string", "description": "目标节点ID"},
					"label":           map[string]any{"type": "string", "description": "边标签"},
					"weight":          map[string]any{"type": "number", "description": "边权重", "default": 1.0},
					"properties_json": map[string]any{"type": "string", "description": "边属性JSON"},
				},
				"required": []string{"src_id", "dst_id", "label"},
			},
		},
		{
			Name:        "graph_rag_query",
			Description: "完整GraphRAG：文本→嵌入→向量搜索→K-hop BFS扩展→返回所有相关节点",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"query":         map[string]any{"type": "string", "description": "查询文本"},
					"vector_top_k":  map[string]any{"type": "integer", "description": "向量搜索top-k", "default": 3},
					"hop_depth":     map[string]any{"type": "integer", "description": "BFS跳数深度", "default": 2},
					"max_keywords":  map[string]any{"type": "integer", "description": "最大关键词数量", "default": 3},
				},
				"required": []string{"query"},
			},
		},
		{
			Name:        "kv_set",
			Description: "在工作记忆中存储值（快速KV）；支持TTL过期",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"key":    map[string]any{"type": "string", "description": "存储键名"},
					"value":  map[string]any{"type": "string", "description": "存储值（字符串或JSON）"},
					"ttl_ms": map[string]any{"type": "integer", "description": "过期时间毫秒（0表示不过期）", "default": 0},
				},
				"required": []string{"key", "value"},
			},
		},
		{
			Name:        "kv_get",
			Description: "通过精确键从工作记忆中检索值",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"key": map[string]any{"type": "string", "description": "检索键名"},
				},
				"required": []string{"key"},
			},
		},
		{
			Name:        "kv_delete",
			Description: "从工作记忆中删除键",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"key": map[string]any{"type": "string", "description": "删除键名"},
				},
				"required": []string{"key"},
			},
		},
		{
			Name:        "memory_recall",
			Description: "记忆召回：通过键或语义查询检索记忆",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"key":            map[string]any{"type": "string", "description": "精确键名"},
					"semantic_query": map[string]any{"type": "string", "description": "语义查询文本"},
					"top_k":          map[string]any{"type": "integer", "description": "返回结果数量", "default": 3},
				},
			},
		},
	}
}

// ─── 向量工具 ──────────────────────────────────────────────────────────────────

// vectorSearch 向量搜索工具
func (tm *toolManagerImpl) vectorSearch(args map[string]any) ([]TextContent, error) {
	query, _ := args["query"].(string)
	if query == "" {
		return nil, fmt.Errorf("缺少必要参数: query")
	}
	
	topK := 5
	if v, ok := args["top_k"].(float64); ok {
		topK = int(v)
	}

	log.Printf("[VectorSearch] query=%q top_k=%d", query, topK)

	// 生成嵌入
	embedding, err := tm.embeddingService.GetEmbedding(query)
	if err != nil {
		return nil, err
	}

	// 执行向量搜索
	results, err := tm.minkvClient.VectorSearch(embedding, topK, "cosine")
	if err != nil {
		return nil, err
	}

	// 格式化结果
	formattedResults := make([]map[string]any, 0, len(results))
	for i, result := range results {
		formattedResults = append(formattedResults, map[string]any{
			"rank":  i + 1,
			"key":   result.Key,
			"score": result.Score,
		})
	}

	log.Printf("[VectorSearch] 找到 %d 个结果", len(formattedResults))
	return []TextContent{{Type: "text", Text: jsonStr(formattedResults)}}, nil
}

// vectorInsert 向量插入工具
func (tm *toolManagerImpl) vectorInsert(args map[string]any) ([]TextContent, error) {
	key, _ := args["key"].(string)
	text, _ := args["text"].(string)
	if key == "" || text == "" {
		return nil, fmt.Errorf("缺少必要参数: key 和 text")
	}

	var metadata map[string]any
	if m, ok := args["metadata"].(map[string]any); ok {
		metadata = m
	}

	log.Printf("[VectorInsert] key=%q text_len=%d", key, len(text))

	// 生成嵌入
	embedding, err := tm.embeddingService.GetEmbedding(text)
	if err != nil {
		return nil, err
	}

	// 准备元数据
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

	// 执行向量插入
	success, err := tm.minkvClient.VectorInsert(key, embedding, string(metaJSON))
	if err != nil {
		return nil, err
	}

	msg := "向量插入成功"
	if !success {
		msg = "插入失败"
	}

	log.Printf("[VectorInsert] success=%v key=%q", success, key)
	return []TextContent{{Type: "text", Text: jsonStr(map[string]any{
		"success": success,
		"key":     key,
		"message": msg,
	})}}, nil
}

// ─── 图工具 ────────────────────────────────────────────────────────────────────

// graphAddNode 添加图节点工具
func (tm *toolManagerImpl) graphAddNode(args map[string]any) ([]TextContent, error) {
	nodeID, _ := args["node_id"].(string)
	if nodeID == "" {
		return nil, fmt.Errorf("缺少必要参数: node_id")
	}

	propsJSON, _ := args["properties_json"].(string)
	if propsJSON == "" {
		propsJSON = "{}"
	}

	// 可选：从text生成嵌入
	var embedding []float64
	if text, ok := args["text"].(string); ok && text != "" {
		var err error
		embedding, err = tm.embeddingService.GetEmbedding(text)
		if err != nil {
			return nil, err
		}
	}

	// 执行图节点添加
	success, err := tm.minkvClient.GraphAddNode(nodeID, propsJSON, embedding)
	if err != nil {
		return nil, err
	}

	return []TextContent{{Type: "text", Text: jsonStr(map[string]any{
		"success": success,
		"node_id": nodeID,
	})}}, nil
}

// graphAddEdge 添加图边工具
func (tm *toolManagerImpl) graphAddEdge(args map[string]any) ([]TextContent, error) {
	srcID, _ := args["src_id"].(string)
	dstID, _ := args["dst_id"].(string)
	label, _ := args["label"].(string)
	if srcID == "" || dstID == "" || label == "" {
		return nil, fmt.Errorf("缺少必要参数: src_id, dst_id, label")
	}

	weight := 1.0
	if w, ok := args["weight"].(float64); ok {
		weight = w
	}

	propsJSON, _ := args["properties_json"].(string)
	if propsJSON == "" {
		propsJSON = "{}"
	}

	// 执行图边添加
	success, err := tm.minkvClient.GraphAddEdge(srcID, dstID, label, weight, propsJSON)
	if err != nil {
		return nil, err
	}

	return []TextContent{{Type: "text", Text: jsonStr(map[string]any{
		"success": success,
		"src_id":  srcID,
		"dst_id":  dstID,
		"label":   label,
	})}}, nil
}

// graphRAGQuery GraphRAG查询工具
func (tm *toolManagerImpl) graphRAGQuery(args map[string]any) ([]TextContent, error) {
	query, _ := args["query"].(string)
	if query == "" {
		return nil, fmt.Errorf("缺少必要参数: query")
	}

	vectorTopK := 3
	if v, ok := args["vector_top_k"].(float64); ok {
		vectorTopK = int(v)
	}

	hopDepth := 2
	if v, ok := args["hop_depth"].(float64); ok {
		hopDepth = int(v)
	}

	maxKeywords := 3
	if v, ok := args["max_keywords"].(float64); ok {
		maxKeywords = int(v)
	}

	log.Printf("[GraphRAGQuery] query=%q vector_top_k=%d hop_depth=%d max_keywords=%d", 
		query, vectorTopK, hopDepth, maxKeywords)

	// 生成查询嵌入
	queryEmbedding, err := tm.embeddingService.GetEmbedding(query)
	if err != nil {
		return nil, err
	}

	// 执行GraphRAG查询
	nodes, err := tm.minkvClient.GraphRAGQuery(queryEmbedding, vectorTopK, hopDepth)
	if err != nil {
		return nil, err
	}

	return []TextContent{{Type: "text", Text: jsonStr(map[string]any{
		"node_count": len(nodes),
		"nodes":      nodes,
	})}}, nil
}

// ─── KV工具 ────────────────────────────────────────────────────────────────────

// kvSet KV设置工具
func (tm *toolManagerImpl) kvSet(args map[string]any) ([]TextContent, error) {
	key, _ := args["key"].(string)
	value, _ := args["value"].(string)
	if key == "" || value == "" {
		return nil, fmt.Errorf("缺少必要参数: key 和 value")
	}

	var ttlMs int64
	if v, ok := args["ttl_ms"].(float64); ok {
		ttlMs = int64(v)
	}

	log.Printf("[KvSet] key=%q ttl_ms=%d", key, ttlMs)

	// 执行KV设置
	success, err := tm.minkvClient.KVSet(key, value, ttlMs)
	if err != nil {
		return nil, err
	}

	msg := "值已存储在工作记忆中"
	if !success {
		msg = "存储失败"
	}

	return []TextContent{{Type: "text", Text: jsonStr(map[string]any{
		"success": success,
		"key":     key,
		"message": msg,
	})}}, nil
}

// kvGet KV获取工具
func (tm *toolManagerImpl) kvGet(args map[string]any) ([]TextContent, error) {
	key, _ := args["key"].(string)
	if key == "" {
		return nil, fmt.Errorf("缺少必要参数: key")
	}

	// 执行KV获取
	val, found, err := tm.minkvClient.KVGet(key)
	if err != nil {
		return nil, err
	}

	if !found {
		return []TextContent{{Type: "text", Text: jsonStr(map[string]any{
			"found": false,
			"key":   key,
			"value": nil,
		})}}, nil
	}

	return []TextContent{{Type: "text", Text: jsonStr(map[string]any{
		"found": true,
		"key":   key,
		"value": val,
	})}}, nil
}

// kvDelete KV删除工具
func (tm *toolManagerImpl) kvDelete(args map[string]any) ([]TextContent, error) {
	key, _ := args["key"].(string)
	if key == "" {
		return nil, fmt.Errorf("缺少必要参数: key")
	}

	// 执行KV删除
	ok, err := tm.minkvClient.KVDelete(key)
	if err != nil {
		return nil, err
	}

	msg := "键已从工作记忆中删除"
	if !ok {
		msg = "键未找到"
	}

	return []TextContent{{Type: "text", Text: jsonStr(map[string]any{
		"success": ok,
		"key":     key,
		"message": msg,
	})}}, nil
}

// ─── 记忆工具 ──────────────────────────────────────────────────────────────────

// memoryRecall 记忆召回工具
func (tm *toolManagerImpl) memoryRecall(args map[string]any) ([]TextContent, error) {
	key, _ := args["key"].(string)
	semanticQuery, _ := args["semantic_query"].(string)
	if key == "" && semanticQuery == "" {
		return nil, fmt.Errorf("至少需要提供 'key' 或 'semantic_query' 中的一个参数")
	}

	topK := 3
	if v, ok := args["top_k"].(float64); ok {
		topK = int(v)
	}

	// 如果有精确键，优先使用KV获取
	if key != "" {
		val, found, err := tm.minkvClient.KVGet(key)
		if err != nil {
			return nil, err
		}
		if found {
			return []TextContent{{Type: "text", Text: jsonStr(map[string]any{
				"type":    "exact_match",
				"key":     key,
				"value":   val,
				"found":   true,
				"results": []map[string]any{{"key": key, "value": val}},
			})}}, nil
		}
	}

	// 如果有语义查询，使用向量搜索
	if semanticQuery != "" {
		embedding, err := tm.embeddingService.GetEmbedding(semanticQuery)
		if err != nil {
			return nil, err
		}

		results, err := tm.minkvClient.VectorSearch(embedding, topK, "cosine")
		if err != nil {
			return nil, err
		}

		formattedResults := make([]map[string]any, 0, len(results))
		for _, result := range results {
			formattedResults = append(formattedResults, map[string]any{
				"key":   result.Key,
				"score": result.Score,
			})
		}

		return []TextContent{{Type: "text", Text: jsonStr(map[string]any{
			"type":    "semantic_search",
			"query":   semanticQuery,
			"found":   len(formattedResults) > 0,
			"results": formattedResults,
		})}}, nil
	}

	// 如果既没有键也没有语义查询（理论上不会走到这里）
	return []TextContent{{Type: "text", Text: jsonStr(map[string]any{
		"found":  false,
		"reason": "没有提供有效的查询参数",
	})}}, nil
}

// ─── 辅助函数 ──────────────────────────────────────────────────────────────────

// jsonStr JSON格式化辅助函数
func jsonStr(v any) string {
	b, _ := json.MarshalIndent(v, "", "  ")
	return string(b)
}