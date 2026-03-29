// MinKV Vector MCP Server (Go)
// Implements MCP protocol over stdio (JSON-RPC 2.0)
// Tools: vector_search, vector_insert
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
)

// ─── MCP JSON-RPC types ───────────────────────────────────────────────────────

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

// ─── MCP protocol types ───────────────────────────────────────────────────────

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

// ─── OpenAI embedding types ───────────────────────────────────────────────────

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

// ─── MinKV HTTP API types ─────────────────────────────────────────────────────

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

type vectorPutRequest struct {
	Key       string `json:"key"`
	Embedding []float64 `json:"embedding"`
	Metadata  string `json:"metadata"`
}

type vectorPutResponse struct {
	Success bool `json:"success"`
}

// Graph API types
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

// ─── Server ───────────────────────────────────────────────────────────────────

type Server struct {
	minkvURL       string
	openaiKey      string
	embeddingModel string
	httpClient     *http.Client
}

func NewServer() *Server {
	host := getEnv("MINKV_HOST", "localhost")
	port := getEnv("MINKV_PORT", "8080")
	return &Server{
		minkvURL:       fmt.Sprintf("http://%s:%s", host, port),
		openaiKey:      os.Getenv("OPENAI_API_KEY"),
		embeddingModel: getEnv("EMBEDDING_MODEL", "text-embedding-3-small"),
		httpClient:     &http.Client{Timeout: 10 * time.Second},
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// getEmbedding calls OpenAI Embeddings API and returns the vector.
func (s *Server) getEmbedding(text string) ([]float64, error) {
	if s.openaiKey == "" {
		return nil, fmt.Errorf("OPENAI_API_KEY not set")
	}

	body, _ := json.Marshal(embeddingRequest{Model: s.embeddingModel, Input: text})
	req, _ := http.NewRequest("POST", "https://api.openai.com/v1/embeddings", bytes.NewReader(body))
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
}

// vectorSearch generates a query embedding then calls MinKV /vector/search.
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

	var searchResp vectorSearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&searchResp); err != nil {
		return nil, fmt.Errorf("decode search response: %w", err)
	}

	results := make([]map[string]any, 0, len(searchResp.Results))
	for i, r := range searchResp.Results {
		results = append(results, map[string]any{
			"rank":     i + 1,
			"key":      r.Key,
			"score":    r.Score,
			"metadata": r.Metadata,
		})
	}
	log.Printf("[VectorSearch] found %d results", len(results))
	return results, nil
}

// vectorInsert generates an embedding for text then calls MinKV /vector/put.
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

// ─── Graph API methods ────────────────────────────────────────────────────────

// graphAddNode calls POST /graph/add_node
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

// graphAddEdge calls POST /graph/add_edge
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

// graphRAGQuery converts query text to embedding then calls POST /graph/rag_query
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

// ─── MCP handler ─────────────────────────────────────────────────────────────

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
					"query":         map[string]any{"type": "string", "description": "Natural language query to search for"},
					"vector_top_k":  map[string]any{"type": "integer", "default": 3, "description": "Number of entry nodes from vector search"},
					"hop_depth":     map[string]any{"type": "integer", "default": 2, "description": "K-hop depth for graph traversal"},
				},
				"required": []string{"query"},
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

	default:
		return nil, fmt.Errorf("unknown tool: %s", name)
	}
}

// ─── stdio loop ───────────────────────────────────────────────────────────────

func (s *Server) Run() {
	log.Println("[MinKV MCP Server] starting on stdio")
	if s.openaiKey == "" {
		log.Println("[WARN] OPENAI_API_KEY not set")
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

// ─── helpers ──────────────────────────────────────────────────────────────────

func jsonStr(v any) string {
	b, _ := json.MarshalIndent(v, "", "  ")
	return string(b)
}

func main() {
	log.SetFlags(log.LstdFlags)
	log.SetOutput(os.Stderr) // MCP uses stdout for protocol; logs go to stderr
	NewServer().Run()
}
