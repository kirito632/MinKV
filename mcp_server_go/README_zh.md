# MinKV MCP Server (Go实现)

基于 Go 标准库实现的 MCP (Model Context Protocol) 服务端，为 Cursor、Claude Desktop 等 AI 工具提供 MinKV 的向量检索、GraphRAG 和 Agent 记忆管理能力。**零外部依赖**设计。

## 架构

```
Cursor / Claude Desktop
        │  MCP协议 (JSON-RPC over stdio)
        ▼
  minkv-mcp-server (本服务端)
        │  HTTP REST API
        ▼
  MinKV HTTP Server (localhost:8080)
        │
        ▼
  GraphStore + ShardedCache (C++核心)
```

## 编译构建

```bash
cd MinKV/mcp_server_go
go build -o minkv-mcp-server .
```

## 配置 Cursor / Claude Desktop

添加 MCP 配置到 `~/.cursor/mcp.json` 或 Claude Desktop 设置：

```json
{
  "mcpServers": {
    "minkv": {
      "command": "/path/to/MinKV/mcp_server_go/minkv-mcp-server",
      "env": {
        "OPENAI_API_KEY": "sk-...",
        "EMBEDDING_PROVIDER": "openai",
        "MINKV_HOST": "localhost",
        "MINKV_PORT": "8080"
      }
    }
  }
}
```

## 环境变量配置

### 基础配置

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `EMBEDDING_PROVIDER` | `openai` | Embedding提供商: `openai` / `azure` / `cohere` / `openrouter` |
| `MINKV_HOST` | `localhost` | MinKV HTTP服务器地址 |
| `MINKV_PORT` | `8080` | MinKV HTTP服务器端口 |

### OpenAI 配置 (默认)

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `OPENAI_API_KEY` | (必需) | OpenAI API密钥 |
| `OPENAI_BASE_URL` | `https://api.openai.com/v1` | OpenAI API端点 |
| `EMBEDDING_MODEL` | `text-embedding-3-small` | Embedding模型名称 |

### Azure OpenAI 配置

| 变量 | 说明 |
|------|------|
| `AZURE_OPENAI_API_KEY` | Azure API密钥 |
| `AZURE_OPENAI_ENDPOINT` | Azure端点 (如 `https://xxx.openai.azure.com`) |
| `AZURE_OPENAI_EMBEDDINGS_DEPLOYMENT` | 部署名称 |
| `AZURE_OPENAI_API_VERSION` | `2024-02-15-preview` |

### Cohere 配置

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `COHERE_API_KEY` | (必需) | Cohere API密钥 |
| `COHERE_BASE_URL` | `https://api.cohere.ai/v1` | Cohere API端点 |
| `EMBEDDING_MODEL` | `text-embedding-3-small` | Cohere模型名称 |

## 工具列表 (9个MCP Tools)

### 向量工具

| 工具 | 说明 |
|------|------|
| `vector_search(query, top_k=5)` | 语义检索 — 将查询文本转为向量，返回最相似的Top-K结果 |
| `vector_insert(key, text, metadata?)` | 文本向量化并存储 — 自动生成Embedding并入库 |

### 图工具

| 工具 | 说明 |
|------|------|
| `graph_add_node(node_id, properties_json?, text?)` | 添加图节点 — 可绑定Embedding |
| `graph_add_edge(src_id, dst_id, label, weight=1.0, properties_json?)` | 添加有向边 |
| `graph_rag_query(query, vector_top_k=3, hop_depth=2)` | **完整GraphRAG** — 向量检索入口 + K-hop BFS扩展 |

### Agent记忆工具

| 工具 | 说明 |
|------|------|
| `kv_set(key, value, ttl_ms=0)` | 工作记忆存储 — 高速KV，支持TTL过期 |
| `kv_get(key)` | 工作记忆检索 — 精确匹配 |
| `kv_delete(key)` | 工作记忆删除 — 清理过期上下文 |
| `memory_recall(key?, semantic_query?, top_k=3)` | **混合记忆检索** — KV精确查找 + 向量语义检索 |

## GraphRAG 示例

```
用户: "Elon Musk的公司制造了哪些火箭？"

graph_rag_query("Elon Musk rocket company", top_k=1, hop=2)

步骤1 — 向量检索命中: Elon_Musk (余弦相似度=0.99)
步骤2 — 1-hop BFS扩展到: SpaceX (通过"创立"边)
步骤3 — 2-hop BFS扩展到: Starship, Falcon9 (通过"产品"边)

结果: [Elon_Musk, SpaceX, Starship, Falcon9]
```

Starship 和 Falcon9 等节点**没有Embedding** —— 纯向量检索会完全遗漏它们，GraphRAG通过关系遍历成功召回。**召回率提升至42.6%**。

## 分层记忆架构示例

```
用户: "我之前问过类似的问题..."

memory_recall(
    key="last_query",           // 工作记忆: 精确检索上次查询
    semantic_query="类似的问题", // 情景记忆: 语义相似检索
    top_k=3
)

返回结果:
{
  "working_memory": {          // < 1ms 延迟
    "key": "last_query",
    "value": "如何优化数据库性能"
  },
  "episodic_memory": [         // ~10ms 延迟
    {"key": "doc:sql-tuning", "score": 0.89},
    {"key": "doc:index-design", "score": 0.85}
  ]
}
```

## 无需HTTP服务器的测试

MCP协议本身可在无HTTP服务器时测试协议层：

```bash
# 列出所有可用工具
echo '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' | ./minkv-mcp-server 2>/dev/null
```

## 完整工作流测试

```bash
# 1. 启动 MinKV HTTP 服务器
./bin/http_server_example

# 2. 配置环境变量
export OPENAI_API_KEY='sk-...'
export EMBEDDING_PROVIDER='openai'

# 3. 运行MCP服务器
./minkv-mcp-server

# 4. 在 Cursor/Claude 中调用工具
# @minkv search for "机器学习算法"
```

## 技术特点

- **零外部依赖**: 仅使用Go标准库，无第三方依赖
- **多提供商支持**: OpenAI / Azure / Cohere / OpenRouter
- **分层记忆**: Working Memory (微秒级) + Episodic Memory (语义检索)
- **GraphRAG**: 向量检索 + 图遍历，召回率42.6%
- **完整错误处理**: API错误、网络超时、参数验证

## 许可证

MIT License
