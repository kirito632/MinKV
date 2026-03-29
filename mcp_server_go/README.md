# MinKV MCP Server (Go)

Go MCP server that exposes MinKV's vector search and **GraphRAG** capabilities to AI tools like Cursor and Claude Desktop. Uses only the Go standard library — zero external dependencies.

## Architecture

```
Cursor / Claude Desktop
        │  MCP protocol (JSON-RPC over stdio)
        ▼
  minkv-mcp-server (this binary)
        │  HTTP REST API
        ▼
  MinKV HTTP Server (localhost:8080)
        │
        ▼
  GraphStore + ShardedCache (C++ core)
```

## Build

```bash
cd MinKV/mcp_server_go
go build -o minkv-mcp-server .
```

## Configure Cursor / Claude Desktop

Add to your MCP config (`~/.cursor/mcp.json` or Claude Desktop settings):

```json
{
  "mcpServers": {
    "minkv": {
      "command": "/path/to/MinKV/mcp_server_go/minkv-mcp-server",
      "env": {
        "OPENAI_API_KEY": "sk-...",
        "MINKV_HOST": "localhost",
        "MINKV_PORT": "8080"
      }
    }
  }
}
```

## Environment Variables

| Variable          | Default                  | Description                        |
|-------------------|--------------------------|------------------------------------|
| `OPENAI_API_KEY`  | (required for RAG tools) | Used to generate text embeddings   |
| `EMBEDDING_MODEL` | `text-embedding-3-small` | OpenAI embedding model             |
| `MINKV_HOST`      | `localhost`              | MinKV HTTP server host             |
| `MINKV_PORT`      | `8080`                   | MinKV HTTP server port             |

## Tools

### Vector Tools
| Tool | Description |
|------|-------------|
| `vector_search(query, top_k=5)` | Semantic search — converts query text to embedding, returns top-k similar vectors |
| `vector_insert(key, text, metadata?)` | Embed text and store in MinKV |

### Graph Tools
| Tool | Description |
|------|-------------|
| `graph_add_node(node_id, properties_json?, text?)` | Add a node; optionally generate embedding from `text` |
| `graph_add_edge(src_id, dst_id, label, weight=1.0, properties_json?)` | Add a directed edge |
| `graph_rag_query(query, vector_top_k=3, hop_depth=2)` | **Full GraphRAG**: text → embedding → vector search → K-hop BFS expansion → returns all related nodes |

## GraphRAG Example

```
User: "What rockets did Elon Musk's company build?"

graph_rag_query("Elon Musk rocket company", top_k=1, hop=2)

Step 1 — Vector search finds: Elon_Musk (cosine=0.99)
Step 2 — 1-hop BFS expands to: SpaceX (via "founded" edge)
Step 3 — 2-hop BFS expands to: Starship, Falcon9 (via "product" edges)

Result: [Elon_Musk, SpaceX, Starship, Falcon9]
```

Nodes like `Starship` and `Falcon9` have no embeddings — pure vector search would miss them entirely. GraphRAG finds them via relationship traversal.

## Test Without HTTP Server

The MCP protocol itself works without a running HTTP server:

```bash
# List all available tools
echo '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' | ./minkv-mcp-server 2>/dev/null
```
