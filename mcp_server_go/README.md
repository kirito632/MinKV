# MinKV Vector MCP Server (Go)

Go rewrite of the Python MCP server. Zero external dependencies — uses only the standard library.

## Build & Run

```bash
cd MinKV/mcp_server_go
go build -o minkv-mcp-server .
```

## Configure Cursor / Claude Desktop

```json
{
  "mcpServers": {
    "minkv-vector": {
      "command": "/path/to/MinKV/mcp_server_go/minkv-mcp-server",
      "env": {
        "OPENAI_API_KEY": "your-key-here",
        "MINKV_HOST": "localhost",
        "MINKV_PORT": "8080"
      }
    }
  }
}
```

## Environment Variables

| Variable          | Default                    | Description                  |
|-------------------|----------------------------|------------------------------|
| `OPENAI_API_KEY`  | (required)                 | OpenAI API key               |
| `EMBEDDING_MODEL` | `text-embedding-3-small`   | OpenAI embedding model       |
| `MINKV_HOST`      | `localhost`                | MinKV HTTP server host       |
| `MINKV_PORT`      | `8080`                     | MinKV HTTP server port       |

## Tools

- `vector_search(query, top_k=5)` — semantic search via cosine similarity
- `vector_insert(key, text, metadata?)` — embed text and store in MinKV
