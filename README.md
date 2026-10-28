# MinKV (FlashCache)

[![Language](https://img.shields.io/badge/Language-C++17-blue.svg)](https://en.cppreference.com/w/cpp/compiler_support)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Platform](https://img.shields.io/badge/Platform-Linux-lightgrey.svg)]()

*Read this in other languages: [English](README.md) | [简体中文](README_zh-CN.md)*

A high-performance, concurrent, in-memory key-value store optimized for C++ applications, with optional WAL persistence, SIMD-accelerated vector search, and a **Graph-on-KV layer** supporting GraphRAG workloads.

---

## 🚀 Key Features

- **Extreme Concurrency** — Sharded Locking architecture (128 independent shards in the current comprehensive benchmark) dramatically reduces lock contention in multi-threaded environments.
- **Read-Heavy Optimization** — Combines `std::shared_mutex` (Reader-Writer Locks) with a **Lazy LRU Promotion** strategy, making 99% of `get` operations virtually lock-free.
- **Industrial-Grade Reliability** — Supports TTL expiration, capacity limits, LRU eviction, and **Write-Ahead Logging (WAL)** with Group Commit for crash consistency.
- **SIMD-Accelerated Vector Search** — AVX2-optimized cosine similarity for high-dimensional embedding workloads (4.58x speedup over scalar).
- **Graph-on-KV + GraphRAG** — Graph database layer built on top of ShardedCache: Node/Edge CRUD, K-hop BFS traversal, and a two-phase GraphRAG query engine (vector search → concurrent BFS expansion).
- **Zero-Copy API** — Interfaces designed with `std::string_view` and move semantics to eliminate unnecessary allocations.

---

## 🛠 Quick Start

Header-only integration — just include and go:

```cpp
#include "core/sharded_cache.h"
#include <string>
#include <iostream>

int main() {
    // Each shard stores up to 1,000 entries; 16 shards in total
    minkv::db::ShardedCache<std::string, std::string> cache(1000, 16);

    // Write with TTL (milliseconds)
    cache.put("user:1001", "Robinson", 5000); // expires in 5 seconds

    // Read
    auto value = cache.get("user:1001");
    if (value) {
        std::cout << "Found: " << *value << std::endl;
    } else {
        std::cout << "Not found or expired" << std::endl;
    }

    // Thread-safe by default — no external locking needed
    return 0;
}
```

---

## 📊 Performance Summary

Environment: Tencent Cloud Shanghai, 4 cores / 8 threads, Ubuntu 22.04, g++ 11, compiled with `-O2 -march=native -mavx2 -mfma`. Full benchmark data and charts are available in [`docs/tests/comprehensive_benchmark.md`](docs/tests/comprehensive_benchmark.md).

| Scenario | Result |
| :--- | :--- |
| KV R90W10 hit-heavy | **24.15M QPS** (256 shards, 8 threads) |
| KV R90W10 miss-heavy | **20.11M QPS** (128 shards, 8 threads) |
| P99 latency | **2.40 μs** hit-heavy / **4.13 μs** miss-heavy |
| WAL ON | **14.13M QPS**, about **32.8%** throughput loss |
| SIMD L2 distance | **4.58x** AVX2 speedup |

---

## 🔍 Vector Search (HNSW)

`ShardedCache` ships an HNSW index (`src/vector/`). It is off by default; enable it
with `enableAnnIndex(dim, ...)`. One metric per index instance (L2 or cosine),
tombstone deletes with automatic compaction, and a four-thread parallel build.
Design notes, measurements and the traps found along the way live in
[docs/HNSW_DESIGN.md](docs/HNSW_DESIGN.md).

Headline numbers (100K real text embeddings, 256 dimensions, `potion-base-8M`,
L2-normalised, medians of three runs):

| Configuration | recall@10 | P50 | Build | Index memory |
|---|---|---|---|---|
| Exact scan (contiguous array) | 1.0000 | 4.15 ms | — | — |
| M=16, ef=32 | 0.9780 | 0.169 ms | 33.5 s | 113 MB |
| M=16, ef=64 | 0.9870 | 0.282 ms | 33.5 s | 113 MB |
| M=24, ef=64 | 0.9905 | 0.363 ms | 54.1 s | 120 MB |
| M=32, ef=64 | 0.9935 | 0.428 ms | 81.0 s | 126 MB |

Two caveats belong next to that table: raising `ef_search` is the cheaper lever
than raising `M` on these corpora, and on adversarial uniform data HNSW can end up
slower than a plain scan once 0.95 recall is required. Both are quantified in the
design doc rather than asserted here.

## 🕸 Graph-on-KV & GraphRAG

MinKV provides a Graph-on-KV layer on top of `ShardedCache<string, string>`. Nodes, edges, adjacency lists, and embeddings share the same KV instance, making it useful for lightweight knowledge graph storage and two-stage GraphRAG queries: vector search -> K-hop graph expansion.

```cpp
#include "graph/graph_store.h"
#include "core/sharded_cache.h"

using namespace minkv::graph;
using GraphKVStore = minkv::db::ShardedCache<std::string, std::string>;

auto kv = std::make_shared<GraphKVStore>(65536, 16);
GraphStore gs(kv);

gs.AddNode({"Elon_Musk", R"({"role":"CEO"})"});
gs.AddNode({"SpaceX",    R"({"type":"company"})"});
gs.AddNode({"Starship",  R"({"type":"rocket"})"});

gs.AddEdge({"Elon_Musk", "SpaceX",  "founded"});
gs.AddEdge({"SpaceX",    "Starship", "product"});

auto results = gs.GraphRAGQuery(query_vec, /*top_k=*/3, /*hop_depth=*/2);
```

Key implementation details include binary adjacency lists, thread-pool concurrent BFS, and crash-time adjacency repair via `RebuildAdjacencyList()`. GraphRAG experiments and run commands are documented in [`docs/tests/graph_function_test.md`](docs/tests/graph_function_test.md).

---

## 🏗 Architecture Design

### Why is `std::map` + `mutex` slow?

A single global mutex forces all threads to serialize. Under high concurrency, threads spend most of their time waiting — this is **Lock Contention**, the primary bottleneck in concurrent systems.

### How MinKV solves it

**1. Sharding**  
Keys are hashed into multiple independent buckets. Each bucket has its own lock, so threads accessing different keys never block each other. In the current O2 comprehensive benchmark, the default run uses 128 shards, and shard scaling reaches about **16x** from 1 to 256 shards.

**2. Reader-Writer Locks**  
~99% of real-world cache traffic is reads. `std::shared_mutex` allows unlimited concurrent readers; only writers need exclusive access. This is the foundation that makes high read throughput possible.

**3. Lazy LRU Promotion**  
Traditional LRU updates the linked list on every read — a write operation under the hood. MinKV's Lazy Promotion skips list updates for accesses within a 1-second window, converting 99% of `get` calls into pure read-lock operations and fully unlocking the potential of the reader-writer lock.

**4. Group Commit WAL**  
A background thread batches WAL entries and performs `append + fsync` per batch. In the current 8-thread R90W10 miss-heavy benchmark, WAL ON reaches **14.13M QPS**, about **32.8%** below the in-memory path.

---

## 📁 Project Structure

```
MinKV/
├── src/
│   ├── core/           # ShardedCache, LRU, expiration
│   ├── persistence/    # WAL, checkpoint, recovery
│   ├── vector/         # SIMD-accelerated vector ops
│   ├── graph/          # Graph-on-KV layer (GraphStore, serializer, types)
│   ├── base/           # Thread pool, logging, utilities
│   └── tests/          # Benchmarks and unit tests
├── tests/
│   └── graph/          # Graph unit tests, PBT, GraphRAG accuracy test
├── mcp_server_go/      # Go MCP server (Cursor/Claude integration)
├── docs/
│   ├── images/         # Benchmark charts
│   └── tests/          # Detailed test reports
└── scripts/            # Chart generation, build helpers
```

---

**License**: MIT | **Author**: Robinson
