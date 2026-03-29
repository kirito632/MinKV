# MinKV (FlashCache)

[![Language](https://img.shields.io/badge/Language-C++17-blue.svg)](https://en.cppreference.com/w/cpp/compiler_support)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Platform](https://img.shields.io/badge/Platform-Linux-lightgrey.svg)]()

*Read this in other languages: [English](README.md) | [简体中文](README_zh-CN.md)*

A high-performance, concurrent, in-memory key-value store optimized for C++ applications, with optional WAL persistence, SIMD-accelerated vector search, and a **Graph-on-KV layer** supporting GraphRAG workloads.

---

## 🚀 Key Features

- **Extreme Concurrency** — Sharded Locking architecture (32 independent shards by default) dramatically reduces lock contention in multi-threaded environments.
- **Read-Heavy Optimization** — Combines `std::shared_mutex` (Reader-Writer Locks) with a **Lazy LRU Promotion** strategy, making 99% of `get` operations virtually lock-free.
- **Industrial-Grade Reliability** — Supports TTL expiration, capacity limits, LRU eviction, and **Write-Ahead Logging (WAL)** with Group Commit for crash consistency.
- **SIMD-Accelerated Vector Search** — AVX2-optimized cosine similarity for high-dimensional embedding workloads (4.58x speedup over scalar).
- **Graph-on-KV + GraphRAG** — Graph database layer built on top of ShardedCache: Node/Edge CRUD, K-hop BFS traversal, and a two-phase GraphRAG query engine (vector search → concurrent BFS expansion).
- **Zero-Copy API** — Interfaces designed with `std::string_view` and move semantics to eliminate unnecessary allocations.

---

## 📊 Benchmarks

> Environment: Tencent Cloud (Shanghai), 4 Cores / 8 Threads, Ubuntu 22.04, g++ 11  
> Compilation: `-O2 -march=native -mavx2 -mfma`  
> (Note: `-O2` outperforms `-O3` on this workload — aggressive inlining inflates hot-path code size and increases L1 icache pressure.)

### Core Metrics at a Glance

| Metric | Value |
| :--- | :--- |
| Peak Throughput | **5.08M QPS** (2 threads, 32 shards, R90W10) |
| P99 Latency | **2.81 μs** |
| WAL Overhead (8 threads) | **10.6%** (Group Commit, 10ms interval) |
| Sharding Gain | **+77%** (1 shard → 32 shards) |
| SIMD Speedup | **4.58x** (512-dim L2 distance, AVX2) |

---

### 1. Concurrent Throughput (R90W10, 32 Shards)

![QPS vs Threads](docs/images/01_qps_vs_threads.png)

Peak QPS hits **5.08M at 2 threads**. Performance plateaus beyond 2 threads due to memory bandwidth saturation — the 100K key range exceeds L3 cache capacity, causing contention on the memory bus rather than on locks.

| Threads | QPS | P50 (μs) | P95 (μs) | P99 (μs) | Hit Rate |
| :---: | :---: | :---: | :---: | :---: | :---: |
| 1 | 3.72M | 0.20 | 0.50 | 0.66 | 10% |
| **2** | **5.08M** | **0.29** | **0.59** | **2.81** | **10%** |
| 4 | 3.93M | 0.52 | 3.26 | 8.85 | 11% |
| 8 | 3.10M | 1.02 | 10.45 | 18.52 | 13% |
| 16 | 2.86M | 1.44 | 25.85 | 51.65 | 16% |

---

### 2. P99 Tail Latency

![P99 Latency](docs/images/05_p99_latency.png)

P99 rises sharply beyond 2 threads due to lock contention, peaking at **51.65 μs at 16 threads**.

---

### 3. Shard Count Ablation (8 Threads, W70R30)

![Shard Ablation](docs/images/04_shard_ablation.png)

Optimal at **32 shards** (+77% over 1 shard). Performance saturates beyond 4 shards; the difference between 16 and 64 shards is under 1%.

| Shards | QPS | Relative |
| :---: | :---: | :---: |
| 1 | 1.71M | 0.57x |
| 4 | 2.99M | 0.99x |
| 8 | 3.02M | 0.99x |
| **32** | **3.04M** | **1.00x** |
| 64 | 2.99M | 0.99x |

---

### 4. WAL Persistence Overhead (Group Commit, 10ms interval)

![WAL Overhead](docs/images/02_wal_overhead.png)

`fsync` overhead drops from **34.9% at 1 thread to 10.6% at 8 threads** thanks to Group Commit batching writes. Group Commit is **199x faster** than sync-every-write (~16K QPS).

The 1-thread overhead is elevated because the WAL background flush thread competes with the main thread for the same physical CPU core. The 8-thread figure (10.6%) is more representative of real production multi-core environments.

| Threads | In-Memory QPS | Group Commit QPS | Overhead |
| :---: | :---: | :---: | :---: |
| 1 | 3.42M | 2.23M | 34.9% |
| 2 | 3.94M | 3.02M | 23.4% |
| 4 | 3.76M | 3.11M | 17.4% |
| 8 | 3.09M | 2.76M | **10.6%** |

---

### 5. SIMD Vector Optimization (512-dim L2 Distance, 100K queries)

![SIMD Speedup](docs/images/03_simd_speedup.png)

AVX2 SIMD achieves a **4.58x QPS speedup** (18.73M vs 4.09M) and reduces average latency from 0.24 μs to 0.05 μs. This is 57.2% of the theoretical 8x maximum (AVX2 processes 8 floats per instruction), with the gap explained by memory bandwidth limits, horizontal reduction overhead, and cache misses.

| Version | QPS | Avg Latency | Speedup |
| :---: | :---: | :---: | :---: |
| Scalar (loop) | 4.09M | 0.24 μs | 1.00x |
| **AVX2 SIMD** | **18.73M** | **0.05 μs** | **4.58x** |

> System-level KV impact is ±3% (Amdahl's Law: vector computation is only ~1–2% of total KV operation time; the bottleneck is hash lookup and lock management).

---

## 🛠 Quick Start

Header-only integration — just include and go:

```cpp
#include "db/sharded_cache.h"
#include <string>
#include <iostream>

int main() {
    // 10,000 capacity per shard × 32 shards = 320,000 total capacity
    minkv::db::ShardedCache<std::string, std::string> cache(10000, 32);

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

## 🕸 Graph-on-KV & GraphRAG

MinKV includes a graph database layer built entirely on top of `ShardedCache<string, string>`. All graph data (nodes, edges, adjacency lists, embeddings) lives in the same KV instance — no separate storage engine needed.

### Key Space Design

```
n:{node_id}           → Node binary serialization
e:{src}:{dst}:{label} → Edge binary serialization
adj:out:{node_id}     → Outgoing adjacency list (binary length-prefix array)
adj:in:{node_id}      → Incoming adjacency list
vec:{node_id}         → Embedding raw bytes (float[])
```

### Usage

```cpp
#include "graph/graph_store.h"
#include "core/sharded_cache.h"

using namespace minkv::graph;
using GraphKVStore = minkv::db::ShardedCache<std::string, std::string>;

auto kv = std::make_shared<GraphKVStore>(65536, 16);
GraphStore gs(kv);

// Build a knowledge graph
gs.AddNode({"Elon_Musk", R"({"role":"CEO"})"});
gs.AddNode({"SpaceX",    R"({"type":"company"})"});
gs.AddNode({"Starship",  R"({"type":"rocket"})"});   // no embedding needed

gs.SetNodeEmbedding("Elon_Musk", embed("Elon Musk founder entrepreneur"));
gs.SetNodeEmbedding("SpaceX",    embed("aerospace rocket launch"));

gs.AddEdge({"Elon_Musk", "SpaceX",  "founded"});
gs.AddEdge({"SpaceX",    "Starship", "product"});

// GraphRAG query: vector search → concurrent K-hop BFS expansion
auto results = gs.GraphRAGQuery(query_vec, /*top_k=*/3, /*hop_depth=*/2);
// Returns Elon_Musk + SpaceX (via vector search) + Starship (via graph traversal)
// Starship has no embedding — pure vector search would miss it entirely
```

### GraphRAG Accuracy (controlled experiment)

| Method | Recall on no-embedding nodes |
|--------|------------------------------|
| Pure vector search | **0%** |
| GraphRAG (2-hop) | **42.6%** |

Nodes like `Starship`, `GPT4`, `AWS` have no embeddings. Vector search cannot find them. GraphRAG traverses the graph and retrieves them via relationship chains.

### Performance Optimizations

- **Binary adjacency list** — Replaced JSON with length-prefix binary encoding. Eliminates JSON escape/parse overhead for hub nodes (high-degree nodes with thousands of edges).
- **Thread pool concurrent BFS** — Multiple entry nodes' BFS runs in parallel. Fixed-size thread pool eliminates `std::async` thread creation overhead. P99 latency: **467ms → 10ms**.
- **Crash recovery** — Write order guarantees edge data precedes adjacency list updates. `RebuildAdjacencyList()` rescans all `e:` keys to restore consistency after a crash.

### Build & Run Tests

```bash
cmake -S MinKV -B MinKV/build -DCMAKE_BUILD_TYPE=Release
cmake --build MinKV/build --target demo_graphrag test_graphrag_accuracy -j$(nproc)

# GraphRAG demo (Elon Musk → SpaceX → Starship chain)
./MinKV/build/bin/demo_graphrag 2>/dev/null

# Accuracy test: GraphRAG vs pure vector search
./MinKV/build/bin/test_graphrag_accuracy 2>/dev/null
```

---

Header-only integration — just include and go:

```cpp
#include "db/sharded_cache.h"
#include <string>
#include <iostream>

int main() {
    // 10,000 capacity per shard × 32 shards = 320,000 total capacity
    minkv::db::ShardedCache<std::string, std::string> cache(10000, 32);

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

## 🏗 Architecture Design

### Why is `std::map` + `mutex` slow?

A single global mutex forces all threads to serialize. Under high concurrency, threads spend most of their time waiting — this is **Lock Contention**, the primary bottleneck in concurrent systems.

### How MinKV solves it

**1. Sharding**  
Keys are hashed into 32 independent buckets. Each bucket has its own lock, so threads accessing different keys never block each other. Ablation tests confirm a **+77% throughput gain** over a single-shard baseline.

**2. Reader-Writer Locks**  
~99% of real-world cache traffic is reads. `std::shared_mutex` allows unlimited concurrent readers; only writers need exclusive access. This is the foundation that makes high read throughput possible.

**3. Lazy LRU Promotion**  
Traditional LRU updates the linked list on every read — a write operation under the hood. MinKV's Lazy Promotion skips list updates for accesses within a 1-second window, converting 99% of `get` calls into pure read-lock operations and fully unlocking the potential of the reader-writer lock.

**4. Group Commit WAL**  
A background thread batches writes and calls `fsync` every 10ms. This amortizes the cost of disk I/O across many operations, keeping persistence overhead at ~10% in multi-threaded workloads while guaranteeing crash consistency.

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
