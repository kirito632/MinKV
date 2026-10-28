# MinKV (FlashCache)

[![Language](https://img.shields.io/badge/Language-C++17-blue.svg)](https://en.cppreference.com/w/cpp/compiler_support)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Platform](https://img.shields.io/badge/Platform-Linux-lightgrey.svg)]()

*其他语言版本: [English](README.md) | [简体中文](README_zh-CN.md)*

专为 C++ 应用设计的高性能并发内存 KV 存储，支持 WAL 持久化、SIMD 加速向量检索，以及面向 **GraphRAG** 场景的图数据库层。

---

## 🚀 核心特性

- **极致并发** — Sharded Locking 架构（当前 comprehensive benchmark 使用 128 个独立分片），大幅减少多线程锁竞争。
- **读多写少优化** — `std::shared_mutex` 读写锁配合 **Lazy LRU Promotion**，99% 的 `get` 操作近乎无锁。
- **工业级可靠性** — 支持 TTL 自动过期、容量上限、LRU 淘汰，以及带 Group Commit 的 **WAL 持久化**，保证崩溃一致性。
- **SIMD 加速向量检索** — AVX2 优化的余弦相似度计算，高维 Embedding 场景 4.58 倍加速。
- **Graph-on-KV + GraphRAG** — 基于 ShardedCache 构建的图数据库层：Node/Edge CRUD、K-hop BFS 遍历、两阶段 GraphRAG 查询引擎（向量检索 → 并发 BFS 扩展）。
- **零拷贝接口** — 基于 `std::string_view` 和移动语义设计，消除不必要的内存拷贝。

---

## 🛠 快速开始

只需包含头文件即可集成：

```cpp
#include "core/sharded_cache.h"
#include <string>
#include <iostream>

int main() {
    // 每个分片最多保存 1000 个条目，共 16 个分片
    minkv::db::ShardedCache<std::string, std::string> cache(1000, 16);

    // 写入数据（TTL 单位：毫秒）
    cache.put("user:1001", "Robinson", 5000); // 5秒后过期

    // 读取数据
    auto value = cache.get("user:1001");
    if (value) {
        std::cout << "Found: " << *value << std::endl;
    } else {
        std::cout << "Not found or expired" << std::endl;
    }

    // 默认线程安全，无需额外加锁
    return 0;
}
```

---

## 📊 性能摘要

测试环境：腾讯云上海区，4核8线程，Ubuntu 22.04，g++ 11，编译参数 `-O2 -march=native -mavx2 -mfma`。完整压测数据和图表见 [`docs/tests/comprehensive_benchmark.md`](docs/tests/comprehensive_benchmark.md)。

| 场景 | 结果 |
| :--- | :--- |
| KV R90W10 hit-heavy | **2415万 QPS**（256分片，8线程） |
| KV R90W10 miss-heavy | **2011万 QPS**（128分片，8线程） |
| P99 延迟 | **2.40 μs** hit-heavy / **4.13 μs** miss-heavy |
| WAL ON | **1413万 QPS**，约 **32.8%** 吞吐损耗 |
| SIMD L2 distance | **4.58x** AVX2 加速 |

---

## 🕸 Graph-on-KV 与 GraphRAG

MinKV 在 `ShardedCache<string, string>` 之上提供 Graph-on-KV 层，节点、边、邻接表和 Embedding 共用同一个 KV 实例。它适合用作轻量知识图谱存储，或在 GraphRAG 场景中执行“向量检索 -> K-hop 图扩展”的两阶段查询。

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

关键实现包括二进制邻接表、线程池并发 BFS，以及崩溃后可通过 `RebuildAdjacencyList()` 重建邻接关系。GraphRAG 对照实验和运行方式见 [`docs/tests/graph_function_test.md`](docs/tests/graph_function_test.md)。

---

## 🏗 架构设计

### 为什么 `std::map` + `mutex` 慢？

单把全局互斥锁迫使所有线程串行化。高并发下线程大部分时间都在等锁，这就是 **Lock Contention（锁竞争）**，高并发系统的首要瓶颈。

### MinKV 的解法

**1. 分片（Sharding）**  
按 Hash 将 key 分散到多个独立 Bucket，每个 Bucket 有独立的锁，访问不同 key 的线程互不阻塞。当前 O2 comprehensive benchmark 使用 128 个默认分片，分片扩展测试中 1→256 分片最高提升约 **16x**。

**2. 读写锁（Reader-Writer Lock）**  
真实缓存流量约 99% 是读。`std::shared_mutex` 允许多个读者同时进入，只有写者需要独占，这是实现高读吞吐的基础。

**3. Lazy LRU Promotion**  
传统 LRU 每次读取都要修改链表（本质是写操作）。MinKV 引入 Lazy Promotion：1秒内重复访问不移动链表，将 99% 的 `get` 还原为纯读锁操作，彻底释放读写锁的威力。

**4. Group Commit WAL**  
后台线程按批次合并 WAL entry，并在每个 batch 中执行 `append + fsync`。当前 8 线程 R90W10 miss-heavy 下 WAL ON 达 **1413万 QPS**，较纯内存下降 **32.8%**。

---

## 📁 项目结构

```
MinKV/
├── src/
│   ├── core/           # ShardedCache、LRU、过期管理
│   ├── persistence/    # WAL、Checkpoint、Recovery
│   ├── vector/         # SIMD 加速向量运算
│   ├── graph/          # Graph-on-KV 层（GraphStore、序列化、类型定义）
│   ├── base/           # 线程池、日志、工具类
│   └── tests/          # 基准测试与单元测试
├── tests/
│   └── graph/          # 图单元测试、PBT、GraphRAG 准确性测试
├── mcp_server_go/      # Go MCP Server（Cursor/Claude 接入）
├── docs/
│   ├── images/         # 压测图表
│   └── tests/          # 详细测试报告
└── scripts/            # 图表生成、构建脚本
```

---

**License**: MIT | **Author**: Robinson
