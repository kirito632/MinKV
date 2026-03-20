# MinKV (FlashCache)

> A high-performance, concurrent, in-memory key-value store for C++ applications.  
> 专为高并发设计的 C++ 本地缓存库。

## 🚀 核心特性 (Key Features)

*   **极致并发**: 采用 **Sharded Locking (分片锁)** 架构，将大锁拆分为 32 个独立分片，大幅减少锁竞争。
*   **读多写少优化**: 引入 **`std::shared_mutex` (读写锁)** 配合 **Lazy LRU Promotion**，让 99% 的 `get` 操作变为无锁并发读。
*   **工业级特性**: 支持 **TTL (自动过期)**、**最大容量限制**、**LRU 淘汰**、**WAL 持久化**。
*   **零拷贝接口**: 精心设计的 API，减少不必要的内存拷贝。

---

## 📊 性能压测 (Benchmark)

> 腾讯云竞价实例，上海8区，4核8线程，Ubuntu 22.04，g++ 11  
> 编译参数：`-O2 -march=native -mavx2 -mfma`（详见 `docs/tests/RELEASE_BENCHMARK.md`）

### 核心性能指标

*   **峰值吞吐量**: **508万 QPS** (2线程, 32分片, R90W10)
*   **P99延迟**: **2.81 μs**
*   **WAL持久化开销**: **10.6%** (8线程多核场景)
*   **分片优化提升**: **+77%** (1分片171万 → 32分片304万 QPS)
*   **SIMD算法加速**: **4.58倍** (向量L2距离计算，效率57.2%)

### 1. 并发吞吐量 (R90W10, 32分片)

![QPS vs Threads](docs/images/01_qps_vs_threads.png)

Peak concurrent QPS reaches **5.08M at 2 threads** (-O2 compilation). Performance scales well up to 2 threads, then plateaus due to memory bandwidth contention (100K key range exceeds L3 cache).

| 线程数 | QPS | P50(μs) | P95(μs) | P99(μs) | 命中率 |
| :---: | :---: | :---: | :---: | :---: | :---: |
| 1 | 372万 | 0.20 | 0.50 | 0.66 | 10% |
| **2** | **508万** | **0.29** | **0.59** | **2.81** | **10%** |
| 4 | 393万 | 0.52 | 3.26 | 8.85 | 11% |
| 8 | 310万 | 1.02 | 10.45 | 18.52 | 13% |
| 16 | 286万 | 1.44 | 25.85 | 51.65 | 16% |

### 2. P99 尾延迟

![P99 Latency](docs/images/05_p99_latency.png)

P99 latency rises sharply beyond 2 threads due to lock contention, peaking at **51.65μs at 16 threads**.

### 3. 分片数消融测试 (8线程, W70R30)

![Shard Ablation](docs/images/04_shard_ablation.png)

Optimal performance at **32 shards**, achieving 3.04M QPS (+77% improvement over 1 shard). Performance saturates beyond 4 shards; 16~64 shards differ by less than 1%.

| 分片数 | QPS | 相对性能 |
| :---: | :---: | :---: |
| 1 | 171万 | 0.57x |
| 4 | 299万 | 0.99x |
| 8 | 302万 | 0.99x |
| **32** | **304万** | **1.00x** |
| 64 | 299万 | 0.99x |

### 4. WAL持久化性能 (Group Commit, 10ms间隔)

![WAL Overhead](docs/images/02_wal_overhead.png)

Persistence (fsync) overhead drops from **34.9% at 1 thread to 10.6% at 8 threads**, thanks to Group Commit batching. Group Commit is **199x faster** than sync-every-write (~1.6万 QPS).

| 线程数 | 纯内存 QPS | Group Commit QPS | 损耗 |
| :---: | :---: | :---: | :---: |
| 1 | 342万 | 223万 | 34.9% |
| 2 | 394万 | 302万 | 23.4% |
| 4 | 376万 | 311万 | 17.4% |
| 8 | 309万 | 276万 | **10.6%** |

> 1线程损耗偏高（WAL后台线程与主线程竞争同一CPU核）；8线程多核场景10.6%更能代表生产实际。

### 5. SIMD向量化优化 (512维L2距离, 10万次查询)

![SIMD Speedup](docs/images/03_simd_speedup.png)

AVX2 SIMD achieves **4.58x QPS speedup** (18.73M vs 4.09M) and reduces average latency from 0.24μs to 0.05μs (57% of theoretical 8x max efficiency). System-level KV impact is ±3% (Amdahl's Law).

| 版本 | QPS | 平均延迟 | 加速比 |
| :---: | :---: | :---: | :---: |
| 标量版本 | 409万 | 0.24 μs | 1.00x |
| **AVX2 SIMD** | **1873万** | **0.05 μs** | **4.58x** |

---

## 🛠️ 快速开始 (Quick Start)

### 集成

只需包含头文件即可使用：

```cpp
#include "db/sharded_cache.h"
#include <string>
#include <iostream>

int main() {
    // 创建一个分片缓存
    // 每个分片容量 10000，共 32 个分片 -> 总容量 320,000
    minkv::db::ShardedCache<std::string, std::string> cache(10000, 32);

    // 1. 写入数据 (支持 TTL)
    cache.put("user:1001", "Robinson", 5000); // 5秒后过期

    // 2. 读取数据
    auto value = cache.get("user:1001");
    if (value) {
        std::cout << "Found: " << *value << std::endl;
    } else {
        std::cout << "Not found or expired" << std::endl;
    }

    // 3. 并发安全
    // 你可以在多个线程中安全地调用 get/put，无需额外加锁
    
    return 0;
}
```


## 🏗️ 架构设计 (Architecture)

### 为什么标准库 (`std::map` + `mutex`) 慢？
标准库方案使用一把全局互斥锁。当多个线程同时访问时，它们必须串行排队。这被称为 **Lock Contention (锁竞争)**，是高并发系统的杀手。

### MinKV 是如何优化的？
1.  **Sharding (分片)**: 将数据按 Hash 分散到 32 个独立的 Bucket，消融测试验证相比单锁提升 **77%**。
2.  **Reader-Writer Lock (读写锁)**: 99% 的请求是读。我们使用 `shared_mutex`，允许多个读者同时进入，只有写者需要独占。
3.  **Lazy LRU**: 传统的 LRU 每次读取都要修改链表（写操作）。我们引入 **Lazy Promotion**，1秒内重复访问不移动链表，将 99% 的 `get` 还原为纯读操作，彻底释放了读写锁的威力。
4.  **Group Commit WAL**: 后台线程每10ms批量fsync，在保证持久化的前提下将性能损耗控制在10%左右。

---

**License**: MIT  
**Author**: Robinson
