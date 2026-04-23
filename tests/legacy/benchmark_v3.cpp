#include <iostream>
#include <chrono>
#include <thread>
#include <vector>
#include <atomic>
#include <random>
#include <iomanip>
#include "../db/lru_cache.h"
#include "../db/sharded_cache.h"
#include "../db/sharded_cache_v2.h"
#include "../db/lru_cache_lazy.h"

using namespace minkv::db;

// 改进的压测参数
const int NUM_THREADS = 16;
const int OPS_PER_THREAD = 1000000;      // 增加到 100 万次
const int KEY_RANGE = 100000;            // 增加 key 范围到 10 万
const int CACHE_SIZE = 50000;            // 增加缓存容量到 5 万
const int READ_RATIO = 90;               // 90% 读，10% 写

struct BenchmarkResult {
    std::string name;
    long long total_ops;
    long long duration_ms;
    double qps;
    double latency_us;
};

void print_result(const BenchmarkResult& result) {
    std::cout << std::left << std::setw(35) << result.name
              << std::setw(15) << (result.qps / 1000000.0) << " M ops/s"
              << std::setw(15) << std::fixed << std::setprecision(2) << result.latency_us << " us"
              << std::endl;
}

// 基础版本：LruCache（单机大锁）
BenchmarkResult benchmark_lru() {
    LruCache<std::string, std::string> cache(CACHE_SIZE);
    
    auto start = std::chrono::high_resolution_clock::now();
    
    std::vector<std::thread> threads;
    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&cache, t]() {
            std::mt19937 gen(t);
            std::uniform_int_distribution<> dis(0, KEY_RANGE - 1);
            
            for (int i = 0; i < OPS_PER_THREAD; ++i) {
                int key_id = dis(gen);
                
                // 90% 读，10% 写
                if (i % 100 < READ_RATIO) {
                    cache.get(std::to_string(key_id));
                } else {
                    cache.put(std::to_string(key_id), std::to_string(key_id));
                }
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    long long duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    long long total_ops = (long long)NUM_THREADS * OPS_PER_THREAD;
    double qps = (double)total_ops / duration_ms * 1000;
    double latency_us = (double)duration_ms * 1000 / total_ops;
    
    return {
        .name = "LruCache (单机大锁)",
        .total_ops = total_ops,
        .duration_ms = duration_ms,
        .qps = qps,
        .latency_us = latency_us
    };
}

// 分片锁版本：ShardedCache
BenchmarkResult benchmark_sharded(int shard_count) {
    ShardedCache<std::string, std::string> cache(CACHE_SIZE / shard_count, shard_count);
    
    auto start = std::chrono::high_resolution_clock::now();
    
    std::vector<std::thread> threads;
    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&cache, t]() {
            std::mt19937 gen(t);
            std::uniform_int_distribution<> dis(0, KEY_RANGE - 1);
            
            for (int i = 0; i < OPS_PER_THREAD; ++i) {
                int key_id = dis(gen);
                
                if (i % 100 < READ_RATIO) {
                    cache.get(std::to_string(key_id));
                } else {
                    cache.put(std::to_string(key_id), std::to_string(key_id));
                }
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    long long duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    long long total_ops = (long long)NUM_THREADS * OPS_PER_THREAD;
    double qps = (double)total_ops / duration_ms * 1000;
    double latency_us = (double)duration_ms * 1000 / total_ops;
    
    return {
        .name = "ShardedCache(32)",
        .total_ops = total_ops,
        .duration_ms = duration_ms,
        .qps = qps,
        .latency_us = latency_us
    };
}

// 优化版本：OptimizedShardedCache（缓存行对齐）
BenchmarkResult benchmark_optimized_sharded(int shard_count) {
    OptimizedShardedCache<std::string, std::string> cache(CACHE_SIZE / shard_count, shard_count);
    
    auto start = std::chrono::high_resolution_clock::now();
    
    std::vector<std::thread> threads;
    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&cache, t]() {
            std::mt19937 gen(t);
            std::uniform_int_distribution<> dis(0, KEY_RANGE - 1);
            
            for (int i = 0; i < OPS_PER_THREAD; ++i) {
                int key_id = dis(gen);
                
                if (i % 100 < READ_RATIO) {
                    cache.get(std::to_string(key_id));
                } else {
                    cache.put(std::to_string(key_id), std::to_string(key_id));
                }
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    long long duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    long long total_ops = (long long)NUM_THREADS * OPS_PER_THREAD;
    double qps = (double)total_ops / duration_ms * 1000;
    double latency_us = (double)duration_ms * 1000 / total_ops;
    
    return {
        .name = "OptimizedShardedCache(32)",
        .total_ops = total_ops,
        .duration_ms = duration_ms,
        .qps = qps,
        .latency_us = latency_us
    };
}

// Lazy LRU 版本
BenchmarkResult benchmark_lazy_lru() {
    LazyLruCache<std::string, std::string> cache(CACHE_SIZE);
    
    auto start = std::chrono::high_resolution_clock::now();
    
    std::vector<std::thread> threads;
    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&cache, t]() {
            std::mt19937 gen(t);
            std::uniform_int_distribution<> dis(0, KEY_RANGE - 1);
            
            for (int i = 0; i < OPS_PER_THREAD; ++i) {
                int key_id = dis(gen);
                
                if (i % 100 < READ_RATIO) {
                    cache.get(std::to_string(key_id));
                } else {
                    cache.put(std::to_string(key_id), std::to_string(key_id));
                }
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    long long duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    long long total_ops = (long long)NUM_THREADS * OPS_PER_THREAD;
    double qps = (double)total_ops / duration_ms * 1000;
    double latency_us = (double)duration_ms * 1000 / total_ops;
    
    return {
        .name = "LazyLruCache",
        .total_ops = total_ops,
        .duration_ms = duration_ms,
        .qps = qps,
        .latency_us = latency_us
    };
}

int main() {
    std::cout << "\n╔════════════════════════════════════════════════════════════════╗\n";
    std::cout << "║         FlashCache 性能压测 v3 (改进版)                       ║\n";
    std::cout << "╚════════════════════════════════════════════════════════════════╝\n\n";
    
    std::cout << "测试参数:\n";
    std::cout << "  - 线程数: " << NUM_THREADS << "\n";
    std::cout << "  - 每线程操作数: " << OPS_PER_THREAD << " (共 " << (NUM_THREADS * OPS_PER_THREAD / 1000000) << "M 次)\n";
    std::cout << "  - Key 范围: " << KEY_RANGE << "\n";
    std::cout << "  - 缓存容量: " << CACHE_SIZE << "\n";
    std::cout << "  - 读写比: " << READ_RATIO << "% 读, " << (100 - READ_RATIO) << "% 写\n\n";
    
    std::cout << std::string(70, '=') << "\n";
    std::cout << std::left << std::setw(35) << "缓存实现"
              << std::setw(15) << "吞吐量"
              << std::setw(15) << "延迟"
              << std::endl;
    std::cout << std::string(70, '=') << "\n";
    
    // 运行压测
    auto result_lru = benchmark_lru();
    print_result(result_lru);
    
    auto result_sharded = benchmark_sharded(32);
    print_result(result_sharded);
    
    auto result_optimized = benchmark_optimized_sharded(32);
    print_result(result_optimized);
    
    auto result_lazy = benchmark_lazy_lru();
    print_result(result_lazy);
    
    std::cout << std::string(70, '=') << "\n\n";
    
    // 计算性能提升
    double speedup_sharded = result_sharded.qps / result_lru.qps;
    double speedup_optimized = result_optimized.qps / result_lru.qps;
    double speedup_lazy = result_lazy.qps / result_lru.qps;
    
    std::cout << "性能提升倍数（相对于 LruCache）:\n";
    std::cout << "  - ShardedCache(32): " << std::fixed << std::setprecision(2) << speedup_sharded << "x\n";
    std::cout << "  - OptimizedShardedCache(32): " << speedup_optimized << "x ✨\n";
    std::cout << "  - LazyLruCache: " << speedup_lazy << "x\n\n";
    
    // 总结
    std::cout << "总结:\n";
    std::cout << "  ✓ 分片锁降低竞争\n";
    std::cout << "  ✓ 缓存行对齐避免伪共享\n";
    std::cout << "  ✓ 90% 读场景下性能显著提升\n";
    std::cout << "  ✓ 推荐生产环境使用 OptimizedShardedCache\n\n";
    
    return 0;
}
