#include <iostream>
#include <chrono>
#include <thread>
#include <vector>
#include <atomic>
#include <random>
#include "../db/lru_cache.h"
#include "../db/sharded_cache.h"
#include "../db/sharded_cache_v2.h"
#include "../db/lru_cache_lazy.h"

using namespace minkv::db;

const int NUM_THREADS = 16;
const int OPS_PER_THREAD = 100000;
const int KEY_RANGE = 10000;

struct BenchmarkResult {
    std::string name;
    long long total_ops;
    long long duration_ms;
    double qps;
};

BenchmarkResult benchmark_lru() {
    LruCache<std::string, std::string> cache(10000);
    
    auto start = std::chrono::high_resolution_clock::now();
    
    std::vector<std::thread> threads;
    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&cache, t]() {
            std::mt19937 gen(t);
            std::uniform_int_distribution<> dis(0, KEY_RANGE - 1);
            
            for (int i = 0; i < OPS_PER_THREAD; ++i) {
                int key_id = dis(gen);
                std::string key = "key_" + std::to_string(key_id);
                std::string value = "value_" + std::to_string(i);
                
                if (i % 10 < 7) {
                    cache.put(key, value);
                } else {
                    cache.get(key);
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
    
    return {
        .name = "LruCache (单机大锁)",
        .total_ops = total_ops,
        .duration_ms = duration_ms,
        .qps = qps
    };
}

BenchmarkResult benchmark_sharded(int shard_count) {
    ShardedCache<std::string, std::string> cache(10000 / shard_count, shard_count);
    
    auto start = std::chrono::high_resolution_clock::now();
    
    std::vector<std::thread> threads;
    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&cache, t]() {
            std::mt19937 gen(t);
            std::uniform_int_distribution<> dis(0, KEY_RANGE - 1);
            
            for (int i = 0; i < OPS_PER_THREAD; ++i) {
                int key_id = dis(gen);
                std::string key = "key_" + std::to_string(key_id);
                std::string value = "value_" + std::to_string(i);
                
                if (i % 10 < 7) {
                    cache.put(key, value);
                } else {
                    cache.get(key);
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
    
    return {
        .name = "ShardedCache(" + std::to_string(shard_count) + " shards)",
        .total_ops = total_ops,
        .duration_ms = duration_ms,
        .qps = qps
    };
}

BenchmarkResult benchmark_optimized_sharded(int shard_count) {
    OptimizedShardedCache<std::string, std::string> cache(10000 / shard_count, shard_count);
    
    auto start = std::chrono::high_resolution_clock::now();
    
    std::vector<std::thread> threads;
    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&cache, t]() {
            std::mt19937 gen(t);
            std::uniform_int_distribution<> dis(0, KEY_RANGE - 1);
            
            for (int i = 0; i < OPS_PER_THREAD; ++i) {
                int key_id = dis(gen);
                std::string key = "key_" + std::to_string(key_id);
                std::string value = "value_" + std::to_string(i);
                
                if (i % 10 < 7) {
                    cache.put(key, value);
                } else {
                    cache.get(key);
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
    
    return {
        .name = "OptimizedShardedCache(" + std::to_string(shard_count) + " shards, 缓存行对齐)",
        .total_ops = total_ops,
        .duration_ms = duration_ms,
        .qps = qps
    };
}

BenchmarkResult benchmark_lazy_lru() {
    LazyLruCache<std::string, std::string> cache(10000);
    
    auto start = std::chrono::high_resolution_clock::now();
    
    std::vector<std::thread> threads;
    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&cache, t]() {
            std::mt19937 gen(t);
            std::uniform_int_distribution<> dis(0, KEY_RANGE - 1);
            
            for (int i = 0; i < OPS_PER_THREAD; ++i) {
                int key_id = dis(gen);
                std::string key = "key_" + std::to_string(key_id);
                std::string value = "value_" + std::to_string(i);
                
                if (i % 10 < 7) {
                    cache.put(key, value);
                } else {
                    cache.get(key);
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
    
    return {
        .name = "LazyLruCache (懒惰 LRU)",
        .total_ops = total_ops,
        .duration_ms = duration_ms,
        .qps = qps
    };
}

int main() {
    std::cout << "========================================" << std::endl;
    std::cout << "FlashCache Benchmark v2 (优化版)" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "Threads: " << NUM_THREADS << std::endl;
    std::cout << "Ops per thread: " << OPS_PER_THREAD << std::endl;
    std::cout << "Total ops: " << NUM_THREADS * OPS_PER_THREAD << std::endl;
    std::cout << "Key range: " << KEY_RANGE << std::endl;
    std::cout << "========================================\n" << std::endl;

    std::cout << "[1/5] Testing LruCache..." << std::endl;
    auto result_lru = benchmark_lru();
    
    std::cout << "[2/5] Testing ShardedCache(32)..." << std::endl;
    auto result_sharded_32 = benchmark_sharded(32);
    
    std::cout << "[3/5] Testing OptimizedShardedCache(32)..." << std::endl;
    auto result_optimized_32 = benchmark_optimized_sharded(32);
    
    std::cout << "[4/5] Testing LazyLruCache..." << std::endl;
    auto result_lazy = benchmark_lazy_lru();
    
    std::cout << "[5/5] Testing OptimizedShardedCache(64)..." << std::endl;
    auto result_optimized_64 = benchmark_optimized_sharded(64);

    std::cout << "\n========================================" << std::endl;
    std::cout << "RESULTS" << std::endl;
    std::cout << "========================================\n" << std::endl;

    auto print_result = [](const BenchmarkResult& r) {
        std::cout << r.name << std::endl;
        std::cout << "  Duration: " << r.duration_ms << " ms" << std::endl;
        std::cout << "  QPS: " << (long long)r.qps << " ops/sec" << std::endl;
        std::cout << std::endl;
    };

    print_result(result_lru);
    print_result(result_sharded_32);
    print_result(result_optimized_32);
    print_result(result_lazy);
    print_result(result_optimized_64);

    std::cout << "========================================" << std::endl;
    std::cout << "PERFORMANCE IMPROVEMENT" << std::endl;
    std::cout << "========================================\n" << std::endl;

    double improvement_sharded = result_sharded_32.qps / result_lru.qps;
    double improvement_optimized = result_optimized_32.qps / result_lru.qps;
    double improvement_lazy = result_lazy.qps / result_lru.qps;
    double improvement_optimized_64 = result_optimized_64.qps / result_lru.qps;

    std::cout << "ShardedCache(32) vs LruCache: " << improvement_sharded << "x" << std::endl;
    std::cout << "OptimizedShardedCache(32) vs LruCache: " << improvement_optimized << "x" << std::endl;
    std::cout << "LazyLruCache vs LruCache: " << improvement_lazy << "x" << std::endl;
    std::cout << "OptimizedShardedCache(64) vs LruCache: " << improvement_optimized_64 << "x" << std::endl;

    return 0;
}
