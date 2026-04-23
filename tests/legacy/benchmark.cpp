#include <iostream>
#include <chrono>
#include <thread>
#include <vector>
#include <atomic>
#include <random>
#include "../db/lru_cache.h"
#include "../db/sharded_cache.h"

using namespace minkv::db;

// 压测参数
const int NUM_THREADS = 16;           // 线程数
const int OPS_PER_THREAD = 100000;    // 每个线程的操作数
const int KEY_RANGE = 10000;          // Key 的范围（模拟热点数据）

// 统计信息
struct BenchmarkResult {
    std::string name;
    long long total_ops;
    long long duration_ms;
    double qps;
};

// 单机大锁版本的压测
BenchmarkResult benchmark_single_lock() {
    LruCache<std::string, std::string> cache(10000); // 容量 10000
    
    auto start = std::chrono::high_resolution_clock::now();
    
    // 创建线程
    std::vector<std::thread> threads;
    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&cache, t]() {
            std::mt19937 gen(t); // 每个线程用不同的随机种子
            std::uniform_int_distribution<> dis(0, KEY_RANGE - 1);
            
            for (int i = 0; i < OPS_PER_THREAD; ++i) {
                int key_id = dis(gen);
                std::string key = "key_" + std::to_string(key_id);
                std::string value = "value_" + std::to_string(i);
                
                // 混合操作：70% put, 30% get
                if (i % 10 < 7) {
                    cache.put(key, value);
                } else {
                    cache.get(key);
                }
            }
        });
    }
    
    // 等待所有线程完成
    for (auto& t : threads) {
        t.join();
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    long long duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    long long total_ops = (long long)NUM_THREADS * OPS_PER_THREAD;
    double qps = (double)total_ops / duration_ms * 1000;
    
    return {
        .name = "Single Lock (LruCache)",
        .total_ops = total_ops,
        .duration_ms = duration_ms,
        .qps = qps
    };
}

// 分片锁版本的压测
BenchmarkResult benchmark_sharded_lock(int shard_count) {
    ShardedCache<std::string, std::string> cache(10000 / shard_count, shard_count); // 总容量 10000，分散到多个分片
    
    auto start = std::chrono::high_resolution_clock::now();
    
    // 创建线程
    std::vector<std::thread> threads;
    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&cache, t]() {
            std::mt19937 gen(t);
            std::uniform_int_distribution<> dis(0, KEY_RANGE - 1);
            
            for (int i = 0; i < OPS_PER_THREAD; ++i) {
                int key_id = dis(gen);
                std::string key = "key_" + std::to_string(key_id);
                std::string value = "value_" + std::to_string(i);
                
                // 混合操作：70% put, 30% get
                if (i % 10 < 7) {
                    cache.put(key, value);
                } else {
                    cache.get(key);
                }
            }
        });
    }
    
    // 等待所有线程完成
    for (auto& t : threads) {
        t.join();
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    long long duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    long long total_ops = (long long)NUM_THREADS * OPS_PER_THREAD;
    double qps = (double)total_ops / duration_ms * 1000;
    
    return {
        .name = "Sharded Lock (" + std::to_string(shard_count) + " shards)",
        .total_ops = total_ops,
        .duration_ms = duration_ms,
        .qps = qps
    };
}

int main() {
    std::cout << "========================================" << std::endl;
    std::cout << "FlashCache Benchmark" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "Threads: " << NUM_THREADS << std::endl;
    std::cout << "Ops per thread: " << OPS_PER_THREAD << std::endl;
    std::cout << "Total ops: " << NUM_THREADS * OPS_PER_THREAD << std::endl;
    std::cout << "Key range: " << KEY_RANGE << std::endl;
    std::cout << "========================================\n" << std::endl;

    // 运行压测
    std::cout << "[1/4] Testing Single Lock (LruCache)..." << std::endl;
    auto result_single = benchmark_single_lock();
    
    std::cout << "[2/4] Testing Sharded Lock (16 shards)..." << std::endl;
    auto result_sharded_16 = benchmark_sharded_lock(16);
    
    std::cout << "[3/4] Testing Sharded Lock (32 shards)..." << std::endl;
    auto result_sharded_32 = benchmark_sharded_lock(32);
    
    std::cout << "[4/4] Testing Sharded Lock (64 shards)..." << std::endl;
    auto result_sharded_64 = benchmark_sharded_lock(64);

    // 打印结果
    std::cout << "\n========================================" << std::endl;
    std::cout << "RESULTS" << std::endl;
    std::cout << "========================================\n" << std::endl;

    auto print_result = [](const BenchmarkResult& r) {
        std::cout << r.name << std::endl;
        std::cout << "  Total Ops: " << r.total_ops << std::endl;
        std::cout << "  Duration: " << r.duration_ms << " ms" << std::endl;
        std::cout << "  QPS: " << (long long)r.qps << " ops/sec" << std::endl;
        std::cout << std::endl;
    };

    print_result(result_single);
    print_result(result_sharded_16);
    print_result(result_sharded_32);
    print_result(result_sharded_64);

    // 计算性能提升倍数
    std::cout << "========================================" << std::endl;
    std::cout << "PERFORMANCE IMPROVEMENT" << std::endl;
    std::cout << "========================================\n" << std::endl;

    double improvement_16 = result_sharded_16.qps / result_single.qps;
    double improvement_32 = result_sharded_32.qps / result_single.qps;
    double improvement_64 = result_sharded_64.qps / result_single.qps;

    std::cout << "Sharded (16) vs Single: " << improvement_16 << "x faster" << std::endl;
    std::cout << "Sharded (32) vs Single: " << improvement_32 << "x faster" << std::endl;
    std::cout << "Sharded (64) vs Single: " << improvement_64 << "x faster" << std::endl;

    return 0;
}
