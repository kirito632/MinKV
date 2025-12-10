#include <iostream>
#include <chrono>
#include <thread>
#include <vector>
#include <atomic>
#include <random>
#include <iomanip>
#include <string>
#include "../db/lru_cache.h"
#include "../db/sharded_cache.h"
#include "../db/sharded_cache_v2.h"
#include "../db/lru_cache_lazy.h"

using namespace minkv::db;

// ==========================================
// 终极压测参数
// ==========================================
const int NUM_THREADS = 16;
const int OPS_PER_THREAD = 200000;       // 每个线程 20 万次操作
const int TOTAL_OPS = NUM_THREADS * OPS_PER_THREAD; // 总操作 320 万次
const int KEY_RANGE = 100000;            // Key 范围 10 万
const int CACHE_SIZE = 50000;            // 缓存容量 5 万
const int READ_RATIO = 90;               // 90% 读

// 全局预生成的数据池
std::vector<std::string> g_keys;
std::string g_value;

// 准备数据函数
void prepare_data() {
    std::cout << "正在预生成测试数据 (Key数量: " << KEY_RANGE << ")..." << std::flush;
    g_keys.reserve(KEY_RANGE);
    for (int i = 0; i < KEY_RANGE; ++i) {
        // 使用较短的 Key，减少哈希计算开销，让锁竞争成为主要瓶颈
        g_keys.push_back("k" + std::to_string(i));
    }
    // 预生成一个固定的 Value，避免压测中构造
    g_value = std::string(100, 'X'); // 100 字节的 Value
    std::cout << " 完成！\n" << std::endl;
}

struct BenchmarkResult {
    std::string name;
    double qps;
    double latency_us;
};

void print_result(const BenchmarkResult& result) {
    std::cout << std::left << std::setw(35) << result.name
              << std::setw(15) << std::fixed << std::setprecision(2) << (result.qps / 1000000.0) << " M ops/s"
              << std::setw(15) << result.latency_us << " us"
              << std::endl;
}

// 通用的压测模板函数
template<typename CacheType>
BenchmarkResult run_benchmark(const std::string& name, CacheType& cache) {
    // 预热：先填充一些数据，避免一开始全是 Cache Miss
    // 这里简单填充一半容量
    for (int i = 0; i < CACHE_SIZE / 2; ++i) {
        cache.put(g_keys[i], g_value);
    }

    std::atomic<long long> total_duration_ms{0};
    
    auto start_global = std::chrono::high_resolution_clock::now();
    
    std::vector<std::thread> threads;
    threads.reserve(NUM_THREADS);

    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&cache, t]() {
            // 每个线程使用独立的随机数生成器，但访问同一个全局 Key 池
            std::mt19937 gen(t + 100); // 不同的种子
            std::uniform_int_distribution<> dis(0, KEY_RANGE - 1);
            std::uniform_int_distribution<> ratio_dis(0, 99);

            // 关键：在计时循环外引用全局数据
            const auto& keys = g_keys;
            const auto& val = g_value;

            for (int i = 0; i < OPS_PER_THREAD; ++i) {
                int key_idx = dis(gen);
                const std::string& key = keys[key_idx]; // 引用传递，无拷贝构造！

                if (ratio_dis(gen) < READ_RATIO) {
                    cache.get(key);
                } else {
                    cache.put(key, val);
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    auto end_global = std::chrono::high_resolution_clock::now();
    long long duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_global - start_global).count();
    
    double qps = (double)TOTAL_OPS / duration_ms * 1000.0;
    double latency = (double)duration_ms * 1000.0 / TOTAL_OPS;

    return {name, qps, latency};
}

int main() {
    std::cout << "\n╔════════════════════════════════════════════════════════════════╗\n";
    std::cout << "║      FlashCache 终极性能压测 (预生成数据 + 真实字符串)        ║\n";
    std::cout << "╚════════════════════════════════════════════════════════════════╝\n";
    
    // 1. 准备数据
    prepare_data();

    std::cout << "测试配置:\n";
    std::cout << "  - 线程数: " << NUM_THREADS << "\n";
    std::cout << "  - 总操作数: " << (TOTAL_OPS / 10000) << "万次\n";
    std::cout << "  - 读写比例: " << READ_RATIO << "% 读 / " << (100 - READ_RATIO) << "% 写\n";
    std::cout << "  - Key 类型: std::string (预生成，无 malloc 开销)\n";
    std::cout << "  - 缓存对齐: alignas(64)\n\n";

    std::cout << std::string(70, '=') << "\n";
    std::cout << std::left << std::setw(35) << "缓存实现"
              << std::setw(15) << "吞吐量"
              << std::setw(15) << "平均延迟"
              << std::endl;
    std::cout << std::string(70, '=') << "\n";

    // 2. 运行各版本压测

    // LruCache (单机大锁)
    {
        LruCache<std::string, std::string> cache(CACHE_SIZE);
        auto res = run_benchmark("LruCache (Mutex)", cache);
        print_result(res);
    }

    // ShardedCache (普通分片)
    {
        ShardedCache<std::string, std::string> cache(CACHE_SIZE / 32, 32);
        auto res = run_benchmark("ShardedCache (32)", cache);
        print_result(res);
    }

    // OptimizedShardedCache (对齐分片)
    BenchmarkResult res_optimized; 
    {
        OptimizedShardedCache<std::string, std::string> cache(CACHE_SIZE / 32, 32);
        res_optimized = run_benchmark("OptimizedSharded (Aligned)", cache);
        print_result(res_optimized);
    }

    std::cout << std::string(70, '=') << "\n\n";

    // 3. 结果分析
    std::cout << ">>> 性能洞察 <<<\n";
    std::cout << "通过预生成 Key，我们剥离了 std::string 构造的开销。\n";
    std::cout << "现在的性能瓶颈主要集中在：\n";
    std::cout << "1. 字符串哈希计算 (std::hash)\n";
    std::cout << "2. 锁竞争 (std::mutex)\n";
    std::cout << "3. 链表操作 (std::list)\n\n";
    
    std::cout << "如果 OptimizedSharded 达到 2.0M+ QPS，说明设计非常成功！\n";

    return 0;
}
