#include "core/sharded_cache.h"
#include <chrono>
#include <thread>
#include <vector>
#include <iostream>
#include <atomic>
#include <random>
#include <iomanip>

/**
 * @brief 专用基准测试：缓存行对齐对伪共享的影响
 * 
 * 设计要点：
 * 1. 使用 uint64_t 作为 Key/Value，消除字符串开销。
 * 2. 极简哈希函数：key % shard_count。
 * 3. 高并发竞争：线程数 = 硬件并发数。
 * 4. 混合读写（50% 写，50% 读），最大化锁竞争。
 * 5. 同时测试对齐版本与不对齐版本。
 */

using namespace minkv::db;

// 配置参数
constexpr size_t SHARD_COUNT = 32;                // 分片数量
constexpr size_t CAPACITY_PER_SHARD = 1000;       // 每个分片容量
constexpr size_t TOTAL_OPERATIONS_PER_THREAD = 1'000'000;  // 每个线程操作数
constexpr int WRITE_PERCENTAGE = 50;              // 写操作百分比

// 线程安全的随机数生成器（每个线程独立）
thread_local std::mt19937 rng(std::random_device{}());

/**
 * @brief 运行单个缓存实例的基准测试
 * @tparam EnableCacheAlign 是否启用缓存行对齐
 * @param thread_count 线程数
 * @return 吞吐量（ops/sec）
 */
template<bool EnableCacheAlign>
double run_benchmark(int thread_count) {
    // 创建缓存实例
    ShardedCache<int, int, EnableCacheAlign> cache(CAPACITY_PER_SHARD, SHARD_COUNT);

    // 预热：预填充 50% 容量
    const size_t warmup_keys = CAPACITY_PER_SHARD * SHARD_COUNT / 2;
    for (int i = 0; i < warmup_keys; ++i) {
        cache.put(i, i * 10);
    }

    std::atomic<bool> start_flag{false};
    std::atomic<int> ready_count{0};
    std::vector<std::thread> threads;
    std::atomic<uint64_t> total_ops{0};

    auto worker = [&](int thread_id) {
        // 准备线程本地随机分布
        std::uniform_int_distribution<int> key_dist(0, warmup_keys * 2);  // 部分 key 在预热范围内，部分在范围外
        std::uniform_int_distribution<int> op_dist(0, 99);

        ready_count.fetch_add(1, std::memory_order_relaxed);
        while (!start_flag.load(std::memory_order_relaxed)) {
            std::this_thread::yield();
        }

        int local_ops = 0;
        for (size_t i = 0; i < TOTAL_OPERATIONS_PER_THREAD; ++i) {
            int key = key_dist(rng);
            if (op_dist(rng) < WRITE_PERCENTAGE) {
                // 写操作
                cache.put(key, key * 10 + thread_id);
            } else {
                // 读操作
                cache.get(key);
            }
            local_ops++;
        }
        total_ops.fetch_add(local_ops, std::memory_order_relaxed);
    };

    // 启动线程
    for (int t = 0; t < thread_count; ++t) {
        threads.emplace_back(worker, t);
    }

    // 等待所有线程准备就绪
    while (ready_count.load(std::memory_order_relaxed) < thread_count) {
        std::this_thread::yield();
    }

    // 开始计时
    auto start_time = std::chrono::high_resolution_clock::now();
    start_flag.store(true, std::memory_order_relaxed);

    // 等待线程完成
    for (auto& th : threads) {
        th.join();
    }
    auto end_time = std::chrono::high_resolution_clock::now();

    // 计算耗时
    auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    double duration_sec = static_cast<double>(duration_ns) / 1e9;
    double throughput = total_ops.load() / duration_sec;

    return throughput;
}

int main() {
    // 获取硬件并发数
    int hw_concurrency = std::thread::hardware_concurrency();
    if (hw_concurrency == 0) {
        hw_concurrency = 8;  // 默认值
    }
    std::cout << "=== 伪共享专项基准测试 ===\n";
    std::cout << "硬件并发数: " << hw_concurrency << " 线程\n";
    std::cout << "缓存行大小: 64 字节\n";
    std::cout << "分片数量: " << SHARD_COUNT << "\n";
    std::cout << "每分片容量: " << CAPACITY_PER_SHARD << "\n";
    std::cout << "每线程操作数: " << TOTAL_OPERATIONS_PER_THREAD << "\n";
    std::cout << "写操作比例: " << WRITE_PERCENTAGE << "%\n";
    std::cout << std::endl;

    // 运行不对齐版本
    std::cout << "运行普通版本（无对齐）..." << std::flush;
    double throughput_normal = run_benchmark<false>(hw_concurrency);
    std::cout << " 完成\n";

    // 运行对齐版本
    std::cout << "运行对齐版本（缓存行对齐）..." << std::flush;
    double throughput_aligned = run_benchmark<true>(hw_concurrency);
    std::cout << " 完成\n";

    // 输出结果
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "\n--- 测试结果 ---\n";
    std::cout << "普通版本（无对齐）:     " << throughput_normal / 1e6 << " M ops/sec\n";
    std::cout << "对齐版本（64字节对齐）: " << throughput_aligned / 1e6 << " M ops/sec\n";
    std::cout << "性能提升:               " << (throughput_aligned / throughput_normal) << "x\n";

    // 判断性能提升
    if (throughput_aligned > throughput_normal * 1.05) {
        std::cout << "\n✅ 缓存行对齐带来显著性能提升（>5%）\n";
        std::cout << "   伪共享是高并发场景下的主要瓶颈\n";
    } else if (throughput_aligned < throughput_normal * 0.95) {
        std::cout << "\n⚠️  缓存行对齐导致性能下降（>5%）\n";
        std::cout << "   内存填充可能增加了缓存缺失或降低了局部性\n";
    } else {
        std::cout << "\n📊 缓存行对齐效果不明显（±5%以内）\n";
        std::cout << "   伪共享不是当前负载的主要瓶颈\n";
    }

    return 0;
}