#include <iostream>
#include <iomanip>
#include <string>
#include <thread>
#include <vector>
#include <random>
#include "../../db/lru_cache.h"
#include "../../db/sharded_cache.h"

using namespace minkv::db;

// 打印统计信息
void print_stats(const std::string& name, const CacheStats& stats) {
    std::cout << "\n╔════════════════════════════════════════════════════════════════╗\n";
    std::cout << "║  " << std::left << std::setw(60) << name << "║\n";
    std::cout << "╠════════════════════════════════════════════════════════════════╣\n";
    
    std::cout << "║  基础指标                                                      ║\n";
    std::cout << "║    命中次数 (Hits):        " << std::setw(35) << stats.hits << "║\n";
    std::cout << "║    未命中次数 (Misses):    " << std::setw(35) << stats.misses << "║\n";
    std::cout << "║    过期次数 (Expired):     " << std::setw(35) << stats.expired << "║\n";
    std::cout << "║    淘汰次数 (Evictions):   " << std::setw(35) << stats.evictions << "║\n";
    std::cout << "║    插入次数 (Puts):        " << std::setw(35) << stats.puts << "║\n";
    std::cout << "║    删除次数 (Removes):     " << std::setw(35) << stats.removes << "║\n";
    
    std::cout << "╠────────────────────────────────────────────────────────────────╣\n";
    std::cout << "║  容量信息                                                      ║\n";
    std::cout << "║    当前大小:               " << std::setw(35) << stats.current_size << "║\n";
    std::cout << "║    总容量:                 " << std::setw(35) << stats.capacity << "║\n";
    
    std::cout << "╠────────────────────────────────────────────────────────────────╣\n";
    std::cout << "║  计算指标                                                      ║\n";
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "║    命中率 (Hit Rate):      " << std::setw(33) << (stats.hit_rate() * 100) << " %║\n";
    std::cout << "║    未命中率 (Miss Rate):   " << std::setw(33) << (stats.miss_rate() * 100) << " %║\n";
    std::cout << "║    容量使用率:             " << std::setw(33) << (stats.usage_rate() * 100) << " %║\n";
    
    std::cout << "╚════════════════════════════════════════════════════════════════╝\n";
}

// 测试 1：基础统计功能
void test_basic_stats() {
    std::cout << "\n========== 测试 1：基础统计功能 ==========\n";
    
    LruCache<std::string, std::string> cache(5);
    
    // 插入数据
    cache.put("key1", "value1");
    cache.put("key2", "value2");
    cache.put("key3", "value3");
    
    // 命中查询
    cache.get("key1");  // 命中
    cache.get("key2");  // 命中
    
    // 未命中查询
    cache.get("key_not_exist");  // 未命中
    cache.get("key_not_exist2"); // 未命中
    
    print_stats("LruCache 基础统计", cache.getStats());
}

// 测试 2：淘汰统计
void test_eviction_stats() {
    std::cout << "\n========== 测试 2：淘汰统计 ==========\n";
    
    LruCache<int, int> cache(3);  // 容量为 3
    
    // 插入 5 个数据，会触发 2 次淘汰
    cache.put(1, 100);
    cache.put(2, 200);
    cache.put(3, 300);
    cache.put(4, 400);  // 淘汰 key=1
    cache.put(5, 500);  // 淘汰 key=2
    
    // 查询
    cache.get(1);  // 未命中（已被淘汰）
    cache.get(2);  // 未命中（已被淘汰）
    cache.get(3);  // 命中
    cache.get(4);  // 命中
    cache.get(5);  // 命中
    
    print_stats("LruCache 淘汰统计", cache.getStats());
}

// 测试 3：TTL 过期统计
void test_ttl_stats() {
    std::cout << "\n========== 测试 3：TTL 过期统计 ==========\n";
    
    LruCache<std::string, std::string> cache(10);
    
    // 插入带 TTL 的数据
    cache.put("temp1", "value1", 100);  // 100ms 过期
    cache.put("temp2", "value2", 100);  // 100ms 过期
    cache.put("perm", "value3");        // 永不过期
    
    // 立即查询
    cache.get("temp1");  // 命中
    cache.get("perm");   // 命中
    
    // 等待过期
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    
    // 再次查询
    cache.get("temp1");  // 未命中（已过期）
    cache.get("temp2");  // 未命中（已过期）
    cache.get("perm");   // 命中
    
    print_stats("LruCache TTL 过期统计", cache.getStats());
}

// 测试 4：分片缓存统计
void test_sharded_stats() {
    std::cout << "\n========== 测试 4：分片缓存统计 ==========\n";
    
    ShardedCache<std::string, std::string> cache(100, 4);  // 4 个分片，每个容量 100
    
    // 插入 200 条数据
    for (int i = 0; i < 200; ++i) {
        cache.put("key_" + std::to_string(i), "value_" + std::to_string(i));
    }
    
    // 命中查询
    for (int i = 0; i < 100; ++i) {
        cache.get("key_" + std::to_string(i));
    }
    
    // 未命中查询
    for (int i = 1000; i < 1050; ++i) {
        cache.get("key_" + std::to_string(i));
    }
    
    print_stats("ShardedCache 统计（4 分片）", cache.getStats());
}

// 测试 5：高并发统计
void test_concurrent_stats() {
    std::cout << "\n========== 测试 5：高并发统计 ==========\n";
    
    ShardedCache<int, int> cache(1000, 16);  // 16 个分片，总容量 16000
    
    const int NUM_THREADS = 8;
    const int OPS_PER_THREAD = 10000;
    
    std::vector<std::thread> threads;
    
    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&cache, t]() {
            std::mt19937 gen(t);
            std::uniform_int_distribution<> dis(0, 5000);
            
            for (int i = 0; i < OPS_PER_THREAD; ++i) {
                int key = dis(gen);
                
                if (i % 10 < 7) {  // 70% 读
                    cache.get(key);
                } else {  // 30% 写
                    cache.put(key, key * 10);
                }
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    print_stats("ShardedCache 高并发统计（16 分片，8 线程）", cache.getStats());
}

// 测试 6：重置统计
void test_reset_stats() {
    std::cout << "\n========== 测试 6：重置统计 ==========\n";
    
    LruCache<int, int> cache(10);
    
    // 产生一些统计
    for (int i = 0; i < 20; ++i) {
        cache.put(i, i * 10);
    }
    for (int i = 0; i < 30; ++i) {
        cache.get(i);
    }
    
    std::cout << "重置前：\n";
    auto stats_before = cache.getStats();
    std::cout << "  命中: " << stats_before.hits << ", 未命中: " << stats_before.misses << "\n";
    
    // 重置统计
    cache.resetStats();
    
    std::cout << "重置后：\n";
    auto stats_after = cache.getStats();
    std::cout << "  命中: " << stats_after.hits << ", 未命中: " << stats_after.misses << "\n";
    std::cout << "  缓存大小: " << stats_after.current_size << " (数据未被清除)\n";
}

int main() {
    std::cout << "\n";
    std::cout << "╔════════════════════════════════════════════════════════════════╗\n";
    std::cout << "║          FlashCache 统计监控功能测试                          ║\n";
    std::cout << "╚════════════════════════════════════════════════════════════════╝\n";
    
    test_basic_stats();
    test_eviction_stats();
    test_ttl_stats();
    test_sharded_stats();
    test_concurrent_stats();
    test_reset_stats();
    
    std::cout << "\n";
    std::cout << "╔════════════════════════════════════════════════════════════════╗\n";
    std::cout << "║                    所有测试完成！                              ║\n";
    std::cout << "╚════════════════════════════════════════════════════════════════╝\n\n";
    
    return 0;
}
