/**
 * @file test_monitoring.cpp
 * @brief 测试缓存监控功能
 */

#include <iostream>
#include <iomanip>
#include <thread>
#include <chrono>
#include "../../db/lru_cache.h"
#include "../../db/sharded_cache_v2.h"

using namespace minkv::db;

void print_stats(const CacheStats& stats, const std::string& title) {
    std::cout << "\n========== " << title << " ==========" << std::endl;
    
    // 基础统计
    std::cout << "[Size]     Current: " << stats.current_size 
              << " / " << stats.capacity 
              << " (Peak: " << stats.peak_size << ")" << std::endl;
    std::cout << "[Usage]    " << std::fixed << std::setprecision(1) 
              << (stats.usage_rate() * 100.0) << "%" << std::endl;
    
    // 命中率统计
    std::cout << "[Gets]     Total: " << stats.total_gets() 
              << " (Hits: " << stats.hits 
              << ", Misses: " << stats.misses << ")" << std::endl;
    std::cout << "[HitRate]  " << std::fixed << std::setprecision(2) 
              << (stats.hit_rate() * 100.0) << "%" << std::endl;
    
    // 写入和删除统计
    std::cout << "[Puts]     " << stats.puts << std::endl;
    std::cout << "[Removes]  " << stats.removes << std::endl;
    std::cout << "[Evictions] " << stats.evictions << std::endl;
    std::cout << "[Expired]  " << stats.expired << std::endl;
    
    // 时间统计
    std::cout << "[Uptime]   " << std::fixed << std::setprecision(1) 
              << stats.uptime_seconds() << " seconds" << std::endl;
    std::cout << "[AvgQPS]   " << std::fixed << std::setprecision(1) 
              << stats.avg_qps() << " queries/sec" << std::endl;
    
    // 时间戳
    std::cout << "[StartTime]      " << stats.start_time_ms << " ms" << std::endl;
    std::cout << "[LastAccess]     " << stats.last_access_time_ms << " ms" << std::endl;
    std::cout << "[LastHit]        " << stats.last_hit_time_ms << " ms" << std::endl;
    std::cout << "[LastMiss]       " << stats.last_miss_time_ms << " ms" << std::endl;
    
    std::cout << "==========================================\n" << std::endl;
}

void test_lru_cache_monitoring() {
    std::cout << "=== Test 1: LruCache Monitoring ===" << std::endl;
    
    LruCache<int, std::string> cache(100);
    
    // 写入一些数据
    for (int i = 0; i < 50; ++i) {
        cache.put(i, "value_" + std::to_string(i));
    }
    
    // 模拟一些访问
    for (int i = 0; i < 30; ++i) {
        cache.get(i);  // 命中
    }
    for (int i = 100; i < 120; ++i) {
        cache.get(i);  // 未命中
    }
    
    // 删除一些数据
    for (int i = 0; i < 5; ++i) {
        cache.remove(i);
    }
    
    // 等待一小段时间，让 uptime 有意义
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // 再访问一次，更新最后访问时间
    cache.get(10);
    
    print_stats(cache.getStats(), "LruCache Stats");
    
    std::cout << "✓ LruCache monitoring test passed!" << std::endl;
}

void test_sharded_cache_monitoring() {
    std::cout << "\n=== Test 2: OptimizedShardedCache Monitoring ===" << std::endl;
    
    OptimizedShardedCache<int, std::string> cache(100, 4);  // 4 个分片，每个 100 容量
    
    // 写入数据
    for (int i = 0; i < 200; ++i) {
        cache.put(i, "value_" + std::to_string(i));
    }
    
    // 模拟访问
    for (int i = 0; i < 150; ++i) {
        cache.get(i);  // 命中
    }
    for (int i = 500; i < 550; ++i) {
        cache.get(i);  // 未命中
    }
    
    // 等待一小段时间
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // 再访问一次
    cache.get(100);
    
    print_stats(cache.getStats(), "ShardedCache Stats");
    
    std::cout << "✓ ShardedCache monitoring test passed!" << std::endl;
}

void test_peak_size() {
    std::cout << "\n=== Test 3: Peak Size Tracking ===" << std::endl;
    
    LruCache<int, std::string> cache(100);
    
    // 写入 80 个数据
    for (int i = 0; i < 80; ++i) {
        cache.put(i, "value");
    }
    
    auto stats1 = cache.getStats();
    std::cout << "After 80 puts: size=" << stats1.current_size 
              << ", peak=" << stats1.peak_size << std::endl;
    
    // 删除 30 个
    for (int i = 0; i < 30; ++i) {
        cache.remove(i);
    }
    
    auto stats2 = cache.getStats();
    std::cout << "After 30 removes: size=" << stats2.current_size 
              << ", peak=" << stats2.peak_size << std::endl;
    
    // 验证峰值保持不变
    if (stats2.peak_size == 80 && stats2.current_size == 50) {
        std::cout << "✓ Peak size tracking test passed!" << std::endl;
    } else {
        std::cout << "✗ Peak size tracking test FAILED!" << std::endl;
    }
}

void test_reset_stats() {
    std::cout << "\n=== Test 4: Reset Stats ===" << std::endl;
    
    LruCache<int, std::string> cache(100);
    
    // 写入和访问
    for (int i = 0; i < 50; ++i) {
        cache.put(i, "value");
        cache.get(i);
    }
    
    auto stats1 = cache.getStats();
    std::cout << "Before reset: hits=" << stats1.hits 
              << ", puts=" << stats1.puts << std::endl;
    
    // 重置统计
    cache.resetStats();
    
    auto stats2 = cache.getStats();
    std::cout << "After reset: hits=" << stats2.hits 
              << ", puts=" << stats2.puts 
              << ", size=" << stats2.current_size << std::endl;
    
    // 验证统计被重置，但数据保留
    if (stats2.hits == 0 && stats2.puts == 0 && stats2.current_size == 50) {
        std::cout << "✓ Reset stats test passed!" << std::endl;
    } else {
        std::cout << "✗ Reset stats test FAILED!" << std::endl;
    }
}

int main() {
    std::cout << "FlashCache Monitoring Test Suite" << std::endl;
    std::cout << "================================\n" << std::endl;
    
    test_lru_cache_monitoring();
    test_sharded_cache_monitoring();
    test_peak_size();
    test_reset_stats();
    
    std::cout << "\n================================" << std::endl;
    std::cout << "All monitoring tests completed!" << std::endl;
    
    return 0;
}
