#include "../db/sharded_cache.h"
#include "../db/wal.h"
#include <iostream>
#include <thread>
#include <chrono>

/**
 * 示例：如何在 MinKV 中使用 WAL 持久化
 * 
 * 场景：
 * 1. 创建缓存并启用持久化
 * 2. 插入数据
 * 3. 模拟宕机（清空内存）
 * 4. 从磁盘恢复数据
 */

int main() {
    using Cache = minkv::db::ShardedCache<std::string, std::string>;
    
    std::cout << "========== MinKV WAL 持久化示例 ==========" << std::endl;
    
    // ========== 第一阶段：正常运行 ==========
    std::cout << "\n【第一阶段】正常运行，写入数据" << std::endl;
    
    // 创建缓存
    Cache cache(10000, 32);  // 每个分片 10000，共 32 个分片
    
    // 启用 WAL 持久化
    // cache.enable_persistence("./data");
    // cache.start_cleanup_thread(1000);  // 启动后台清理线程
    
    // 插入数据
    std::cout << "插入 1000 条数据..." << std::endl;
    for (int i = 0; i < 1000; ++i) {
        std::string key = "user:" + std::to_string(i);
        std::string value = "data_" + std::to_string(i);
        cache.put(key, value, 0);  // TTL = 0，永不过期
    }
    
    // 查询几条数据
    std::cout << "查询数据：" << std::endl;
    for (int i = 0; i < 5; ++i) {
        std::string key = "user:" + std::to_string(i);
        auto value = cache.get(key);
        if (value) {
            std::cout << "  " << key << " -> " << *value << std::endl;
        }
    }
    
    // 获取统计信息
    auto stats = cache.getStats();
    std::cout << "\n缓存统计：" << std::endl;
    std::cout << "  当前大小: " << stats.current_size << std::endl;
    std::cout << "  容量: " << stats.capacity << std::endl;
    std::cout << "  命中率: " << stats.hit_rate() * 100 << "%" << std::endl;
    
    // 创建快照
    std::cout << "\n创建快照..." << std::endl;
    // cache.create_snapshot();
    
    // ========== 第二阶段：模拟宕机 ==========
    std::cout << "\n【第二阶段】模拟宕机（清空内存）" << std::endl;
    std::cout << "清空缓存..." << std::endl;
    cache.clear();
    
    auto stats_after_clear = cache.getStats();
    std::cout << "清空后的缓存大小: " << stats_after_clear.current_size << std::endl;
    
    // ========== 第三阶段：恢复数据 ==========
    std::cout << "\n【第三阶段】从磁盘恢复数据" << std::endl;
    
    // 创建新的缓存实例（模拟重启）
    Cache cache_recovered(10000, 32);
    
    // 从磁盘恢复
    std::cout << "从磁盘恢复数据..." << std::endl;
    // cache_recovered.recover_from_disk("./data");
    
    auto stats_recovered = cache_recovered.getStats();
    std::cout << "恢复后的缓存大小: " << stats_recovered.current_size << std::endl;
    
    // 验证恢复的数据
    std::cout << "\n验证恢复的数据：" << std::endl;
    int recovered_count = 0;
    for (int i = 0; i < 1000; ++i) {
        std::string key = "user:" + std::to_string(i);
        auto value = cache_recovered.get(key);
        if (value) {
            recovered_count++;
            if (i < 5) {
                std::cout << "  " << key << " -> " << *value << std::endl;
            }
        }
    }
    std::cout << "恢复成功: " << recovered_count << " / 1000" << std::endl;
    
    // ========== 性能测试 ==========
    std::cout << "\n【性能测试】对比有无 WAL 的性能" << std::endl;
    
    // 测试 1：无 WAL 的性能
    std::cout << "\n测试 1：无 WAL 的性能" << std::endl;
    Cache cache_no_wal(10000, 32);
    
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < 100000; ++i) {
        std::string key = "key:" + std::to_string(i);
        std::string value = "value_" + std::to_string(i);
        cache_no_wal.put(key, value, 0);
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto duration_no_wal = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    
    std::cout << "  插入 100,000 条数据耗时: " << duration_no_wal << " ms" << std::endl;
    std::cout << "  吞吐量: " << (100000.0 / duration_no_wal * 1000) << " ops/sec" << std::endl;
    
    // 测试 2：有 WAL 的性能（异步）
    std::cout << "\n测试 2：有 WAL 的性能（异步）" << std::endl;
    Cache cache_with_wal(10000, 32);
    // cache_with_wal.enable_persistence("./data2");
    // cache_with_wal.start_cleanup_thread(1000);
    
    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < 100000; ++i) {
        std::string key = "key:" + std::to_string(i);
        std::string value = "value_" + std::to_string(i);
        cache_with_wal.put(key, value, 0);
    }
    end = std::chrono::high_resolution_clock::now();
    auto duration_with_wal = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    
    std::cout << "  插入 100,000 条数据耗时: " << duration_with_wal << " ms" << std::endl;
    std::cout << "  吞吐量: " << (100000.0 / duration_with_wal * 1000) << " ops/sec" << std::endl;
    
    // 性能对比
    std::cout << "\n性能对比：" << std::endl;
    std::cout << "  无 WAL: " << duration_no_wal << " ms" << std::endl;
    std::cout << "  有 WAL: " << duration_with_wal << " ms" << std::endl;
    double overhead = (duration_with_wal - duration_no_wal) / static_cast<double>(duration_no_wal) * 100;
    std::cout << "  开销: " << overhead << "%" << std::endl;
    
    std::cout << "\n========== 示例完成 ==========" << std::endl;
    
    return 0;
}
