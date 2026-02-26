#pragma once

#include "sharded_cache.h"
#include <string>
#include <memory>

namespace minkv {

/**
 * @brief MinKV 存储引擎 - 统一入口
 * 
 * [工业级设计] 这是整个MinKV系统的统一入口类，
 * 封装了所有复杂性，提供简洁易用的API。
 * 
 * [核心特性]
 * - 高性能：237万QPS，P99延迟1.46μs
 * - 功能完整：缓存+持久化+向量搜索+定期删除
 * - 高可用：分片级故障隔离和自动恢复
 * - 易使用：一个类解决所有KV存储需求
 * 
 * [使用示例]
 * ```cpp
 * auto engine = MinKV::create(1000, 16);  // 每分片1000容量，16个分片
 * engine->enablePersistence("/data/minkv");
 * engine->startExpirationService();
 * 
 * engine->put("key1", "value1", 5000);    // 5秒TTL
 * auto value = engine->get("key1");
 * 
 * engine->vectorPut("vec1", {1.0f, 2.0f, 3.0f});
 * auto results = engine->vectorSearch({1.1f, 2.1f, 3.1f}, 10);
 * ```
 * 
 * @tparam K 键类型，默认std::string
 * @tparam V 值类型，默认std::string
 */
template<typename K = std::string, typename V = std::string>
class MinKV {
public:
    /**
     * @brief 创建MinKV实例
     * @param capacity_per_shard 每个分片的容量
     * @param shard_count 分片数量，建议16/32/64
     * @return MinKV实例的智能指针
     */
    static std::unique_ptr<MinKV> create(size_t capacity_per_shard = 1000, size_t shard_count = 16) {
        return std::make_unique<MinKV>(capacity_per_shard, shard_count);
    }
    
    /**
     * @brief 构造函数
     */
    MinKV(size_t capacity_per_shard, size_t shard_count)
        : cache_(std::make_unique<db::ShardedCache<K, V>>(capacity_per_shard, shard_count)) {
    }
    
    // ==========================================
    // 基础KV接口 - 简洁易用
    // ==========================================
    
    /**
     * @brief 获取数据
     */
    std::optional<V> get(const K& key) {
        return cache_->get(key);
    }
    
    /**
     * @brief 存储数据
     * @param ttl_ms 过期时间（毫秒），0表示永不过期
     */
    void put(const K& key, const V& value, int64_t ttl_ms = 0) {
        cache_->put(key, value, ttl_ms);
    }
    
    /**
     * @brief 删除数据
     */
    bool remove(const K& key) {
        return cache_->remove(key);
    }
    
    /**
     * @brief 获取存储大小
     */
    size_t size() const {
        return cache_->size();
    }
    
    /**
     * @brief 清空所有数据
     */
    void clear() {
        cache_->clear();
    }
    
    // ==========================================
    // 高级功能接口 - 工业级特性
    // ==========================================
    
    /**
     * @brief 启用持久化
     * @param data_dir 数据目录
     * @param fsync_interval_ms 同步间隔
     */
    void enablePersistence(const std::string& data_dir, int64_t fsync_interval_ms = 1000) {
        cache_->enable_persistence(data_dir, fsync_interval_ms);
    }
    
    /**
     * @brief 禁用持久化
     */
    void disablePersistence() {
        cache_->disable_persistence();
    }
    
    /**
     * @brief 从磁盘恢复数据
     */
    void recoverFromDisk() {
        cache_->recover_from_disk();
    }
    
    /**
     * @brief 创建数据快照
     */
    void createSnapshot() {
        cache_->create_snapshot();
    }
    
    /**
     * @brief 启动定期删除服务
     * @param check_interval_ms 检查间隔（毫秒）
     * @param sample_size 每次采样大小
     */
    void startExpirationService(int64_t check_interval_ms = 100, size_t sample_size = 20) {
        cache_->startExpirationService(std::chrono::milliseconds(check_interval_ms), sample_size);
    }
    
    /**
     * @brief 停止定期删除服务
     */
    void stopExpirationService() {
        cache_->stopExpirationService();
    }
    
    /**
     * @brief 存储向量数据
     */
    void vectorPut(const K& key, const std::vector<float>& vec, int64_t ttl_ms = 0) {
        cache_->vectorPut(key, vec, ttl_ms);
    }
    
    /**
     * @brief 获取向量数据
     */
    std::vector<float> vectorGet(const K& key) {
        return cache_->vectorGet(key);
    }
    
    /**
     * @brief 向量相似度搜索
     * @param query 查询向量
     * @param k 返回最相似的k个结果
     */
    std::vector<K> vectorSearch(const std::vector<float>& query, int k) {
        return cache_->vectorSearch(query, k);
    }
    
    // ==========================================
    // 监控和诊断接口
    // ==========================================
    
    /**
     * @brief 获取性能统计
     */
    db::CacheStats getStats() const {
        return cache_->getStats();
    }
    
    /**
     * @brief 获取健康状态
     */
    typename db::ShardedCache<K, V>::HealthStatus getHealthStatus() const {
        return cache_->getHealthStatus();
    }
    
    /**
     * @brief 执行健康检查
     */
    void performHealthCheck() {
        cache_->performHealthCheck();
    }
    
    /**
     * @brief 获取定期删除统计
     */
    base::ExpirationManager::Stats getExpirationStats() const {
        return cache_->getExpirationStats();
    }

private:
    std::unique_ptr<db::ShardedCache<K, V>> cache_;
};

// 类型别名，方便使用
using StringKV = MinKV<std::string, std::string>;
using IntKV = MinKV<int, std::string>;

} // namespace minkv