#pragma once

#include "lru_cache.h"
#include <vector>
#include <memory>
#include <functional>

namespace minkv {
namespace db {

/**
 * @brief 模板化的分片缓存 (Sharded Cache)
 * 
 * 核心思想：将缓存分为多个分片，每个分片是一个独立的 LruCache。
 * 根据 Key 的哈希值决定去哪个分片，这样可以大幅降低锁竞争。
 * 
 * 性能优势：
 * - 单机大锁：1000 个线程竞争 1 把锁，吞吐量受限。
 * - 分片锁（64 个分片）：1000 个线程分散到 64 把锁，吞吐量提升 ~64 倍。
 * 
 * 参考：Java ConcurrentHashMap 的设计思想。
 * 
 * @tparam K 键类型（必须支持 std::hash 和 operator==）
 * @tparam V 值类型
 */
template<typename K, typename V>
class ShardedCache {
public:
    /**
     * @brief 构造函数
     * 
     * @param capacity_per_shard 每个分片的容量
     * @param shard_count 分片数量（默认 16，建议 16/32/64）
     */
    ShardedCache(size_t capacity_per_shard, size_t shard_count = 16);

    // 禁止拷贝和赋值
    ShardedCache(const ShardedCache&) = delete;
    ShardedCache& operator=(const ShardedCache&) = delete;

    /**
     * @brief 获取数据
     * 
     * @param key 键
     * @return 如果存在且未过期返回 value，否则返回 std::nullopt
     */
    std::optional<V> get(const K& key);

    /**
     * @brief 插入或更新数据
     * 
     * @param key 键
     * @param value 值
     * @param ttl_ms 过期时间（毫秒）。0 表示永不过期。
     */
    void put(const K& key, const V& value, int64_t ttl_ms = 0);

    /**
     * @brief 删除数据
     */
    bool remove(const K& key);

    /**
     * @brief 获取所有分片的总大小
     */
    size_t size() const;

    /**
     * @brief 获取分片数量
     */
    size_t shard_count() const { return shards_.size(); }

    /**
     * @brief 获取缓存统计信息
     * 
     * 聚合所有分片的统计数据，返回总体的缓存统计。
     */
    CacheStats getStats() const;
    
    /**
     * @brief 重置统计信息
     * 
     * 清除所有分片的统计计数器。
     */
    void resetStats();
    
    /**
     * @brief 获取总容量
     */
    size_t capacity() const;

    /**
     * @brief 全量清空缓存
     * 
     * 遍历所有分片并清空它们。慎用！
     */
    void clear();

private:
    // 分片数组（使用 unique_ptr 因为 LruCache 禁止拷贝）
    std::vector<std::unique_ptr<LruCache<K, V>>> shards_;

    /**
     * @brief 根据 Key 计算分片索引
     * 
     * 使用 std::hash 计算 Key 的哈希值，然后模分片数量。
     * 这样可以保证同一个 Key 总是去同一个分片。
     */
    size_t get_shard_index(const K& key) const;
};

// ============ 模板实现 ============

template<typename K, typename V>
ShardedCache<K, V>::ShardedCache(size_t capacity_per_shard, size_t shard_count) {
    for (size_t i = 0; i < shard_count; ++i) {
        shards_.push_back(std::make_unique<LruCache<K, V>>(capacity_per_shard));
    }
}

template<typename K, typename V>
size_t ShardedCache<K, V>::get_shard_index(const K& key) const {
    std::hash<K> hasher;
    size_t hash_value = hasher(key);
    return hash_value % shards_.size();
}

template<typename K, typename V>
std::optional<V> ShardedCache<K, V>::get(const K& key) {
    size_t shard_idx = get_shard_index(key);
    return shards_[shard_idx]->get(key);
}

template<typename K, typename V>
void ShardedCache<K, V>::put(const K& key, const V& value, int64_t ttl_ms) {
    size_t shard_idx = get_shard_index(key);
    shards_[shard_idx]->put(key, value, ttl_ms);
}

template<typename K, typename V>
bool ShardedCache<K, V>::remove(const K& key) {
    size_t shard_idx = get_shard_index(key);
    return shards_[shard_idx]->remove(key);
}

template<typename K, typename V>
size_t ShardedCache<K, V>::size() const {
    size_t total = 0;
    for (const auto& shard : shards_) {
        total += shard->size();
    }
    return total;
}

template<typename K, typename V>
size_t ShardedCache<K, V>::capacity() const {
    size_t total = 0;
    for (const auto& shard : shards_) {
        total += shard->capacity();
    }
    return total;
}

template<typename K, typename V>
CacheStats ShardedCache<K, V>::getStats() const {
    CacheStats total_stats;
    for (const auto& shard : shards_) {
        CacheStats shard_stats = shard->getStats();
        total_stats.hits += shard_stats.hits;
        total_stats.misses += shard_stats.misses;
        total_stats.expired += shard_stats.expired;
        total_stats.evictions += shard_stats.evictions;
        total_stats.puts += shard_stats.puts;
        total_stats.removes += shard_stats.removes;
        total_stats.current_size += shard_stats.current_size;
        total_stats.capacity += shard_stats.capacity;
    }
    return total_stats;
}

template<typename K, typename V>
void ShardedCache<K, V>::resetStats() {
    for (auto& shard : shards_) {
        shard->resetStats();
    }
}

template<typename K, typename V>
void ShardedCache<K, V>::clear() {
    for (auto& shard : shards_) {
        shard->clear();
    }
}

} // namespace db
} // namespace minkv
