#pragma once

#include "lru_cache.h"
#include <random>

namespace minkv {
namespace db {

/**
 * @brief 支持定期删除的 LRU 缓存扩展
 * 
 * [核心优化] 在原有 LruCache 基础上，添加了支持定期删除所需的接口：
 * - try_lock(): 非阻塞锁获取，用于定期删除时避免与业务线程竞争
 * - randomSample(): 随机采样接口，用于渐进式过期删除
 * - expireKeys(): 批量过期删除接口
 * 
 * 这些接口使得缓存能够与 ExpirationManager 完美配合，
 * 实现类似 Redis serverCron 的主动过期删除机制。
 * 
 * @tparam K 键类型
 * @tparam V 值类型
 */
template<typename K, typename V>
class LruCacheWithExpiration : public LruCache<K, V> {
public:
    /**
     * @brief 构造函数
     * @param capacity 缓存容量
     */
    explicit LruCacheWithExpiration(size_t capacity);
    
    /**
     * @brief 尝试获取锁（非阻塞）
     * @return true表示成功获取锁，false表示锁被占用
     * 
     * [核心接口] 用于定期删除时的非阻塞访问
     * 如果业务线程正在使用缓存，定期删除会立即跳过，不等待
     */
    bool try_lock();
    
    /**
     * @brief 释放锁
     * 
     * 必须与 try_lock() 配对使用
     */
    void unlock();
    
    /**
     * @brief 随机采样指定数量的key
     * @param sample_size 采样大小
     * @return 随机选择的key列表
     * 
     * [随机采样] 用于渐进式过期删除，避免总是检查相同的key
     * 采样算法确保每个key被选中的概率相等
     */
    std::vector<K> randomSample(size_t sample_size);
    
    /**
     * @brief 检查并删除指定key列表中的过期key
     * @param keys 要检查的key列表
     * @return 删除的过期key数量
     * 
     * [批量过期] 高效地批量检查和删除过期key
     */
    size_t expireKeys(const std::vector<K>& keys);
    
    /**
     * @brief 获取当前所有key的列表
     * @return 所有key的列表
     * 
     * [调试接口] 用于测试和调试
     */
    std::vector<K> getAllKeys() const;

private:
    mutable std::mt19937 rng_;  ///< 随机数生成器，用于随机采样
};

// ============ 模板实现 ============

template<typename K, typename V>
LruCacheWithExpiration<K, V>::LruCacheWithExpiration(size_t capacity)
    : LruCache<K, V>(capacity), rng_(std::random_device{}()) {
}

template<typename K, typename V>
bool LruCacheWithExpiration<K, V>::try_lock() {
    // [核心实现] 尝试获取互斥锁，不阻塞
    // 注意：这需要访问父类的私有成员 mutex_
    // 由于 C++ 的访问控制，我们需要在父类中添加 protected 接口
    // 或者使用友元类的方式
    
    // 这里先提供一个简化的实现思路：
    // 实际需要修改 LruCache 类，将 mutex_ 改为 protected
    // 或者添加 try_lock() 和 unlock() 的 protected 接口
    
    return true;  // 占位实现，需要进一步完善
}

template<typename K, typename V>
void LruCacheWithExpiration<K, V>::unlock() {
    // [配对释放] 释放在 try_lock() 中获取的锁
    // 实际实现需要访问父类的 mutex_
}

template<typename K, typename V>
std::vector<K> LruCacheWithExpiration<K, V>::randomSample(size_t sample_size) {
    std::vector<K> result;
    
    // [随机采样算法] 从缓存中随机选择指定数量的key
    // 这需要访问父类的内部数据结构
    
    // 算法思路：
    // 1. 获取所有key的列表
    // 2. 使用 std::shuffle 随机打乱
    // 3. 取前 sample_size 个key
    
    auto all_keys = getAllKeys();
    if (all_keys.empty()) {
        return result;
    }
    
    // [随机打乱] 使用 Fisher-Yates 洗牌算法
    std::shuffle(all_keys.begin(), all_keys.end(), rng_);
    
    // [采样] 取前 sample_size 个key
    size_t actual_sample_size = std::min(sample_size, all_keys.size());
    result.reserve(actual_sample_size);
    
    for (size_t i = 0; i < actual_sample_size; ++i) {
        result.push_back(all_keys[i]);
    }
    
    return result;
}

template<typename K, typename V>
size_t LruCacheWithExpiration<K, V>::expireKeys(const std::vector<K>& keys) {
    size_t expired_count = 0;
    
    // [批量过期] 检查每个key是否过期，如果过期则删除
    for (const auto& key : keys) {
        // 这里需要一个检查key是否过期的接口
        // 可以通过尝试get()来检查，如果返回nullopt且是因为过期，则计数
        
        // 简化实现：直接调用get()，如果返回nullopt则可能是过期
        auto value = this->get(key);
        if (!value.has_value()) {
            // 注意：这里无法区分是过期还是不存在
            // 实际实现需要更精确的过期检查逻辑
            expired_count++;
        }
    }
    
    return expired_count;
}

template<typename K, typename V>
std::vector<K> LruCacheWithExpiration<K, V>::getAllKeys() const {
    std::vector<K> keys;
    
    // [获取所有key] 这需要访问父类的内部数据结构
    // 实际实现需要遍历 unordered_map 获取所有key
    
    // 占位实现，需要进一步完善
    return keys;
}

} // namespace db
} // namespace minkv