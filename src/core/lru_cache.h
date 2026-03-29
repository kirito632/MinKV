#pragma once

#include <list>
#include <unordered_map>
#include <shared_mutex>
#include <optional>
#include <chrono>
#include <iostream>
#include <atomic>
#include <thread>
#include <map>
#include <functional>

namespace minkv {
namespace db {

/**
 * @brief 缓存统计信息结构体
 * 
 * [监控体系] 记录缓存的命中、未命中、过期、淘汰等统计数据。
 * 用于监控缓存效果和调优缓存策略，这是生产环境必备的可观测性组件。
 * 
 * 统计维度：
 * - 基础指标：命中率、未命中率、过期率、淘汰率
 * - 时间指标：启动时间、最后访问时间、运行时长
 * - 峰值指标：历史最大容量、峰值QPS
 * - 容量指标：当前大小、总容量、使用率
 */
struct CacheStats {
    // ==================== 基础统计 ====================
    uint64_t hits = 0;           // 命中次数（get 成功返回数据）
    uint64_t misses = 0;         // 未命中次数（get 返回 nullopt）
    uint64_t expired = 0;        // 过期次数（因 TTL 过期而删除）
    uint64_t evictions = 0;      // 淘汰次数（因容量满而删除）
    uint64_t puts = 0;           // 插入次数（put 调用次数）
    uint64_t removes = 0;        // 删除次数（remove 成功次数）
    size_t current_size = 0;     // 当前缓存大小
    size_t capacity = 0;         // 缓存容量
    
    // ==================== 时间戳统计 ====================
    uint64_t start_time_ms = 0;       // 缓存启动时间（毫秒时间戳）
    uint64_t last_access_time_ms = 0; // 最后访问时间（毫秒时间戳）
    uint64_t last_hit_time_ms = 0;    // 最后命中时间（毫秒时间戳）
    uint64_t last_miss_time_ms = 0;   // 最后未命中时间（毫秒时间戳）
    
    // ==================== 峰值统计 ====================
    size_t peak_size = 0;             // 历史峰值大小
    uint64_t peak_qps = 0;            // 历史峰值 QPS（每秒查询数）
    
    // ==================== 便捷方法 ====================
    
    // 总查询次数
    uint64_t total_gets() const { return hits + misses; }
    
    // 总写入次数
    uint64_t total_puts() const { return puts; }
    
    // 总删除次数
    uint64_t total_removes() const { return removes; }
    
    // 计算命中率
    double hit_rate() const {
        uint64_t total = hits + misses;
        return total > 0 ? static_cast<double>(hits) / total : 0.0;
    }
    
    // 计算未命中率
    double miss_rate() const {
        uint64_t total = hits + misses;
        return total > 0 ? static_cast<double>(misses) / total : 0.0;
    }
    
    // 计算过期率（过期次数 / 未命中次数）
    double expiry_rate() const {
        return misses > 0 ? static_cast<double>(expired) / misses : 0.0;
    }
    
    // 计算容量使用率
    double usage_rate() const {
        return capacity > 0 ? static_cast<double>(current_size) / capacity : 0.0;
    }
    
    // 计算运行时长（秒）
    double uptime_seconds() const {
        if (start_time_ms == 0 || last_access_time_ms == 0) return 0.0;
        return static_cast<double>(last_access_time_ms - start_time_ms) / 1000.0;
    }
    
    // 计算平均 QPS
    double avg_qps() const {
        double uptime = uptime_seconds();
        return uptime > 0 ? static_cast<double>(hits + misses) / uptime : 0.0;
    }
};

/**
 * @brief 模板化的 LRU 缓存实现 (支持 TTL)
 * 
 * 核心逻辑：
 * 1. 使用 std::list 维护数据的访问顺序，头部是最新的，尾部是最旧的。
 * 2. 使用 std::unordered_map 维护 Key 到 List Iterator 的映射，实现 O(1) 查找。
 * 3. ThreadSafe=true 时内部加锁（独立使用场景）；
 *    ThreadSafe=false 时不加锁，由外层（EnhancedLruShard）统一管理锁，消除双重加锁开销。
 * 4. 支持 TTL (Time To Live)：每个 Key 可以设置过期时间，过期自动删除。
 * 
 * @tparam K 键类型（必须支持 std::hash 和 operator==）
 * @tparam V 值类型
 * @tparam ThreadSafe 是否启用内部锁，默认 true；被 ShardedCache 包装时设为 false
 */
template<typename K, typename V, bool ThreadSafe = true>
class LruCache {
public:
    // 构造函数，指定缓存容量
    explicit LruCache(size_t capacity);

    // 禁止拷贝和赋值
    LruCache(const LruCache&) = delete;
    LruCache& operator=(const LruCache&) = delete;

    /**
     * @brief 获取数据
     * 如果 key 存在且未过期，将该节点移动到链表头部（标记为最近使用），并返回 value。
     * 如果 key 过期，自动删除并返回 nullopt。
     * @return 如果存在且未过期返回 value，否则返回 std::nullopt
     */
    std::optional<V> get(const K& key);

    /**
     * @brief 插入或更新数据
     * 1. 如果 key 存在：更新 value，移动到头部。
     * 2. 如果 key 不存在：
     *    - 如果缓存满了：删除链表尾部（最久未使用）的数据，新数据插入头部。
     *    - 如果未满：直接插入头部。
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

    // 获取当前缓存大小
    size_t size() const;
    
    // 获取缓存容量
    size_t capacity() const { return capacity_; }

    /**
     * @brief 获取缓存统计信息
     * 
     * 返回当前缓存的统计数据，包括命中率、未命中率、过期率等。
     * 用于监控和调优缓存性能。
     */
    CacheStats getStats() const;
    
    /**
     * @brief 重置统计信息
     * 
     * 清除所有统计计数器，但不影响缓存数据。
     */
    void resetStats();
    
    /**
     * @brief 清空所有缓存数据
     */
    void clear();

    /**
     * @brief 启动后台过期键清理线程
     * 
     * 定期扫描缓存中的过期键并删除，防止内存泄漏。
     * 清理间隔可通过 cleanup_interval_ms 参数设置。
     * 
     * @param cleanup_interval_ms 清理间隔（毫秒），默认 1000ms
     */
    void start_cleanup_thread(int64_t cleanup_interval_ms = 1000);

    /**
     * @brief 停止后台清理线程
     * 
     * 调用此方法会等待清理线程优雅退出。
     * 通常在缓存销毁前调用。
     */
    void stop_cleanup_thread();

    /**
     * @brief 手动触发一次过期键清理
     * 
     * 扫描所有键，删除已过期的键。
     * 返回本次清理删除的键数量。
     */
    size_t cleanup_expired_keys();

    /**
     * @brief 获取所有有效的键值对（用于向量搜索等场景）
     * 
     * 返回所有未过期的键值对。
     * 注意：这个操作会获取读锁，性能较好。
     * 
     * @return 包含所有有效键值对的 map
     */
    std::map<K, V> get_all() const;

private:
    size_t capacity_; // 最大容量
    
    // 双向链表：存储实际的 Key-Value 对
    struct Node {
        K key;              // 键
        V value;            // 值
        int64_t expiry_time_ms;  // 过期时间戳（毫秒），0 表示永不过期
    };
    std::list<Node> cache_list_;  // 真正存数据的地方

    // 哈希表：Key -> 链表迭代器
    using ListIterator = typename std::list<Node>::iterator;
    std::unordered_map<K, ListIterator> map_;  // 导航 (索引)

    // 互斥锁（ThreadSafe=false 时为空结构体，零开销）
    struct NullMutex {
        void lock() {}
        void unlock() {}
        bool try_lock() { return true; }
    };
    using MutexType = std::conditional_t<ThreadSafe, std::shared_mutex, NullMutex>;
    mutable MutexType mutex_;
    
    // ==================== 基础统计计数器 ====================
    mutable std::atomic<uint64_t> stats_hits_{0};
    mutable std::atomic<uint64_t> stats_misses_{0};
    mutable std::atomic<uint64_t> stats_expired_{0};
    mutable std::atomic<uint64_t> stats_evictions_{0};
    mutable std::atomic<uint64_t> stats_puts_{0};
    mutable std::atomic<uint64_t> stats_removes_{0};
    
    // Lazy LRU: 上次提升节点的时间
    mutable std::atomic<uint64_t> last_promote_time_ms_{0};

    // ==================== 时间戳统计 ====================
    uint64_t start_time_ms_{0};                          // 缓存启动时间
    mutable std::atomic<uint64_t> last_access_time_ms_{0};  // 最后访问时间
    mutable std::atomic<uint64_t> last_hit_time_ms_{0};     // 最后命中时间
    mutable std::atomic<uint64_t> last_miss_time_ms_{0};    // 最后未命中时间
    
    // ==================== 峰值统计 ====================
    mutable std::atomic<size_t> peak_size_{0};           // 历史峰值大小

    // ==================== 后台清理线程 ====================
    std::thread cleanup_thread_;                         // 后台清理线程
    mutable std::atomic<bool> cleanup_running_{false};   // 清理线程是否运行中
    int64_t cleanup_interval_ms_{1000};                  // 清理间隔（毫秒）
    
    // 辅助函数：检查节点是否过期
    bool is_expired(const Node& node) const;

    // 辅助函数：获取当前时间戳（毫秒）
    static int64_t current_time_ms();
    
    // 辅助函数：更新峰值大小
    void update_peak_size() const;

    // 辅助函数：后台清理线程的主函数
    void cleanup_thread_main();
};

// ============ 模板实现 ============

template<typename K, typename V, bool ThreadSafe>
int64_t LruCache<K, V, ThreadSafe>::current_time_ms() {
    auto now = std::chrono::high_resolution_clock::now();
    auto duration = now.time_since_epoch();
    return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
}

template<typename K, typename V, bool ThreadSafe>
bool LruCache<K, V, ThreadSafe>::is_expired(const Node& node) const {
    if (node.expiry_time_ms == 0) {
        return false;
    }
    return current_time_ms() > node.expiry_time_ms;
}

template<typename K, typename V, bool ThreadSafe>
LruCache<K, V, ThreadSafe>::LruCache(size_t capacity) 
    : capacity_(capacity)
    , start_time_ms_(static_cast<uint64_t>(current_time_ms())) {}

template<typename K, typename V, bool ThreadSafe>
std::optional<V> LruCache<K, V, ThreadSafe>::get(const K& key) {
    uint64_t now = static_cast<uint64_t>(current_time_ms());
    last_access_time_ms_.store(now, std::memory_order_relaxed);

    // ThreadSafe=true 时走双路径（读锁快路径 + 写锁慢路径）
    // ThreadSafe=false 时外层 Shard 已持锁，直接走单路径
    if constexpr (ThreadSafe) {
        // 1. 快速路径 (Fast Path)：只加读锁
        {
            std::shared_lock<std::shared_mutex> lock(mutex_);
            auto it = map_.find(key);
            if (it == map_.end()) {
                ++stats_misses_;
                last_miss_time_ms_.store(now, std::memory_order_relaxed);
                return std::nullopt;
            }
            if (!is_expired(*it->second)) {
                uint64_t last = last_promote_time_ms_.load(std::memory_order_relaxed);
                if (now >= last && (now - last) <= 1000) {
                    ++stats_hits_;
                    last_hit_time_ms_.store(now, std::memory_order_relaxed);
                    return it->second->value;
                }
            }
        }
        // 2. 慢速路径 (Slow Path)：加写锁
        std::lock_guard<std::shared_mutex> lock(mutex_);
        auto it = map_.find(key);
        if (it == map_.end()) {
            ++stats_misses_;
            return std::nullopt;
        }
        if (is_expired(*it->second)) {
            auto list_it = it->second;
            map_.erase(it);
            cache_list_.erase(list_it);
            ++stats_expired_;
            ++stats_misses_;
            return std::nullopt;
        }
        uint64_t last = last_promote_time_ms_.load(std::memory_order_relaxed);
        if (now >= last && (now - last) > 1000) {
            cache_list_.splice(cache_list_.begin(), cache_list_, it->second);
            last_promote_time_ms_.store(now, std::memory_order_relaxed);
        }
        ++stats_hits_;
        last_hit_time_ms_.store(now, std::memory_order_relaxed);
        return it->second->value;
    } else {
        // ThreadSafe=false：无锁单路径，外层 Shard 已持锁
        auto it = map_.find(key);
        if (it == map_.end()) {
            ++stats_misses_;
            last_miss_time_ms_.store(now, std::memory_order_relaxed);
            return std::nullopt;
        }
        if (is_expired(*it->second)) {
            auto list_it = it->second;
            map_.erase(it);
            cache_list_.erase(list_it);
            ++stats_expired_;
            ++stats_misses_;
            return std::nullopt;
        }
        uint64_t last = last_promote_time_ms_.load(std::memory_order_relaxed);
        if (now >= last && (now - last) > 1000) {
            cache_list_.splice(cache_list_.begin(), cache_list_, it->second);
            last_promote_time_ms_.store(now, std::memory_order_relaxed);
        }
        ++stats_hits_;
        last_hit_time_ms_.store(now, std::memory_order_relaxed);
        return it->second->value;
    }
}

template<typename K, typename V, bool ThreadSafe>
void LruCache<K, V, ThreadSafe>::put(const K& key, const V& value, int64_t ttl_ms) {
    std::lock_guard<MutexType> lock(mutex_);

    int64_t expiry_time = 0;
    if (ttl_ms > 0) {
        expiry_time = current_time_ms() + ttl_ms;
    }

    auto it = map_.find(key);
    if (it != map_.end()) {
        it->second->value = value;
        it->second->expiry_time_ms = expiry_time;
        cache_list_.splice(cache_list_.begin(), cache_list_, it->second);
        ++stats_puts_;
        update_peak_size();
        return;
    }

    if (map_.size() >= capacity_) {
        auto last = cache_list_.end();
        --last;
        K key_to_remove = last->key;
        cache_list_.push_front({key, value, expiry_time});
        try {
            map_[key] = cache_list_.begin();
            map_.erase(key_to_remove);
            cache_list_.pop_back();
            ++stats_evictions_;
        } catch (...) {
            cache_list_.pop_front();
            throw;
        }
    } else {
        cache_list_.push_front({key, value, expiry_time});
        try {
            map_[key] = cache_list_.begin();
        } catch (...) {
            cache_list_.pop_front();
            throw;
        }
    }
    
    ++stats_puts_;
    update_peak_size();
}

template<typename K, typename V, bool ThreadSafe>
bool LruCache<K, V, ThreadSafe>::remove(const K& key) {
    std::lock_guard<MutexType> lock(mutex_);

    auto it = map_.find(key);
    if (it == map_.end()) {
        return false;
    }

    cache_list_.erase(it->second);
    map_.erase(it);
    ++stats_removes_;
    return true;
}

template<typename K, typename V, bool ThreadSafe>
size_t LruCache<K, V, ThreadSafe>::size() const {
    std::lock_guard<MutexType> lock(mutex_);
    return map_.size();
}

template<typename K, typename V, bool ThreadSafe>
CacheStats LruCache<K, V, ThreadSafe>::getStats() const {
    std::lock_guard<MutexType> lock(mutex_);
    CacheStats stats;
    stats.hits = stats_hits_.load(std::memory_order_relaxed);
    stats.misses = stats_misses_.load(std::memory_order_relaxed);
    stats.expired = stats_expired_.load(std::memory_order_relaxed);
    stats.evictions = stats_evictions_.load(std::memory_order_relaxed);
    stats.puts = stats_puts_.load(std::memory_order_relaxed);
    stats.removes = stats_removes_.load(std::memory_order_relaxed);
    stats.current_size = map_.size();
    stats.capacity = capacity_;
    stats.start_time_ms = start_time_ms_;
    stats.last_access_time_ms = last_access_time_ms_.load(std::memory_order_relaxed);
    stats.last_hit_time_ms = last_hit_time_ms_.load(std::memory_order_relaxed);
    stats.last_miss_time_ms = last_miss_time_ms_.load(std::memory_order_relaxed);
    stats.peak_size = peak_size_.load(std::memory_order_relaxed);
    return stats;
}

template<typename K, typename V, bool ThreadSafe>
void LruCache<K, V, ThreadSafe>::resetStats() {
    std::lock_guard<MutexType> lock(mutex_);
    stats_hits_.store(0, std::memory_order_relaxed);
    stats_misses_.store(0, std::memory_order_relaxed);
    stats_expired_.store(0, std::memory_order_relaxed);
    stats_evictions_.store(0, std::memory_order_relaxed);
    stats_puts_.store(0, std::memory_order_relaxed);
    stats_removes_.store(0, std::memory_order_relaxed);
    start_time_ms_ = static_cast<uint64_t>(current_time_ms());
    last_access_time_ms_.store(0, std::memory_order_relaxed);
    last_hit_time_ms_.store(0, std::memory_order_relaxed);
    last_miss_time_ms_.store(0, std::memory_order_relaxed);
    peak_size_.store(0, std::memory_order_relaxed);
}

template<typename K, typename V, bool ThreadSafe>
void LruCache<K, V, ThreadSafe>::update_peak_size() const {
    size_t current = map_.size();
    size_t peak = peak_size_.load(std::memory_order_relaxed);
    while (current > peak) {
        if (peak_size_.compare_exchange_weak(peak, current, std::memory_order_relaxed)) {
            break;
        }
    }
}

template<typename K, typename V, bool ThreadSafe>
void LruCache<K, V, ThreadSafe>::clear() {
    std::lock_guard<MutexType> lock(mutex_);
    cache_list_.clear();
    map_.clear();
}

template<typename K, typename V, bool ThreadSafe>
size_t LruCache<K, V, ThreadSafe>::cleanup_expired_keys() {
    std::lock_guard<MutexType> lock(mutex_);
    size_t removed_count = 0;
    auto it = cache_list_.begin();
    while (it != cache_list_.end()) {
        if (is_expired(*it)) {
            map_.erase(it->key);
            it = cache_list_.erase(it);
            ++removed_count;
            ++stats_expired_;
        } else {
            ++it;
        }
    }
    return removed_count;
}

template<typename K, typename V, bool ThreadSafe>
void LruCache<K, V, ThreadSafe>::cleanup_thread_main() {
    while (cleanup_running_.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(cleanup_interval_ms_));
        if (!cleanup_running_.load(std::memory_order_relaxed)) {
            break;
        }
        cleanup_expired_keys();
    }
}

template<typename K, typename V, bool ThreadSafe>
void LruCache<K, V, ThreadSafe>::start_cleanup_thread(int64_t cleanup_interval_ms) {
    if (cleanup_running_.load(std::memory_order_relaxed)) {
        return;
    }
    cleanup_interval_ms_ = cleanup_interval_ms;
    cleanup_running_.store(true, std::memory_order_relaxed);
    cleanup_thread_ = std::thread([this]() {
        this->cleanup_thread_main();
    });
}

template<typename K, typename V, bool ThreadSafe>
void LruCache<K, V, ThreadSafe>::stop_cleanup_thread() {
    cleanup_running_.store(false, std::memory_order_relaxed);
    if (cleanup_thread_.joinable()) {
        cleanup_thread_.join();
    }
}

template<typename K, typename V, bool ThreadSafe>
std::map<K, V> LruCache<K, V, ThreadSafe>::get_all() const {
    std::lock_guard<MutexType> lock(mutex_);
    std::map<K, V> result;
    for (const auto& node : cache_list_) {
        if (!is_expired(node)) {
            result[node.key] = node.value;
        }
    }
    return result;
}

} // namespace db
} // namespace minkv

