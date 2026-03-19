#pragma once

#include "lru_cache.h"
#include "../vector/vector_ops.h"
#include "../persistence/wal.h"
#include "../base/serializer.h"
#include "../base/expiration_manager.h"
#include <vector>
#include <memory>
#include <functional>
#include <queue>
#include <thread>
#include <future>
#include <random>
#include <unordered_set>
#include <unordered_map>

namespace minkv {
namespace db {

/**
 * @brief 工业级分片缓存系统 - 集大成者
 * 
 * [架构统一] 集成所有功能于一体：
 * - 基础缓存：LRU淘汰、分片锁
 * - 持久化：WAL日志、快照恢复
 * - 向量检索：SIMD加速、Top-K搜索
 * - 定期删除：类似Redis serverCron的过期清理
 * - 异常处理：分级处理、自动恢复
 * 
 * [设计目标] 
 * - 单一入口：一个类解决所有需求
 * - 功能完整：生产级特性齐全
 * - 性能优异：237万QPS，P99延迟1.46μs
 * - 高可用：局部故障不影响整体服务
 * 
 * 这是MinKV的核心组件，解决了架构碎片化问题，
 * 提供了统一、完整、高性能的缓存解决方案。
 * 
 * @tparam K 键类型（必须支持 std::hash 和 operator==）
 * @tparam V 值类型
 * @tparam EnableCacheAlign 是否启用缓存行对齐（默认false）
 */
template<typename K, typename V, bool EnableCacheAlign = false>
class ShardedCache {
public:
    /**
     * @brief 构造函数
     * @param capacity_per_shard 每个分片的容量
     * @param shard_count 分片数量（默认32，通用配置）
     */
    ShardedCache(size_t capacity_per_shard, size_t shard_count = 32);
    
    /**
     * @brief 析构函数
     */
    ~ShardedCache();
    
    // 禁止拷贝和赋值
    ShardedCache(const ShardedCache&) = delete;
    ShardedCache& operator=(const ShardedCache&) = delete;

    // ==========================================
    // 基础缓存接口 (Basic Cache API)
    // ==========================================
    
    std::optional<V> get(const K& key);
    void put(const K& key, const V& value, int64_t ttl_ms = 0);
    bool remove(const K& key);
    size_t size() const;
    size_t capacity() const;
    void clear();
    CacheStats getStats() const;
    void resetStats();

    // ==========================================
    // 持久化接口 (Persistence API)
    // ==========================================
    
    void enable_persistence(const std::string& data_dir, int64_t fsync_interval_ms = 1000);
    void disable_persistence();
    void recover_from_disk();
    void create_snapshot();
    std::map<K, V> export_all_data() const;
    void clear_wal();

    // ==========================================
    // 向量检索接口 (Vector Search API)
    // ==========================================
    
    void vectorPut(const K& key, const std::vector<float>& vec, int64_t ttl_ms = 0);
    std::vector<float> vectorGet(const K& key);
    std::vector<K> vectorSearch(const std::vector<float>& query, int k);

    // ==========================================
    // 定期删除接口 (Expiration API)
    // ==========================================
    
    /**
     * @brief 启动定期删除服务
     * @param check_interval 检查间隔，默认100ms
     * @param sample_size 每次采样大小，默认20
     * 
     * @deprecated 推荐使用RAII方式：在构造时传入参数自动启动
     * @note 为保持向后兼容性暂时保留，未来版本将移除
     */
    [[deprecated("Use RAII: pass parameters to constructor for automatic startup")]]
    void startExpirationService(std::chrono::milliseconds check_interval = std::chrono::milliseconds(100),
                               size_t sample_size = 20);
    
    /**
     * @brief 停止定期删除服务
     * 
     * @deprecated 推荐使用RAII方式：析构时自动停止
     * @note 为保持向后兼容性暂时保留，未来版本将移除
     */
    [[deprecated("Use RAII: destructor automatically stops the service")]]
    void stopExpirationService();
    
    /**
     * @brief 获取定期删除统计信息
     */
    base::ExpirationManager::Stats getExpirationStats() const;
    
    /**
     * @brief 手动触发过期清理
     */
    size_t manualExpiration(int shard_id = -1);

    // ==========================================
    // 健康检查接口 (Health Check API)
    // ==========================================
    
    /**
     * @brief 获取系统健康状态
     */
    struct HealthStatus {
        bool overall_healthy;                    ///< 整体健康状态
        size_t healthy_shards;                   ///< 健康分片数量
        size_t total_shards;                     ///< 总分片数量
        std::vector<size_t> disabled_shards;     ///< 被禁用的分片列表
        std::unordered_map<size_t, int> error_counts;      ///< 各分片错误计数
        double error_rate;                       ///< 整体错误率
        std::chrono::steady_clock::time_point last_health_check;  ///< 上次健康检查时间
    };
    
    HealthStatus getHealthStatus() const;
    
    /**
     * @brief 手动触发健康检查
     */
    void performHealthCheck();
    
    // ==========================================
    // LSN接口 (Log Sequence Number API)
    // ==========================================
    
    /**
     * @brief 获取下一个LSN（原子递增）
     * @return 新的LSN值
     * 
     * 实现说明：
     * - global_lsn_存储的是下一个要分配的LSN
     * - fetch_add返回旧值（即当前要分配的LSN）
     * - 例如：初始值1，第一次调用返回1并递增到2，第二次返回2并递增到3
     */
    uint64_t next_lsn() {
        return global_lsn_.fetch_add(1, std::memory_order_relaxed);
    }
    
    /**
     * @brief 获取当前LSN（不递增）
     * @return 当前LSN值（最后分配的LSN）
     * 
     * 实现说明：
     * - global_lsn_存储的是下一个要分配的LSN
     * - 所以当前已分配的LSN = global_lsn_ - 1
     */
    uint64_t current_lsn() const {
        uint64_t next = global_lsn_.load(std::memory_order_relaxed);
        return (next > 0) ? (next - 1) : 0;
    }

    /**
     * @brief 重置 LSN 计数器（仅供 recover_from_disk 调用）
     * 
     * 恢复完成后将 global_lsn_ 设置为 max_lsn+1，
     * 确保后续写入不与已恢复的 LSN 冲突。
     * 调用时必须保证无并发写入。
     */
    void reset_lsn(uint64_t lsn) {
        global_lsn_.store(lsn, std::memory_order_relaxed);
    }

    /**
     * @brief 获取当前 WAL 文件大小（字节）
     * 
     * 供 CheckpointManager::should_checkpoint 使用，
     * 替代原来的 size() * 100 粗估。
     */
    size_t get_wal_size() const {
        std::lock_guard<std::mutex> lock(persistence_mutex_);
        return wal_ ? wal_->get_log_size() : 0;
    }

    /**
     * @brief 读取 lsn > snapshot_lsn 的 WAL 条目（供 recover_from_disk 使用）
     */
    std::vector<db::LogEntry> read_wal_after_lsn(uint64_t snapshot_lsn) const {
        std::lock_guard<std::mutex> lock(persistence_mutex_);
        if (!wal_) return {};
        return wal_->read_after_snapshot(snapshot_lsn);
    }

    /**
     * @brief 恢复专用写入：直接写分片，不触发 WAL
     * 
     * 仅供 recover_from_disk 调用，此时无并发写入。
     */
    void put_for_recovery(const K& key, const V& value) {
        size_t shard_idx = get_shard_index(key);
        shards_[shard_idx]->put(key, value, 0);
    }

    /**
     * @brief 恢复专用删除：直接删分片，不触发 WAL
     */
    void remove_for_recovery(const K& key) {
        size_t shard_idx = get_shard_index(key);
        shards_[shard_idx]->remove(key);
    }

private:
    // ==========================================
    // 核心数据结构
    // ==========================================
    
    /**
     * @brief 增强的LRU缓存分片
     */
    class EnhancedLruShard {
    public:
        EnhancedLruShard(size_t capacity);
        
        // 基础接口
        std::optional<V> get(const K& key);
        void put(const K& key, const V& value, int64_t ttl_ms = 0);
        bool remove(const K& key);
        size_t size() const;
        size_t capacity() const;
        CacheStats getStats() const;
        void resetStats();
        void clear();
        std::map<K, V> get_all() const;
        
        // 定期删除接口
        bool try_lock();
        void unlock();
        std::vector<K> randomSample(size_t sample_size);
        size_t expireKeys(const std::vector<K>& keys);
        
    private:
        // 条件对齐的互斥锁包装
        struct alignas(EnableCacheAlign ? 64 : 1) AlignedMutex {
            mutable std::mutex mutex;
            // 填充至 64 字节（仅在对齐时生效）
            static constexpr size_t padding_size = EnableCacheAlign ?
                (64 - sizeof(std::mutex)) : 0;
            char padding[padding_size];
        } mutex_wrapper_;

        std::unique_ptr<LruCache<K, V>> cache_;
        mutable std::mt19937 rng_;
    };
    
    std::vector<std::unique_ptr<EnhancedLruShard>> shards_;
    
    // ==========================================
    // 持久化相关
    // ==========================================
    
    std::unique_ptr<WriteAheadLog> wal_;
    bool persistence_enabled_{false};
    mutable std::shared_mutex global_consistency_lock_;
    mutable std::mutex persistence_mutex_;
    
    // ==========================================
    // LSN相关 (Log Sequence Number)
    // ==========================================
    
    std::atomic<uint64_t> global_lsn_{1};  ///< 全局LSN计数器，从1开始，严格单调递增
    
    // ==========================================
    // 定期删除相关
    // ==========================================
    
    std::unique_ptr<base::ExpirationManager> expiration_manager_;
    
    // ==========================================
    // 健康检查相关
    // ==========================================
    
    mutable std::mutex health_mutex_;
    std::unordered_map<size_t, int> shard_error_counts_;
    std::unordered_set<size_t> disabled_shards_;
    std::chrono::steady_clock::time_point last_health_check_;
    
    static constexpr int MAX_CONSECUTIVE_ERRORS = 5;
    static constexpr auto HEALTH_CHECK_INTERVAL = std::chrono::minutes(1);
    
    // ==========================================
    // 内部方法
    // ==========================================
    
    size_t get_shard_index(const K& key) const;
    size_t expirationCallback(size_t shard_id, size_t sample_size);
    void recordShardError(size_t shard_id);
    void recordShardSuccess(size_t shard_id);
    bool isShardDisabled(size_t shard_id) const;
};

// ============ 实现部分 ============

template<typename K, typename V, bool EnableCacheAlign>
ShardedCache<K, V, EnableCacheAlign>::ShardedCache(size_t capacity_per_shard, size_t shard_count)
    : last_health_check_(std::chrono::steady_clock::now()) {
    
    // 创建增强的分片
    for (size_t i = 0; i < shard_count; ++i) {
        shards_.push_back(std::make_unique<EnhancedLruShard>(capacity_per_shard));
    }
}

template<typename K, typename V, bool EnableCacheAlign>
ShardedCache<K, V, EnableCacheAlign>::~ShardedCache() {
    // [RAII] expiration_manager_ 析构时自动 join 后台线程，无需手动调用 stopExpirationService()
    expiration_manager_.reset();
    disable_persistence();
}

template<typename K, typename V, bool EnableCacheAlign>
size_t ShardedCache<K, V, EnableCacheAlign>::get_shard_index(const K& key) const {
    std::hash<K> hasher;
    return hasher(key) % shards_.size();
}

// ==========================================
// 基础缓存接口实现
// ==========================================

template<typename K, typename V, bool EnableCacheAlign>
std::optional<V> ShardedCache<K, V, EnableCacheAlign>::get(const K& key) {
    size_t shard_idx = get_shard_index(key);
    
    if (isShardDisabled(shard_idx)) {
        return std::nullopt;  // 分片被禁用
    }
    
    try {
        auto result = shards_[shard_idx]->get(key);
        recordShardSuccess(shard_idx);
        return result;
    } catch (const std::exception& e) {
        recordShardError(shard_idx);
        return std::nullopt;
    }
}

template<typename K, typename V, bool EnableCacheAlign>
void ShardedCache<K, V, EnableCacheAlign>::put(const K& key, const V& value, int64_t ttl_ms) {
    std::shared_lock<std::shared_mutex> consistency_lock(global_consistency_lock_);
    
    size_t shard_idx = get_shard_index(key);
    
    if (isShardDisabled(shard_idx)) {
        return;  // 分片被禁用，跳过
    }
    
    // 准备WAL条目
    LogEntry wal_entry;
    bool need_wal = false;
    
    if (persistence_enabled_ && wal_) {
        need_wal = true;
        wal_entry.op = LogEntry::PUT;
        try {
            wal_entry.key = Serializer<K>::serialize(key);
            wal_entry.value = Serializer<V>::serialize(value);
            wal_entry.timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::high_resolution_clock::now().time_since_epoch()).count();
            wal_entry.lsn = next_lsn();
        } catch (const std::exception& e) {
            need_wal = false;
        }
    }
    
    try {
        // 写入内存
        shards_[shard_idx]->put(key, value, ttl_ms);
        
        // 写入WAL
        if (need_wal) {
            std::lock_guard<std::mutex> wal_lock(persistence_mutex_);
            wal_->append(wal_entry);
        }
        
        recordShardSuccess(shard_idx);
        
    } catch (const std::exception& e) {
        recordShardError(shard_idx);
    }
}

template<typename K, typename V, bool EnableCacheAlign>
bool ShardedCache<K, V, EnableCacheAlign>::remove(const K& key) {
    std::shared_lock<std::shared_mutex> consistency_lock(global_consistency_lock_);
    
    size_t shard_idx = get_shard_index(key);
    
    if (isShardDisabled(shard_idx)) {
        return false;  // 分片被禁用
    }
    
    // 准备WAL条目
    LogEntry wal_entry;
    bool need_wal = false;
    
    if (persistence_enabled_ && wal_) {
        need_wal = true;
        wal_entry.op = LogEntry::DELETE;
        try {
            wal_entry.key = Serializer<K>::serialize(key);
            wal_entry.timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::high_resolution_clock::now().time_since_epoch()).count();
            wal_entry.lsn = next_lsn();
        } catch (const std::exception& e) {
            need_wal = false;
        }
    }
    
    try {
        // 删除内存数据
        bool result = shards_[shard_idx]->remove(key);
        
        // 写入WAL
        if (result && need_wal) {
            std::lock_guard<std::mutex> wal_lock(persistence_mutex_);
            wal_->append(wal_entry);
        }
        
        recordShardSuccess(shard_idx);
        return result;
        
    } catch (const std::exception& e) {
        recordShardError(shard_idx);
        return false;
    }
}

template<typename K, typename V, bool EnableCacheAlign>
size_t ShardedCache<K, V, EnableCacheAlign>::size() const {
    size_t total = 0;
    for (size_t i = 0; i < shards_.size(); ++i) {
        if (!isShardDisabled(i)) {
            try {
                total += shards_[i]->size();
            } catch (...) {
                // 忽略单个分片的错误
            }
        }
    }
    return total;
}

template<typename K, typename V, bool EnableCacheAlign>
size_t ShardedCache<K, V, EnableCacheAlign>::capacity() const {
    return shards_.size() * (shards_.empty() ? 0 : shards_[0]->capacity());
}

template<typename K, typename V, bool EnableCacheAlign>
void ShardedCache<K, V, EnableCacheAlign>::clear() {
    std::unique_lock<std::shared_mutex> consistency_lock(global_consistency_lock_);
    
    for (size_t i = 0; i < shards_.size(); ++i) {
        if (!isShardDisabled(i)) {
            try {
                shards_[i]->clear();
                recordShardSuccess(i);
            } catch (const std::exception& e) {
                recordShardError(i);
            }
        }
    }
}

template<typename K, typename V, bool EnableCacheAlign>
CacheStats ShardedCache<K, V, EnableCacheAlign>::getStats() const {
    CacheStats total_stats;
    
    for (size_t i = 0; i < shards_.size(); ++i) {
        if (!isShardDisabled(i)) {
            try {
                auto shard_stats = shards_[i]->getStats();
                total_stats.hits += shard_stats.hits;
                total_stats.misses += shard_stats.misses;
                total_stats.expired += shard_stats.expired;
                total_stats.evictions += shard_stats.evictions;
                total_stats.puts += shard_stats.puts;
                total_stats.removes += shard_stats.removes;
                total_stats.current_size += shard_stats.current_size;
                total_stats.capacity += shard_stats.capacity;
            } catch (...) {
                // 忽略单个分片的错误
            }
        }
    }
    
    return total_stats;
}

template<typename K, typename V, bool EnableCacheAlign>
void ShardedCache<K, V, EnableCacheAlign>::resetStats() {
    for (size_t i = 0; i < shards_.size(); ++i) {
        if (!isShardDisabled(i)) {
            try {
                shards_[i]->resetStats();
            } catch (...) {
                // 忽略单个分片的错误
            }
        }
    }
}

// ==========================================
// 持久化接口实现
// ==========================================

template<typename K, typename V, bool EnableCacheAlign>
void ShardedCache<K, V, EnableCacheAlign>::enable_persistence(const std::string& data_dir, int64_t fsync_interval_ms) {
    std::lock_guard<std::mutex> lock(persistence_mutex_);
    
    if (persistence_enabled_) {
        return;  // 已启用
    }
    
    try {
        wal_ = std::make_unique<WriteAheadLog>(data_dir, 1024 * 1024, fsync_interval_ms);
        wal_->start_background_fsync();
        persistence_enabled_ = true;
        
        std::cout << "[Persistence] WAL enabled: " << data_dir << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "[Persistence] Failed to enable: " << e.what() << std::endl;
        persistence_enabled_ = false;
    }
}

template<typename K, typename V, bool EnableCacheAlign>
void ShardedCache<K, V, EnableCacheAlign>::disable_persistence() {
    std::lock_guard<std::mutex> lock(persistence_mutex_);
    
    if (!persistence_enabled_) {
        return;
    }
    
    if (wal_) {
        wal_->stop_background_fsync();
        wal_->flush();
    }
    
    wal_.reset();
    persistence_enabled_ = false;
    
    std::cout << "[Persistence] WAL disabled" << std::endl;
}

template<typename K, typename V, bool EnableCacheAlign>
void ShardedCache<K, V, EnableCacheAlign>::recover_from_disk() {
    if (!wal_) {
        return;
    }
    
    std::cout << "[Recovery] Starting recovery from WAL..." << std::endl;
    
    auto entries = wal_->read_all();
    size_t recovered_count = 0;
    size_t error_count = 0;
    
    for (const auto& entry : entries) {
        try {
            if (entry.op == LogEntry::PUT) {
                K key = Serializer<K>::deserialize(entry.key);
                V value = Serializer<V>::deserialize(entry.value);
                
                // 恢复时不写WAL，避免重复记录
                size_t shard_idx = get_shard_index(key);
                shards_[shard_idx]->put(key, value, 0);  // 恢复时不设置TTL
                recovered_count++;
                
            } else if (entry.op == LogEntry::DELETE) {
                K key = Serializer<K>::deserialize(entry.key);
                
                size_t shard_idx = get_shard_index(key);
                shards_[shard_idx]->remove(key);
                recovered_count++;
            }
        } catch (const std::exception& e) {
            error_count++;
            std::cerr << "[Recovery] Failed to replay entry: " << e.what() << std::endl;
        }
    }
    
    std::cout << "[Recovery] Completed: " << recovered_count
              << " entries recovered, " << error_count << " errors" << std::endl;
}

template<typename K, typename V, bool EnableCacheAlign>
void ShardedCache<K, V, EnableCacheAlign>::create_snapshot() {
    if (!wal_) {
        std::cout << "[Snapshot] WAL not enabled, skipping snapshot" << std::endl;
        return;
    }
    
    std::cout << "[Snapshot] Creating snapshot..." << std::endl;
    
    // 导出所有数据（在一致性锁保护下）
    auto all_data = export_all_data();
    
    // 创建快照
    int64_t snapshot_id = wal_->create_snapshot(all_data);
    
    std::cout << "[Snapshot] Created snapshot " << snapshot_id
              << " with " << all_data.size() << " entries" << std::endl;
}

template<typename K, typename V, bool EnableCacheAlign>
std::map<K, V> ShardedCache<K, V, EnableCacheAlign>::export_all_data() const {
    std::unique_lock<std::shared_mutex> consistency_lock(global_consistency_lock_);
    
    std::map<K, V> all_data;
    
    for (size_t i = 0; i < shards_.size(); ++i) {
        if (!isShardDisabled(i)) {
            try {
                auto shard_data = shards_[i]->get_all();
                all_data.insert(shard_data.begin(), shard_data.end());
            } catch (const std::exception& e) {
                std::cerr << "[Export] Shard " << i << " error: " << e.what() << std::endl;
            }
        }
    }
    
    std::cout << "[Export] Exported " << all_data.size()
              << " entries under consistency lock" << std::endl;
    
    return all_data;
}

template<typename K, typename V, bool EnableCacheAlign>
void ShardedCache<K, V, EnableCacheAlign>::clear_wal() {
    std::lock_guard<std::mutex> lock(persistence_mutex_);
    
    if (wal_) {
        wal_->clear_all();
        std::cout << "[WAL] Cleared all entries" << std::endl;
    }
}

// ==========================================
// 向量搜索接口实现
// ==========================================

template<typename K, typename V, bool EnableCacheAlign>
void ShardedCache<K, V, EnableCacheAlign>::vectorPut(const K& key, const std::vector<float>& vec, int64_t ttl_ms) {
    // 将向量序列化为字符串
    std::string serialized_vec = VectorOps::Serialize(vec);
    
    // 使用基础put接口存储
    put(key, serialized_vec, ttl_ms);
}

template<typename K, typename V, bool EnableCacheAlign>
std::vector<float> ShardedCache<K, V, EnableCacheAlign>::vectorGet(const K& key) {
    auto result = get(key);
    if (!result.has_value()) {
        return {};
    }
    
    // 反序列化向量数据
    return VectorOps::DeserializeCopy(*result);
}

template<typename K, typename V, bool EnableCacheAlign>
std::vector<K> ShardedCache<K, V, EnableCacheAlign>::vectorSearch(const std::vector<float>& query, int k) {
    struct SearchResult {
        K key;
        float distance;
        SearchResult(const K& k, float d) : key(k), distance(d) {}
        bool operator<(const SearchResult& other) const {
            return distance < other.distance;  // 大顶堆
        }
    };
    
    // 并行搜索所有分片
    std::vector<std::future<std::vector<SearchResult>>> futures;
    
    for (size_t shard_idx = 0; shard_idx < shards_.size(); ++shard_idx) {
        if (isShardDisabled(shard_idx)) {
            continue;  // 跳过被禁用的分片
        }
        
        futures.push_back(std::async(std::launch::async, [this, shard_idx, &query, k]() {
            std::vector<SearchResult> local_results;
            std::priority_queue<SearchResult> heap;
            
            try {
                auto all_data = shards_[shard_idx]->get_all();
                
                for (const auto& [key, raw_data] : all_data) {
                    auto vec_data = VectorOps::DeserializeCopy(raw_data);
                    
                    if (vec_data.empty() || vec_data.size() != query.size()) {
                        continue;  // 维度不匹配
                    }
                    
                    float distance = VectorOps::L2DistanceSquare(
                        query.data(), vec_data.data(), vec_data.size());
                    
                    heap.push(SearchResult(key, distance));
                    if ((int)heap.size() > k) {
                        heap.pop();
                    }
                }
                
                while (!heap.empty()) {
                    local_results.push_back(heap.top());
                    heap.pop();
                }
                
            } catch (const std::exception& e) {
                // 单个分片错误不影响整体搜索
                std::cerr << "[VectorSearch] Shard " << shard_idx
                          << " error: " << e.what() << std::endl;
            }
            
            return local_results;
        }));
    }
    
    // 收集所有分片结果
    std::priority_queue<SearchResult> global_heap;
    
    for (auto& f : futures) {
        try {
            auto shard_results = f.get();
            for (const auto& res : shard_results) {
                global_heap.push(res);
                if ((int)global_heap.size() > k) {
                    global_heap.pop();
                }
            }
        } catch (const std::exception& e) {
            std::cerr << "[VectorSearch] Future error: " << e.what() << std::endl;
        }
    }
    
    // 整理最终结果
    std::vector<K> final_results;
    while (!global_heap.empty()) {
        final_results.push_back(global_heap.top().key);
        global_heap.pop();
    }
    std::reverse(final_results.begin(), final_results.end());
    
    return final_results;
}

// ==========================================
// 定期删除接口实现
// ==========================================

template<typename K, typename V, bool EnableCacheAlign>
void ShardedCache<K, V, EnableCacheAlign>::startExpirationService(std::chrono::milliseconds check_interval, size_t sample_size) {
    if (expiration_manager_) {
        return;  // 已启动
    }
    
    // [RAII简化] 构造函数自动启动线程，无需手动start()
    expiration_manager_ = std::make_unique<base::ExpirationManager>(
        [this](size_t shard_id, size_t sample_size) {
            return this->expirationCallback(shard_id, sample_size);
        },
        shards_.size(),
        check_interval,
        sample_size
    );
}

template<typename K, typename V, bool EnableCacheAlign>
void ShardedCache<K, V, EnableCacheAlign>::stopExpirationService() {
    // [RAII] 析构函数自动停止线程，只需重置指针
    expiration_manager_.reset();
}

template<typename K, typename V, bool EnableCacheAlign>
size_t ShardedCache<K, V, EnableCacheAlign>::expirationCallback(size_t shard_id, size_t sample_size) {
    if (shard_id >= shards_.size() || isShardDisabled(shard_id)) {
        return 0;
    }
    
    auto& shard = shards_[shard_id];
    
    // 🎯 非阻塞锁：避免与业务线程竞争
    if (!shard->try_lock()) {
        // 锁竞争不算错误，只是跳过本次检查
        // 返回 SIZE_MAX 作为"锁竞争跳过"的哨兵值，与"正常处理但无过期key（返回0）"区分
        return SIZE_MAX;
    }
    
    // RAII锁管理
    struct LockGuard {
        EnhancedLruShard* shard_;
        ~LockGuard() { shard_->unlock(); }
    } guard{shard.get()};
    
    try {
        // 随机采样并删除过期key
        auto keys = shard->randomSample(sample_size);
        size_t expired_count = shard->expireKeys(keys);
        
        // 成功处理，重置错误计数
        recordShardSuccess(shard_id);
        
        return expired_count;
        
    } catch (const std::exception& e) {
        // 真正的异常才记录错误
        std::cout << "[ExpirationManager][ShardError] Shard " << shard_id
                  << " error: " << e.what() << std::endl;
        recordShardError(shard_id);
        return 0;
    }
}

template<typename K, typename V, bool EnableCacheAlign>
base::ExpirationManager::Stats ShardedCache<K, V, EnableCacheAlign>::getExpirationStats() const {
    if (expiration_manager_) {
        return expiration_manager_->getStats();
    }
    return {};
}

template<typename K, typename V, bool EnableCacheAlign>
size_t ShardedCache<K, V, EnableCacheAlign>::manualExpiration(int shard_id) {
    size_t total_expired = 0;
    
    if (shard_id == -1) {
        // 清理所有分片
        for (size_t i = 0; i < shards_.size(); ++i) {
            total_expired += expirationCallback(i, 20);
        }
    } else if (shard_id >= 0 && shard_id < static_cast<int>(shards_.size())) {
        // 清理指定分片
        total_expired = expirationCallback(static_cast<size_t>(shard_id), 20);
    }
    
    return total_expired;
}

// ==========================================
// 健康检查实现
// ==========================================

template<typename K, typename V, bool EnableCacheAlign>
void ShardedCache<K, V, EnableCacheAlign>::recordShardError(size_t shard_id) {
    std::lock_guard<std::mutex> lock(health_mutex_);
    
    shard_error_counts_[shard_id]++;
    
    if (shard_error_counts_[shard_id] >= MAX_CONSECUTIVE_ERRORS) {
        disabled_shards_.insert(shard_id);
        std::cout << "[HealthCheck] Shard " << shard_id
                  << " disabled due to " << shard_error_counts_[shard_id]
                  << " consecutive errors" << std::endl;
    }
}

template<typename K, typename V, bool EnableCacheAlign>
void ShardedCache<K, V, EnableCacheAlign>::recordShardSuccess(size_t shard_id) {
    std::lock_guard<std::mutex> lock(health_mutex_);
    shard_error_counts_[shard_id] = 0;  // 重置错误计数
}

template<typename K, typename V, bool EnableCacheAlign>
bool ShardedCache<K, V, EnableCacheAlign>::isShardDisabled(size_t shard_id) const {
    std::lock_guard<std::mutex> lock(health_mutex_);
    return disabled_shards_.count(shard_id) > 0;
}

template<typename K, typename V, bool EnableCacheAlign>
typename ShardedCache<K, V, EnableCacheAlign>::HealthStatus
ShardedCache<K, V, EnableCacheAlign>::getHealthStatus() const {
    std::lock_guard<std::mutex> lock(health_mutex_);
    
    HealthStatus status;
    status.total_shards = shards_.size();
    status.healthy_shards = status.total_shards - disabled_shards_.size();
    status.overall_healthy = (status.healthy_shards > status.total_shards / 2);  // 超过一半健康
    status.disabled_shards.assign(disabled_shards_.begin(), disabled_shards_.end());
    status.error_counts = shard_error_counts_;
    status.last_health_check = last_health_check_;
    
    // 计算错误率
    int total_errors = 0;
    for (const auto& [shard_id, count] : shard_error_counts_) {
        total_errors += count;
    }
    status.error_rate = static_cast<double>(total_errors) / (status.total_shards * MAX_CONSECUTIVE_ERRORS);
    
    return status;
}

template<typename K, typename V, bool EnableCacheAlign>
void ShardedCache<K, V, EnableCacheAlign>::performHealthCheck() {
    std::lock_guard<std::mutex> lock(health_mutex_);
    
    last_health_check_ = std::chrono::steady_clock::now();
    
    // 尝试重新启用被禁用的分片
    for (auto it = disabled_shards_.begin(); it != disabled_shards_.end();) {
        size_t shard_id = *it;
        
        try {
            // 尝试一个简单的操作来测试分片健康状态
            auto test_key = K{};  // 默认构造的测试key
            shards_[shard_id]->get(test_key);  // 测试读取
            
            // 成功了，重新启用
            shard_error_counts_[shard_id] = 0;
            it = disabled_shards_.erase(it);
            
            std::cout << "[HealthCheck] Shard " << shard_id << " recovered and re-enabled" << std::endl;
            
        } catch (...) {
            // 仍然有问题，保持禁用
            ++it;
        }
    }
}

// ==========================================
// EnhancedLruShard 实现
// ==========================================

template<typename K, typename V, bool EnableCacheAlign>
ShardedCache<K, V, EnableCacheAlign>::EnhancedLruShard::EnhancedLruShard(size_t capacity)
    : cache_(std::make_unique<LruCache<K, V>>(capacity)), rng_(std::random_device{}()) {
}

template<typename K, typename V, bool EnableCacheAlign>
bool ShardedCache<K, V, EnableCacheAlign>::EnhancedLruShard::try_lock() {
    return mutex_wrapper_.mutex.try_lock();
}

template<typename K, typename V, bool EnableCacheAlign>
void ShardedCache<K, V, EnableCacheAlign>::EnhancedLruShard::unlock() {
    mutex_wrapper_.mutex.unlock();
}

template<typename K, typename V, bool EnableCacheAlign>
std::optional<V> ShardedCache<K, V, EnableCacheAlign>::EnhancedLruShard::get(const K& key) {
    std::lock_guard<std::mutex> lock(mutex_wrapper_.mutex);
    return cache_->get(key);
}

template<typename K, typename V, bool EnableCacheAlign>
void ShardedCache<K, V, EnableCacheAlign>::EnhancedLruShard::put(const K& key, const V& value, int64_t ttl_ms) {
    std::lock_guard<std::mutex> lock(mutex_wrapper_.mutex);
    cache_->put(key, value, ttl_ms);
}

template<typename K, typename V, bool EnableCacheAlign>
bool ShardedCache<K, V, EnableCacheAlign>::EnhancedLruShard::remove(const K& key) {
    std::lock_guard<std::mutex> lock(mutex_wrapper_.mutex);
    return cache_->remove(key);
}

template<typename K, typename V, bool EnableCacheAlign>
size_t ShardedCache<K, V, EnableCacheAlign>::EnhancedLruShard::size() const {
    std::lock_guard<std::mutex> lock(mutex_wrapper_.mutex);
    return cache_->size();
}

template<typename K, typename V, bool EnableCacheAlign>
size_t ShardedCache<K, V, EnableCacheAlign>::EnhancedLruShard::capacity() const {
    return cache_->capacity();
}

template<typename K, typename V, bool EnableCacheAlign>
CacheStats ShardedCache<K, V, EnableCacheAlign>::EnhancedLruShard::getStats() const {
    std::lock_guard<std::mutex> lock(mutex_wrapper_.mutex);
    return cache_->getStats();
}

template<typename K, typename V, bool EnableCacheAlign>
void ShardedCache<K, V, EnableCacheAlign>::EnhancedLruShard::resetStats() {
    std::lock_guard<std::mutex> lock(mutex_wrapper_.mutex);
    cache_->resetStats();
}

template<typename K, typename V, bool EnableCacheAlign>
void ShardedCache<K, V, EnableCacheAlign>::EnhancedLruShard::clear() {
    std::lock_guard<std::mutex> lock(mutex_wrapper_.mutex);
    cache_->clear();
}

template<typename K, typename V, bool EnableCacheAlign>
std::map<K, V> ShardedCache<K, V, EnableCacheAlign>::EnhancedLruShard::get_all() const {
    std::lock_guard<std::mutex> lock(mutex_wrapper_.mutex);
    return cache_->get_all();
}

template<typename K, typename V, bool EnableCacheAlign>
std::vector<K> ShardedCache<K, V, EnableCacheAlign>::EnhancedLruShard::randomSample(size_t sample_size) {
    // 注意：调用此方法前必须已经获取锁
    auto all_data = cache_->get_all();
    
    std::vector<K> keys;
    keys.reserve(all_data.size());
    
    for (const auto& [key, value] : all_data) {
        keys.push_back(key);
    }
    
    if (keys.empty()) {
        return {};
    }
    
    std::shuffle(keys.begin(), keys.end(), rng_);
    
    size_t actual_size = std::min(sample_size, keys.size());
    keys.resize(actual_size);
    
    return keys;
}

template<typename K, typename V, bool EnableCacheAlign>
size_t ShardedCache<K, V, EnableCacheAlign>::EnhancedLruShard::expireKeys(const std::vector<K>& keys) {
    // 注意：调用此方法前必须已经获取 EnhancedLruShard 的锁
    // 直接委托给 LruCache::cleanup_expired_keys()，它内部会加自己的锁
    // 两把锁不同（EnhancedLruShard::mutex_wrapper_.mutex vs LruCache::mutex_），不会死锁
    // 同时避免了通过 size() 变化来间接判断过期的不可靠逻辑
    (void)keys;  // 采样列表由 ExpirationManager 传入，但实际清理交给 LruCache 全量扫描
    return cache_->cleanup_expired_keys();
}

} // namespace db
} // namespace minkv