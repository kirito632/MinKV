#pragma once

#include "lru_cache.h"
#include "vector_ops.h"
#include "wal.h"
#include <vector>
#include <memory>
#include <functional>
#include <queue>
#include <thread>
#include <future>

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

    // ==========================================
    // 持久化接口 (Persistence API)
    // ==========================================
    
    /**
     * @brief 启用 WAL 持久化
     * 
     * @param data_dir 数据目录
     * @param fsync_interval_ms 后台 fsync 间隔（毫秒），默认 1000ms
     */
    void enable_persistence(const std::string& data_dir, int64_t fsync_interval_ms = 1000);
    
    /**
     * @brief 禁用 WAL 持久化
     */
    void disable_persistence();
    
    /**
     * @brief 从磁盘恢复数据
     * 
     * 通常在启动时调用，从 WAL 日志恢复数据。
     */
    void recover_from_disk();
    
    /**
     * @brief 创建快照
     * 
     * 将当前所有数据的完整副本写入快照文件。
     */
    void create_snapshot();

    // ==========================================
    // 向量检索接口 (Vector Search API)
    // ==========================================
    
    /**
     * @brief 存储向量数据
     * 
     * 将 std::vector<float> 转换为二进制格式存储在缓存中。
     * 利用 Zero-Copy 技术，避免额外的内存拷贝。
     * 
     * @param key 向量的键
     * @param vec 浮点数向量
     * @param ttl_ms 过期时间（毫秒）。0 表示永不过期。
     * 
     * @note 这个接口复用了现有的 string 存储能力，
     *       通过 reinterpret_cast 将 float 数组当作字节流存储。
     */
    void vectorPut(const K& key, const std::vector<float>& vec, int64_t ttl_ms = 0);

    /**
     * @brief 向量相似度搜索 (Top-K Search)
     * 
     * 在缓存中查找与查询向量最相似的 K 个向量。
     * 使用 Map-Reduce 架构并行搜索所有分片。
     * 
     * @param query 查询向量
     * @param k 返回最相似的 K 个结果
     * @return 包含 K 个最相似向量的 Key 列表（按相似度排序）
     * 
     * @note 使用 AVX2 SIMD 指令集加速距离计算。
     *       距离计算采用欧式距离平方（无需开根号以提高性能）。
     */
    std::vector<K> vectorSearch(const std::vector<float>& query, int k);

    /**
     * @brief 从缓存读取向量数据
     * 
     * @param key 向量的键
     * @return 如果存在返回向量，否则返回空向量
     */
    std::vector<float> vectorGet(const K& key);

private:
    // 分片数组（使用 unique_ptr 因为 LruCache 禁止拷贝）
    std::vector<std::unique_ptr<LruCache<K, V>>> shards_;
    
    // WAL 持久化相关
    std::unique_ptr<WriteAheadLog> wal_;
    bool persistence_enabled_{false};
    mutable std::mutex persistence_mutex_;

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
    
    // 如果启用持久化，先准备 WAL 条目但不写入（避免死锁）
    LogEntry wal_entry;
    bool need_wal = false;
    
    if (persistence_enabled_ && wal_) {
        need_wal = true;
        wal_entry.op = LogEntry::PUT;
        
        // 准备 WAL 数据（类型转换）
        if (std::is_same<K, int>::value) {
            wal_entry.key = std::to_string(*reinterpret_cast<const int*>(&key));
        } else if (std::is_same<K, std::string>::value) {
            wal_entry.key = *reinterpret_cast<const std::string*>(&key);
        }
        
        if (std::is_same<V, int>::value) {
            wal_entry.value = std::to_string(*reinterpret_cast<const int*>(&value));
        } else if (std::is_same<V, std::string>::value) {
            wal_entry.value = *reinterpret_cast<const std::string*>(&value);
        }
        
        wal_entry.timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch()
        ).count();
    }
    
    // 统一锁顺序：先 WAL 锁，再 LRU 锁（防止死锁）
    std::unique_lock<std::mutex> wal_lock;
    if (need_wal) {
        wal_lock = std::unique_lock<std::mutex>(persistence_mutex_);
    }
    
    // 然后写入内存（已持有 WAL 锁，避免死锁）
    shards_[shard_idx]->put(key, value, ttl_ms);
    
    // 最后写入 WAL（已持有锁）
    if (need_wal) {
        wal_->append(wal_entry);
    }
}

template<typename K, typename V>
bool ShardedCache<K, V>::remove(const K& key) {
    size_t shard_idx = get_shard_index(key);
    
    // 如果启用持久化，先准备 WAL 条目（避免死锁）
    LogEntry wal_entry;
    bool need_wal = false;
    
    if (persistence_enabled_ && wal_) {
        need_wal = true;
        wal_entry.op = LogEntry::DELETE;
        
        if (std::is_same<K, int>::value) {
            wal_entry.key = std::to_string(*reinterpret_cast<const int*>(&key));
        } else if (std::is_same<K, std::string>::value) {
            wal_entry.key = *reinterpret_cast<const std::string*>(&key);
        }
        
        wal_entry.value = "";
        wal_entry.timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch()
        ).count();
    }
    
    // 统一锁顺序：先 WAL 锁，再 LRU 锁（防止死锁）
    std::unique_lock<std::mutex> wal_lock;
    if (need_wal) {
        wal_lock = std::unique_lock<std::mutex>(persistence_mutex_);
    }
    
    // 删除内存数据
    bool result = shards_[shard_idx]->remove(key);
    
    // 如果删除成功且需要 WAL，写入日志
    if (result && need_wal) {
        wal_->append(wal_entry);
    }
    
    return result;
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

// ==========================================
// 向量检索实现 (Vector Search Implementation)
// ==========================================

template<typename K, typename V>
void ShardedCache<K, V>::vectorPut(const K& key, const std::vector<float>& vec, int64_t ttl_ms) {
    // 将 vector<float> 转换为 string (二进制存储)
    // 这是 Zero-Copy 的关键：直接复用 string 的内存管理
    std::string raw_data = VectorOps::Serialize(vec);
    
    // 复用现有的 put 接口存储
    size_t shard_idx = get_shard_index(key);
    shards_[shard_idx]->put(key, raw_data, ttl_ms);
}

template<typename K, typename V>
std::vector<float> ShardedCache<K, V>::vectorGet(const K& key) {
    size_t shard_idx = get_shard_index(key);
    auto raw_data = shards_[shard_idx]->get(key);
    
    if (!raw_data) {
        return {}; // 返回空向量
    }
    
    // 从 string 反序列化为 vector<float>
    size_t dim = 0;
    const float* ptr = VectorOps::DeserializeView(*raw_data, dim);
    if (!ptr) {
        return {}; // 数据格式不对
    }
    
    return std::vector<float>(ptr, ptr + dim);
}

template<typename K, typename V>
std::vector<K> ShardedCache<K, V>::vectorSearch(const std::vector<float>& query, int k) {
    // Map-Reduce 架构：并行搜索所有分片
    
    // 定义单个分片的搜索结果
    struct SearchResult {
        K key;
        float distance;
        SearchResult() : key(), distance(0.0f) {}
        SearchResult(const K& k, float d) : key(k), distance(d) {}
        bool operator<(const SearchResult& other) const {
            return distance < other.distance; // 大顶堆：距离大的在前
        }
    };
    
    // 1. Map Phase: 启动多个线程，每个线程搜索一个分片
    std::vector<std::future<std::vector<SearchResult>>> futures;
    
    for (size_t shard_idx = 0; shard_idx < shards_.size(); ++shard_idx) {
        futures.push_back(std::async(std::launch::async, [this, shard_idx, &query, k]() {
            std::vector<SearchResult> local_results;
            std::priority_queue<SearchResult> heap; // 大顶堆
            
            // 获取当前分片的所有数据
            auto all_data = shards_[shard_idx]->get_all();
            
            // 遍历分片中的所有键值对
            for (const auto& [key, raw_data] : all_data) {
                // 安全反序列化：使用拷贝避免指针悬空
                auto vec_data = VectorOps::DeserializeCopy(raw_data);
                
                if (vec_data.empty() || vec_data.size() != query.size()) {
                    continue; // 维度不匹配或数据无效，跳过
                }
                
                // 计算欧式距离平方（使用 SIMD 加速）
                float distance = VectorOps::L2DistanceSquare_AVX2(query.data(), vec_data.data(), vec_data.size());
                
                // 维护 Top-K 堆
                heap.push(SearchResult(key, distance));
                if ((int)heap.size() > k) {
                    heap.pop();
                }
            }
            
            // 将堆中的结果转换为向量
            while (!heap.empty()) {
                local_results.push_back(heap.top());
                heap.pop();
            }
            
            return local_results;
        }));
    }
    
    // 2. Reduce Phase: 收集所有分片的结果，做全局 Top-K
    std::priority_queue<SearchResult> global_heap;
    
    for (auto& f : futures) {
        auto shard_results = f.get();
        for (const auto& res : shard_results) {
            global_heap.push(res);
            if ((int)global_heap.size() > k) {
                global_heap.pop();
            }
        }
    }
    
    // 3. 整理结果（从小到大排序）
    std::vector<K> final_results;
    while (!global_heap.empty()) {
        final_results.push_back(global_heap.top().key);
        global_heap.pop();
    }
    std::reverse(final_results.begin(), final_results.end());
    
    return final_results;
}

// ==========================================
// 持久化实现 (Persistence Implementation)
// ==========================================

template<typename K, typename V>
void ShardedCache<K, V>::enable_persistence(const std::string& data_dir, int64_t fsync_interval_ms) {
    std::lock_guard<std::mutex> lock(persistence_mutex_);
    
    if (persistence_enabled_) {
        return;  // 已启用，直接返回
    }
    
    try {
        wal_ = std::make_unique<WriteAheadLog>(data_dir, 1024 * 1024, fsync_interval_ms);
        wal_->start_background_fsync();
        persistence_enabled_ = true;
    } catch (const std::exception& e) {
        std::cerr << "Failed to enable persistence: " << e.what() << std::endl;
        persistence_enabled_ = false;
    }
}

template<typename K, typename V>
void ShardedCache<K, V>::disable_persistence() {
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
}

template<typename K, typename V>
void ShardedCache<K, V>::recover_from_disk() {
    if (!wal_) {
        return;
    }
    
    std::cout << "Recovering from disk..." << std::endl;
    
    auto entries = wal_->read_all();
    
    size_t recovered_count = 0;
    for (const auto& entry : entries) {
        try {
            if (entry.op == LogEntry::PUT) {
                K key;
                V value;
                
                // 类型转换：从 string 转换为实际类型
                if (std::is_same<K, int>::value) {
                    *reinterpret_cast<int*>(&key) = std::stoi(entry.key);
                } else if (std::is_same<K, std::string>::value) {
                    *reinterpret_cast<std::string*>(&key) = entry.key;
                }
                
                if (std::is_same<V, int>::value) {
                    *reinterpret_cast<int*>(&value) = std::stoi(entry.value);
                } else if (std::is_same<V, std::string>::value) {
                    *reinterpret_cast<std::string*>(&value) = entry.value;
                }
                
                put(key, value, 0);
                recovered_count++;
            } else if (entry.op == LogEntry::DELETE) {
                K key;
                
                if (std::is_same<K, int>::value) {
                    *reinterpret_cast<int*>(&key) = std::stoi(entry.key);
                } else if (std::is_same<K, std::string>::value) {
                    *reinterpret_cast<std::string*>(&key) = entry.key;
                }
                
                remove(key);
                recovered_count++;
            }
        } catch (const std::exception& e) {
            std::cerr << "Failed to replay entry: " << e.what() << std::endl;
        }
    }
    
    std::cout << "Recovered " << recovered_count << " entries" << std::endl;
}

template<typename K, typename V>
void ShardedCache<K, V>::create_snapshot() {
    if (!wal_) {
        return;
    }
    
    std::cout << "Creating snapshot..." << std::endl;
    
    std::map<K, V> all_data;
    for (auto& shard : shards_) {
        auto shard_data = shard->get_all();
        all_data.insert(shard_data.begin(), shard_data.end());
    }
    
    int64_t snapshot_id = wal_->create_snapshot(all_data);
    
    std::cout << "Snapshot created with ID: " << snapshot_id << std::endl;
}

} // namespace db
} // namespace minkv
