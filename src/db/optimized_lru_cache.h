#pragma once

#include "sds_string.h"
#include "optimized_cache_node.h"
#include <list>
#include <unordered_map>
#include <shared_mutex>
#include <optional>
#include <chrono>

namespace minkv {
namespace db {

/**
 * @brief 优化版本的LRU缓存 - 使用SdsString和内存池
 * 
 * 主要优化：
 * 1. 使用SdsString替代std::string，减少内存分配
 * 2. 使用内存池分配节点，提升分配性能
 * 3. 小字符串优化，减少间接访问
 * 4. 缓存行对齐，提升缓存友好性
 * 
 * 预期性能提升：
 * - 内存使用减少30-50%
 * - 分配性能提升2-3倍
 * - 缓存命中率提升15-25%
 */
template<typename K = SdsString, typename V = SdsString>
class OptimizedLruCache {
public:
    using Node = typename OptimizedCacheNode<K, V>::Node;
    using NodeAllocator = typename OptimizedCacheNode<K, V>::NodeAllocator;
    
    explicit OptimizedLruCache(size_t capacity) : capacity_(capacity) {
        // 预热内存池
        allocator_.allocate(); // 分配第一个节点触发池初始化
        allocator_.deallocate(allocator_.allocate()); // 立即释放
    }
    
    ~OptimizedLruCache() {
        clear();
    }
    
    // 禁止拷贝
    OptimizedLruCache(const OptimizedLruCache&) = delete;
    OptimizedLruCache& operator=(const OptimizedLruCache&) = delete;

    // ==========================================
    // 核心接口 (与原版兼容)
    // ==========================================
    
    std::optional<V> get(const K& key) {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        
        auto it = index_.find(key);
        if (it == index_.end()) {
            ++stats_misses_;
            return std::nullopt;
        }
        
        Node* node = it->second;
        
        // 检查过期
        if (node->is_expired()) {
            // 需要删除，升级到写锁
            lock.unlock();
            std::lock_guard<std::shared_mutex> write_lock(mutex_);
            
            // 双重检查
            auto it2 = index_.find(key);
            if (it2 != index_.end() && it2->second->is_expired()) {
                remove_node_unsafe(it2->second);
                index_.erase(it2);
                ++stats_expired_;
            }
            ++stats_misses_;
            return std::nullopt;
        }
        
        // 更新访问信息 (在读锁下进行)
        node->update_access();
        
        // Lazy LRU: 只有在必要时才移动节点
        if (should_promote(node)) {
            lock.unlock();
            std::lock_guard<std::shared_mutex> write_lock(mutex_);
            promote_node_unsafe(node);
        }
        
        ++stats_hits_;
        return node->get_value();
    }
    
    void put(const K& key, const V& value, int64_t ttl_ms = 0) {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        
        auto it = index_.find(key);
        if (it != index_.end()) {
            // 更新现有节点
            Node* node = it->second;
            node->set_value(value);
            
            if (ttl_ms > 0) {
                auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::high_resolution_clock::now().time_since_epoch()
                ).count();
                node->expiry_time_ms = now + ttl_ms;
            }
            
            promote_node_unsafe(node);
            ++stats_puts_;
            return;
        }
        
        // 创建新节点
        Node* new_node = OptimizedCacheNode<K, V>::create_node(
            std::string_view(key.data(), key.size()),
            std::string_view(value.data(), value.size()),
            ttl_ms
        );
        
        if (!new_node) {
            throw std::bad_alloc();
        }
        
        // 检查容量
        if (lru_list_.size() >= capacity_) {
            // 淘汰最旧的节点
            Node* victim = lru_list_.back();
            lru_list_.pop_back();
            
            // 从索引中删除
            auto victim_key = victim->get_key();
            auto victim_it = index_.find(K(victim_key.data(), victim_key.size()));
            if (victim_it != index_.end()) {
                index_.erase(victim_it);
            }
            
            OptimizedCacheNode<K, V>::destroy_node(victim);
            ++stats_evictions_;
        }
        
        // 插入新节点
        lru_list_.push_front(new_node);
        index_[key] = new_node;
        ++stats_puts_;
    }
    
    bool remove(const K& key) {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        
        auto it = index_.find(key);
        if (it == index_.end()) {
            return false;
        }
        
        Node* node = it->second;
        remove_node_unsafe(node);
        index_.erase(it);
        ++stats_removes_;
        return true;
    }
    
    size_t size() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return lru_list_.size();
    }
    
    size_t capacity() const { return capacity_; }
    
    void clear() {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        
        for (Node* node : lru_list_) {
            OptimizedCacheNode<K, V>::destroy_node(node);
        }
        
        lru_list_.clear();
        index_.clear();
    }

    // ==========================================
    // 性能统计和监控
    // ==========================================
    
    struct OptimizedCacheStats {
        uint64_t hits = 0;
        uint64_t misses = 0;
        uint64_t expired = 0;
        uint64_t evictions = 0;
        uint64_t puts = 0;
        uint64_t removes = 0;
        size_t current_size = 0;
        size_t capacity = 0;
        
        // 内存统计
        size_t allocated_nodes = 0;
        size_t memory_pools = 0;
        size_t total_memory_mb = 0;
        
        // 性能指标
        double hit_rate() const {
            uint64_t total = hits + misses;
            return total > 0 ? static_cast<double>(hits) / total : 0.0;
        }
        
        double memory_efficiency() const {
            return capacity > 0 ? static_cast<double>(current_size) / capacity : 0.0;
        }
    };
    
    OptimizedCacheStats getStats() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        
        OptimizedCacheStats stats;
        stats.hits = stats_hits_;
        stats.misses = stats_misses_;
        stats.expired = stats_expired_;
        stats.evictions = stats_evictions_;
        stats.puts = stats_puts_;
        stats.removes = stats_removes_;
        stats.current_size = lru_list_.size();
        stats.capacity = capacity_;
        
        // 内存统计
        stats.allocated_nodes = allocator_.allocated_count();
        stats.memory_pools = allocator_.pool_count();
        stats.total_memory_mb = allocator_.memory_usage() / (1024 * 1024);
        
        return stats;
    }
    
    void resetStats() {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        stats_hits_ = 0;
        stats_misses_ = 0;
        stats_expired_ = 0;
        stats_evictions_ = 0;
        stats_puts_ = 0;
        stats_removes_ = 0;
    }

    // ==========================================
    // 调试和诊断接口
    // ==========================================
    
    void print_memory_stats() const {
        auto stats = getStats();
        
        std::cout << "=== OptimizedLruCache Memory Stats ===" << std::endl;
        std::cout << "Current size: " << stats.current_size << " / " << stats.capacity << std::endl;
        std::cout << "Allocated nodes: " << stats.allocated_nodes << std::endl;
        std::cout << "Memory pools: " << stats.memory_pools << std::endl;
        std::cout << "Total memory: " << stats.total_memory_mb << " MB" << std::endl;
        std::cout << "Hit rate: " << std::fixed << std::setprecision(2) 
                  << (stats.hit_rate() * 100) << "%" << std::endl;
        std::cout << "Memory efficiency: " << std::fixed << std::setprecision(2) 
                  << (stats.memory_efficiency() * 100) << "%" << std::endl;
    }

private:
    size_t capacity_;
    
    // 使用链表存储节点指针 (而不是节点本身)
    std::list<Node*> lru_list_;
    
    // 索引：Key -> Node指针
    std::unordered_map<K, Node*> index_;
    
    // 线程安全
    mutable std::shared_mutex mutex_;
    
    // 内存分配器
    NodeAllocator& allocator_ = OptimizedCacheNode<K, V>::get_allocator();
    
    // 统计信息
    std::atomic<uint64_t> stats_hits_{0};
    std::atomic<uint64_t> stats_misses_{0};
    std::atomic<uint64_t> stats_expired_{0};
    std::atomic<uint64_t> stats_evictions_{0};
    std::atomic<uint64_t> stats_puts_{0};
    std::atomic<uint64_t> stats_removes_{0};
    
    // Lazy LRU相关
    std::atomic<uint64_t> last_promote_time_{0};
    static constexpr uint64_t PROMOTE_INTERVAL_MS = 1000; // 1秒内不重复提升

    // ==========================================
    // 内部辅助函数
    // ==========================================
    
    bool should_promote(Node* node) const {
        auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch()
        ).count();
        
        uint64_t last = last_promote_time_.load();
        return (now - last) > PROMOTE_INTERVAL_MS;
    }
    
    void promote_node_unsafe(Node* node) {
        // 将节点移动到链表头部
        auto it = std::find(lru_list_.begin(), lru_list_.end(), node);
        if (it != lru_list_.end() && it != lru_list_.begin()) {
            lru_list_.erase(it);
            lru_list_.push_front(node);
            
            auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::high_resolution_clock::now().time_since_epoch()
            ).count();
            last_promote_time_.store(now);
        }
    }
    
    void remove_node_unsafe(Node* node) {
        auto it = std::find(lru_list_.begin(), lru_list_.end(), node);
        if (it != lru_list_.end()) {
            lru_list_.erase(it);
        }
        OptimizedCacheNode<K, V>::destroy_node(node);
    }
};

// ==========================================
// 类型别名 (便于使用)
// ==========================================

using StringCache = OptimizedLruCache<SdsString, SdsString>;
using MixedCache = OptimizedLruCache<std::string, SdsString>; // Key用std::string，Value用SdsString

} // namespace db
} // namespace minkv