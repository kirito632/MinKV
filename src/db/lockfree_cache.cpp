#include "lockfree_cache.h"
#include <algorithm>

namespace minkv {
namespace db {

int64_t OptimisticLruCache::current_time_ms() {
    auto now = std::chrono::high_resolution_clock::now();
    auto duration = now.time_since_epoch();
    return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
}

bool OptimisticLruCache::is_expired(const Node& node) const {
    if (node.expiry_time_ms == 0) return false;
    return current_time_ms() > node.expiry_time_ms;
}

OptimisticLruCache::OptimisticLruCache(size_t capacity) : capacity_(capacity) {}

size_t OptimisticLruCache::hash_key(const std::string& key) const {
    std::hash<std::string> hasher;
    return hasher(key) % HASH_TABLE_SIZE;
}

std::optional<std::string> OptimisticLruCache::get(const std::string& key) {
    size_t hash_idx = hash_key(key);
    
    // 第一步：用原子操作读取哈希表（无锁）
    // memory_order_acquire：确保读取的可见性
    void* node_ptr_void = hash_table_[hash_idx].iter_ptr.load(std::memory_order_acquire);
    
    if (node_ptr_void == nullptr) {
        return std::nullopt;
    }

    // 转换回节点指针
    Node* node_ptr = static_cast<Node*>(node_ptr_void);

    // 第二步：检查过期时间（无锁）
    if (is_expired(*node_ptr)) {
        // 过期了，需要删除。这里需要加锁。
        std::lock_guard<std::mutex> lock(list_mutex_);
        
        // 再次检查，因为可能有其他线程已经删除了
        if (hash_table_[hash_idx].iter_ptr.load(std::memory_order_acquire) == node_ptr_void) {
            // 找到这个节点并删除
            for (auto it = cache_list_.begin(); it != cache_list_.end(); ++it) {
                if (&(*it) == node_ptr) {
                    cache_list_.erase(it);
                    break;
                }
            }
            hash_table_[hash_idx].iter_ptr.store(nullptr, std::memory_order_release);
        }
        return std::nullopt;
    }

    // 第三步：如果需要更新 LRU 顺序，加锁
    {
        std::lock_guard<std::mutex> lock(list_mutex_);
        
        // 再次检查节点是否有效
        if (hash_table_[hash_idx].iter_ptr.load(std::memory_order_acquire) == node_ptr_void) {
            // 找到这个节点并移到头部
            for (auto it = cache_list_.begin(); it != cache_list_.end(); ++it) {
                if (&(*it) == node_ptr) {
                    cache_list_.splice(cache_list_.begin(), cache_list_, it);
                    break;
                }
            }
        }
    }

    return node_ptr->value;
}

void OptimisticLruCache::put(const std::string& key, const std::string& value, int64_t ttl_ms) {
    size_t hash_idx = hash_key(key);
    int64_t expiry_time = (ttl_ms > 0) ? (current_time_ms() + ttl_ms) : 0;

    // 第一步：检查 Key 是否已存在（无锁读）
    void* existing_node_void = hash_table_[hash_idx].iter_ptr.load(std::memory_order_acquire);
    
    if (existing_node_void != nullptr) {
        // Key 已存在，更新值
        Node* existing_node = static_cast<Node*>(existing_node_void);
        
        std::lock_guard<std::mutex> lock(list_mutex_);
        
        // double-check
        if (hash_table_[hash_idx].iter_ptr.load(std::memory_order_acquire) == existing_node_void) {
            existing_node->value = value;
            existing_node->expiry_time_ms = expiry_time;
            
            // 找到这个节点并移到头部
            for (auto it = cache_list_.begin(); it != cache_list_.end(); ++it) {
                if (&(*it) == existing_node) {
                    cache_list_.splice(cache_list_.begin(), cache_list_, it);
                    break;
                }
            }
        }
        return;
    }

    // 第二步：Key 不存在，需要插入
    {
        std::lock_guard<std::mutex> lock(list_mutex_);

        // 检查容量，如果满了则删除尾部
        if (cache_list_.size() >= capacity_) {
            auto last = cache_list_.end();
            last--;
            
            // 从哈希表中删除
            size_t last_hash_idx = hash_key(last->key);
            hash_table_[last_hash_idx].iter_ptr.store(nullptr, std::memory_order_release);
            
            cache_list_.pop_back();
        }

        // 插入新节点
        cache_list_.push_front({key, value, expiry_time});
        
        // 用 CAS 操作更新哈希表（无锁）
        Node* new_node = &(*cache_list_.begin());
        void* new_node_void = static_cast<void*>(new_node);
        void* expected = nullptr;
        
        // 这里我们用 CAS 尝试原子地更新哈希表
        while (!hash_table_[hash_idx].iter_ptr.compare_exchange_weak(
            expected, new_node_void,
            std::memory_order_release,  // success: release
            std::memory_order_acquire   // failure: acquire
        )) {
            // CAS 失败，说明有其他线程插入了相同的 Key
            expected = nullptr;
            cache_list_.pop_front();
            return;
        }
    }
}

bool OptimisticLruCache::remove(const std::string& key) {
    size_t hash_idx = hash_key(key);

    // 第一步：无锁读取哈希表
    void* node_ptr_void = hash_table_[hash_idx].iter_ptr.load(std::memory_order_acquire);
    
    if (node_ptr_void == nullptr) {
        return false;
    }

    Node* node_ptr = static_cast<Node*>(node_ptr_void);

    // 第二步：加锁删除
    {
        std::lock_guard<std::mutex> lock(list_mutex_);
        
        // double-check
        if (hash_table_[hash_idx].iter_ptr.load(std::memory_order_acquire) == node_ptr_void) {
            // 找到这个节点并删除
            for (auto it = cache_list_.begin(); it != cache_list_.end(); ++it) {
                if (&(*it) == node_ptr) {
                    cache_list_.erase(it);
                    hash_table_[hash_idx].iter_ptr.store(nullptr, std::memory_order_release);
                    return true;
                }
            }
        }
    }

    return false;
}

size_t OptimisticLruCache::size() const {
    std::lock_guard<std::mutex> lock(list_mutex_);
    return cache_list_.size();
}

} // namespace db
} // namespace minkv
