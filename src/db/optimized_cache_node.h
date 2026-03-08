#pragma once

#include "sds_string.h"
#include <memory>
#include <cstdint>
#include <string_view>

namespace minkv {
namespace db {

/**
 * @brief 优化的缓存节点 - 内存布局优化版本
 * 
 * 设计目标：
 * 1. 减少内存碎片 - 连续内存分配
 * 2. 提升缓存友好性 - 数据局部性
 * 3. 减少间接访问 - 内联小字符串
 * 4. 支持零拷贝 - string_view接口
 */
template<typename K, typename V>
class OptimizedCacheNode {
public:
    // ==========================================
    // 小字符串优化 (SSO - Small String Optimization)
    // ==========================================
    
    static constexpr size_t SMALL_STRING_SIZE = 23; // 23字节内联存储
    
    struct SmallString {
        union {
            struct {
                char data[SMALL_STRING_SIZE];
                uint8_t size; // 最后一个字节存储大小
            } small;
            
            struct {
                SdsString* ptr;
                size_t size;
                size_t capacity;
            } large;
        };
        
        bool is_small() const {
            return small.size <= SMALL_STRING_SIZE;
        }
        
        size_t size() const {
            return is_small() ? small.size : large.size;
        }
        
        const char* data() const {
            return is_small() ? small.data : large.ptr->data();
        }
        
        std::string_view view() const {
            return std::string_view(data(), size());
        }
    };

    // ==========================================
    // 节点结构 (内存对齐优化)
    // ==========================================
    
    struct alignas(64) Node {  // 缓存行对齐
        // 热数据区 (前32字节) - 经常访问的数据
        int64_t expiry_time_ms;     // 8字节 - 过期时间
        uint32_t access_count;      // 4字节 - 访问计数 (用于LFU)
        uint32_t flags;             // 4字节 - 标志位
        
        // 键值数据区
        SmallString key;            // 24字节 - 键 (支持SSO)
        SmallString value;          // 24字节 - 值 (支持SSO)
        
        // 冷数据区 - 不经常访问的数据
        int64_t create_time_ms;     // 8字节 - 创建时间
        int64_t last_access_ms;     // 8字节 - 最后访问时间
        
        // 构造函数
        Node() : expiry_time_ms(0), access_count(0), flags(0), 
                 create_time_ms(0), last_access_ms(0) {}
        
        // 设置键值
        void set_key(std::string_view k) {
            set_string(key, k);
        }
        
        void set_value(std::string_view v) {
            set_string(value, v);
        }
        
        // 获取键值视图 (零拷贝)
        std::string_view get_key() const {
            return key.view();
        }
        
        std::string_view get_value() const {
            return value.view();
        }
        
        // 检查是否过期
        bool is_expired() const {
            if (expiry_time_ms == 0) return false;
            auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::high_resolution_clock::now().time_since_epoch()
            ).count();
            return now > expiry_time_ms;
        }
        
        // 更新访问信息
        void update_access() {
            access_count++;
            last_access_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::high_resolution_clock::now().time_since_epoch()
            ).count();
        }
        
    private:
        void set_string(SmallString& target, std::string_view sv) {
            if (sv.size() <= SMALL_STRING_SIZE) {
                // 小字符串优化：直接内联存储
                memcpy(target.small.data, sv.data(), sv.size());
                target.small.size = static_cast<uint8_t>(sv.size());
            } else {
                // 大字符串：使用SdsString
                if (target.is_small() || !target.large.ptr) {
                    target.large.ptr = new SdsString(sv);
                } else {
                    *target.large.ptr = SdsString(sv);
                }
                target.large.size = sv.size();
                target.large.capacity = target.large.ptr->capacity();
            }
        }
    };

    // ==========================================
    // 内存池分配器 (减少malloc调用)
    // ==========================================
    
    class NodeAllocator {
    public:
        static constexpr size_t POOL_SIZE = 1024; // 每个池1024个节点
        
        struct NodePool {
            alignas(64) Node nodes[POOL_SIZE];
            std::bitset<POOL_SIZE> used;
            size_t next_free = 0;
            std::unique_ptr<NodePool> next;
        };
        
        NodeAllocator() {
            current_pool_ = std::make_unique<NodePool>();
        }
        
        Node* allocate() {
            std::lock_guard<std::mutex> lock(mutex_);
            
            // 在当前池中查找空闲节点
            NodePool* pool = current_pool_.get();
            while (pool) {
                for (size_t i = pool->next_free; i < POOL_SIZE; ++i) {
                    if (!pool->used[i]) {
                        pool->used[i] = true;
                        pool->next_free = i + 1;
                        allocated_count_++;
                        return &pool->nodes[i];
                    }
                }
                
                // 当前池满了，尝试下一个池
                if (!pool->next) {
                    pool->next = std::make_unique<NodePool>();
                    pool_count_++;
                }
                pool = pool->next.get();
            }
            
            return nullptr; // 理论上不会到这里
        }
        
        void deallocate(Node* node) {
            std::lock_guard<std::mutex> lock(mutex_);
            
            // 找到节点所属的池
            NodePool* pool = current_pool_.get();
            while (pool) {
                if (node >= pool->nodes && node < pool->nodes + POOL_SIZE) {
                    size_t index = node - pool->nodes;
                    pool->used[index] = false;
                    pool->next_free = std::min(pool->next_free, index);
                    allocated_count_--;
                    
                    // 清理节点数据
                    node->~Node();
                    new (node) Node(); // placement new重新初始化
                    return;
                }
                pool = pool->next.get();
            }
        }
        
        // 统计信息
        size_t allocated_count() const { return allocated_count_; }
        size_t pool_count() const { return pool_count_; }
        size_t memory_usage() const { 
            return pool_count_ * sizeof(NodePool); 
        }
        
    private:
        std::unique_ptr<NodePool> current_pool_;
        std::mutex mutex_;
        std::atomic<size_t> allocated_count_{0};
        std::atomic<size_t> pool_count_{1};
    };

    // ==========================================
    // 全局分配器实例
    // ==========================================
    
    static NodeAllocator& get_allocator() {
        static NodeAllocator allocator;
        return allocator;
    }
    
    // ==========================================
    // 便捷接口
    // ==========================================
    
    static Node* create_node(std::string_view key, std::string_view value, int64_t ttl_ms = 0) {
        Node* node = get_allocator().allocate();
        if (node) {
            node->set_key(key);
            node->set_value(value);
            
            auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::high_resolution_clock::now().time_since_epoch()
            ).count();
            
            node->create_time_ms = now;
            node->last_access_ms = now;
            
            if (ttl_ms > 0) {
                node->expiry_time_ms = now + ttl_ms;
            }
        }
        return node;
    }
    
    static void destroy_node(Node* node) {
        if (node) {
            get_allocator().deallocate(node);
        }
    }
};

// ==========================================
// 性能基准测试工具
// ==========================================

class CacheNodeBenchmark {
public:
    struct BenchmarkResult {
        double std_string_time_ms;
        double sds_string_time_ms;
        double optimized_node_time_ms;
        size_t std_string_memory_mb;
        size_t sds_string_memory_mb;
        size_t optimized_node_memory_mb;
        double speedup_factor;
        double memory_saving_percent;
    };
    
    static BenchmarkResult run_benchmark(size_t num_operations = 1000000) {
        BenchmarkResult result{};
        
        // TODO: 实现具体的基准测试
        // 1. 测试std::string性能
        // 2. 测试SdsString性能  
        // 3. 测试OptimizedNode性能
        // 4. 对比内存使用量
        
        return result;
    }
};

} // namespace db
} // namespace minkv