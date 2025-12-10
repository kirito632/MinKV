#pragma once

#include <atomic>
#include <optional>
#include <string>
#include <memory>
#include <mutex>
#include <list>
#include <array>
#include <chrono>

namespace minkv {
namespace db {

/**
 * @brief 乐观锁 LRU 缓存 (Optimistic LRU Cache)
 * 
 * 核心优化：
 * 1. 哈希表部分使用无锁设计（CAS 操作）。
 * 2. LRU 链表部分使用细粒度锁（仅在修改链表时加锁）。
 * 3. 读操作尽量避免加锁，只在必要时才获取锁。
 * 
 * 设计说明：
 * - 这不是严格的 Lock-Free（学术定义）。
 * - 而是"读写分离 + 细粒度锁"的混合策略。
 * - 完全无锁的双向链表实现（如 Harris List）过于复杂，容易引入 Bug。
 * - 所以采用了折中方案：读取哈希表无锁，修改链表加锁。
 * 
 * 性能特点：
 * - 高并发读：多个线程可以同时读取哈希表，无竞争。
 * - 低延迟写：写操作使用 CAS 循环，避免上下文切换。
 * - 内存安全：使用 std::atomic 和 memory_order 保证可见性。
 * 
 * 面试讲述：
 * "我采用了无锁读取 + 细粒度锁更新的混合策略。
 *  完全无锁的双向链表极其复杂，这个折中方案在工程上更实用。"
 */
class OptimisticLruCache {
public:
    explicit OptimisticLruCache(size_t capacity);

    OptimisticLruCache(const OptimisticLruCache&) = delete;
    OptimisticLruCache& operator=(const OptimisticLruCache&) = delete;

    /**
     * @brief 获取数据（尽量无锁）
     * 
     * 流程：
     * 1. 用原子操作查找哈希表（无锁）。
     * 2. 如果找到，检查过期时间（无锁）。
     * 3. 如果需要更新 LRU 顺序，才加锁修改链表。
     */
    std::optional<std::string> get(const std::string& key);

    /**
     * @brief 插入或更新数据
     * 
     * 流程：
     * 1. 用 CAS 操作尝试插入哈希表（无锁重试）。
     * 2. 如果容量满，加锁删除尾部节点。
     * 3. 加锁插入新节点到链表头部。
     */
    void put(const std::string& key, const std::string& value, int64_t ttl_ms = 0);

    /**
     * @brief 删除数据
     */
    bool remove(const std::string& key);

    size_t size() const;

private:
    size_t capacity_;

    // 链表节点
    struct Node {
        std::string key;
        std::string value;
        int64_t expiry_time_ms;
    };

    // 链表（用于维护 LRU 顺序）
    std::list<Node> cache_list_;
    
    // 哈希表：Key -> 链表迭代器
    using ListIterator = std::list<Node>::iterator;
    
    // 简化的无锁哈希表实现：用 std::atomic 包装迭代器
    struct AtomicEntry {
        // 存储迭代器的原始指针（指向 std::list 内部的节点）
        std::atomic<void*> iter_ptr{nullptr};
    };
    
    // 固定大小的哈希表（为了简化，不做动态扩展）
    static constexpr size_t HASH_TABLE_SIZE = 1024;
    std::array<AtomicEntry, HASH_TABLE_SIZE> hash_table_;

    // 保护链表修改的锁（只在修改链表时使用）
    mutable std::mutex list_mutex_;

    // 保护哈希表内存分配的锁（CAS 失败时使用）
    mutable std::mutex hash_mutex_;

    // 辅助函数
    size_t hash_key(const std::string& key) const;
    bool is_expired(const Node& node) const;
    static int64_t current_time_ms();
};

} // namespace db
} // namespace minkv
