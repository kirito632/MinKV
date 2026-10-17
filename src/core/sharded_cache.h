#pragma once

#include <atomic>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <random>
#include <shared_mutex>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "../base/expiration_manager.h"
#include "../base/serializer.h"
#include "../persistence/group_commit.h"
#include "../persistence/wal.h"
#include "../vector/vector_ops.h"
#include "lru_cache.h"

namespace minkv {
namespace db {

/**
 * @brief 分片缓存系统
 *
 * 集成基础缓存、持久化、向量检索、过期清理与异常处理。
 *
 * @tparam K 键类型（必须支持 std::hash 和 operator==）
 * @tparam V 值类型
 * @tparam EnableCacheAlign 是否启用缓存行对齐（默认false）
 */
template <typename K, typename V, bool EnableCacheAlign = false>
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
  ShardedCache(const ShardedCache &) = delete;
  ShardedCache &operator=(const ShardedCache &) = delete;

  /**
   * @brief 查询缓存中的值
   * @param key 要查询的键
   * @return 如果命中且未过期，返回对应的值；否则返回 std::nullopt
   * @note 命中时会将该条目移到LRU链表头部（最近使用）
   */
  std::optional<V> get(const K &key);

  /**
   * @brief 写入一个键值对
   * @param key   键
   * @param value 值
   * @param ttl_ms 过期时间（毫秒），0 表示永不过期
   * @note 如果启用了持久化，写入返回前只保证 WAL entry 已进入异步提交队列，
   *       不保证已经 fsync 到磁盘。如果 WAL 序列化或入队失败，不会写内存。
   *       如果 key 已存在，会覆盖旧值并刷新 TTL
   */
  void put(const K &key, const V &value, int64_t ttl_ms = 0);

  /**
   * @brief 删除一个键值对
   * @param key 要删除的键
   * @return true 表示删除成功（key 存在），false 表示 key 不存在
   * @note 如果启用了持久化，删除返回前只保证 DELETE entry 已进入异步提交队列，
   *       不保证已经 fsync 到磁盘。如果 WAL 序列化或入队失败，不会删内存。
   */
  bool remove(const K &key);

  /**
   * @brief 返回当前缓存中存活的条目总数
   * @return 所有健康分片的 size() 之和
   * @note 被禁用的分片不计入统计；单个分片出错时跳过，不影响其他分片
   */
  size_t size() const;

  /**
   * @brief 返回缓存的最大容量（所有分片容量之和）
   * @return shard_count * capacity_per_shard
   */
  size_t capacity() const;

  /**
   * @brief 清空所有分片的缓存数据
   * @note 会持有全局一致性写锁，期间所有 put/remove 会阻塞
   */
  void clear();

  /**
   * @brief 获取聚合后的缓存统计信息
   * @return 所有健康分片的 hits/misses/evictions 等指标之和
   */
  CacheStats getStats() const;

  /**
   * @brief 重置所有分片的统计计数器（归零）
   */
  void resetStats();

  /**
   * @brief 原子 read-modify-write 操作
   *
   * 在分片锁内完成读旧值 → 应用更新函数 → 写新值的 RMW 周期，
   * 确保并发安全，消除写覆盖问题。
   *
   * 与 put() 一样会持 global_consistency_lock_ 的 shared_lock，
   * 保证与 export_all_data 的互斥。
   * 如果启用了持久化，会先把新值对应的 WAL entry 放入异步提交队列，
   * 再更新内存；不保证返回时已经 fsync 到磁盘。
   *
   * @tparam F  回调类型，签名: V(const std::optional<V>& old_val)
   * @param key      要更新的键
   * @param updater  接收旧值（std::nullopt 表示 key 不存在），返回新值
   * @return 更新后的新值
   *
   * @note 回调在分片锁内执行，应尽量轻量。
   */
  template <typename F>
  V update_in_place(const K &key, F &&updater);

  /**
   * @brief 启用 WAL 持久化
   * @param data_dir          WAL 文件存放目录
   * @param fsync_interval_ms GCM 批处理等待间隔（毫秒），默认 1000ms
   * @note 当前业务写入热路径由 GroupCommitManager 执行 sync-each-batch，
   *       该参数控制后台批次等待时间，不是 WriteAheadLog 的后台 fsync 策略。
   *       重复调用无效（已启用时直接返回）。
   */
  void enable_persistence(const std::string &data_dir, int64_t fsync_interval_ms = 1000);

  /**
   * @brief 关闭 WAL 持久化，flush 并释放 WAL 资源
   */
  void disable_persistence();

  /**
   * @brief 在全局一致性写锁下导出所有数据
   * @return 所有健康分片的键值对合并结果（std::map 有序）
   * @note 持锁期间 put/remove 会阻塞，确保导出数据的一致性
   */
  std::map<K, V> export_all_data() const;

  struct CheckpointCut {
    std::map<K, V> data;
    uint64_t snapshot_lsn {0};
    std::string rotated_wal_path;
  };

  /**
   * @brief 在独占锁下建立 checkpoint cut
   */
  CheckpointCut capture_checkpoint_cut();

  /**
   * @brief 快照成功后清理已被 snapshot_lsn 覆盖的旧 WAL
   */
  void cleanup_wal_up_to(uint64_t snapshot_lsn);

  /**
   * @brief 写入一个向量（自动序列化后存入缓存）
   * @param key    键
   * @param vec    浮点向量数据
   * @param ttl_ms 过期时间（毫秒），0 表示永不过期
   */
  void vectorPut(const K &key, const std::vector<float> &vec, int64_t ttl_ms = 0);

  /**
   * @brief 读取一个向量（自动反序列化）
   * @param key 键
   * @return 对应的浮点向量；key 不存在或反序列化失败时返回空 vector
   */
  std::vector<float> vectorGet(const K &key);

  /**
   * @brief Top-K 近似最近邻搜索（L2 距离）
   * @param query 查询向量
   * @param k     返回最近的 k 个结果
   * @return 按距离从近到远排列的 key 列表（最多 k 个）
   * @note 并行搜索所有分片（std::async），每个分片维护局部 Top-K 堆，
   *       最后合并为全局 Top-K。维度不匹配的条目会被跳过。
   */
  std::vector<K> vectorSearch(const std::vector<float> &query, int k);

  /**
   * @brief 启动定期删除服务
   * @param check_interval 检查间隔，默认100ms
   * @param sample_size 每次采样大小，默认20
   */
  void startExpirationService(
      std::chrono::milliseconds check_interval = std::chrono::milliseconds(100),
      size_t sample_size = 20);

  /**
   * @brief 停止定期删除服务
   */
  void stopExpirationService();

  /**
   * @brief 获取定期删除统计信息
   */
  base::ExpirationManager::Stats getExpirationStats() const;

  /**
   * @brief 手动触发过期清理
   */
  size_t manualExpiration(int shard_id = -1);

  /**
   * @brief 获取系统健康状态
   */
  struct HealthStatus {
    bool overall_healthy;                                    ///< 整体健康状态
    size_t healthy_shards;                                   ///< 健康分片数量
    size_t total_shards;                                     ///< 总分片数量
    std::vector<size_t> disabled_shards;                     ///< 被禁用的分片列表
    std::unordered_map<size_t, int> error_counts;            ///< 各分片错误计数
    double error_rate;                                       ///< 整体错误率
    std::chrono::steady_clock::time_point last_health_check; ///< 上次健康检查时间
  };

  HealthStatus getHealthStatus() const;

  /**
   * @brief 手动触发健康检查
   */
  void performHealthCheck();

  /**
   * @brief 获取下一个LSN（原子递增）
   * @return 新的LSN值
   *
   * global_lsn_ 存储的是下一个要分配的 LSN，
   * fetch_add 返回旧值（即当前要分配的 LSN）。
   */
  uint64_t next_lsn() { return global_lsn_.fetch_add(1, std::memory_order_relaxed); }

  /**
   * @brief 获取当前LSN（不递增）
   * @return 当前LSN值（最后分配的LSN）
   *
   * global_lsn_ 存储的是下一个要分配的 LSN，
   * 当前已分配的 LSN = global_lsn_ - 1。
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
  void reset_lsn(uint64_t lsn) { global_lsn_.store(lsn, std::memory_order_relaxed); }

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
    if (!wal_)
      return {};
    return wal_->read_after_snapshot(snapshot_lsn);
  }

  /**
   * @brief 恢复专用写入：直接写分片，不触发 WAL
   *
   * 仅供 recover_from_disk 调用，此时无并发写入。
   */
  void put_for_recovery(const K &key, const V &value) {
    size_t shard_idx = get_shard_index(key);
    shards_[shard_idx]->put(key, value, 0);
  }

  /**
   * @brief 恢复专用删除：直接删分片，不触发 WAL
   */
  void remove_for_recovery(const K &key) {
    size_t shard_idx = get_shard_index(key);
    shards_[shard_idx]->remove(key);
  }

private:
  /**
   * @brief 增强的LRU缓存分片
   */
  class EnhancedLruShard {
  public:
    EnhancedLruShard(size_t capacity);

    // 基础接口
    std::optional<V> get(const K &key);
    void put(const K &key, const V &value, int64_t ttl_ms = 0);
    bool remove(const K &key);
    /** @brief 返回该分片当前存活的条目数（加锁读取） */
    size_t size() const;
    /** @brief 返回该分片的最大容量（无锁，构造后不变） */
    size_t capacity() const;
    CacheStats getStats() const;
    void resetStats();
    void clear();
    /** @brief 返回该分片所有键值对的快照（加锁，用于导出/快照） */
    std::map<K, V> get_all() const;

    // 定期删除接口
    /** @brief 非阻塞尝试加锁，成功返回 true（供 ExpirationManager 使用） */
    bool try_lock();
    /** @brief 释放锁 */
    void unlock();
    /**
     * @brief 从分片中做有界渐进采样
     * @param sample_size 期望采样数量
     * @return 打乱后的 key 列表（实际数量 ≤ sample_size）
     * @note 调用前必须已持有该分片的锁
     */
    std::vector<K> randomSample(size_t sample_size);
    /**
     * @brief 仅删除采样列表里已过期的 key
     * @param keys 采样列表
     * @return 本次清理的过期条目数
     * @note 调用前必须已持有该分片的锁
     */
    size_t expireKeys(const std::vector<K> &keys);
    /**
     * @brief 对整个分片做一次全量过期清理
     * @return 本次清理的过期条目数
     */
    size_t cleanupExpiredKeysFullScan();

    /**
     * @brief 原子 read-modify-write
     *
     * 调用者必须已持有 global_consistency_lock_ 的 shared_lock，
     * 本方法只负责分片锁内的 RMW 操作。
     *
     * @param key      要更新的键
     * @param updater  回调函数，接收旧值的 optional，返回新值
     * @return 更新后的新值
     */
    template <typename F>
    V update_in_place(const K &key, F &&updater) {
      return update_in_place_with_precommit(
          key, std::forward<F>(updater), [](const V &) {});
    }

    template <typename F, typename Precommit>
    V update_in_place_with_precommit(const K &key, F &&updater, Precommit &&precommit) {
      std::lock_guard<std::mutex> lock(mutex_wrapper_.mutex);
      auto old_val = cache_->get(key);
      auto new_val = updater(old_val);
      precommit(new_val);
      cache_->put(key, new_val, 0);
      return new_val;
    }

  private:
    // 条件对齐的互斥锁包装
    struct alignas(EnableCacheAlign ? 64 : 1) AlignedMutex {
      mutable std::mutex mutex;
      // 填充至 64 字节（仅在对齐时生效）
      static constexpr size_t padding_size =
          EnableCacheAlign ? (64 - sizeof(std::mutex)) : 0;
      char padding[padding_size];
    } mutex_wrapper_;

    std::unique_ptr<LruCache<K, V, false>>
        cache_; // ThreadSafe=false：锁由 mutex_wrapper_ 统一管理，消除双重加锁
    mutable std::mt19937 rng_;
  };

  std::vector<std::unique_ptr<EnhancedLruShard>> shards_;

  std::unique_ptr<WriteAheadLog> wal_;
  std::unique_ptr<base::GroupCommitManager> gcm_;
  std::atomic<bool> persistence_enabled_ {false};
  std::string persistence_data_dir_;
  int64_t persistence_fsync_interval_ms_ {1000};
  mutable std::shared_mutex global_consistency_lock_;
  mutable std::mutex persistence_mutex_;

  std::atomic<uint64_t> global_lsn_ {1}; ///< 全局LSN计数器，从1开始，严格单调递增

  std::unique_ptr<base::ExpirationManager> expiration_manager_;

  mutable std::mutex health_mutex_;
  std::unordered_map<size_t, int> shard_error_counts_;
  std::unordered_set<size_t> disabled_shards_;
  std::chrono::steady_clock::time_point last_health_check_;

  static constexpr int MAX_CONSECUTIVE_ERRORS = 5;
  static constexpr auto HEALTH_CHECK_INTERVAL = std::chrono::minutes(1);

  size_t get_shard_index(const K &key) const;
  size_t expirationCallback(size_t shard_id, size_t sample_size);
  void recordShardError(size_t shard_id);
  void recordShardSuccess(size_t shard_id);
  bool isShardDisabled(size_t shard_id) const;
};

template <typename K, typename V, bool EnableCacheAlign>
ShardedCache<K, V, EnableCacheAlign>::ShardedCache(size_t capacity_per_shard,
                                                   size_t shard_count)
    : last_health_check_(std::chrono::steady_clock::now()) {
  if (shard_count == 0) {
    throw std::invalid_argument("ShardedCache: shard_count must be > 0");
  }

  // 创建分片
  for (size_t i = 0; i < shard_count; ++i) {
    shards_.push_back(std::make_unique<EnhancedLruShard>(capacity_per_shard));
  }
}

template <typename K, typename V, bool EnableCacheAlign>
ShardedCache<K, V, EnableCacheAlign>::~ShardedCache() {
  // expiration_manager_ 析构时自动 join 后台线程，无需手动调用
  // stopExpirationService()
  expiration_manager_.reset();
  disable_persistence();
}

template <typename K, typename V, bool EnableCacheAlign>
size_t ShardedCache<K, V, EnableCacheAlign>::get_shard_index(const K &key) const {
  std::hash<K> hasher;
  return hasher(key) % shards_.size();
}

template <typename K, typename V, bool EnableCacheAlign>
std::optional<V> ShardedCache<K, V, EnableCacheAlign>::get(const K &key) {
  size_t shard_idx = get_shard_index(key);

  // EnhancedLruShard::get() 是 noexcept 操作，无需健康检查
  return shards_[shard_idx]->get(key);
}

template <typename K, typename V, bool EnableCacheAlign>
void ShardedCache<K, V, EnableCacheAlign>::put(const K &key,
                                               const V &value,
                                               int64_t ttl_ms) {
  const bool persistence_active = persistence_enabled_.load(std::memory_order_acquire);

  std::shared_lock<std::shared_mutex> consistency_lock(global_consistency_lock_,
                                                       std::defer_lock);
  if (persistence_active) [[unlikely]] {
    consistency_lock.lock();
  }

  size_t shard_idx = get_shard_index(key);

  const bool write_wal =
      persistence_active && persistence_enabled_.load(std::memory_order_acquire) && gcm_;
  if (write_wal) [[unlikely]] {
    std::string serialized;
    try {
      LogEntry entry;
      entry.op = LogEntry::PUT;
      entry.key = Serializer<K>::serialize(key);
      entry.value = Serializer<V>::serialize(value);
      entry.timestamp_ms =
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::high_resolution_clock::now().time_since_epoch())
              .count();
      entry.lsn = next_lsn();
      serialized = WriteAheadLog::serialize_entry(entry);
    } catch (const std::exception &e) {
      throw std::runtime_error("[WAL] put serialization failed: " +
                               std::string(e.what()));
    }

    if (!gcm_->commitAsync(std::move(serialized))) {
      throw std::runtime_error("[WAL] put enqueue failed");
    }
  }

  // 写内存
  try {
    shards_[shard_idx]->put(key, value, ttl_ms);
  } catch (const std::exception &e) {
    // Shard put 是 noexcept 操作，仅捕获极端异常
    recordShardError(shard_idx);
  }
}

template <typename K, typename V, bool EnableCacheAlign>
template <typename F>
V ShardedCache<K, V, EnableCacheAlign>::update_in_place(const K &key, F &&updater) {
  const bool persistence_active = persistence_enabled_.load(std::memory_order_acquire);

  std::shared_lock<std::shared_mutex> consistency_lock(global_consistency_lock_,
                                                       std::defer_lock);
  if (persistence_active) [[unlikely]] {
    consistency_lock.lock();
  }

  size_t shard_idx = get_shard_index(key);

  const bool write_wal =
      persistence_active && persistence_enabled_.load(std::memory_order_acquire) && gcm_;

  bool wal_precommit_failed = false;
  try {
    return shards_[shard_idx]->update_in_place_with_precommit(
        key, std::forward<F>(updater), [&](const V &new_val) {
          if (!write_wal) [[likely]] {
            return;
          }

          try {
            LogEntry entry;
            entry.op = LogEntry::PUT;
            entry.key = Serializer<K>::serialize(key);
            entry.value = Serializer<V>::serialize(new_val);
            entry.timestamp_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::high_resolution_clock::now().time_since_epoch())
                    .count();
            entry.lsn = next_lsn();
            std::string serialized = WriteAheadLog::serialize_entry(entry);
            if (!gcm_->commitAsync(std::move(serialized))) {
              throw std::runtime_error("enqueue failed");
            }
          } catch (const std::exception &e) {
            wal_precommit_failed = true;
            throw std::runtime_error("[WAL] update_in_place precommit failed: " +
                                     std::string(e.what()));
          }
        });
  } catch (const std::exception &e) {
    if (!wal_precommit_failed) {
      recordShardError(shard_idx);
    }
    throw;
  }
}

template <typename K, typename V, bool EnableCacheAlign>
bool ShardedCache<K, V, EnableCacheAlign>::remove(const K &key) {
  const bool persistence_active = persistence_enabled_.load(std::memory_order_acquire);

  std::shared_lock<std::shared_mutex> consistency_lock(global_consistency_lock_,
                                                       std::defer_lock);
  if (persistence_active) [[unlikely]] {
    consistency_lock.lock();
  }

  size_t shard_idx = get_shard_index(key);

  const bool write_wal =
      persistence_active && persistence_enabled_.load(std::memory_order_acquire) && gcm_;
  if (write_wal) [[unlikely]] {
    std::string serialized;
    try {
      LogEntry entry;
      entry.op = LogEntry::DELETE;
      entry.key = Serializer<K>::serialize(key);
      entry.timestamp_ms =
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::high_resolution_clock::now().time_since_epoch())
              .count();
      entry.lsn = next_lsn();
      serialized = WriteAheadLog::serialize_entry(entry);
    } catch (const std::exception &e) {
      throw std::runtime_error("[WAL] remove serialization failed: " +
                               std::string(e.what()));
    }

    if (!gcm_->commitAsync(std::move(serialized))) {
      throw std::runtime_error("[WAL] remove enqueue failed");
    }
  }

  // 删内存数据
  try {
    return shards_[shard_idx]->remove(key);
  } catch (const std::exception &e) {
    recordShardError(shard_idx);
    return false;
  }
}

template <typename K, typename V, bool EnableCacheAlign>
size_t ShardedCache<K, V, EnableCacheAlign>::size() const {
  size_t total = 0;
  // 遍历所有分片，跳过被禁用的分片，累加各分片的存活条目数
  for (size_t i = 0; i < shards_.size(); ++i) {
    if (!isShardDisabled(i)) {
      try {
        total += shards_[i]->size();
      } catch (...) {
        // 忽略单个分片的错误，不影响其他分片的统计
      }
    }
  }
  return total;
}

template <typename K, typename V, bool EnableCacheAlign>
size_t ShardedCache<K, V, EnableCacheAlign>::capacity() const {
  return shards_.size() * (shards_.empty() ? 0 : shards_[0]->capacity());
}

template <typename K, typename V, bool EnableCacheAlign>
void ShardedCache<K, V, EnableCacheAlign>::clear() {
  std::unique_lock<std::shared_mutex> consistency_lock(global_consistency_lock_);

  for (size_t i = 0; i < shards_.size(); ++i) {
    if (!isShardDisabled(i)) {
      try {
        shards_[i]->clear();
        recordShardSuccess(i);
      } catch (const std::exception &e) {
        recordShardError(i);
      }
    }
  }
}

template <typename K, typename V, bool EnableCacheAlign>
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

template <typename K, typename V, bool EnableCacheAlign>
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

template <typename K, typename V, bool EnableCacheAlign>
void ShardedCache<K, V, EnableCacheAlign>::enable_persistence(const std::string &data_dir,
                                                              int64_t fsync_interval_ms) {
  std::lock_guard<std::mutex> lock(persistence_mutex_);

  if (persistence_enabled_.load(std::memory_order_acquire)) {
    return; // 已启用
  }

  std::unique_ptr<WriteAheadLog> new_wal;
  std::unique_ptr<base::GroupCommitManager> new_gcm;

  try {
    new_wal = std::make_unique<WriteAheadLog>(data_dir, 1024 * 1024, fsync_interval_ms);
    new_gcm = std::make_unique<base::GroupCommitManager>(
        new_wal->current_wal_path(), 4096, std::chrono::milliseconds(fsync_interval_ms));
    new_gcm->start();

    std::unique_lock<std::shared_mutex> consistency_lock(global_consistency_lock_);
    wal_ = std::move(new_wal);
    gcm_ = std::move(new_gcm);
    persistence_data_dir_ = data_dir;
    persistence_fsync_interval_ms_ = fsync_interval_ms;
    persistence_enabled_.store(true, std::memory_order_release);
  } catch (const std::exception &e) {
    std::cerr << "[Persistence] Failed to enable: " << e.what() << std::endl;
    if (new_gcm) {
      new_gcm->stop();
    }
    persistence_enabled_.store(false, std::memory_order_release);
  }
}

template <typename K, typename V, bool EnableCacheAlign>
void ShardedCache<K, V, EnableCacheAlign>::disable_persistence() {
  std::unique_ptr<base::GroupCommitManager> old_gcm;
  std::unique_ptr<WriteAheadLog> old_wal;

  std::lock_guard<std::mutex> lock(persistence_mutex_);

  if (!persistence_enabled_.load(std::memory_order_acquire)) {
    return;
  }

  {
    std::unique_lock<std::shared_mutex> consistency_lock(global_consistency_lock_);
    if (!persistence_enabled_.load(std::memory_order_acquire)) {
      return;
    }

    persistence_enabled_.store(false, std::memory_order_release);
    old_gcm = std::move(gcm_);
    old_wal = std::move(wal_);
    persistence_data_dir_.clear();
  }

  // 全局锁已释放；继续持有 persistence_mutex_，避免并发 enable 复用同一路径。
  if (old_gcm) {
    old_gcm->flush();
    old_gcm->stop();
  }

  if (old_wal) {
    old_wal->flush();
  }
}

template <typename K, typename V, bool EnableCacheAlign>
std::map<K, V> ShardedCache<K, V, EnableCacheAlign>::export_all_data() const {
  std::unique_lock<std::shared_mutex> consistency_lock(global_consistency_lock_);

  std::map<K, V> all_data;

  for (size_t i = 0; i < shards_.size(); ++i) {
    if (!isShardDisabled(i)) {
      try {
        auto shard_data = shards_[i]->get_all();
        all_data.insert(shard_data.begin(), shard_data.end());
      } catch (const std::exception &e) {
        std::cerr << "[Export] Shard " << i << " error: " << e.what() << std::endl;
      }
    }
  }

  return all_data;
}

template <typename K, typename V, bool EnableCacheAlign>
typename ShardedCache<K, V, EnableCacheAlign>::CheckpointCut
ShardedCache<K, V, EnableCacheAlign>::capture_checkpoint_cut() {
  CheckpointCut cut;
  std::lock_guard<std::mutex> lock(persistence_mutex_);
  std::unique_lock<std::shared_mutex> consistency_lock(global_consistency_lock_);

  if (gcm_) {
    gcm_->flush();
    gcm_->stop();
    gcm_.reset();
  }
  if (wal_) {
    wal_->flush();
    cut.rotated_wal_path = wal_->rotate();
  }

  cut.data.clear();
  for (size_t i = 0; i < shards_.size(); ++i) {
    if (!isShardDisabled(i)) {
      try {
        auto shard_data = shards_[i]->get_all();
        cut.data.insert(shard_data.begin(), shard_data.end());
      } catch (const std::exception &e) {
        std::cerr << "[Checkpoint] Shard " << i << " export error: " << e.what()
                  << std::endl;
      }
    }
  }
  cut.snapshot_lsn = current_lsn();

  if (wal_ && !persistence_data_dir_.empty()) {
    gcm_ = std::make_unique<base::GroupCommitManager>(
        wal_->current_wal_path(),
        4096,
        std::chrono::milliseconds(persistence_fsync_interval_ms_));
    gcm_->start();
  }

  return cut;
}

template <typename K, typename V, bool EnableCacheAlign>
void ShardedCache<K, V, EnableCacheAlign>::cleanup_wal_up_to(uint64_t snapshot_lsn) {
  std::lock_guard<std::mutex> lock(persistence_mutex_);
  if (wal_) {
    wal_->cleanup_logs_up_to(snapshot_lsn);
  }
}

template <typename K, typename V, bool EnableCacheAlign>
void ShardedCache<K, V, EnableCacheAlign>::vectorPut(const K &key,
                                                     const std::vector<float> &vec,
                                                     int64_t ttl_ms) {
  // 将向量序列化为字符串
  std::string serialized_vec = VectorOps::Serialize(vec);

  // 使用基础 put 接口存储
  put(key, serialized_vec, ttl_ms);
}

template <typename K, typename V, bool EnableCacheAlign>
std::vector<float> ShardedCache<K, V, EnableCacheAlign>::vectorGet(const K &key) {
  auto result = get(key);
  if (!result.has_value()) {
    return {};
  }

  // 反序列化向量数据
  return VectorOps::DeserializeCopy(*result);
}

template <typename K, typename V, bool EnableCacheAlign>
std::vector<K> ShardedCache<K, V, EnableCacheAlign>::vectorSearch(
    const std::vector<float> &query, int k) {
  struct SearchResult {
    K key;
    float distance;

    SearchResult(const K &k, float d) : key(k), distance(d) {}

    bool operator<(const SearchResult &other) const {
      return distance < other.distance; // 大顶堆
    }
  };

  // 并行搜索所有分片
  std::vector<std::future<std::vector<SearchResult>>> futures;

  for (size_t shard_idx = 0; shard_idx < shards_.size(); ++shard_idx) {
    if (isShardDisabled(shard_idx)) {
      continue; // 跳过被禁用的分片
    }

    futures.push_back(std::async(std::launch::async, [this, shard_idx, &query, k]() {
      std::vector<SearchResult> local_results;
      std::priority_queue<SearchResult> heap;

      try {
        auto all_data = shards_[shard_idx]->get_all();

        for (const auto &[key, raw_data] : all_data) {
          auto vec_data = VectorOps::DeserializeCopy(raw_data);

          if (vec_data.empty() || vec_data.size() != query.size()) {
            continue; // 维度不匹配
          }

          float distance =
              VectorOps::L2DistanceSquare(query.data(), vec_data.data(), vec_data.size());

          heap.push(SearchResult(key, distance));
          if ((int)heap.size() > k) {
            heap.pop();
          }
        }

        while (!heap.empty()) {
          local_results.push_back(heap.top());
          heap.pop();
        }

      } catch (const std::exception &e) {
        // 单个分片错误不影响整体搜索
        std::cerr << "[VectorSearch] Shard " << shard_idx << " error: " << e.what()
                  << std::endl;
      }

      return local_results;
    }));
  }

  // 收集所有分片结果
  std::priority_queue<SearchResult> global_heap;

  for (auto &f : futures) {
    try {
      auto shard_results = f.get();
      for (const auto &res : shard_results) {
        global_heap.push(res);
        if ((int)global_heap.size() > k) {
          global_heap.pop();
        }
      }
    } catch (const std::exception &e) {
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

template <typename K, typename V, bool EnableCacheAlign>
void ShardedCache<K, V, EnableCacheAlign>::startExpirationService(
    std::chrono::milliseconds check_interval, size_t sample_size) {
  if (expiration_manager_) {
    return; // 已启动
  }

  // 构造函数自动启动线程
  expiration_manager_ = std::make_unique<base::ExpirationManager>(
      [this](size_t shard_id, size_t sample_size) {
        return this->expirationCallback(shard_id, sample_size);
      },
      shards_.size(),
      check_interval,
      sample_size);
}

template <typename K, typename V, bool EnableCacheAlign>
void ShardedCache<K, V, EnableCacheAlign>::stopExpirationService() {
  // 析构函数自动停止线程，只需重置指针
  expiration_manager_.reset();
}

template <typename K, typename V, bool EnableCacheAlign>
size_t ShardedCache<K, V, EnableCacheAlign>::expirationCallback(size_t shard_id,
                                                                size_t sample_size) {
  if (shard_id >= shards_.size() || isShardDisabled(shard_id)) {
    return 0;
  }

  auto &shard = shards_[shard_id];

  // 非阻塞锁：避免与业务线程竞争
  if (!shard->try_lock()) {
    // 锁竞争不算错误，返回 SIZE_MAX 作为哨兵值
    return SIZE_MAX;
  }

  // RAII锁管理
  struct LockGuard {
    EnhancedLruShard *shard_;

    ~LockGuard() { shard_->unlock(); }
  } guard {shard.get()};

  try {
    // 随机采样并删除过期key
    auto keys = shard->randomSample(sample_size);
    size_t expired_count = shard->expireKeys(keys);

    // 成功处理，重置错误计数
    recordShardSuccess(shard_id);

    return expired_count;

  } catch (const std::exception &e) {
    // 真正的异常才记录错误
    recordShardError(shard_id);
    return 0;
  }
}

template <typename K, typename V, bool EnableCacheAlign>
base::ExpirationManager::Stats ShardedCache<K, V, EnableCacheAlign>::getExpirationStats()
    const {
  if (expiration_manager_) {
    return expiration_manager_->getStats();
  }
  return {};
}

template <typename K, typename V, bool EnableCacheAlign>
size_t ShardedCache<K, V, EnableCacheAlign>::manualExpiration(int shard_id) {
  size_t total_expired = 0;

  if (shard_id == -1) {
    for (auto &shard : shards_) {
      total_expired += shard->cleanupExpiredKeysFullScan();
    }
  } else if (shard_id >= 0 && shard_id < static_cast<int>(shards_.size())) {
    total_expired = shards_[static_cast<size_t>(shard_id)]->cleanupExpiredKeysFullScan();
  }

  return total_expired;
}

template <typename K, typename V, bool EnableCacheAlign>
void ShardedCache<K, V, EnableCacheAlign>::recordShardError(size_t shard_id) {
  std::lock_guard<std::mutex> lock(health_mutex_);

  shard_error_counts_[shard_id]++;

  if (shard_error_counts_[shard_id] >= MAX_CONSECUTIVE_ERRORS) {
    disabled_shards_.insert(shard_id);
  }
}

template <typename K, typename V, bool EnableCacheAlign>
void ShardedCache<K, V, EnableCacheAlign>::recordShardSuccess(size_t shard_id) {
  std::lock_guard<std::mutex> lock(health_mutex_);
  shard_error_counts_[shard_id] = 0; // 重置错误计数
}

template <typename K, typename V, bool EnableCacheAlign>
bool ShardedCache<K, V, EnableCacheAlign>::isShardDisabled(size_t shard_id) const {
  std::lock_guard<std::mutex> lock(health_mutex_);
  return disabled_shards_.count(shard_id) > 0;
}

template <typename K, typename V, bool EnableCacheAlign>
typename ShardedCache<K, V, EnableCacheAlign>::HealthStatus
ShardedCache<K, V, EnableCacheAlign>::getHealthStatus() const {
  std::lock_guard<std::mutex> lock(health_mutex_);

  HealthStatus status;
  status.total_shards = shards_.size();
  status.healthy_shards = status.total_shards - disabled_shards_.size();
  status.overall_healthy =
      (status.healthy_shards > status.total_shards / 2); // 超过一半健康
  status.disabled_shards.assign(disabled_shards_.begin(), disabled_shards_.end());
  status.error_counts = shard_error_counts_;
  status.last_health_check = last_health_check_;

  // 计算错误率
  int total_errors = 0;
  for (const auto &[shard_id, count] : shard_error_counts_) {
    total_errors += count;
  }
  status.error_rate =
      static_cast<double>(total_errors) / (status.total_shards * MAX_CONSECUTIVE_ERRORS);

  return status;
}

template <typename K, typename V, bool EnableCacheAlign>
void ShardedCache<K, V, EnableCacheAlign>::performHealthCheck() {
  std::lock_guard<std::mutex> lock(health_mutex_);

  last_health_check_ = std::chrono::steady_clock::now();

  // 尝试重新启用被禁用的分片
  for (auto it = disabled_shards_.begin(); it != disabled_shards_.end();) {
    size_t shard_id = *it;

    try {
      // 测试分片健康状态
      auto test_key = K {};
      shards_[shard_id]->get(test_key);

      // 重新启用
      shard_error_counts_[shard_id] = 0;
      it = disabled_shards_.erase(it);
    } catch (...) {
      // 保持禁用
      ++it;
    }
  }
}

template <typename K, typename V, bool EnableCacheAlign>
ShardedCache<K, V, EnableCacheAlign>::EnhancedLruShard::EnhancedLruShard(size_t capacity)
    : cache_(std::make_unique<LruCache<K, V, false>>(capacity)),
      rng_(std::random_device {}()) {}

template <typename K, typename V, bool EnableCacheAlign>
bool ShardedCache<K, V, EnableCacheAlign>::EnhancedLruShard::try_lock() {
  return mutex_wrapper_.mutex.try_lock();
}

template <typename K, typename V, bool EnableCacheAlign>
void ShardedCache<K, V, EnableCacheAlign>::EnhancedLruShard::unlock() {
  mutex_wrapper_.mutex.unlock();
}

template <typename K, typename V, bool EnableCacheAlign>
std::optional<V> ShardedCache<K, V, EnableCacheAlign>::EnhancedLruShard::get(
    const K &key) {
  std::lock_guard<std::mutex> lock(mutex_wrapper_.mutex);
  return cache_->get(key);
}

template <typename K, typename V, bool EnableCacheAlign>
void ShardedCache<K, V, EnableCacheAlign>::EnhancedLruShard::put(const K &key,
                                                                 const V &value,
                                                                 int64_t ttl_ms) {
  std::lock_guard<std::mutex> lock(mutex_wrapper_.mutex);
  cache_->put(key, value, ttl_ms);
}

template <typename K, typename V, bool EnableCacheAlign>
bool ShardedCache<K, V, EnableCacheAlign>::EnhancedLruShard::remove(const K &key) {
  std::lock_guard<std::mutex> lock(mutex_wrapper_.mutex);
  return cache_->remove(key);
}

template <typename K, typename V, bool EnableCacheAlign>
size_t ShardedCache<K, V, EnableCacheAlign>::EnhancedLruShard::size() const {
  std::lock_guard<std::mutex> lock(mutex_wrapper_.mutex);
  return cache_->size();
}

template <typename K, typename V, bool EnableCacheAlign>
size_t ShardedCache<K, V, EnableCacheAlign>::EnhancedLruShard::capacity() const {
  return cache_->capacity();
}

template <typename K, typename V, bool EnableCacheAlign>
CacheStats ShardedCache<K, V, EnableCacheAlign>::EnhancedLruShard::getStats() const {
  std::lock_guard<std::mutex> lock(mutex_wrapper_.mutex);
  return cache_->getStats();
}

template <typename K, typename V, bool EnableCacheAlign>
void ShardedCache<K, V, EnableCacheAlign>::EnhancedLruShard::resetStats() {
  std::lock_guard<std::mutex> lock(mutex_wrapper_.mutex);
  cache_->resetStats();
}

template <typename K, typename V, bool EnableCacheAlign>
void ShardedCache<K, V, EnableCacheAlign>::EnhancedLruShard::clear() {
  std::lock_guard<std::mutex> lock(mutex_wrapper_.mutex);
  cache_->clear();
}

template <typename K, typename V, bool EnableCacheAlign>
std::map<K, V> ShardedCache<K, V, EnableCacheAlign>::EnhancedLruShard::get_all() const {
  std::lock_guard<std::mutex> lock(mutex_wrapper_.mutex);
  return cache_->get_all();
}

template <typename K, typename V, bool EnableCacheAlign>
std::vector<K> ShardedCache<K, V, EnableCacheAlign>::EnhancedLruShard::randomSample(
    size_t sample_size) {
  // 调用前必须已获取锁
  return cache_->sample_keys(sample_size, rng_);
}

template <typename K, typename V, bool EnableCacheAlign>
size_t ShardedCache<K, V, EnableCacheAlign>::EnhancedLruShard::expireKeys(
    const std::vector<K> &keys) {
  // 调用前必须已获取 EnhancedLruShard 的锁
  // LruCache<K,V,false> 内部使用 NullMutex，锁由外层 mutex_wrapper_ 统一管理
  return cache_->expire_keys(keys);
}

template <typename K, typename V, bool EnableCacheAlign>
size_t
ShardedCache<K, V, EnableCacheAlign>::EnhancedLruShard::cleanupExpiredKeysFullScan() {
  std::lock_guard<std::mutex> lock(mutex_wrapper_.mutex);
  return cache_->cleanup_expired_keys();
}

} // namespace db
} // namespace minkv
