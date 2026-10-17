#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "../base/append_file.h"

namespace minkv {
namespace base {

/**
 * @brief Group Commit 批量提交管理器
 *
 * 将多个写入操作批量提交到磁盘，大幅减少 fsync() 系统调用次数，提升 I/O 性能。
 *
 * 设计特点：
 * - 双重触发机制：批量大小阈值 + 时间间隔阈值
 * - 异步处理：前端提交不阻塞，后台线程负责实际 I/O
 * - 批次统一回调：同一批次共享一次 append + fsync 的成功/失败结果
 * - 回调机制：支持异步和同步两种提交方式
 *
 * 当前实现采用 sync-each-batch 持久化策略：每个后台批次都会写入 WAL 文件并
 * 调用 fsync。GroupCommitManager 直接持有 AppendFile，因此它既负责聚合，也负责
 * 当前热路径的实际 append/fsync。
 *
 * @note 批次统一回调不等于事务级原子提交。如果底层 write 在部分写入后失败，
 *       文件中可能已经包含该批次的前缀数据。
 *
 * 这是 MySQL InnoDB、PostgreSQL 等数据库系统的核心技术，
 * 在 MinKV 中用于 WAL 日志的高性能写入。
 *
 * @note 线程安全，支持多线程并发提交
 */
class GroupCommitManager {
public:
  /**
   * @brief 提交完成回调函数类型
   * @param success true表示提交成功，false表示失败
   */
  using CommitCallback = std::function<void(bool success)>;

  /**
   * @brief 提交请求结构体
   *
   * 封装单个提交请求的所有信息，包括数据、回调和时间戳
   */
  struct CommitRequest {
    std::string data;                                ///< 要提交的数据
    CommitCallback callback;                         ///< 完成回调函数
    std::chrono::steady_clock::time_point timestamp; ///< 请求时间戳，用于延迟统计

    /**
     * @brief 构造提交请求
     * @param d 要提交的数据
     * @param cb 完成回调函数
     *
     * 使用 move 语义避免不必要的数据拷贝
     */
    CommitRequest(std::string d, CommitCallback cb)
        : data(std::move(d)),
          callback(std::move(cb)),
          timestamp(std::chrono::steady_clock::now()) {}
  };

  /**
   * @brief 构造Group Commit管理器
   * @param filename 目标文件名
   * @param batchSize 批量大小阈值，默认4KB
   * @param syncInterval 后台批处理等待间隔，默认10ms
   *
   * 双重触发机制：
   * - batchSize 用于判断当前积累的数据是否足够形成一个批次
   * - syncInterval 控制后台线程检查并处理 pending 队列的等待间隔
   *
   * 当前等待逻辑以 syncInterval 为主要唤醒节奏；请求入队时的 notify 不保证
   * 立即执行 I/O。syncInterval 不是 Redis-style everysec fsync 策略，而是 GCM
   * 的批处理等待参数。实际 fsync 发生在每个成功处理的 batch 内。
   */
  explicit GroupCommitManager(const std::string &filename,
                              size_t batchSize = 4096, ///< 4KB批量大小，平衡内存和I/O效率
                              std::chrono::milliseconds syncInterval =
                                  std::chrono::milliseconds(10)); ///< 10ms最大延迟

  /**
   * @brief 析构函数，确保资源正确释放
   *
   * 析构时自动停止后台线程，处理剩余请求
   */
  ~GroupCommitManager();

  // 禁止拷贝和赋值，确保资源管理的唯一性
  GroupCommitManager(const GroupCommitManager &) = delete;
  GroupCommitManager &operator=(const GroupCommitManager &) = delete;

  /**
   * @brief 异步提交数据
   * @param data 要提交的数据
   * @param callback 完成回调函数，可选
   *
   * 前端线程调用此接口提交数据，立即返回不阻塞。
   * @return true 表示数据已进入待处理队列，false 表示管理器已停止。
   *         入队成功不代表已经 append/fsync 到磁盘，实际 I/O 由后台线程批量处理。
   */
  bool commitAsync(std::string data, CommitCallback callback = nullptr);

  /**
   * @brief 同步提交数据（阻塞等待完成）
   * @param data 要提交的数据
   * @return true表示提交成功，false表示失败
   *
   * 内部使用 promise/future 机制实现同步等待
   * 适用于需要确认提交结果的场景
   */
  bool commitSync(const std::string &data);

  /**
   * @brief 强制刷新所有待提交数据
   *
   * 立即处理当前队列中的所有请求，不等待批量或时间阈值
   * 通常在程序退出前调用，确保数据完整性
   */
  void flush();

  /**
   * @brief 启动后台同步线程
   *
   * 重复调用安全，线程已运行时直接返回。
   */
  void start();

  /**
   * @brief 停止后台同步线程
   *
   * 等待后台线程完成所有工作后再退出；重复调用安全。
   */
  void stop();

  /**
   * @brief 性能统计信息结构体
   *
   * 提供详细的性能指标，用于监控和调优
   */
  struct Stats {
    uint64_t totalCommits;                ///< 总提交请求数
    uint64_t totalBatches;                ///< 总批次数
    uint64_t totalBytes;                  ///< 总字节数
    double avgBatchSize;                  ///< 平均批次大小
    std::chrono::milliseconds avgLatency; ///< 平均延迟
  };

  /**
   * @brief 获取性能统计信息
   * @return 当前的性能统计数据
   *
   * 用于性能分析和系统调优
   */
  Stats getStats() const;

private:
  /**
   * @brief 后台同步线程主函数
   *
   * 使用条件变量等待提交请求，
   * 根据双重触发条件决定何时批量处理
   */
  void syncThreadFunc();

  /**
   * @brief 处理当前批次的所有请求
   *
   * 批量写入文件并调用 fsync()，
   * 同一批次的所有请求会收到相同的成功/失败状态。
   *
   * @note 这里的相同状态来自同一次 append + fsync 调用结果，不提供底层文件写入
   *       失败时的回滚或 batch framing。
   */
  void processBatch();

  /**
   * @brief 将一个批次合并为连续缓冲区后一次写入并 fsync
   * @return true 表示写入和同步成功，false 表示 I/O 失败
   */
  bool writeBatch(const std::vector<CommitRequest> &batch, size_t batchBytes);

  /**
   * @brief 检查是否应该触发同步
   * @return true表示应该同步，false表示继续等待
   *
   * 检查批量大小和时间间隔两个条件
   */
  bool shouldSync() const;

  std::unique_ptr<AppendFile> file_;             ///< 文件写入器，封装系统调用
  const size_t batchSize_;                       ///< 批量大小阈值
  const std::chrono::milliseconds syncInterval_; ///< 后台批处理等待间隔

  std::atomic<bool> running_; ///< 运行状态标志，原子操作保证线程安全
  std::thread syncThread_;    ///< 后台同步线程
  std::mutex lifecycleMutex_; ///< 保护后台线程启动/停止生命周期

  // 保护共享数据的同步原语
  mutable std::mutex mutex_;                  ///< 保护请求队列的互斥锁
  std::condition_variable cond_;              ///< 条件变量，用于线程间通信
  std::queue<CommitRequest> pendingRequests_; ///< 待处理请求队列

  // 性能统计数据，使用独立的锁避免影响主路径性能
  mutable std::mutex statsMutex_;                      ///< 保护统计数据的互斥锁
  uint64_t totalCommits_;                              ///< 总提交数统计
  uint64_t totalBatches_;                              ///< 总批次数统计
  uint64_t totalBytes_;                                ///< 总字节数统计
  std::chrono::steady_clock::time_point lastSyncTime_; ///< 上次同步时间

  // 当前批次状态信息
  size_t currentBatchSize_;                              ///< 当前批次累积大小
  std::chrono::steady_clock::time_point batchStartTime_; ///< 当前批次开始时间
};

} // namespace base
} // namespace minkv
