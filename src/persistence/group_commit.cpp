#include "group_commit.h"

#include <future>
#include <iomanip>
#include <iostream>
#include <mutex>

namespace minkv {

#ifdef GCM_LATENCY_TRACE
namespace {
// 保护验证日志 stdout 输出的互斥锁，防止多线程日志交错
std::mutex g_validate_log_mutex;

// ==== 延迟追踪统计（thread_local 累积报告） ====
struct GcmLatencyTrace {
  // commitAsync: 等待 mutex_ 的时间
  uint64_t commit_async_lock_wait_ns_sum = 0;
  uint64_t commit_async_lock_wait_count = 0;
  uint64_t commit_async_lock_wait_max_ns = 0;

  // processBatch: append + sync 耗时
  uint64_t batch_append_ns_sum = 0;
  uint64_t batch_sync_ns_sum = 0;
  uint64_t batch_count = 0;
  uint64_t batch_requests_sum = 0;
  uint64_t batch_bytes_sum = 0;

  // syncThreadFunc: 从 wait_for 返回到锁释放的总耗时
  uint64_t sync_cycle_ns_sum = 0;
  uint64_t sync_cycle_count = 0;
};

thread_local GcmLatencyTrace tls_trace;

// 每 N 次采样输出一次汇总
constexpr uint64_t TRACE_REPORT_INTERVAL = 100;

void report_trace_if_needed(const char *label) {
  auto &t = tls_trace;
  if (t.batch_count >= TRACE_REPORT_INTERVAL) {
    std::lock_guard<std::mutex> cout_lock(g_validate_log_mutex);
    auto avg_lock_wait =
        t.commit_async_lock_wait_count
            ? (t.commit_async_lock_wait_ns_sum / t.commit_async_lock_wait_count)
            : 0;
    auto avg_append = t.batch_count ? (t.batch_append_ns_sum / t.batch_count) : 0;
    auto avg_sync = t.batch_count ? (t.batch_sync_ns_sum / t.batch_count) : 0;
    auto avg_cycle = t.sync_cycle_count ? (t.sync_cycle_ns_sum / t.sync_cycle_count) : 0;
    auto avg_batch_reqs = t.batch_count ? (t.batch_requests_sum / t.batch_count) : 0;
    auto avg_batch_bytes = t.batch_count ? (t.batch_bytes_sum / t.batch_count) : 0;

    std::cout << "[GCM-TRACE][" << label << "] " << "lock_wait_avg=" << avg_lock_wait
              << "ns" << ", lock_wait_max=" << t.commit_async_lock_wait_max_ns << "ns"
              << ", lock_wait_cnt=" << t.commit_async_lock_wait_count
              << ", append_avg=" << avg_append << "ns" << ", sync_avg=" << avg_sync
              << "ns" << ", cycle_avg=" << avg_cycle << "ns"
              << ", batch_reqs_avg=" << avg_batch_reqs
              << ", batch_bytes_avg=" << avg_batch_bytes << std::endl;
    // 重置
    t = GcmLatencyTrace {};
  }
}
} // namespace
#endif

namespace base {

GroupCommitManager::GroupCommitManager(const std::string &filename,
                                       size_t batchSize,
                                       std::chrono::milliseconds syncInterval)
    : batchSize_(batchSize),
      syncInterval_(syncInterval),
      running_(false),
      totalCommits_(0),
      totalBatches_(0),
      totalBytes_(0),
      currentBatchSize_(0) {
  try {
    // 创建高性能文件写入器，封装 open/write/fsync 系统调用
    // AppendFile 使用 O_APPEND 模式，确保多进程写入的原子性
    file_ = std::make_unique<AppendFile>(filename);

    // 记录初始时间，用于计算批次间隔和性能统计
    lastSyncTime_ = std::chrono::steady_clock::now();
    batchStartTime_ = lastSyncTime_;
  } catch (const std::exception &e) {
    // 构造失败时抛出明确的错误信息，避免资源泄漏
    throw std::runtime_error("Failed to create GroupCommitManager: " +
                             std::string(e.what()));
  }
}

GroupCommitManager::~GroupCommitManager() {
  // 析构时确保后台线程正确停止，避免数据丢失和资源泄漏
  if (running_) {
    stop();
  }
}

void GroupCommitManager::start() {
  // 防止重复启动，使用原子标志位检查
  if (running_) {
    return;
  }

  // 设置运行标志，后台线程会检查此标志决定是否继续工作
  running_ = true;

  // 启动后台同步线程
  syncThread_ = std::thread(&GroupCommitManager::syncThreadFunc, this);
}

void GroupCommitManager::stop() {
  // 防止重复停止，检查运行状态
  if (!running_) {
    return;
  }

  // 设置停止标志，通知后台线程准备退出
  running_ = false;

  // 唤醒所有等待的线程，确保后台线程能及时响应停止信号
  cond_.notify_all();

  // 等待后台线程完成所有工作后再退出，避免数据丢失
  if (syncThread_.joinable()) {
    syncThread_.join();
  }

  // 处理剩余的请求，确保所有提交的数据都被写入磁盘
  flush();
}

bool GroupCommitManager::commitAsync(std::string data, CommitCallback callback) {
  // 如果管理器已停止，立即调用回调通知失败，避免阻塞
  if (!running_) {
    if (callback) {
      callback(false);
    }
    return false;
  }

  {
#ifdef GCM_LATENCY_TRACE
    // 测量等待 mutex_ 的时间
    auto lock_start = std::chrono::steady_clock::now();
#endif
    std::lock_guard<std::mutex> lock(mutex_);
#ifdef GCM_LATENCY_TRACE
    auto lock_end = std::chrono::steady_clock::now();
    uint64_t lock_wait_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(lock_end - lock_start)
            .count();
    auto &t = tls_trace;
    t.commit_async_lock_wait_ns_sum += lock_wait_ns;
    t.commit_async_lock_wait_count++;
    if (lock_wait_ns > t.commit_async_lock_wait_max_ns) {
      t.commit_async_lock_wait_max_ns = lock_wait_ns;
    }
#endif

    // 将请求加入队列，使用 move 语义避免二次拷贝
    // by value 参数 + std::move = 调用方 move 一次，入队 move 一次 = 零额外拷贝
    // 必须在 std::move(data) 之前保存 size，因为 move 后 data.size() 未定义
    size_t data_size = data.size();
    bool was_empty = pendingRequests_.empty();
    pendingRequests_.emplace(std::move(data), std::move(callback));

    // 累积当前批次的数据大小，用于触发条件判断
    // 使用预先保存的 size（std::move 后 data 为空）
    currentBatchSize_ += data_size;

    // CRITICAL: Only notify on empty→non-empty transition.
    //
    // Previously we called notify_one() unconditionally on every push (18M/s).
    // But the syncThread predicate is `!running_` (always false while running),
    // causing every notification to be a futile wakeup:
    //   wake → lock mutex_ → check predicate(false) → sleep → futex_wake
    // This created a 18M/s wake storm + mutex cacheline bouncing storm.
    //
    // Fix: wake syncThread only when the queue transitions from empty→non-empty.
    // The timeout-based flush (syncInterval_) handles the case where data
    // arrives gradually and never fills the queue.
    if (was_empty) {
      cond_.notify_one();
    }
  }

  return true;
}

bool GroupCommitManager::commitSync(const std::string &data) {
  // 如果管理器已停止，立即返回失败
  if (!running_) {
    return false;
  }

  // 使用 promise/future 实现同步等待
  // promise 在回调中设置结果，future 在此处等待结果
  std::promise<bool> promise;
  auto future = promise.get_future();

  // 使用引用捕获 promise，在回调中设置结果
  commitAsync(data, [&promise](bool success) { promise.set_value(success); });

  // 同步提交需要立即触发批量处理，否则如果 shouldSync() 返回 false，
  // syncThreadFunc 不会调用 processBatch()，导致 future.get() 永久阻塞。
  // 这里直接触发一次批量处理，确保当前请求能被及时处理。
  // processBatch() 会处理队列中所有 pending 请求，不会破坏批量效果。
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!pendingRequests_.empty()) {
      processBatch();
    }
  }

  // 等待异步操作完成并返回结果
  return future.get();
}

void GroupCommitManager::flush() {
  // 获取锁并立即处理当前队列中的所有请求
  // 不等待批量或时间阈值，通常在程序退出前调用
  std::lock_guard<std::mutex> lock(mutex_);
  if (!pendingRequests_.empty()) {
    processBatch();
  }
}

void GroupCommitManager::syncThreadFunc() {
  // 后台线程主循环，负责批量处理提交请求
#ifdef GCM_LATENCY_TRACE
  // [VALIDATION LOG] 追踪无效唤醒率
  // 由于谓词只检查 !running_（正常运行时为 false），每次 notify_one() 都会
  // 导致线程醒来又立即睡回——即"无效唤醒"。下面的计数器追踪这种比率。
  static thread_local uint64_t tls_total_wakeups = 0;
  static thread_local uint64_t tls_futile_wakeups = 0;
  static thread_local uint64_t tls_actual_processings = 0;
  static thread_local auto tls_last_process_time = std::chrono::steady_clock::now();
#endif

  while (running_) {
    // -- 第1阶段：在锁保护下快速搬出队列 --
    std::vector<CommitRequest> batch;
    size_t batchBytes = 0;

    {
      std::unique_lock<std::mutex> lock(mutex_);

      // CRITICAL FIX: Only `!running_` in the predicate to eliminate CPU spin-loop.
      //
      // OLD bug: `!pendingRequests_.empty()` was in the predicate, causing wait_for to
      // return immediately when data was queued. But the process gate also required
      // `shouldSync()`. When data < batchSize_, the thread spun infinitely:
      //   wake → shouldSync=false → skip → release → re-acquire → wake → ... SPIN
      // This cacheline-bounced the mutex_, destroying producer throughput.
      //
      // NEW approach:
      // 1. Thread sleeps until timeout (syncInterval_) or stop signal.
      // 2. On wake: if shouldSync() ⇒ drain ALL pending to local vector under lock,
      //    release lock, do file I/O outside lock.
      // 3. No spin: predicate is always false while running_.
      cond_.wait_for(lock, syncInterval_, [this] { return !running_; });

      if (!running_)
        break;

#ifdef GCM_LATENCY_TRACE
      tls_total_wakeups++;
#endif

      // Only flush when enough data (or enough time has passed).
      // This preserves batching: all writes that arrived between timeouts
      // are processed in a single append + fsync.
      if (!pendingRequests_.empty() && shouldSync()) {
        while (!pendingRequests_.empty()) {
          batch.push_back(std::move(pendingRequests_.front()));
          batchBytes += batch.back().data.size();
          pendingRequests_.pop();
        }
        currentBatchSize_ = 0;
        batchStartTime_ = std::chrono::steady_clock::now();
#ifdef GCM_LATENCY_TRACE
        tls_actual_processings++;
#endif
      }
#ifdef GCM_LATENCY_TRACE
      else {
        tls_futile_wakeups++;
      }
#endif

#ifdef GCM_LATENCY_TRACE
      // [VALIDATION LOG] 每 100 次真实处理输出一次无效唤醒率
      if (tls_actual_processings >= 100) {
        auto now = std::chrono::steady_clock::now();
        auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              now - tls_last_process_time)
                              .count();
        double futile_ratio =
            (double)tls_futile_wakeups / (double)tls_total_wakeups * 100.0;

        std::lock_guard<std::mutex> cout_lock(g_validate_log_mutex);
        std::cout << "[GCM-VALIDATE] syncThread: total_wakeups=" << tls_total_wakeups
                  << ", actual_processings=" << tls_actual_processings
                  << ", futile_wakeups=" << tls_futile_wakeups
                  << ", futile_ratio=" << futile_ratio << "%"
                  << ", last_processing_interval_ms=" << elapsed_ms
                  << ", batchSize=" << batchSize_
                  << ", syncInterval_ms=" << syncInterval_.count() << std::endl;
        // 重置计数器
        tls_total_wakeups = 0;
        tls_futile_wakeups = 0;
        tls_actual_processings = 0;
        tls_last_process_time = now;
      }
#endif
    } // mutex_ released — producers are NOT blocked during file I/O

    if (batch.empty())
      continue;

      // -- 第2阶段：文件 I/O（不持有 mutex_，不阻塞生产者） --
#ifdef GCM_LATENCY_TRACE
    auto append_start = std::chrono::steady_clock::now();
#endif

    bool success = true;
    try {
      for (const auto &req : batch) {
        file_->append(req.data.c_str(), req.data.size());
      }

#ifdef GCM_LATENCY_TRACE
      auto sync_start = std::chrono::steady_clock::now();
      uint64_t append_ns =
          std::chrono::duration_cast<std::chrono::nanoseconds>(sync_start - append_start)
              .count();
#endif

      file_->sync();

#ifdef GCM_LATENCY_TRACE
      auto sync_end = std::chrono::steady_clock::now();
      uint64_t sync_ns =
          std::chrono::duration_cast<std::chrono::nanoseconds>(sync_end - sync_start)
              .count();
      {
        auto &t = tls_trace;
        t.batch_append_ns_sum += append_ns;
        t.batch_sync_ns_sum += sync_ns;
        t.batch_count++;
        t.batch_requests_sum += batch.size();
        t.batch_bytes_sum += batchBytes;
      }
#endif

    } catch (const std::exception &e) {
      std::cerr << "Group commit failed: " << e.what() << std::endl;
      success = false;
    }

    // 更新统计（独立锁，不影响主路径）
    {
      std::lock_guard<std::mutex> statsLock(statsMutex_);
      totalCommits_ += batch.size();
      totalBatches_++;
      totalBytes_ += batchBytes;
      lastSyncTime_ = std::chrono::steady_clock::now();
    }

    // 回调通知（无锁）
    for (const auto &req : batch) {
      if (req.callback) {
        req.callback(success);
      }
    }

#ifdef GCM_LATENCY_TRACE
    {
      auto &t = tls_trace;
      t.sync_cycle_ns_sum += 0; // not needed, keep placeholder
      t.sync_cycle_count++;
      report_trace_if_needed("syncThread");
    }
#endif
  }

  // 程序退出时，确保所有剩余请求都被处理
  flush();
}

void GroupCommitManager::processBatch() {
  // 如果队列为空，直接返回，避免无效处理
  if (pendingRequests_.empty()) {
    return;
  }

  // 记录批次开始时间，用于延迟分析
  auto batchStart = std::chrono::steady_clock::now();
  std::vector<CommitRequest> batch;
  size_t batchBytes = 0;

  // 将队列中的所有请求移动到本地 vector 中
  // 这样可以快速释放锁，减少锁持有时间，提升并发性能
  while (!pendingRequests_.empty()) {
    // 使用 move 避免数据拷贝，提升性能
    batch.push_back(std::move(pendingRequests_.front()));
    batchBytes += batch.back().data.size();
    pendingRequests_.pop();
  }

  // 重置当前批次状态，为下一个批次做准备
  currentBatchSize_ = 0;
  batchStartTime_ = std::chrono::steady_clock::now();

  bool success = true;

  try {
#ifdef GCM_LATENCY_TRACE
    auto append_start = std::chrono::steady_clock::now();
#endif

    // 将所有数据连续写入文件，减少系统调用次数
    // 这是 Group Commit 的核心：将多个逻辑写入合并为一次物理写入
    for (const auto &request : batch) {
      file_->append(request.data.c_str(), request.data.size());
    }

#ifdef GCM_LATENCY_TRACE
    auto sync_start = std::chrono::steady_clock::now();
    uint64_t append_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(sync_start - append_start)
            .count();
#endif

    // 调用 fsync() 将数据强制刷新到磁盘
    // 一次 fsync() 确保整个批次的持久化
    // 相比每个请求单独 fsync()，大幅减少了昂贵的磁盘同步操作
    file_->sync();

#ifdef GCM_LATENCY_TRACE
    auto sync_end = std::chrono::steady_clock::now();
    uint64_t sync_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(sync_end - sync_start)
            .count();

    // 累积到 thread_local trace
    {
      auto &t = tls_trace;
      t.batch_append_ns_sum += append_ns;
      t.batch_sync_ns_sum += sync_ns;
      t.batch_count++;
      t.batch_requests_sum += batch.size();
      t.batch_bytes_sum += batchBytes;
    }
#endif

    // 更新统计信息，使用独立的锁避免影响主路径性能
    {
      std::lock_guard<std::mutex> statsLock(statsMutex_);
      totalCommits_ += batch.size();                    // 累计提交请求数
      totalBatches_++;                                  // 累计批次数
      totalBytes_ += batchBytes;                        // 累计字节数
      lastSyncTime_ = std::chrono::steady_clock::now(); // 更新最后同步时间
    }

  } catch (const std::exception &e) {
    // 捕获 I/O 异常，记录错误信息
    std::cerr << "Group commit failed: " << e.what() << std::endl;
    success = false;
  }

  // 调用所有请求的回调函数，通知提交结果
  // 无论成功还是失败，都要通知所有等待的客户端
  for (const auto &request : batch) {
    if (request.callback) {
      request.callback(success);
    }
  }
}

bool GroupCommitManager::shouldSync() const {
  // Group Commit 的核心逻辑：检查是否应该触发同步
  // 这是数据库系统中的经典优化，平衡吞吐量和延迟

  // 触发条件1：批次大小达到阈值（保证吞吐量）
  // 当累积的数据量达到设定阈值时，立即触发同步
  // 这样可以充分利用磁盘的顺序写入性能
  if (currentBatchSize_ >= batchSize_) {
    return true;
  }

  // 触发条件2：时间间隔达到阈值（控制延迟）
  // 即使数据量不够，也要在规定时间内强制同步
  // 这样可以保证写入延迟不会无限增长
  auto now = std::chrono::steady_clock::now();
  auto elapsed =
      std::chrono::duration_cast<std::chrono::milliseconds>(now - batchStartTime_);

  return elapsed >= syncInterval_;
}

GroupCommitManager::Stats GroupCommitManager::getStats() const {
  // 使用独立的统计锁，避免影响主路径的性能
  std::lock_guard<std::mutex> lock(statsMutex_);

  Stats stats;
  stats.totalCommits = totalCommits_;
  stats.totalBatches = totalBatches_;
  stats.totalBytes = totalBytes_;

  // 计算平均批次大小，用于评估 Group Commit 的效果
  // 批次大小越大，说明批量效果越好，fsync() 调用次数越少
  if (totalBatches_ > 0) {
    stats.avgBatchSize = static_cast<double>(totalCommits_) / totalBatches_;
  } else {
    stats.avgBatchSize = 0.0;
  }

  // 简化的平均延迟计算
  // 实际项目中可以维护更详细的延迟直方图
  stats.avgLatency = syncInterval_;

  return stats;
}

} // namespace base
} // namespace minkv
