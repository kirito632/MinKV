#include "expiration_manager.h"
#include "async_logger.h"
#include <algorithm>
#include <numeric>
#include <stdexcept>

namespace minkv {
namespace base {

ExpirationManager::ExpirationManager(ExpirationCallback callback,
                                     size_t shard_count,
                                     std::chrono::milliseconds check_interval,
                                     size_t sample_size)
    : shard_count_(shard_count), check_interval_(check_interval),
      sample_size_(sample_size), callback_(std::move(callback)),
      running_(false),
      // stats_mutex_ is default-constructed (no initialization needed)
      total_checks_(0), total_expired_(0), total_skipped_(0),
      // expired_ratios_ is default-constructed (empty vector)
      rng_(std::random_device{}()), shard_dist_(0, shard_count - 1)
// cron_thread_ is NOT initialized here - will be started in constructor body
{

  // [参数验证] 在线程启动前验证所有参数，确保强异常安全保证
  if (!callback_) {
    throw std::invalid_argument("ExpirationManager: callback cannot be null");
  }
  if (shard_count == 0) {
    throw std::invalid_argument("ExpirationManager: shard_count must be > 0");
  }
  if (check_interval.count() <= 0) {
    throw std::invalid_argument(
        "ExpirationManager: check_interval must be > 0");
  }
  if (sample_size == 0) {
    throw std::invalid_argument("ExpirationManager: sample_size must be > 0");
  }

  // [性能优化] 预分配过期比例历史记录空间，避免动态扩容
  expired_ratios_.reserve(1000);

  LOG_INFO << "[ExpirationManager] Initialized with " << shard_count
           << " shards, check_interval=" << check_interval_.count()
           << "ms, sample_size=" << sample_size;

  // [RAII] 构造时自动启动后台线程
  // [内存序] 使用 memory_order_release 确保所有初始化对线程可见
  running_.store(true, std::memory_order_release);
  cron_thread_ = std::thread(&ExpirationManager::cronThreadFunc, this);

  LOG_INFO << "[ExpirationManager] Started expiration cleanup service";
}

ExpirationManager::~ExpirationManager() noexcept {
  // [RAII] 析构时自动停止后台线程
  // [异常安全] 提供无抛出保证

  // [原子操作] 无条件设置停止标志
  // 注意：不能提前 return，否则 cron_thread_ 可能仍是 joinable 状态，
  // 导致析构时 std::terminate 被调用
  running_.store(false, std::memory_order_release);

  // [快速唤醒] 唤醒可能正在睡眠的线程
  {
    std::lock_guard<std::mutex> lock(stop_mutex_);
    stop_cv_.notify_all();
  }

  // [异常安全] joinable() 自动处理线程已结束的情况，无需额外判断
  if (cron_thread_.joinable()) {
    cron_thread_.join();
  }

  LOG_INFO << "[ExpirationManager] Stopped expiration cleanup service (RAII)";
}

void ExpirationManager::cronThreadFunc() {
  LOG_INFO << "[ExpirationManager] Cron thread started";

  // [定时任务] 类似 Redis serverCron 的主循环
  while (running_) {
    auto start_time = std::chrono::steady_clock::now();

    // [核心算法] 遍历所有分片，进行过期检查
    size_t total_expired_this_round = 0;
    size_t total_skipped_this_round = 0;

    for (size_t shard_id = 0; shard_id < shard_count_; ++shard_id) {
      if (!running_)
        break; // 检查停止标志

      size_t expired_count = processShard(shard_id);

      // [防御性编程] 对 processShard 返回值做三层分类：
      //   1. SIZE_MAX → 锁竞争或异常，计入 skipped
      //   2. 正常值 (0 或正数) → 正常处理，计入 expired
      //   3. 其他非法值 → 按 skipped 处理，防止统计污染
      if (expired_count == SIZE_MAX) {
        // [性能统计] SIZE_MAX 是锁竞争/异常哨兵值，表示本次被跳过
        total_skipped_this_round++;
      } else if (expired_count > sample_size_) {
        // [安全防护] 返回值超过 sample_size_ 属于非法值
        // （单次最多删除 sample_size_ 个 key），按 skipped 处理
        total_skipped_this_round++;
      } else {
        // expired_count 为 0 表示正常处理但无过期 key，不计入 skipped
        total_expired_this_round += expired_count;
      }
    }

    // [性能统计] 更新统计信息
    {
      std::lock_guard<std::mutex> lock(stats_mutex_);
      total_checks_++;
      total_expired_ += total_expired_this_round;
      total_skipped_ += total_skipped_this_round;

      // [自适应频率] 记录过期比例，用于动态调整检查频率
      if (total_expired_this_round > 0) {
        double expired_ratio = static_cast<double>(total_expired_this_round) /
                               (shard_count_ * sample_size_);
        expired_ratios_.push_back(expired_ratio);

        // [内存管理] 限制历史记录大小，避免内存无限增长
        if (expired_ratios_.size() > 1000) {
          expired_ratios_.erase(expired_ratios_.begin(),
                                expired_ratios_.begin() + 500);
        }
      }
    }

    // [性能监控] 记录本轮检查耗时
    auto end_time = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time);

    // [日志记录] 定期输出统计信息（每100轮输出一次）
    if (total_checks_ % 100 == 0) {
      LOG_INFO << "[ExpirationManager] Round " << total_checks_
               << ": expired=" << total_expired_this_round
               << ", skipped=" << total_skipped_this_round
               << ", elapsed=" << elapsed.count() << "ms";
    }

    // [定时控制] 等待下一个检查周期
    // 使用条件变量实现可中断的睡眠，支持快速停止
    if (elapsed < check_interval_) {
      std::unique_lock<std::mutex> lock(stop_mutex_);
      stop_cv_.wait_for(lock, check_interval_ - elapsed, [this] {
        return !running_.load(std::memory_order_acquire);
      });
    }

    // [快速退出] 如果被唤醒且 running_ 为 false，立即退出循环
    if (!running_.load(std::memory_order_acquire)) {
      break;
    }
  }

  LOG_INFO << "[ExpirationManager] Cron thread stopped";
}

size_t ExpirationManager::processShard(size_t shard_id) {
  // [核心优化] 调用回调函数处理指定分片的过期检查
  // 回调函数内部应该使用 try_lock() 实现非阻塞访问
  try {
    return callback_(shard_id, sample_size_);
  } catch (const std::exception &e) {
    // [异常处理] 捕获回调函数中的异常，避免影响整个定时任务
    // [BUG FIX] 异常分片应返回 SIZE_MAX 以计入 total_skipped，
    //           而不是返回 0（0 表示"正常处理但无过期 key"）。
    //           测试 test_stats_with_exceptions 验证此行为。
    LOG_ERROR << "[ExpirationManager] Exception in shard " << shard_id
              << " processing: " << e.what();
    return SIZE_MAX;
  } catch (...) {
    // [异常安全] 捕获所有未知异常，确保线程继续运行
    // [BUG FIX] 同 std::exception 处理，返回 SIZE_MAX 计入 skipped
    LOG_ERROR << "[ExpirationManager] Unknown exception in shard " << shard_id;
    return SIZE_MAX;
  }
}

ExpirationManager::Stats ExpirationManager::getStats() const {
  // [线程安全] 使用锁保护统计数据的读取
  std::lock_guard<std::mutex> lock(stats_mutex_);

  Stats stats;
  stats.total_checks = total_checks_;
  stats.total_expired = total_expired_;
  stats.total_skipped = total_skipped_;

  // [性能指标] 计算平均过期比例
  if (!expired_ratios_.empty()) {
    double sum =
        std::accumulate(expired_ratios_.begin(), expired_ratios_.end(), 0.0);
    stats.avg_expired_ratio = sum / expired_ratios_.size();
  } else {
    stats.avg_expired_ratio = 0.0;
  }

  // [延迟统计] 简化的平均检查时间计算
  // 实际项目中可以维护更详细的时间统计
  stats.avg_check_time = check_interval_;

  return stats;
}

} // namespace base
} // namespace minkv
