#include "expiration_manager.h"
#include "async_logger.h"
#include <algorithm>
#include <numeric>

namespace minkv {
namespace base {

ExpirationManager::ExpirationManager(size_t shard_count,
                                   std::chrono::milliseconds check_interval,
                                   size_t sample_size)
    : shard_count_(shard_count),
      check_interval_(check_interval),
      sample_size_(sample_size),
      running_(false),
      total_checks_(0),
      total_expired_(0),
      total_skipped_(0),
      rng_(std::random_device{}()),
      shard_dist_(0, shard_count - 1) {
    
    // [性能优化] 预分配过期比例历史记录空间，避免动态扩容
    expired_ratios_.reserve(1000);
    
    LOG_INFO << "[ExpirationManager] Initialized with " << shard_count 
             << " shards, check_interval=" << check_interval_.count() 
             << "ms, sample_size=" << sample_size;
}

ExpirationManager::~ExpirationManager() {
    // [RAII] 析构时确保后台线程正确停止
    if (running_) {
        stop();
    }
}

void ExpirationManager::start(ExpirationCallback callback) {
    // [线程安全] 防止重复启动
    if (running_) {
        LOG_WARN << "[ExpirationManager] Already running, ignoring start request";
        return;
    }
    
    // [参数验证] 确保回调函数有效
    if (!callback) {
        LOG_ERROR << "[ExpirationManager] Invalid callback function";
        return;
    }
    
    callback_ = std::move(callback);
    
    // [原子操作] 设置运行标志
    running_ = true;
    
    // [生产者-消费者] 启动后台定时线程
    cron_thread_ = std::thread(&ExpirationManager::cronThreadFunc, this);
    
    LOG_INFO << "[ExpirationManager] Started expiration cleanup service";
}

void ExpirationManager::stop() {
    // [线程安全] 防止重复停止
    if (!running_) {
        return;
    }
    
    // [原子操作] 设置停止标志
    running_ = false;
    
    // [异常安全] 等待后台线程完成当前工作后再退出
    if (cron_thread_.joinable()) {
        cron_thread_.join();
    }
    
    LOG_INFO << "[ExpirationManager] Stopped expiration cleanup service";
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
            if (!running_) break;  // 检查停止标志
            
            size_t expired_count = processShard(shard_id);
            if (expired_count == 0) {
                // [性能统计] 如果返回0，可能是锁竞争导致的跳过
                total_skipped_this_round++;
            } else {
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
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        
        // [日志记录] 定期输出统计信息（每100轮输出一次）
        if (total_checks_ % 100 == 0) {
            LOG_INFO << "[ExpirationManager] Round " << total_checks_ 
                     << ": expired=" << total_expired_this_round
                     << ", skipped=" << total_skipped_this_round
                     << ", elapsed=" << elapsed.count() << "ms";
        }
        
        // [定时控制] 等待下一个检查周期
        // 如果本轮处理时间超过了检查间隔，立即开始下一轮
        if (elapsed < check_interval_) {
            std::this_thread::sleep_for(check_interval_ - elapsed);
        }
    }
    
    LOG_INFO << "[ExpirationManager] Cron thread stopped";
}

size_t ExpirationManager::processShard(size_t shard_id) {
    // [核心优化] 调用回调函数处理指定分片的过期检查
    // 回调函数内部应该使用 try_lock() 实现非阻塞访问
    try {
        return callback_(shard_id, sample_size_);
    } catch (const std::exception& e) {
        // [异常处理] 捕获回调函数中的异常，避免影响整个定时任务
        LOG_ERROR << "[ExpirationManager] Exception in shard " << shard_id 
                  << " processing: " << e.what();
        return 0;
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
        double sum = std::accumulate(expired_ratios_.begin(), expired_ratios_.end(), 0.0);
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