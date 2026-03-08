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
    
    expired_ratios_.reserve(1000);
    
    LOG_INFO << "[ExpirationManager] Initialized with " << shard_count 
             << " shards, check_interval=" << check_interval_.count() 
             << "ms, sample_size=" << sample_size;
}

ExpirationManager::~ExpirationManager() {
    if (running_) {
        stop();
    }
}

void ExpirationManager::start(ExpirationCallback callback) {
    if (running_) {
        LOG_WARN << "[ExpirationManager] Already running, ignoring start request";
        return;
    }
    
    if (!callback) {
        LOG_ERROR << "[ExpirationManager] Invalid callback function";
        return;
    }
    
    callback_ = std::move(callback);
    running_ = true;
    cron_thread_ = std::thread(&ExpirationManager::cronThreadFunc, this);
    
    LOG_INFO << "[ExpirationManager] Started expiration cleanup service";
}

void ExpirationManager::stop() {
    if (!running_) {
        return;
    }
    
    running_ = false;
    
    if (cron_thread_.joinable()) {
        cron_thread_.join();
    }
    
    LOG_INFO << "[ExpirationManager] Stopped expiration cleanup service";
}

void ExpirationManager::cronThreadFunc() {
    LOG_INFO << "[ExpirationManager] Cron thread started";
    
    while (running_) {
        auto start_time = std::chrono::steady_clock::now();
        
        size_t total_expired_this_round = 0;
        size_t total_skipped_this_round = 0;
        
        for (size_t shard_id = 0; shard_id < shard_count_; ++shard_id) {
            if (!running_) break;
            
            size_t expired_count = processShard(shard_id);
            if (expired_count == 0) {
                total_skipped_this_round++;
            } else {
                total_expired_this_round += expired_count;
            }
        }
        
        {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            total_checks_++;
            total_expired_ += total_expired_this_round;
            total_skipped_ += total_skipped_this_round;
            
            if (total_expired_this_round > 0) {
                double expired_ratio = static_cast<double>(total_expired_this_round) / 
                                     (shard_count_ * sample_size_);
                expired_ratios_.push_back(expired_ratio);
                
                if (expired_ratios_.size() > 1000) {
                    expired_ratios_.erase(expired_ratios_.begin(), 
                                        expired_ratios_.begin() + 500);
                }
            }
        }
        
        auto end_time = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        
        if (total_checks_ % 100 == 0) {
            LOG_INFO << "[ExpirationManager] Round " << total_checks_ 
                     << ": expired=" << total_expired_this_round
                     << ", skipped=" << total_skipped_this_round
                     << ", elapsed=" << elapsed.count() << "ms";
        }
        
        if (elapsed < check_interval_) {
            std::this_thread::sleep_for(check_interval_ - elapsed);
        }
    }
    
    LOG_INFO << "[ExpirationManager] Cron thread stopped";
}

size_t ExpirationManager::processShard(size_t shard_id) {
    try {
        return callback_(shard_id, sample_size_);
    } catch (const std::exception& e) {
        LOG_ERROR << "[ExpirationManager] Exception in shard " << shard_id 
                  << " processing: " << e.what();
        return 0;
    }
}

ExpirationManager::Stats ExpirationManager::getStats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    
    Stats stats;
    stats.total_checks = total_checks_;
    stats.total_expired = total_expired_;
    stats.total_skipped = total_skipped_;
    
    if (!expired_ratios_.empty()) {
        double sum = std::accumulate(expired_ratios_.begin(), expired_ratios_.end(), 0.0);
        stats.avg_expired_ratio = sum / expired_ratios_.size();
    } else {
        stats.avg_expired_ratio = 0.0;
    }
    
    stats.avg_check_time = check_interval_;
    
    return stats;
}

} // namespace base
} // namespace minkv
