#include "group_commit.h"
#include <iostream>
#include <future>

namespace minkv {
namespace base {

GroupCommitManager::GroupCommitManager(const std::string& filename, 
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
        file_ = std::make_unique<AppendFile>(filename);
        lastSyncTime_ = std::chrono::steady_clock::now();
        batchStartTime_ = lastSyncTime_;
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to create GroupCommitManager: " + std::string(e.what()));
    }
}

GroupCommitManager::~GroupCommitManager() {
    if (running_) {
        stop();
    }
}

void GroupCommitManager::start() {
    if (running_) {
        return;
    }
    
    running_ = true;
    syncThread_ = std::thread(&GroupCommitManager::syncThreadFunc, this);
}

void GroupCommitManager::stop() {
    if (!running_) {
        return;
    }
    
    running_ = false;
    cond_.notify_all();
    
    if (syncThread_.joinable()) {
        syncThread_.join();
    }
    
    // 处理剩余的请求
    flush();
}

void GroupCommitManager::commitAsync(const std::string& data, CommitCallback callback) {
    if (!running_) {
        if (callback) {
            callback(false);
        }
        return;
    }
    
    {
        std::lock_guard<std::mutex> lock(mutex_);
        pendingRequests_.emplace(data, std::move(callback));
        currentBatchSize_ += data.size();
    }
    
    // 通知同步线程
    cond_.notify_one();
}

bool GroupCommitManager::commitSync(const std::string& data) {
    if (!running_) {
        return false;
    }
    
    std::promise<bool> promise;
    auto future = promise.get_future();
    
    commitAsync(data, [&promise](bool success) {
        promise.set_value(success);
    });
    
    return future.get();
}

void GroupCommitManager::flush() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!pendingRequests_.empty()) {
        processBatch();
    }
}

void GroupCommitManager::syncThreadFunc() {
    while (running_) {
        std::unique_lock<std::mutex> lock(mutex_);
        
        // 等待条件：有数据要写 或者 超时 或者 程序退出
        cond_.wait_for(lock, syncInterval_, [this] {
            return !pendingRequests_.empty() || !running_ || shouldSync();
        });
        
        if (!pendingRequests_.empty() && (shouldSync() || !running_)) {
            processBatch();
        }
    }
    
    // 处理剩余请求
    std::lock_guard<std::mutex> lock(mutex_);
    if (!pendingRequests_.empty()) {
        processBatch();
    }
}

void GroupCommitManager::processBatch() {
    if (pendingRequests_.empty()) {
        return;
    }
    
    auto batchStart = std::chrono::steady_clock::now();
    std::vector<CommitRequest> batch;
    size_t batchBytes = 0;
    
    // 收集当前批次的所有请求
    while (!pendingRequests_.empty()) {
        batch.push_back(std::move(pendingRequests_.front()));
        batchBytes += batch.back().data.size();
        pendingRequests_.pop();
    }
    
    currentBatchSize_ = 0;
    batchStartTime_ = std::chrono::steady_clock::now();
    
    bool success = true;
    
    try {
        // 批量写入数据
        for (const auto& request : batch) {
            file_->append(request.data.c_str(), request.data.size());
        }
        
        // 同步到磁盘
        file_->sync();
        
        // 更新统计信息
        {
            std::lock_guard<std::mutex> statsLock(statsMutex_);
            totalCommits_ += batch.size();
            totalBatches_++;
            totalBytes_ += batchBytes;
            lastSyncTime_ = std::chrono::steady_clock::now();
        }
        
    } catch (const std::exception& e) {
        std::cerr << "Group commit failed: " << e.what() << std::endl;
        success = false;
    }
    
    // 调用所有回调函数
    for (const auto& request : batch) {
        if (request.callback) {
            request.callback(success);
        }
    }
}

bool GroupCommitManager::shouldSync() const {
    // 检查是否应该同步：
    // 1. 批次大小达到阈值
    // 2. 距离上次同步时间超过阈值
    
    if (currentBatchSize_ >= batchSize_) {
        return true;
    }
    
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - batchStartTime_);
    
    return elapsed >= syncInterval_;
}

GroupCommitManager::Stats GroupCommitManager::getStats() const {
    std::lock_guard<std::mutex> lock(statsMutex_);
    
    Stats stats;
    stats.totalCommits = totalCommits_;
    stats.totalBatches = totalBatches_;
    stats.totalBytes = totalBytes_;
    
    if (totalBatches_ > 0) {
        stats.avgBatchSize = static_cast<double>(totalCommits_) / totalBatches_;
    } else {
        stats.avgBatchSize = 0.0;
    }
    
    // 简化的平均延迟计算
    stats.avgLatency = syncInterval_;
    
    return stats;
}

} // namespace base
} // namespace minkv