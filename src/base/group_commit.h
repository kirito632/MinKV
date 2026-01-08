#pragma once

#include "append_file.h"
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <chrono>
#include <queue>
#include <functional>

namespace minkv {
namespace base {

// Group Commit 管理器
// 将多个写入操作批量提交到磁盘，提升I/O性能
class GroupCommitManager {
public:
    using CommitCallback = std::function<void(bool success)>;
    
    struct CommitRequest {
        std::string data;
        CommitCallback callback;
        std::chrono::steady_clock::time_point timestamp;
        
        CommitRequest(std::string d, CommitCallback cb)
            : data(std::move(d)), callback(std::move(cb)), 
              timestamp(std::chrono::steady_clock::now()) {}
    };
    
    explicit GroupCommitManager(const std::string& filename, 
                               size_t batchSize = 4096,  // 4KB批量大小
                               std::chrono::milliseconds syncInterval = std::chrono::milliseconds(10));
    ~GroupCommitManager();
    
    // 禁止拷贝
    GroupCommitManager(const GroupCommitManager&) = delete;
    GroupCommitManager& operator=(const GroupCommitManager&) = delete;
    
    // 异步提交数据
    void commitAsync(const std::string& data, CommitCallback callback = nullptr);
    
    // 同步提交数据（等待完成）
    bool commitSync(const std::string& data);
    
    // 强制刷新所有待提交数据
    void flush();
    
    // 启动和停止
    void start();
    void stop();
    
    // 获取统计信息
    struct Stats {
        uint64_t totalCommits;
        uint64_t totalBatches;
        uint64_t totalBytes;
        double avgBatchSize;
        std::chrono::milliseconds avgLatency;
    };
    
    Stats getStats() const;
    
private:
    void syncThreadFunc();
    void processBatch();
    bool shouldSync() const;
    
    std::unique_ptr<AppendFile> file_;
    const size_t batchSize_;
    const std::chrono::milliseconds syncInterval_;
    
    std::atomic<bool> running_;
    std::thread syncThread_;
    
    mutable std::mutex mutex_;
    std::condition_variable cond_;
    std::queue<CommitRequest> pendingRequests_;
    
    // 统计信息
    mutable std::mutex statsMutex_;
    uint64_t totalCommits_;
    uint64_t totalBatches_;
    uint64_t totalBytes_;
    std::chrono::steady_clock::time_point lastSyncTime_;
    
    // 当前批次信息
    size_t currentBatchSize_;
    std::chrono::steady_clock::time_point batchStartTime_;
};

} // namespace base
} // namespace minkv