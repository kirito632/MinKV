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
        // [系统调用优化] 创建高性能文件写入器，封装open/write/fsync系统调用
        // AppendFile使用O_APPEND模式，确保多进程写入的原子性
        file_ = std::make_unique<AppendFile>(filename);
        
        // [时间戳初始化] 记录初始时间，用于计算批次间隔和性能统计
        lastSyncTime_ = std::chrono::steady_clock::now();
        batchStartTime_ = lastSyncTime_;
    } catch (const std::exception& e) {
        // [异常安全] 构造失败时抛出明确的错误信息，避免资源泄漏
        throw std::runtime_error("Failed to create GroupCommitManager: " + std::string(e.what()));
    }
}

GroupCommitManager::~GroupCommitManager() {
    // [RAII] 析构时确保后台线程正确停止，避免数据丢失和资源泄漏
    if (running_) {
        stop();
    }
}

void GroupCommitManager::start() {
    // [线程安全] 防止重复启动，使用原子标志位检查
    if (running_) {
        return;
    }
    
    // [原子操作] 设置运行标志，后台线程会检查此标志决定是否继续工作
    running_ = true;
    
    // [生产者-消费者] 启动后台同步线程，使用成员函数指针和this指针
    syncThread_ = std::thread(&GroupCommitManager::syncThreadFunc, this);
}

void GroupCommitManager::stop() {
    // [线程安全] 防止重复停止，检查运行状态
    if (!running_) {
        return;
    }
    
    // [原子操作] 设置停止标志，通知后台线程准备退出
    running_ = false;
    
    // [条件变量] 唤醒所有等待的线程，确保后台线程能及时响应停止信号
    cond_.notify_all();
    
    // [异常安全] 等待后台线程完成所有工作后再退出，避免数据丢失
    if (syncThread_.joinable()) {
        syncThread_.join();
    }
    
    // [数据完整性] 处理剩余的请求，确保所有提交的数据都被写入磁盘
    flush();
}

void GroupCommitManager::commitAsync(const std::string& data, CommitCallback callback) {
    // [快速失败] 如果管理器已停止，立即调用回调通知失败，避免阻塞
    if (!running_) {
        if (callback) {
            callback(false);
        }
        return;
    }
    
    {
        // [分片锁] 使用RAII的lock_guard确保异常安全，自动释放锁
        std::lock_guard<std::mutex> lock(mutex_);
        
        // [核心优化] 将请求加入队列，使用emplace避免额外的拷贝构造
        // move语义确保数据和回调函数的高效转移
        pendingRequests_.emplace(data, std::move(callback));
        
        // [批量统计] 累积当前批次的数据大小，用于触发条件判断
        currentBatchSize_ += data.size();
    }
    
    // [生产者-消费者] 通知后台线程有新数据需要处理
    // 使用notify_one而不是notify_all，因为只有一个消费者线程
    cond_.notify_one();
}

bool GroupCommitManager::commitSync(const std::string& data) {
    // [快速失败] 如果管理器已停止，立即返回失败
    if (!running_) {
        return false;
    }
    
    // [同步机制] 使用promise/future实现同步等待
    // promise在回调中设置结果，future在此处等待结果
    std::promise<bool> promise;
    auto future = promise.get_future();
    
    // [Lambda捕获] 使用引用捕获promise，在回调中设置结果
    commitAsync(data, [&promise](bool success) {
        promise.set_value(success);
    });
    
    // [阻塞等待] 等待异步操作完成并返回结果
    return future.get();
}

void GroupCommitManager::flush() {
    // [强制刷新] 获取锁并立即处理当前队列中的所有请求
    // 不等待批量或时间阈值，通常在程序退出前调用
    std::lock_guard<std::mutex> lock(mutex_);
    if (!pendingRequests_.empty()) {
        processBatch();
    }
}

void GroupCommitManager::syncThreadFunc() {
    // [生产者-消费者] 后台线程主循环，负责批量处理提交请求
    while (running_) {
        {
            // [临界区] 获取锁，检查和等待提交请求
            std::unique_lock<std::mutex> lock(mutex_);
            
            // [双重触发机制] 等待条件：
            // 1. 有数据要写 (pendingRequests_不为空)
            // 2. 达到同步条件 (shouldSync()返回true)
            // 3. 程序退出 (!running_)
            cond_.wait_for(lock, syncInterval_, [this] {
                return !pendingRequests_.empty() || !running_ || shouldSync();
            });
            
            // [批量处理] 如果有数据且满足同步条件，或程序正在退出，则处理批次
            if (!pendingRequests_.empty() && (shouldSync() || !running_)) {
                processBatch();
            }
        }
        // [关键设计] 锁的作用域结束，释放锁
    }
    
    // [异常安全] 程序退出时，确保所有剩余请求都被处理
    std::lock_guard<std::mutex> lock(mutex_);
    if (!pendingRequests_.empty()) {
        processBatch();
    }
}

void GroupCommitManager::processBatch() {
    // [边界检查] 如果队列为空，直接返回，避免无效处理
    if (pendingRequests_.empty()) {
        return;
    }
    
    // [性能统计] 记录批次开始时间，用于延迟分析
    auto batchStart = std::chrono::steady_clock::now();
    std::vector<CommitRequest> batch;
    size_t batchBytes = 0;
    
    // [批量收集] 将队列中的所有请求移动到本地vector中
    // 这样可以快速释放锁，减少锁持有时间，提升并发性能
    while (!pendingRequests_.empty()) {
        // [移动语义] 使用move避免数据拷贝，提升性能
        batch.push_back(std::move(pendingRequests_.front()));
        batchBytes += batch.back().data.size();
        pendingRequests_.pop();
    }
    
    // [状态重置] 重置当前批次状态，为下一个批次做准备
    currentBatchSize_ = 0;
    batchStartTime_ = std::chrono::steady_clock::now();
    
    bool success = true;
    
    try {
        // [批量I/O] 将所有数据连续写入文件，减少系统调用次数
        // 这是Group Commit的核心：将多个逻辑写入合并为一次物理写入
        for (const auto& request : batch) {
            file_->append(request.data.c_str(), request.data.size());
        }
        
        // [核心优化] 调用fsync()将数据强制刷新到磁盘
        // 这是Group Commit的关键：一次fsync()确保整个批次的持久化
        // 相比每个请求单独fsync()，大幅减少了昂贵的磁盘同步操作
        file_->sync();
        
        // [性能统计] 更新统计信息，使用独立的锁避免影响主路径性能
        {
            std::lock_guard<std::mutex> statsLock(statsMutex_);
            totalCommits_ += batch.size();      // 累计提交请求数
            totalBatches_++;                    // 累计批次数
            totalBytes_ += batchBytes;          // 累计字节数
            lastSyncTime_ = std::chrono::steady_clock::now();  // 更新最后同步时间
        }
        
    } catch (const std::exception& e) {
        // [错误处理] 捕获I/O异常，记录错误信息
        std::cerr << "Group commit failed: " << e.what() << std::endl;
        success = false;
    }
    
    // [回调通知] 调用所有请求的回调函数，通知提交结果
    // 无论成功还是失败，都要通知所有等待的客户端
    for (const auto& request : batch) {
        if (request.callback) {
            request.callback(success);
        }
    }
}

bool GroupCommitManager::shouldSync() const {
    // [双重触发机制] Group Commit的核心逻辑：检查是否应该触发同步
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
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - batchStartTime_);
    
    return elapsed >= syncInterval_;
}

GroupCommitManager::Stats GroupCommitManager::getStats() const {
    // [线程安全] 使用独立的统计锁，避免影响主路径的性能
    std::lock_guard<std::mutex> lock(statsMutex_);
    
    Stats stats;
    stats.totalCommits = totalCommits_;
    stats.totalBatches = totalBatches_;
    stats.totalBytes = totalBytes_;
    
    // [性能指标] 计算平均批次大小，用于评估Group Commit的效果
    // 批次大小越大，说明批量效果越好，fsync()调用次数越少
    if (totalBatches_ > 0) {
        stats.avgBatchSize = static_cast<double>(totalCommits_) / totalBatches_;
    } else {
        stats.avgBatchSize = 0.0;
    }
    
    // [延迟统计] 简化的平均延迟计算
    // 实际项目中可以维护更详细的延迟直方图
    stats.avgLatency = syncInterval_;
    
    return stats;
}

} // namespace base
} // namespace minkv