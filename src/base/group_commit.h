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

/**
 * @brief Group Commit 批量提交管理器
 * 
 * [核心优化] 实现数据库系统中的经典优化技术 - Group Commit（组提交）
 * 将多个写入操作批量提交到磁盘，大幅减少fsync()系统调用次数，提升I/O性能。
 * 
 * 设计特点：
 * - 双重触发机制：批量大小阈值 + 时间间隔阈值
 * - 异步处理：前端提交不阻塞，后台线程负责实际I/O
 * - 原子性保证：同一批次的所有请求要么全部成功，要么全部失败
 * - 回调机制：支持异步和同步两种提交方式
 * 
 * 这是MySQL InnoDB、PostgreSQL等数据库系统的核心技术，
 * 在MinKV中用于WAL日志的高性能写入。
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
        std::string data;                                    ///< 要提交的数据
        CommitCallback callback;                             ///< 完成回调函数
        std::chrono::steady_clock::time_point timestamp;     ///< 请求时间戳，用于延迟统计
        
        /**
         * @brief 构造提交请求
         * @param d 要提交的数据
         * @param cb 完成回调函数
         * 
         * [性能优化] 使用move语义避免不必要的数据拷贝
         */
        CommitRequest(std::string d, CommitCallback cb)
            : data(std::move(d)), callback(std::move(cb)), 
              timestamp(std::chrono::steady_clock::now()) {}
    };
    
    /**
     * @brief 构造Group Commit管理器
     * @param filename 目标文件名
     * @param batchSize 批量大小阈值，默认4KB
     * @param syncInterval 同步时间间隔，默认10ms
     * 
     * [双重触发机制] 
     * - 当累积数据达到batchSize时立即提交（保证吞吐量）
     * - 当距离上次提交超过syncInterval时强制提交（控制延迟）
     */
    explicit GroupCommitManager(const std::string& filename, 
                               size_t batchSize = 4096,  ///< 4KB批量大小，平衡内存和I/O效率
                               std::chrono::milliseconds syncInterval = std::chrono::milliseconds(10)); ///< 10ms最大延迟
    
    /**
     * @brief 析构函数，确保资源正确释放
     * 
     * [RAII] 析构时自动停止后台线程，处理剩余请求
     */
    ~GroupCommitManager();
    
    // 禁止拷贝和赋值，确保资源管理的唯一性
    GroupCommitManager(const GroupCommitManager&) = delete;
    GroupCommitManager& operator=(const GroupCommitManager&) = delete;
    
    /**
     * @brief 异步提交数据
     * @param data 要提交的数据
     * @param callback 完成回调函数，可选
     * 
     * [核心接口] 前端线程调用此接口提交数据，立即返回不阻塞
     * 数据会被加入待处理队列，由后台线程批量处理
     */
    void commitAsync(const std::string& data, CommitCallback callback = nullptr);
    
    /**
     * @brief 同步提交数据（阻塞等待完成）
     * @param data 要提交的数据
     * @return true表示提交成功，false表示失败
     * 
     * [同步接口] 内部使用promise/future机制实现同步等待
     * 适用于需要确认提交结果的场景
     */
    bool commitSync(const std::string& data);
    
    /**
     * @brief 强制刷新所有待提交数据
     * 
     * 立即处理当前队列中的所有请求，不等待批量或时间阈值
     * 通常在程序退出前调用，确保数据完整性
     */
    void flush();
    
    /**
     * @brief 启动后台同步线程
     */
    void start();
    
    /**
     * @brief 停止后台同步线程
     * 
     * [异常安全] 等待后台线程完成所有工作后再退出
     */
    void stop();
    
    /**
     * @brief 性能统计信息结构体
     * 
     * 提供详细的性能指标，用于监控和调优
     */
    struct Stats {
        uint64_t totalCommits;                      ///< 总提交请求数
        uint64_t totalBatches;                      ///< 总批次数
        uint64_t totalBytes;                        ///< 总字节数
        double avgBatchSize;                        ///< 平均批次大小
        std::chrono::milliseconds avgLatency;       ///< 平均延迟
    };
    
    /**
     * @brief 获取性能统计信息
     * @return 当前的性能统计数据
     * 
     * [监控接口] 用于性能分析和系统调优
     */
    Stats getStats() const;
    
private:
    /**
     * @brief 后台同步线程主函数
     * 
     * [生产者-消费者] 使用条件变量等待提交请求，
     * 根据双重触发条件决定何时批量处理
     */
    void syncThreadFunc();
    
    /**
     * @brief 处理当前批次的所有请求
     * 
     * [原子性保证] 批量写入文件并调用fsync()，
     * 确保同一批次的所有请求具有相同的成功/失败状态
     */
    void processBatch();
    
    /**
     * @brief 检查是否应该触发同步
     * @return true表示应该同步，false表示继续等待
     * 
     * [双重触发机制] 检查批量大小和时间间隔两个条件
     */
    bool shouldSync() const;
    
    std::unique_ptr<AppendFile> file_;                    ///< 文件写入器，封装系统调用
    const size_t batchSize_;                              ///< 批量大小阈值
    const std::chrono::milliseconds syncInterval_;       ///< 同步时间间隔阈值
    
    std::atomic<bool> running_;                           ///< 运行状态标志，原子操作保证线程安全
    std::thread syncThread_;                              ///< 后台同步线程
    
    // [分片锁] 保护共享数据的同步原语
    mutable std::mutex mutex_;                            ///< 保护请求队列的互斥锁
    std::condition_variable cond_;                        ///< 条件变量，用于线程间通信
    std::queue<CommitRequest> pendingRequests_;           ///< 待处理请求队列
    
    // 性能统计数据，使用独立的锁避免影响主路径性能
    mutable std::mutex statsMutex_;                       ///< 保护统计数据的互斥锁
    uint64_t totalCommits_;                               ///< 总提交数统计
    uint64_t totalBatches_;                               ///< 总批次数统计
    uint64_t totalBytes_;                                 ///< 总字节数统计
    std::chrono::steady_clock::time_point lastSyncTime_; ///< 上次同步时间
    
    // 当前批次状态信息
    size_t currentBatchSize_;                             ///< 当前批次累积大小
    std::chrono::steady_clock::time_point batchStartTime_; ///< 当前批次开始时间
};

} // namespace base
} // namespace minkv