#pragma once

#include <thread>
#include <atomic>
#include <chrono>
#include <functional>
#include <vector>
#include <random>
#include <mutex>

namespace minkv {
namespace base {

/**
 * @brief 定期删除管理器 - 仿 Redis serverCron 机制
 * 
 * [核心优化] 实现类似 Redis 的主动过期删除策略，避免过期数据占用内存
 * 采用"渐进式删除"策略，每次只处理少量数据，不影响正常业务性能。
 * 
 * 设计特点：
 * - 非阻塞设计：使用 try_lock() 避免与业务线程竞争
 * - 随机采样：每次随机选择少量 key 检查，分摊删除成本
 * - 自适应频率：根据过期 key 比例动态调整检查频率
 * - 分片友好：支持分片缓存，每个分片独立处理
 * 
 * 这是 Redis、Memcached 等缓存系统的核心技术，
 * 在 MinKV 中用于主动清理过期数据，释放内存空间。
 * 
 * @note 线程安全，支持多分片并发处理
 */
class ExpirationManager {
public:
    /**
     * @brief 过期检查回调函数类型
     * @param shard_id 分片ID
     * @param sample_size 本次采样大小
     * @return 返回实际删除的过期key数量
     * 
     * 回调函数应该：
     * 1. 使用 try_lock() 尝试获取分片锁
     * 2. 如果获取失败，立即返回 0（说明业务繁忙）
     * 3. 如果获取成功，随机采样指定数量的 key
     * 4. 检查并删除过期的 key，返回删除数量
     */
    using ExpirationCallback = std::function<size_t(size_t shard_id, size_t sample_size)>;
    
    /**
     * @brief 构造定期删除管理器
     * @param shard_count 分片数量
     * @param check_interval 检查间隔，默认100ms
     * @param sample_size 每次采样大小，默认20个key
     * 
     * [性能平衡] 
     * - 检查间隔：100ms 平衡了及时性和性能开销
     * - 采样大小：20个key 是 Redis 的经典配置，经过大量实践验证
     */
    explicit ExpirationManager(size_t shard_count,
                              std::chrono::milliseconds check_interval = std::chrono::milliseconds(100),
                              size_t sample_size = 20);
    
    /**
     * @brief 析构函数，确保资源正确释放
     * 
     * [RAII] 析构时自动停止后台线程
     */
    ~ExpirationManager();
    
    // 禁止拷贝和赋值，确保资源管理的唯一性
    ExpirationManager(const ExpirationManager&) = delete;
    ExpirationManager& operator=(const ExpirationManager&) = delete;
    
    /**
     * @brief 启动定期删除服务
     * @param callback 过期检查回调函数
     * 
     * [生产者-消费者] 启动后台线程，定期调用回调函数处理过期数据
     */
    void start(ExpirationCallback callback);
    
    /**
     * @brief 停止定期删除服务
     * 
     * [异常安全] 等待后台线程完成当前工作后再退出
     */
    void stop();
    
    /**
     * @brief 性能统计信息结构体
     */
    struct Stats {
        uint64_t total_checks;          ///< 总检查次数
        uint64_t total_expired;         ///< 总过期删除数
        uint64_t total_skipped;         ///< 总跳过次数（锁竞争）
        double avg_expired_ratio;       ///< 平均过期比例
        std::chrono::milliseconds avg_check_time;  ///< 平均检查耗时
    };
    
    /**
     * @brief 获取性能统计信息
     * @return 当前的性能统计数据
     * 
     * [监控接口] 用于性能分析和调优
     */
    Stats getStats() const;
    
private:
    /**
     * @brief 后台定期检查线程主函数
     * 
     * [定时任务] 类似 Redis serverCron，定期遍历所有分片
     * 使用 try_lock() 实现非阻塞检查，避免影响业务性能
     */
    void cronThreadFunc();
    
    /**
     * @brief 处理单个分片的过期检查
     * @param shard_id 分片ID
     * @return 本次删除的过期key数量
     * 
     * [核心算法] 随机采样 + 非阻塞锁 + 自适应频率
     */
    size_t processShard(size_t shard_id);
    
    const size_t shard_count_;                              ///< 分片数量
    const std::chrono::milliseconds check_interval_;       ///< 检查间隔
    const size_t sample_size_;                              ///< 采样大小
    
    std::atomic<bool> running_;                             ///< 运行状态标志
    std::thread cron_thread_;                               ///< 后台定时线程
    ExpirationCallback callback_;                           ///< 过期检查回调
    
    // 性能统计数据
    mutable std::mutex stats_mutex_;                        ///< 保护统计数据的互斥锁
    uint64_t total_checks_;                                 ///< 总检查次数
    uint64_t total_expired_;                                ///< 总过期删除数
    uint64_t total_skipped_;                                ///< 总跳过次数
    std::vector<double> expired_ratios_;                    ///< 过期比例历史记录
    
    // 随机数生成器
    std::mt19937 rng_;                                      ///< 随机数生成器
    std::uniform_int_distribution<size_t> shard_dist_;      ///< 分片选择分布
};

} // namespace base
} // namespace minkv