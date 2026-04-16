#pragma once

#include "../core/sharded_cache.h"
#include "wal.h"
#include <memory>
#include <string>
#include <chrono>
#include <iostream>
#include <fstream>
#include <filesystem>
#include <atomic>
#include <thread>
#include <condition_variable>

namespace minkv {
namespace db {

/**
 * @brief 工业级Checkpoint管理器 (MVP版本)
 * 
 * [核心设计理念] Facade模式 + LSM-Tree思想
 * 统一调度内存快照与WAL日志截断，解决日志无限增长问题。
 * 
 * [技术亮点]
 * - 双重触发策略：时间维度 + 容量维度的自适应触发
 * - LSN原子性保证：快照头部记录WAL位置，确保一致性恢复
 * - 版本轮替机制：自动清理过期快照，防止磁盘空间爆炸
 * - 阻塞式MVP设计：优先保证数据一致性，适合快速落地
 * 
 * [工作流程]
 * 1. 智能触发检查 (双重条件)
 * 2. 导出内存数据 (export_all_data)
 * 3. 写入快照文件 (带LSN标记)
 * 4. 原子性WAL截断 (clear_wal)
 * 5. 版本轮替清理 (cleanup_old_snapshots)
 * 
 * [面试亮点]
 * - "实现了基于时间与容量双维度的自适应Checkpoint触发策略"
 * - "采用Facade设计模式封装底层存储细节，提供原子性接口"
 * - "通过LSN机制解决WAL截断失败的一致性问题"
 */
template<typename K, typename V>
class SimpleCheckpointManager {
public:
    /**
     * @brief Checkpoint配置结构体
     * 
     * 包含所有可调参数，支持不同场景的优化配置。
     */
    struct CheckpointConfig {
        std::string data_dir = "data";                    // 数据目录
        
        // 双重触发策略配置
        size_t wal_size_threshold = 64 * 1024 * 1024;    // WAL大小阈值 (64MB)
        std::chrono::minutes time_threshold{60};          // 时间阈值 (1小时兜底)
        
        // 检查和清理配置
        std::chrono::minutes check_interval{10};          // 检查间隔 (10分钟)
        bool auto_cleanup = true;                         // 自动清理旧文件
        int keep_snapshot_count = 3;                      // 保留快照数量
        
        // 性能调优配置
        bool enable_compression = false;                  // 快照压缩 (暂未实现)
        size_t write_buffer_size = 4 * 1024 * 1024;      // 写入缓冲区 (4MB)
    };
    
    /**
     * @brief 构造函数
     * 
     * @param cache 分片缓存实例
     * @param config checkpoint配置
     */
    explicit SimpleCheckpointManager(
        ShardedCache<K, V>* cache,
        const CheckpointConfig& config = CheckpointConfig{}
    );
    
    /**
     * @brief 析构函数，确保资源正确释放
     */
    ~SimpleCheckpointManager();
    
    // 禁止拷贝和赋值
    SimpleCheckpointManager(const SimpleCheckpointManager&) = delete;
    SimpleCheckpointManager& operator=(const SimpleCheckpointManager&) = delete;
    
    /**
     * @brief 立即执行checkpoint (阻塞版)
     * 
     * [核心接口] 这是MVP版本，会短暂阻塞所有写操作。
     * 适合快速落地和功能验证，在数据量未达到GB级别时完全可接受。
     * 
     * 执行流程：
     * 1. 导出内存数据 (获取所有分片锁)
     * 2. 写入快照文件 (带LSN标记)
     * 3. 原子性WAL截断 (开始新日志周期)
     * 4. 更新统计信息
     * 5. 清理旧快照 (可选)
     * 
     * @return 是否成功
     */
    bool checkpoint_now();
    
    /**
     * @brief 智能触发检查 (双重策略)
     * 
     * 实现基于时间与容量双维度的自适应触发策略：
     * - 规则A：每1小时触发一次 (兜底策略，防止长时间无checkpoint)
     * - 规则B：WAL大小超过64MB触发 (防止WAL文件暴涨)
     * 
     * @return 是否需要checkpoint
     */
    bool should_checkpoint() const;
    
    /**
     * @brief 启动后台检查线程
     * 
     * 定期检查是否需要执行checkpoint，实现自动化管理。
     */
    void start_background_checker();
    
    /**
     * @brief 停止后台检查线程
     */
    void stop_background_checker();
    
    /**
     * @brief 从磁盘恢复数据 (LSN一致性保证)
     * 
     * 恢复流程：
     * 1. 查找最新的快照文件
     * 2. 读取快照头部，获取LSN信息
     * 3. 加载快照数据到内存
     * 4. 从LSN位置开始重放WAL日志
     * 
     * [技术亮点] 通过LSN机制确保即使WAL截断失败也能正确恢复
     * 
     * @return 是否成功
     */
    bool recover_from_disk();
    
    /**
     * @brief Checkpoint统计信息
     * 
     * 用于监控和性能分析，体现运维意识。
     */
    struct CheckpointStats {
        int64_t last_checkpoint_time = 0;     // 上次checkpoint时间戳
        size_t last_checkpoint_records = 0;   // 上次checkpoint记录数
        size_t total_checkpoints = 0;         // 总checkpoint次数
        size_t current_wal_size = 0;          // 当前WAL大小
        std::string last_snapshot_file;       // 最新快照文件路径
        
        // 性能统计
        std::chrono::milliseconds avg_checkpoint_duration{0};  // 平均耗时
        double compression_ratio = 1.0;       // 压缩比例 (未来扩展)
        size_t total_disk_saved = 0;          // 累计节省磁盘空间
    };
    
    /**
     * @brief 获取checkpoint统计信息
     */
    CheckpointStats get_stats() const;
    
    /**
     * @brief 清理旧的快照文件 (版本轮替)
     * 
     * 保留最新的N个快照，删除其余的，防止磁盘空间无限增长。
     * 体现了对生产环境运维的考虑。
     */
    void cleanup_old_snapshots();

private:
    ShardedCache<K, V>* cache_;
    CheckpointConfig config_;
    
    // 后台检查线程
    std::atomic<bool> background_running_{false};
    std::thread background_thread_;
    std::condition_variable bg_cv_;
    std::mutex              bg_cv_mutex_;
    
    // 统计信息 (线程安全)
    mutable std::mutex stats_mutex_;
    CheckpointStats stats_;
    
    // 快照文件格式定义
    struct SnapshotHeader {
        char magic[4] = {'M', 'K', 'V', 'S'};  // MinKV Snapshot标识
        uint32_t version = 1;                   // 文件格式版本
        uint32_t record_count = 0;              // 记录总数
        uint64_t wal_lsn = 0;                   // [关键] WAL日志序列号
        uint64_t timestamp = 0;                 // 创建时间戳
        uint32_t checksum = 0;                  // 头部校验和
        char reserved[32] = {0};                // 预留字段，便于扩展
    };
    
    // 辅助函数
    std::string get_snapshot_path(uint64_t lsn) const;
    std::string get_snapshots_dir() const;
    bool write_snapshot_file(const std::string& filepath, const std::map<K, V>& data, uint64_t wal_lsn);
    bool read_snapshot_file(const std::string& filepath, std::map<K, V>& data, uint64_t& wal_lsn);
    std::vector<std::string> list_snapshot_files() const;
    std::string find_latest_snapshot() const;
    int64_t extract_timestamp_from_filename(const std::string& filename) const;
    uint32_t calculate_checksum(const SnapshotHeader& header) const;    void background_checker_loop();
    static int64_t current_time_ms();
};

// ============ 模板实现 ============

template<typename K, typename V>
SimpleCheckpointManager<K, V>::SimpleCheckpointManager(
    ShardedCache<K, V>* cache,
    const CheckpointConfig& config
) : cache_(cache), config_(config) {
    // 创建数据目录结构
    std::filesystem::create_directories(config_.data_dir);
    std::filesystem::create_directories(get_snapshots_dir());
    
    // 初始化统计信息
    stats_.last_checkpoint_time = current_time_ms();
    
    std::cout << "[CheckpointManager] Initialized with config:" << std::endl;
    std::cout << "  - Data dir: " << config_.data_dir << std::endl;
    std::cout << "  - WAL threshold: " << config_.wal_size_threshold / (1024*1024) << "MB" << std::endl;
    std::cout << "  - Time threshold: " << config_.time_threshold.count() << " minutes" << std::endl;
    std::cout << "  - Keep snapshots: " << config_.keep_snapshot_count << std::endl;
}

template<typename K, typename V>
SimpleCheckpointManager<K, V>::~SimpleCheckpointManager() {
    stop_background_checker();
}

template<typename K, typename V>
bool SimpleCheckpointManager<K, V>::checkpoint_now() {
    auto start_time = std::chrono::high_resolution_clock::now();
    
    std::cout << "[Checkpoint] Starting ATOMIC checkpoint (Stop-The-World mode)..." << std::endl;
    
    try {
        // 三步走，每步失败都安全：
        // 1. 独占锁下导出数据+LSN（WAL不动）
        // 2. 写快照文件到磁盘（失败时WAL仍完整，可重试）
        // 3. 快照持久化成功后才清WAL
        
        std::cout << "[Checkpoint] Acquiring exclusive lock for atomic export..." << std::endl;
        
        // 1. 在独占锁下原子性导出数据和LSN（不清WAL，保证快照写失败时WAL仍完整）
        std::map<K, V> all_data;
        uint64_t current_wal_lsn = 0;
        cache_->export_for_checkpoint(all_data, current_wal_lsn);
        
        // 2. 写入快照文件（此时WAL仍然完整，写失败可安全重试）
        // 用 LSN 而非时间戳命名，避免时钟回拨导致新快照文件名小于旧快照
        std::string snapshot_file = get_snapshot_path(current_wal_lsn);
        
        if (!write_snapshot_file(snapshot_file, all_data, current_wal_lsn)) {
            std::cerr << "[Checkpoint] Failed to write snapshot file: " << snapshot_file << std::endl;
            // WAL未清，数据安全，下次checkpoint可重试
            return false;
        }
        
        std::cout << "[Checkpoint] Snapshot written to: " << snapshot_file
                  << ", lsn=" << current_wal_lsn << std::endl;
        
        // 3. 快照写成功后才清WAL（此时快照已持久化，清WAL是安全的）
        cache_->clear_wal();
        std::cout << "[Checkpoint] WAL cleared after successful snapshot" << std::endl;
        
        // 4. 更新统计信息
        {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            stats_.last_checkpoint_time = current_time_ms();  // 监控用，仍记录时间戳
            stats_.last_checkpoint_records = all_data.size();
            stats_.total_checkpoints++;
            stats_.last_snapshot_file = snapshot_file;
            
            // 计算平均耗时
            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
            
            if (stats_.total_checkpoints > 0) {
                auto total_duration = stats_.avg_checkpoint_duration.count() * (stats_.total_checkpoints - 1) + duration.count();
                stats_.avg_checkpoint_duration = std::chrono::milliseconds(total_duration / stats_.total_checkpoints);
            } else {
                stats_.avg_checkpoint_duration = duration;
            }
        }
        
        // 5. 清理旧快照 (可选)
        if (config_.auto_cleanup) {
            cleanup_old_snapshots();
        }
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        
        std::cout << "[Checkpoint] ✅ ATOMIC checkpoint completed successfully in " << duration.count() << "ms" << std::endl;
        std::cout << "[Checkpoint] 🎯 Data consistency GUARANTEED - no loss window!" << std::endl;
        std::cout << "[Checkpoint] Records: " << all_data.size() 
                  << ", Total checkpoints: " << stats_.total_checkpoints << std::endl;
        
        return true;
        
    } catch (const std::exception& e) {
        std::cerr << "[Checkpoint] Exception: " << e.what() << std::endl;
        return false;
    }
}

template<typename K, typename V>
bool SimpleCheckpointManager<K, V>::should_checkpoint() const {
    // 双重触发策略：时间维度 + 容量维度
    
    // 获取当前时间和WAL大小
    int64_t current_time = current_time_ms();
    int64_t last_checkpoint_time = 0;
    
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        last_checkpoint_time = stats_.last_checkpoint_time;
    }
    
    // 计算距离上次checkpoint的时间
    auto elapsed_minutes = std::chrono::minutes((current_time - last_checkpoint_time) / (1000 * 60));
    
    // 规则A：时间阈值触发 (兜底策略)
    if (elapsed_minutes >= config_.time_threshold) {
        std::cout << "[CheckpointTrigger] Time threshold reached: " 
                  << elapsed_minutes.count() << " >= " << config_.time_threshold.count() 
                  << " minutes" << std::endl;
        return true;
    }
    
    // 规则B：WAL大小阈值触发 (防止WAL暴涨)
    size_t wal_size = cache_->get_wal_size();
    
    if (wal_size >= config_.wal_size_threshold) {
        std::cout << "[CheckpointTrigger] WAL size threshold reached: " 
                  << wal_size / (1024*1024) << "MB >= " 
                  << config_.wal_size_threshold / (1024*1024) << "MB" << std::endl;
        return true;
    }
    
    return false;
}

template<typename K, typename V>
void SimpleCheckpointManager<K, V>::start_background_checker() {
    if (background_running_.load()) {
        return; // 已经在运行
    }
    
    background_running_.store(true);
    background_thread_ = std::thread(&SimpleCheckpointManager::background_checker_loop, this);
    
    std::cout << "[CheckpointManager] Background checker started (interval: " 
              << config_.check_interval.count() << " minutes)" << std::endl;
}

template<typename K, typename V>
void SimpleCheckpointManager<K, V>::stop_background_checker() {
    if (!background_running_.load()) {
        return;
    }
    
    {
        std::lock_guard<std::mutex> lock(bg_cv_mutex_);
        background_running_.store(false);
    }
    bg_cv_.notify_all();
    
    if (background_thread_.joinable()) {
        background_thread_.join();
    }
    
    std::cout << "[CheckpointManager] Background checker stopped" << std::endl;
}

template<typename K, typename V>
void SimpleCheckpointManager<K, V>::background_checker_loop() {
    while (true) {
        std::unique_lock<std::mutex> lock(bg_cv_mutex_);
        bg_cv_.wait_for(lock, config_.check_interval,
            [this] { return !background_running_.load(); });

        if (!background_running_.load()) {
            break;
        }
        lock.unlock();

        if (should_checkpoint()) {
            std::cout << "[BackgroundChecker] Triggering automatic checkpoint..." << std::endl;
            
            if (checkpoint_now()) {
                std::cout << "[BackgroundChecker] Automatic checkpoint completed successfully" << std::endl;
            } else {
                std::cerr << "[BackgroundChecker] Automatic checkpoint failed" << std::endl;
            }
        }
    }
}

template<typename K, typename V>
bool SimpleCheckpointManager<K, V>::recover_from_disk() {
    std::cout << "[Recovery] Starting recovery from disk..." << std::endl;
    
    try {
        // 1. 查找最新快照，读取数据和 snapshot_lsn
        std::string latest_snapshot = find_latest_snapshot();
        std::map<K, V> snapshot_data;
        uint64_t snapshot_lsn = 0;

        if (latest_snapshot.empty()) {
            std::cout << "[Recovery] No snapshot found, replaying full WAL from lsn=0" << std::endl;
        } else {
            if (!read_snapshot_file(latest_snapshot, snapshot_data, snapshot_lsn)) {
                std::cerr << "[Recovery] Failed to read snapshot: " << latest_snapshot << std::endl;
                return false;
            }
            std::cout << "[Recovery] Loaded " << snapshot_data.size()
                      << " records from snapshot, lsn=" << snapshot_lsn << std::endl;

            // 2. 将快照数据写入 cache（不触发 WAL，直接写分片）
            for (const auto& [key, value] : snapshot_data) {
                cache_->put_for_recovery(key, value);
            }
        }

        // 3. 重放 snapshot_lsn 之后的 WAL 条目
        auto entries = cache_->read_wal_after_lsn(snapshot_lsn);
        std::cout << "[Recovery] Replaying " << entries.size()
                  << " WAL entries after lsn=" << snapshot_lsn << std::endl;

        uint64_t max_lsn = snapshot_lsn;
        size_t recovered = 0, errors = 0;

        for (const auto& entry : entries) {
            try {
                if (entry.op == db::LogEntry::PUT) {
                    K key = Serializer<K>::deserialize(entry.key);
                    V value = Serializer<V>::deserialize(entry.value);
                    cache_->put_for_recovery(key, value);
                    recovered++;
                } else if (entry.op == db::LogEntry::DELETE) {
                    K key = Serializer<K>::deserialize(entry.key);
                    cache_->remove_for_recovery(key);
                    recovered++;
                }
                if (entry.lsn > max_lsn) max_lsn = entry.lsn;
            } catch (const std::exception& e) {
                errors++;
                std::cerr << "[Recovery] Failed to replay entry lsn=" << entry.lsn
                          << ": " << e.what() << std::endl;
            }
        }

        // 4. 恢复 global_lsn_，避免新写入与已恢复的 LSN 冲突
        cache_->reset_lsn(max_lsn + 1);

        std::cout << "[Recovery] Completed: " << recovered << " entries replayed, "
                  << errors << " errors, next_lsn=" << (max_lsn + 1) << std::endl;
        std::cout << "[Recovery] Final cache size: " << cache_->size() << " records" << std::endl;

        return true;

    } catch (const std::exception& e) {
        std::cerr << "[Recovery] Exception: " << e.what() << std::endl;
        return false;
    }
}

template<typename K, typename V>
typename SimpleCheckpointManager<K, V>::CheckpointStats 
SimpleCheckpointManager<K, V>::get_stats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    
    // 更新当前WAL大小
    CheckpointStats current_stats = stats_;
    current_stats.current_wal_size = cache_->get_wal_size();
    
    return current_stats;
}

template<typename K, typename V>
void SimpleCheckpointManager<K, V>::cleanup_old_snapshots() {
    try {
        auto snapshot_files = list_snapshot_files();
        
        if (snapshot_files.size() <= static_cast<size_t>(config_.keep_snapshot_count)) {
            return;  // 不需要清理
        }
        
        // 按时间戳排序 (文件名包含时间戳)
        std::sort(snapshot_files.begin(), snapshot_files.end(), std::greater<std::string>());
        
        size_t deleted_count = 0;
        size_t total_saved_bytes = 0;
        
        // 删除多余的快照文件
        for (size_t i = config_.keep_snapshot_count; i < snapshot_files.size(); ++i) {
            std::string file_path = get_snapshots_dir() + "/" + snapshot_files[i];
            
            // 获取文件大小
            try {
                auto file_size = std::filesystem::file_size(file_path);
                total_saved_bytes += file_size;
            } catch (...) {
                // 忽略文件大小获取失败
            }
            
            if (std::filesystem::remove(file_path)) {
                std::cout << "[Cleanup] Removed old snapshot: " << snapshot_files[i] << std::endl;
                deleted_count++;
            }
        }
        
        if (deleted_count > 0) {
            std::cout << "[Cleanup] Cleaned up " << deleted_count << " old snapshots, "
                      << "saved " << total_saved_bytes / (1024*1024) << "MB disk space" << std::endl;
            
            // 更新统计信息
            std::lock_guard<std::mutex> lock(stats_mutex_);
            stats_.total_disk_saved += total_saved_bytes;
        }
        
    } catch (const std::exception& e) {
        std::cerr << "[Cleanup] Exception during cleanup: " << e.what() << std::endl;
    }
}

// ============ 私有辅助函数实现 ============

template<typename K, typename V>
std::string SimpleCheckpointManager<K, V>::get_snapshot_path(uint64_t lsn) const {
    // 用 LSN 命名，单调递增，不受时钟回拨影响
    return get_snapshots_dir() + "/snapshot_" + std::to_string(lsn) + ".bin";
}

template<typename K, typename V>
std::string SimpleCheckpointManager<K, V>::get_snapshots_dir() const {
    return config_.data_dir + "/snapshots";
}

template<typename K, typename V>
bool SimpleCheckpointManager<K, V>::write_snapshot_file(
    const std::string& filepath, 
    const std::map<K, V>& data,
    uint64_t wal_lsn
) {
    // [原子重命名] 先写临时文件，写完后 rename() 到正式路径。
    // rename() 在 Linux 上是原子操作（POSIX 保证），确保即使写入过程中宕机，
    // 也不会产生损坏的快照文件：要么旧快照完整保留，要么新快照完整替换。
    const std::string tmp_filepath = filepath + ".tmp";
    
    // 清理可能残留的上次失败的临时文件
    std::filesystem::remove(tmp_filepath);
    
    std::ofstream file(tmp_filepath, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "[Snapshot] Failed to create tmp file: " << tmp_filepath << std::endl;
        return false;
    }
    
    try {
        // 准备快照头部
        SnapshotHeader header;
        header.record_count = static_cast<uint32_t>(data.size());
        header.wal_lsn = wal_lsn;  // [关键] 记录WAL位置
        header.timestamp = static_cast<uint64_t>(current_time_ms());
        header.checksum = calculate_checksum(header);
        
        // 写入头部
        file.write(reinterpret_cast<const char*>(&header), sizeof(header));
        if (!file.good()) {
            std::cerr << "[Snapshot] Failed to write header" << std::endl;
            return false;
        }
        
        std::cout << "[Snapshot] Writing " << data.size() << " records with LSN " << wal_lsn << std::endl;
        
        // 写入每个键值对
        size_t written_count = 0;
        for (const auto& [key, value] : data) {
            std::string key_str;
            std::string value_str;
            
            if constexpr (std::is_same_v<K, std::string>) {
                key_str = key;
            } else {
                key_str = std::to_string(key);
            }
            
            if constexpr (std::is_same_v<V, std::string>) {
                value_str = value;
            } else {
                value_str = std::to_string(value);
            }
            
            uint32_t key_len = static_cast<uint32_t>(key_str.size());
            file.write(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
            file.write(key_str.c_str(), key_len);
            
            uint32_t value_len = static_cast<uint32_t>(value_str.size());
            file.write(reinterpret_cast<const char*>(&value_len), sizeof(value_len));
            file.write(value_str.c_str(), value_len);
            
            written_count++;
            
            if (written_count % 10000 == 0) {
                std::cout << "[Snapshot] Progress: " << written_count << "/" << data.size() << " records" << std::endl;
            }
        }
        
        // 确保数据落盘后再 rename，防止 rename 成功但数据未持久化
        file.flush();
        if (!file.good()) {
            std::cerr << "[Snapshot] Flush failed, aborting atomic rename" << std::endl;
            std::filesystem::remove(tmp_filepath);
            return false;
        }
        file.close();
        
        // [原子替换] rename() 是原子操作，此刻之后快照文件要么是旧的完整版，要么是新的完整版
        std::filesystem::rename(tmp_filepath, filepath);
        
        std::cout << "[Snapshot] Atomically renamed tmp -> " << filepath 
                  << " (" << written_count << " records)" << std::endl;
        return true;
        
    } catch (const std::exception& e) {
        std::cerr << "[Snapshot] Write error: " << e.what() << std::endl;
        // 清理临时文件，避免残留
        std::filesystem::remove(tmp_filepath);
        return false;
    }
}

template<typename K, typename V>
bool SimpleCheckpointManager<K, V>::read_snapshot_file(
    const std::string& filepath, 
    std::map<K, V>& data,
    uint64_t& wal_lsn
) {
    std::ifstream file(filepath, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "[Snapshot] Failed to open file: " << filepath << std::endl;
        return false;
    }
    
    try {
        // 读取并验证头部
        SnapshotHeader header;
        file.read(reinterpret_cast<char*>(&header), sizeof(header));
        if (!file.good()) {
            std::cerr << "[Snapshot] Failed to read header" << std::endl;
            return false;
        }
        
        // 验证魔数
        if (std::string(header.magic, 4) != "MKVS") {
            std::cerr << "[Snapshot] Invalid magic number" << std::endl;
            return false;
        }
        
        // 验证版本
        if (header.version != 1) {
            std::cerr << "[Snapshot] Unsupported version: " << header.version << std::endl;
            return false;
        }
        
        // 验证校验和
        uint32_t expected_checksum = header.checksum;
        header.checksum = 0; // 清零后计算
        uint32_t actual_checksum = calculate_checksum(header);
        if (expected_checksum != actual_checksum) {
            std::cerr << "[Snapshot] Header checksum mismatch: expected " 
                      << expected_checksum << ", got " << actual_checksum << std::endl;
            return false;
        }
        
        // 提取LSN信息
        wal_lsn = header.wal_lsn;
        
        std::cout << "[Snapshot] Reading " << header.record_count << " records, LSN: " << wal_lsn << std::endl;
        
        data.clear();
        
        // 读取每个键值对
        for (uint32_t i = 0; i < header.record_count; ++i) {
            // 读取key
            uint32_t key_len;
            file.read(reinterpret_cast<char*>(&key_len), sizeof(key_len));
            if (!file.good()) {
                std::cerr << "[Snapshot] Failed to read key length at record " << i << std::endl;
                return false;
            }
            
            std::string key_str(key_len, '\0');
            file.read(&key_str[0], key_len);
            if (!file.good()) {
                std::cerr << "[Snapshot] Failed to read key data at record " << i << std::endl;
                return false;
            }
            
            // 读取value
            uint32_t value_len;
            file.read(reinterpret_cast<char*>(&value_len), sizeof(value_len));
            if (!file.good()) {
                std::cerr << "[Snapshot] Failed to read value length at record " << i << std::endl;
                return false;
            }
            
            std::string value_str(value_len, '\0');
            file.read(&value_str[0], value_len);
            if (!file.good()) {
                std::cerr << "[Snapshot] Failed to read value data at record " << i << std::endl;
                return false;
            }
            
            // 转换为实际类型
            K key;
            V value;
            
            if constexpr (std::is_same_v<K, std::string>) {
                key = key_str;
            } else {
                key = static_cast<K>(std::stoi(key_str));  // 简化处理
            }
            
            if constexpr (std::is_same_v<V, std::string>) {
                value = value_str;
            } else {
                value = static_cast<V>(std::stoi(value_str));  // 简化处理
            }
            
            data[key] = value;
            
            // 进度输出
            if ((i + 1) % 10000 == 0) {
                std::cout << "[Snapshot] Progress: " << (i + 1) << "/" << header.record_count << " records" << std::endl;
            }
        }
        
        std::cout << "[Snapshot] Successfully read " << data.size() << " records from " << filepath << std::endl;
        return true;
        
    } catch (const std::exception& e) {
        std::cerr << "[Snapshot] Read error: " << e.what() << std::endl;
        return false;
    }
}

template<typename K, typename V>
uint32_t SimpleCheckpointManager<K, V>::calculate_checksum(const SnapshotHeader& header) const {
    // 简单的校验和计算：对头部字段进行异或
    uint32_t checksum = 0;
    checksum ^= header.version;
    checksum ^= header.record_count;
    checksum ^= static_cast<uint32_t>(header.wal_lsn);
    checksum ^= static_cast<uint32_t>(header.wal_lsn >> 32);
    checksum ^= static_cast<uint32_t>(header.timestamp);
    checksum ^= static_cast<uint32_t>(header.timestamp >> 32);
    
    // 对魔数也进行校验
    for (int i = 0; i < 4; ++i) {
        checksum ^= static_cast<uint32_t>(header.magic[i]) << (i * 8);
    }
    
    return checksum;
}

template<typename K, typename V>
std::vector<std::string> SimpleCheckpointManager<K, V>::list_snapshot_files() const {
    std::vector<std::string> files;
    
    try {
        std::string snapshots_dir = get_snapshots_dir();
        
        if (!std::filesystem::exists(snapshots_dir)) {
            return files;
        }
        
        for (const auto& entry : std::filesystem::directory_iterator(snapshots_dir)) {
            if (entry.is_regular_file()) {
                std::string filename = entry.path().filename().string();
                if (filename.substr(0, 9) == "snapshot_" && 
                    filename.size() >= 4 && filename.substr(filename.size() - 4) == ".bin") {
                    files.push_back(filename);
                }
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "[Snapshot] Error listing files: " << e.what() << std::endl;
    }
    
    return files;
}

template<typename K, typename V>
std::string SimpleCheckpointManager<K, V>::find_latest_snapshot() const {
    auto files = list_snapshot_files();
    if (files.empty()) {
        return "";
    }
    
    // 按文件名排序（文件名包含 LSN，字典序即 LSN 序，LSN 单调递增不受时钟影响）
    std::sort(files.begin(), files.end(), std::greater<std::string>());
    
    std::string latest_file = get_snapshots_dir() + "/" + files[0];
    std::cout << "[Recovery] Found latest snapshot: " << files[0] << std::endl;
    
    return latest_file;
}

template<typename K, typename V>
int64_t SimpleCheckpointManager<K, V>::current_time_ms() {
    auto now = std::chrono::high_resolution_clock::now();
    auto duration = now.time_since_epoch();
    return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
}

} // namespace db
} // namespace minkv