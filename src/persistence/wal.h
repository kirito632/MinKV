#pragma once

#include <string>
#include <vector>
#include <fstream>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <thread>
#include <condition_variable>
#include <atomic>
#include <map>
#include <optional>
#include <type_traits>
#include <filesystem>
#include <algorithm>
#include <fcntl.h>
#include <unistd.h>

namespace minkv {
namespace db {

/**
 * @brief WAL (Write-Ahead Log) 日志条目
 * 
 * 每个日志条目记录一次数据库操作（PUT、DELETE、SNAPSHOT）。
 * 格式：[OpType(1B)][KeyLen(4B)][Key][ValueLen(4B)][Value][Timestamp(8B)][Checksum(4B)]
 */
struct LogEntry {
    enum OpType : uint8_t {
        PUT = 1,
        DELETE = 2,
        SNAPSHOT = 3
    };
    
    OpType op;                    // 操作类型
    std::string key;              // 键
    std::string value;            // 值（DELETE 时为空）
    int64_t timestamp_ms;         // 时间戳（毫秒，保留用于调试/审计）
    uint64_t lsn{0};              // Log Sequence Number，严格单调递增，由 ShardedCache::next_lsn() 分配
    
    // 计算校验和（key + value，lsn 不参与）
    uint32_t compute_checksum() const;
};

/**
 * @brief Write-Ahead Log 实现
 * 
 * 核心功能：
 * 1. 异步日志写入：日志先写到内存缓冲，后台线程定期 fsync
 * 2. 快照支持：定期创建快照，加速恢复
 * 3. 宕机恢复：从快照 + 日志重放恢复数据
 * 
 * 线程安全：所有操作都是线程安全的
 */
class WriteAheadLog {
public:
    /**
     * @brief 构造函数
     * 
     * @param data_dir 数据目录（存放 wal.log 和 snapshot.bin）
     * @param buffer_size 日志缓冲区大小（字节），默认 1MB
     * @param fsync_interval_ms 后台 fsync 间隔（毫秒），默认 1000ms
     */
    explicit WriteAheadLog(
        const std::string& data_dir,
        size_t buffer_size = 1024 * 1024,
        int64_t fsync_interval_ms = 1000
    );
    
    ~WriteAheadLog();
    
    // 禁止拷贝
    WriteAheadLog(const WriteAheadLog&) = delete;
    WriteAheadLog& operator=(const WriteAheadLog&) = delete;
    
    /**
     * @brief 追加日志条目
     * 
     * 将日志条目追加到内存缓冲区。
     * 缓冲区满或定时器触发时，自动 fsync 到磁盘。
     * 
     * @param entry 日志条目
     * @return 是否成功
     */
    bool append(const LogEntry& entry);
    
    /**
     * @brief 读取所有日志条目
     * 
     * 从快照之后的所有日志条目。
     * 用于宕机恢复时重放日志。
     * 
     * @return 日志条目列表
     */
    std::vector<LogEntry> read_all();
    
    /**
     * @brief 读取快照之后的日志（按 LSN 过滤）
     * 
     * @param snapshot_lsn 快照时刻的 LSN（来自 SnapshotHeader::wal_lsn）
     * @return 所有 entry.lsn > snapshot_lsn 的条目，顺序与文件一致
     */
    std::vector<LogEntry> read_after_snapshot(uint64_t snapshot_lsn);
    
    /**
     * @brief 创建快照
     * 
     * 将当前的内存数据序列化到快照文件。
     * 快照文件名：snapshot_<timestamp>.bin
     * 
     * @param data 要快照的数据（key-value 对）
     * @return 快照 ID（时间戳），失败返回 0
     */
    template<typename K, typename V>
    int64_t create_snapshot(const std::map<K, V>& data);
    
    /**
     * @brief 读取最新快照
     * 
     * 扫描 snapshot_dir_，找文件名最大的 snapshot_*.bin（字典序 = 时间序），
     * 读取并校验 SnapshotHeader，提取 wal_lsn 和数据。
     * 
     * @param data 输出参数，快照数据
     * @return 快照时刻的 wal_lsn；无快照或读取失败返回 0，data 不被修改
     */
    template<typename K, typename V>
    uint64_t read_latest_snapshot(std::map<K, V>& data);
    
    /**
     * @brief 强制 fsync 到磁盘
     * 
     * 通常不需要调用，后台线程会自动 fsync。
     * 用于关键操作后确保数据持久化。
     * 
     * @return 是否成功
     */
    bool flush();
    
    /**
     * @brief 启动后台 fsync 线程
     * 
     * 后台线程定期将缓冲区数据 fsync 到磁盘。
     */
    void start_background_fsync();
    
    /**
     * @brief 停止后台 fsync 线程
     * 
     * 调用此方法会等待后台线程退出。
     */
    void stop_background_fsync();
    
    /**
     * @brief 获取当前日志大小（字节）
     */
    size_t get_log_size() const;
    
    /**
     * @brief 获取缓冲区中待 fsync 的字节数
     */
    size_t get_buffer_size() const;
    
    /**
     * @brief 清空所有日志和快照
     * 
     * 谨慎使用！这会删除所有持久化数据。
     */
    void clear_all();

private:
    std::string data_dir_;                    // 数据目录
    std::string wal_file_;                    // WAL 文件路径
    std::string snapshot_dir_;                // 快照目录
    
    size_t buffer_size_;                      // 缓冲区大小
    int64_t fsync_interval_ms_;               // fsync 间隔
    
    std::vector<uint8_t> buffer_;             // 内存缓冲区
    mutable std::mutex buffer_mutex_;         // 缓冲区互斥锁
    
    std::ofstream wal_stream_;                // WAL 文件输出流（用于读取）
    int wal_fd_{-1};                          // WAL 文件描述符（用于 write + fsync）
    
    // 后台 fsync 线程
    std::thread fsync_thread_;
    mutable std::atomic<bool> fsync_running_{false};
    std::condition_variable fsync_cv_;
    std::mutex              fsync_cv_mutex_;
    
    // 辅助函数
    static int64_t current_time_ms();
    void fsync_thread_main();
    bool write_to_buffer(const LogEntry& entry);
    bool flush_buffer_to_disk();
    
    // 序列化/反序列化
    static std::vector<uint8_t> serialize_entry(const LogEntry& entry);
    // 返回 nullopt 表示校验和不匹配（条目损坏）
    static std::optional<LogEntry> deserialize_entry(const uint8_t* data, size_t size);
};

// ============ 模板实现 ============

// SnapshotHeader 与 SimpleCheckpointManager 保持一致的快照文件格式
struct WalSnapshotHeader {
    char     magic[4];       // 'M','K','V','S'
    uint32_t version;        // 1
    uint32_t record_count;
    uint64_t wal_lsn;        // 快照时刻的 LSN
    uint64_t timestamp;
    uint32_t checksum;
    char     reserved[32];
};

template<typename K, typename V>
int64_t WriteAheadLog::create_snapshot(const std::map<K, V>& data) {
    int64_t snapshot_id = current_time_ms();
    std::string snapshot_file = snapshot_dir_ + "/snapshot_" + std::to_string(snapshot_id) + ".bin";
    
    std::ofstream file(snapshot_file, std::ios::binary);
    if (!file.is_open()) {
        return 0;
    }
    
    // 写入快照 ID
    file.write(reinterpret_cast<const char*>(&snapshot_id), sizeof(snapshot_id));
    
    // 写入数据个数
    uint32_t count = data.size();
    file.write(reinterpret_cast<const char*>(&count), sizeof(count));
    
    // 写入每个 key-value 对
    for (const auto& [key, value] : data) {
        std::string key_str;
        if constexpr (std::is_same_v<K, std::string>) {
            key_str = key;
        } else {
            key_str = std::to_string(key);
        }
        uint32_t key_len = key_str.size();
        file.write(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
        file.write(key_str.c_str(), key_len);
        
        std::string value_str;
        if constexpr (std::is_same_v<V, std::string>) {
            value_str = value;
        } else {
            value_str = std::to_string(value);
        }
        uint32_t value_len = value_str.size();
        file.write(reinterpret_cast<const char*>(&value_len), sizeof(value_len));
        file.write(value_str.c_str(), value_len);
    }
    
    file.close();
    return snapshot_id;
}

template<typename K, typename V>
uint64_t WriteAheadLog::read_latest_snapshot(std::map<K, V>& data) {
    namespace fs = std::filesystem;

    // 1. 扫描 snapshot_dir_，找文件名最大的 snapshot_*.bin（字典序 = 时间序）
    std::vector<std::string> files;
    try {
        if (!fs::exists(snapshot_dir_)) return 0;
        for (const auto& entry : fs::directory_iterator(snapshot_dir_)) {
            if (!entry.is_regular_file()) continue;
            std::string name = entry.path().filename().string();
            if (name.size() > 9 && name.substr(0, 9) == "snapshot_" &&
                name.size() > 4 && name.substr(name.size() - 4) == ".bin") {
                files.push_back(name);
            }
        }
    } catch (...) {
        return 0;
    }
    if (files.empty()) return 0;

    std::string latest = *std::max_element(files.begin(), files.end());
    std::string full_path = snapshot_dir_ + "/" + latest;

    std::ifstream file(full_path, std::ios::binary);
    if (!file.is_open()) return 0;

    // 2. 读取并校验 SnapshotHeader
    WalSnapshotHeader header{};
    file.read(reinterpret_cast<char*>(&header), sizeof(header));
    if (!file.good()) return 0;

    if (std::string(header.magic, 4) != "MKVS") return 0;
    if (header.version != 1) return 0;

    // 校验 checksum（与 SimpleCheckpointManager::calculate_checksum 保持一致）
    uint32_t stored = header.checksum;
    uint32_t calc = 0;
    calc ^= header.version;
    calc ^= header.record_count;
    calc ^= static_cast<uint32_t>(header.wal_lsn);
    calc ^= static_cast<uint32_t>(header.wal_lsn >> 32);
    calc ^= static_cast<uint32_t>(header.timestamp);
    calc ^= static_cast<uint32_t>(header.timestamp >> 32);
    for (int i = 0; i < 4; ++i)
        calc ^= static_cast<uint32_t>(static_cast<uint8_t>(header.magic[i])) << (i * 8);
    if (stored != calc) return 0;

    uint64_t snapshot_lsn = header.wal_lsn;

    // 3. 读取 record_count 条 length-prefixed key/value，填充临时 map
    std::map<K, V> tmp;
    for (uint32_t i = 0; i < header.record_count; ++i) {
        uint32_t key_len = 0;
        file.read(reinterpret_cast<char*>(&key_len), 4);
        if (!file.good()) return 0;
        std::string key_str(key_len, '\0');
        file.read(&key_str[0], key_len);
        if (!file.good()) return 0;

        uint32_t val_len = 0;
        file.read(reinterpret_cast<char*>(&val_len), 4);
        if (!file.good()) return 0;
        std::string val_str(val_len, '\0');
        file.read(&val_str[0], val_len);
        if (!file.good()) return 0;

        K key;
        V val;
        if constexpr (std::is_same_v<K, std::string>) {
            key = key_str;
        } else {
            key = static_cast<K>(std::stoi(key_str));
        }
        if constexpr (std::is_same_v<V, std::string>) {
            val = val_str;
        } else {
            val = static_cast<V>(std::stoi(val_str));
        }
        tmp[key] = val;
    }

    // 4. 全部读取成功后才写入 data（失败时 data 不被修改）
    data = std::move(tmp);
    return snapshot_lsn;
}

} // namespace db
} // namespace minkv
