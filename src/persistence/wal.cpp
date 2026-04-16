#include "wal.h"
#include <chrono>
#include <filesystem>
#include <algorithm>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>

namespace minkv {
namespace db {

// ============ 辅助函数 ============

int64_t WriteAheadLog::current_time_ms() {
    auto now = std::chrono::high_resolution_clock::now();
    auto duration = now.time_since_epoch();
    return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
}

uint32_t LogEntry::compute_checksum() const {
    // 简单的校验和：对 key + value 计算 hash
    std::string data = key + value;
    uint32_t checksum = 0;
    for (char c : data) {
        checksum = checksum * 31 + static_cast<uint8_t>(c);
    }
    return checksum;
}

// ============ WriteAheadLog 实现 ============

WriteAheadLog::WriteAheadLog(
    const std::string& data_dir,
    size_t buffer_size,
    int64_t fsync_interval_ms
)
    : data_dir_(data_dir)
    , buffer_size_(buffer_size)
    , fsync_interval_ms_(fsync_interval_ms)
{
    try {
        // 创建数据目录
        std::filesystem::create_directories(data_dir);
        
        // 设置文件路径
        wal_file_ = data_dir + "/wal.log";
        snapshot_dir_ = data_dir + "/snapshots";
        std::filesystem::create_directories(snapshot_dir_);
        
        // 初始化缓冲区（可能抛 bad_alloc）
        buffer_.reserve(buffer_size);
        
        // 用 POSIX open 打开 WAL 文件，O_SYNC 保证每次 write 都落盘
        // 这里不用 O_SYNC，而是手动 fsync，以便批量刷盘控制性能
        wal_fd_ = ::open(wal_file_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (wal_fd_ < 0) {
            throw std::runtime_error(
                std::string("Failed to open WAL file: ") + wal_file_ +
                " (" + std::strerror(errno) + ")"
            );
        }

        // wal_stream_ 仅用于 read_all() 读取，保持只读打开
        wal_stream_.open(wal_file_, std::ios::binary | std::ios::in);
    } catch (...) {
        if (wal_fd_ >= 0) {
            ::close(wal_fd_);
            wal_fd_ = -1;
        }
        if (wal_stream_.is_open()) {
            wal_stream_.close();
        }
        throw;
    }
}

WriteAheadLog::~WriteAheadLog() {
    stop_background_fsync();
    flush();
    if (wal_fd_ >= 0) {
        ::close(wal_fd_);
        wal_fd_ = -1;
    }
    if (wal_stream_.is_open()) {
        wal_stream_.close();
    }
}

bool WriteAheadLog::append(const LogEntry& entry) {
    std::lock_guard<std::mutex> lock(buffer_mutex_);
    if (!write_to_buffer(entry)) {
        return false;
    }
    // fsync_interval_ms_ == 0 表示同步刷盘模式：每次写入立即 fsync
    if (fsync_interval_ms_ == 0) {
        return flush_buffer_to_disk();
    }
    return true;
}

bool WriteAheadLog::write_to_buffer(const LogEntry& entry) {
    auto serialized = serialize_entry(entry);
    
    // 检查缓冲区是否满
    if (buffer_.size() + serialized.size() > buffer_size_) {
        // 缓冲区满，先 fsync
        if (!flush_buffer_to_disk()) {
            return false;
        }
    }
    
    // 追加到缓冲区
    buffer_.insert(buffer_.end(), serialized.begin(), serialized.end());
    return true;
}

bool WriteAheadLog::flush_buffer_to_disk() {
    if (buffer_.empty()) {
        return true;
    }

    if (wal_fd_ < 0) {
        return false;
    }

    // 用 POSIX write 写入，处理短写（partial write）
    const uint8_t* ptr = buffer_.data();
    size_t remaining = buffer_.size();
    while (remaining > 0) {
        ssize_t written = ::write(wal_fd_, ptr, remaining);
        if (written < 0) {
            if (errno == EINTR) continue;  // 被信号中断，重试
            return false;                  // 真正的写入错误（磁盘满等）
        }
        ptr += written;
        remaining -= written;
    }

    // 真正的 fsync：把 OS page cache 刷到磁盘
    if (::fsync(wal_fd_) != 0) {
        return false;
    }

    buffer_.clear();
    return true;
}

bool WriteAheadLog::flush() {
    std::lock_guard<std::mutex> lock(buffer_mutex_);
    return flush_buffer_to_disk();
}

std::vector<LogEntry> WriteAheadLog::read_all() {
    std::vector<LogEntry> entries;
    
    std::ifstream file(wal_file_, std::ios::binary);
    if (!file.is_open()) {
        return entries;
    }
    
    // 读取文件内容
    file.seekg(0, std::ios::end);
    size_t file_size = file.tellg();
    file.seekg(0, std::ios::beg);
    
    std::vector<uint8_t> data(file_size);
    file.read(reinterpret_cast<char*>(data.data()), file_size);
    file.close();
    
    // 反序列化日志条目
    size_t offset = 0;
    while (offset < data.size()) {
        // 读取条目大小（前 4 字节）
        if (offset + 4 > data.size()) break;
        
        uint32_t entry_size = *reinterpret_cast<const uint32_t*>(data.data() + offset);
        offset += 4;
        
        if (offset + entry_size > data.size()) break;
        
        auto entry = deserialize_entry(data.data() + offset, entry_size);
        if (!entry) {
            // 校验和不匹配：遇到损坏条目，截断到此处
            // 这是 WAL 恢复的标准做法：只重放到最后一个完整提交点
            break;
        }
        entries.push_back(std::move(*entry));
        offset += entry_size;
    }
    
    return entries;
}

std::vector<LogEntry> WriteAheadLog::read_after_snapshot(uint64_t snapshot_lsn) {
    auto all_entries = read_all();
    
    std::vector<LogEntry> result;
    for (const auto& entry : all_entries) {
        if (entry.lsn > snapshot_lsn) {
            result.push_back(entry);
        }
    }
    
    return result;
}

void WriteAheadLog::start_background_fsync() {
    if (fsync_running_.load(std::memory_order_relaxed)) {
        return;
    }
    
    fsync_running_.store(true, std::memory_order_relaxed);
    fsync_thread_ = std::thread([this]() {
        this->fsync_thread_main();
    });
}

void WriteAheadLog::stop_background_fsync() {
    {
        std::lock_guard<std::mutex> lock(fsync_cv_mutex_);
        fsync_running_.store(false, std::memory_order_relaxed);
    }
    fsync_cv_.notify_all();
    
    if (fsync_thread_.joinable()) {
        fsync_thread_.join();
    }
}

void WriteAheadLog::fsync_thread_main() {
    // fsync_interval_ms_ == 0 时走同步刷盘模式，后台线程不参与
    if (fsync_interval_ms_ == 0) {
        return;
    }
    while (true) {
        std::unique_lock<std::mutex> lock(fsync_cv_mutex_);
        fsync_cv_.wait_for(lock,
            std::chrono::milliseconds(fsync_interval_ms_),
            [this] { return !fsync_running_.load(std::memory_order_relaxed); });

        if (!fsync_running_.load(std::memory_order_relaxed)) {
            break;
        }

        lock.unlock();
        flush();
    }
}

size_t WriteAheadLog::get_log_size() const {
    // 磁盘文件大小
    size_t disk_size = 0;
    std::ifstream file(wal_file_, std::ios::binary);
    if (file.is_open()) {
        file.seekg(0, std::ios::end);
        disk_size = static_cast<size_t>(file.tellg());
    }

    // 加上内存缓冲区中还未 fsync 的部分
    size_t buf_size = 0;
    {
        std::lock_guard<std::mutex> lock(buffer_mutex_);
        buf_size = buffer_.size();
    }

    return disk_size + buf_size;
}

size_t WriteAheadLog::get_buffer_size() const {
    std::lock_guard<std::mutex> lock(buffer_mutex_);
    return buffer_.size();
}

void WriteAheadLog::clear_all() {
    std::lock_guard<std::mutex> lock(buffer_mutex_);

    // 关闭现有文件
    if (wal_fd_ >= 0) {
        ::close(wal_fd_);
        wal_fd_ = -1;
    }
    if (wal_stream_.is_open()) {
        wal_stream_.close();
    }

    // 只删除 WAL 文件，保留快照目录（快照由 CheckpointManager 管理）
    std::filesystem::remove(wal_file_);

    // 清空缓冲区
    buffer_.clear();

    // 重新打开
    wal_fd_ = ::open(wal_file_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (wal_fd_ < 0) {
        throw std::runtime_error(
            std::string("clear_all: failed to reopen WAL file: ") + wal_file_ +
            " (" + std::strerror(errno) + ")"
        );
    }
    wal_stream_.open(wal_file_, std::ios::binary | std::ios::in);
}

// ============ 序列化/反序列化 ============

std::vector<uint8_t> WriteAheadLog::serialize_entry(const LogEntry& entry) {
    // 检查长度限制，防止整型溢出
    constexpr size_t MAX_STRING_SIZE = std::numeric_limits<uint32_t>::max();
    
    if (entry.key.size() > MAX_STRING_SIZE) {
        throw std::invalid_argument("Key too large: " + std::to_string(entry.key.size()) + 
                                  " bytes (max: " + std::to_string(MAX_STRING_SIZE) + ")");
    }
    
    if (entry.value.size() > MAX_STRING_SIZE) {
        throw std::invalid_argument("Value too large: " + std::to_string(entry.value.size()) + 
                                  " bytes (max: " + std::to_string(MAX_STRING_SIZE) + ")");
    }

    std::vector<uint8_t> data;
    
    // 预留空间用于条目大小
    size_t size_offset = data.size();
    data.resize(data.size() + 4);  // 预留 4 字节用于条目大小
    
    // 写入操作类型
    data.push_back(static_cast<uint8_t>(entry.op));
    
    // 安全转换：已经检查过大小限制
    uint32_t key_len = static_cast<uint32_t>(entry.key.size());
    uint32_t value_len = static_cast<uint32_t>(entry.value.size());
    
    // 写入 key
    data.insert(data.end(),
        reinterpret_cast<const uint8_t*>(&key_len),
        reinterpret_cast<const uint8_t*>(&key_len) + 4
    );
    data.insert(data.end(), entry.key.begin(), entry.key.end());
    
    // 写入 value
    data.insert(data.end(),
        reinterpret_cast<const uint8_t*>(&value_len),
        reinterpret_cast<const uint8_t*>(&value_len) + 4
    );
    data.insert(data.end(), entry.value.begin(), entry.value.end());
    
    // 写入时间戳
    data.insert(data.end(),
        reinterpret_cast<const uint8_t*>(&entry.timestamp_ms),
        reinterpret_cast<const uint8_t*>(&entry.timestamp_ms) + 8
    );
    
    // 写入 LSN（位于 timestamp 之后、checksum 之前）
    data.insert(data.end(),
        reinterpret_cast<const uint8_t*>(&entry.lsn),
        reinterpret_cast<const uint8_t*>(&entry.lsn) + 8
    );
    
    // 写入校验和（覆盖范围：key + value，lsn 不参与）
    uint32_t checksum = entry.compute_checksum();
    data.insert(data.end(),
        reinterpret_cast<const uint8_t*>(&checksum),
        reinterpret_cast<const uint8_t*>(&checksum) + 4
    );
    
    // 回填条目大小（不包括大小字段本身）
    uint32_t entry_size = static_cast<uint32_t>(data.size() - 4);
    std::memcpy(data.data() + size_offset, &entry_size, 4);
    
    return data;
}

std::optional<LogEntry> WriteAheadLog::deserialize_entry(const uint8_t* data, size_t size) {
    LogEntry entry;
    
    size_t offset = 0;
    
    // 读取操作类型
    entry.op = static_cast<LogEntry::OpType>(data[offset++]);
    
    // 读取 key
    uint32_t key_len = *reinterpret_cast<const uint32_t*>(data + offset);
    offset += 4;
    entry.key = std::string(reinterpret_cast<const char*>(data + offset), key_len);
    offset += key_len;
    
    // 读取 value
    uint32_t value_len = *reinterpret_cast<const uint32_t*>(data + offset);
    offset += 4;
    entry.value = std::string(reinterpret_cast<const char*>(data + offset), value_len);
    offset += value_len;
    
    // 读取时间戳
    entry.timestamp_ms = *reinterpret_cast<const int64_t*>(data + offset);
    offset += 8;
    
    // 读取 LSN（位于 timestamp 之后、checksum 之前）
    entry.lsn = *reinterpret_cast<const uint64_t*>(data + offset);
    offset += 8;
    
    // 验证校验和：防止崩溃时 partial write 导致的数据损坏
    uint32_t stored_checksum = *reinterpret_cast<const uint32_t*>(data + offset);
    uint32_t computed_checksum = entry.compute_checksum();
    if (stored_checksum != computed_checksum) {
        return std::nullopt;  // 条目损坏，通知调用方截断
    }
    
    return entry;
}

} // namespace db
} // namespace minkv
