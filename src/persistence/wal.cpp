#include "wal.h"
#include <chrono>
#include <filesystem>
#include <algorithm>

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
        
        // 打开 WAL 文件（追加模式）
        wal_stream_.open(wal_file_, std::ios::binary | std::ios::app);
        if (!wal_stream_.is_open()) {
            throw std::runtime_error("Failed to open WAL file: " + wal_file_);
        }
    } catch (...) {
        // 构造失败时的清理工作
        if (wal_stream_.is_open()) {
            wal_stream_.close();
        }
        // 注意：不删除目录，因为可能包含用户数据
        throw;  // 重新抛出异常
    }
}

WriteAheadLog::~WriteAheadLog() {
    stop_background_fsync();
    flush();
    if (wal_stream_.is_open()) {
        wal_stream_.close();
    }
}

bool WriteAheadLog::append(const LogEntry& entry) {
    std::lock_guard<std::mutex> lock(buffer_mutex_);
    return write_to_buffer(entry);
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
    
    // 写入 WAL 文件
    wal_stream_.write(reinterpret_cast<const char*>(buffer_.data()), buffer_.size());
    if (!wal_stream_.good()) {
        return false;
    }
    
    // fsync 到磁盘
    wal_stream_.flush();
    // 注意：在实际应用中，应该调用 fsync(fileno(wal_stream_))
    // 这里简化处理
    
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
        
        LogEntry entry = deserialize_entry(data.data() + offset, entry_size);
        entries.push_back(entry);
        offset += entry_size;
    }
    
    return entries;
}

std::vector<LogEntry> WriteAheadLog::read_after_snapshot(int64_t snapshot_id) {
    auto all_entries = read_all();
    
    // 过滤出 snapshot_id 之后的条目
    std::vector<LogEntry> result;
    for (const auto& entry : all_entries) {
        if (entry.timestamp_ms > snapshot_id) {
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
    fsync_running_.store(false, std::memory_order_relaxed);
    
    if (fsync_thread_.joinable()) {
        fsync_thread_.join();
    }
}

void WriteAheadLog::fsync_thread_main() {
    while (fsync_running_.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(fsync_interval_ms_));
        
        if (!fsync_running_.load(std::memory_order_relaxed)) {
            break;
        }
        
        flush();
    }
}

size_t WriteAheadLog::get_log_size() const {
    std::ifstream file(wal_file_, std::ios::binary);
    if (!file.is_open()) {
        return 0;
    }
    
    file.seekg(0, std::ios::end);
    size_t size = file.tellg();
    file.close();
    
    return size;
}

size_t WriteAheadLog::get_buffer_size() const {
    std::lock_guard<std::mutex> lock(buffer_mutex_);
    return buffer_.size();
}

void WriteAheadLog::clear_all() {
    std::lock_guard<std::mutex> lock(buffer_mutex_);
    
    // 关闭 WAL 文件
    if (wal_stream_.is_open()) {
        wal_stream_.close();
    }
    
    // 删除 WAL 文件
    std::filesystem::remove(wal_file_);
    
    // 删除快照目录
    std::filesystem::remove_all(snapshot_dir_);
    std::filesystem::create_directories(snapshot_dir_);
    
    // 清空缓冲区
    buffer_.clear();
    
    // 重新打开 WAL 文件
    wal_stream_.open(wal_file_, std::ios::binary | std::ios::app);
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
    
    // 写入校验和
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

LogEntry WriteAheadLog::deserialize_entry(const uint8_t* data, size_t size) {
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
    
    // 读取校验和（这里简化处理，不验证）
    uint32_t checksum = *reinterpret_cast<const uint32_t*>(data + offset);
    
    return entry;
}

} // namespace db
} // namespace minkv
