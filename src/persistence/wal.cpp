#include "wal.h"

#include <fcntl.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>

namespace minkv {
namespace db {

std::string WriteAheadLog::wal_filename_for_index(uint64_t index) {
  return "wal_" + std::to_string(index) + ".log";
}

bool WriteAheadLog::parse_wal_index(const std::string &filename, uint64_t &index) {
  if (filename.size() < 9 || filename.rfind("wal_", 0) != 0 ||
      filename.substr(filename.size() - 4) != ".log") {
    return false;
  }
  try {
    index = std::stoull(filename.substr(4, filename.size() - 8));
    return true;
  } catch (...) {
    return false;
  }
}

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

WriteAheadLog::WriteAheadLog(const std::string &data_dir,
                             size_t buffer_size,
                             int64_t fsync_interval_ms)
    : data_dir_(data_dir),
      buffer_size_(buffer_size),
      fsync_interval_ms_(fsync_interval_ms) {
  try {
    std::filesystem::create_directories(data_dir);
    snapshot_dir_ = data_dir + "/snapshots";
    std::filesystem::create_directories(snapshot_dir_);

    buffer_.reserve(buffer_size_);

    uint64_t max_index = 0;
    for (const auto &path : list_wal_files()) {
      uint64_t idx = 0;
      if (parse_wal_index(std::filesystem::path(path).filename().string(), idx)) {
        max_index = std::max(max_index, idx);
      }
    }

    wal_file_index_ = (max_index == 0) ? 1 : max_index;
    reopen_wal_file_locked(data_dir_ + "/" + wal_filename_for_index(wal_file_index_));
  } catch (...) {
    if (wal_fd_ >= 0) {
      ::close(wal_fd_);
      wal_fd_ = -1;
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
}

bool WriteAheadLog::append(const LogEntry &entry) {
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

bool WriteAheadLog::write_to_buffer(const LogEntry &entry) {
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
  const uint8_t *ptr = buffer_.data();
  size_t remaining = buffer_.size();
  while (remaining > 0) {
    ssize_t written = ::write(wal_fd_, ptr, remaining);
    if (written < 0) {
      if (errno == EINTR)
        continue;   // 被信号中断，重试
      return false; // 真正的写入错误（磁盘满等）
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

std::vector<LogEntry> WriteAheadLog::read_file(const std::string &filepath) const {
  std::vector<LogEntry> entries;

  int fd = ::open(filepath.c_str(), O_RDONLY);
  if (fd == -1) {
    return entries;
  }

  ::posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
  ::posix_fadvise(fd, 0, 0, POSIX_FADV_WILLNEED);

  off_t file_size = ::lseek(fd, 0, SEEK_END);
  ::lseek(fd, 0, SEEK_SET);
  if (file_size <= 0) {
    ::close(fd);
    return entries;
  }

  std::vector<uint8_t> data(static_cast<size_t>(file_size));
  ssize_t bytes_read = ::read(fd, data.data(), data.size());
  ::close(fd);

  if (bytes_read != file_size) {
    return entries;
  }

  size_t offset = 0;
  while (offset < data.size()) {
    if (offset + 4 > data.size())
      break;

    uint32_t entry_size = 0;
    std::memcpy(&entry_size, data.data() + offset, sizeof(entry_size));
    offset += sizeof(entry_size);

    if (entry_size == 0 || entry_size > data.size() - offset)
      break;

    auto entry = deserialize_entry(data.data() + offset, entry_size);
    if (!entry) {
      break;
    }
    entries.push_back(std::move(*entry));
    offset += entry_size;
  }

  return entries;
}

std::vector<LogEntry> WriteAheadLog::read_all() {
  std::vector<LogEntry> entries;
  for (const auto &path : list_wal_files()) {
    auto file_entries = read_file(path);
    entries.insert(entries.end(), file_entries.begin(), file_entries.end());
  }
  return entries;
}

std::vector<LogEntry> WriteAheadLog::read_after_snapshot(uint64_t snapshot_lsn) {
  auto all_entries = read_all();

  std::vector<LogEntry> result;
  for (const auto &entry : all_entries) {
    if (entry.lsn > snapshot_lsn) {
      result.push_back(entry);
    }
  }

  return result;
}

std::string WriteAheadLog::rotate() {
  std::lock_guard<std::mutex> lock(buffer_mutex_);
  if (!flush_buffer_to_disk()) {
    throw std::runtime_error("rotate: failed to flush current WAL");
  }

  std::string old_path = wal_file_;
  ++wal_file_index_;
  reopen_wal_file_locked(data_dir_ + "/" + wal_filename_for_index(wal_file_index_));
  return old_path;
}

void WriteAheadLog::start_background_fsync() {
  std::lock_guard<std::mutex> lifecycle_lock(fsync_lifecycle_mutex_);

  if (fsync_interval_ms_ == 0 || fsync_running_.load(std::memory_order_acquire)) {
    return;
  }

  if (fsync_thread_.joinable()) {
    fsync_thread_.join();
  }

  fsync_running_.store(true, std::memory_order_release);
  try {
    fsync_thread_ = std::thread([this]() { this->fsync_thread_main(); });
  } catch (...) {
    fsync_running_.store(false, std::memory_order_release);
    throw;
  }
}

void WriteAheadLog::stop_background_fsync() {
  std::lock_guard<std::mutex> lifecycle_lock(fsync_lifecycle_mutex_);

  bool was_running = fsync_running_.exchange(false, std::memory_order_acq_rel);
  if (!was_running && !fsync_thread_.joinable()) {
    return;
  }

  fsync_cv_.notify_all();

  if (fsync_thread_.joinable()) {
    fsync_thread_.join();
  }
}

void WriteAheadLog::fsync_thread_main() {
  // 防御性检查：同步刷盘模式不会启动后台线程。
  if (fsync_interval_ms_ == 0) {
    return;
  }
  while (true) {
    std::unique_lock<std::mutex> lock(fsync_cv_mutex_);
    fsync_cv_.wait_for(lock, std::chrono::milliseconds(fsync_interval_ms_), [this] {
      return !fsync_running_.load(std::memory_order_relaxed);
    });

    if (!fsync_running_.load(std::memory_order_relaxed)) {
      break;
    }

    lock.unlock();
    flush();
  }
}

size_t WriteAheadLog::get_log_size() const {
  size_t total_size = 0;
  for (const auto &path : list_wal_files()) {
    std::error_code ec;
    total_size += std::filesystem::file_size(path, ec);
  }

  size_t buf_size = 0;
  {
    std::lock_guard<std::mutex> lock(buffer_mutex_);
    buf_size = buffer_.size();
  }

  return total_size + buf_size;
}

std::string WriteAheadLog::current_wal_path() const {
  return wal_file_;
}

std::vector<std::string> WriteAheadLog::list_wal_files() const {
  std::vector<std::pair<uint64_t, std::string>> indexed;
  if (!std::filesystem::exists(data_dir_)) {
    return {};
  }

  for (const auto &entry : std::filesystem::directory_iterator(data_dir_)) {
    if (!entry.is_regular_file()) {
      continue;
    }
    uint64_t index = 0;
    auto filename = entry.path().filename().string();
    if (parse_wal_index(filename, index)) {
      indexed.emplace_back(index, entry.path().string());
    }
  }

  std::sort(indexed.begin(), indexed.end(), [](const auto &lhs, const auto &rhs) {
    return lhs.first < rhs.first;
  });

  std::vector<std::string> files;
  files.reserve(indexed.size());
  for (const auto &it : indexed) {
    files.push_back(it.second);
  }
  return files;
}

void WriteAheadLog::cleanup_logs_up_to(uint64_t snapshot_lsn) {
  auto files = list_wal_files();
  if (files.size() <= 1) {
    return;
  }

  for (size_t i = 0; i + 1 < files.size(); ++i) {
    auto entries = read_file(files[i]);
    uint64_t max_lsn = 0;
    for (const auto &entry : entries) {
      max_lsn = std::max(max_lsn, entry.lsn);
    }
    if (max_lsn <= snapshot_lsn) {
      std::error_code ec;
      std::filesystem::remove(files[i], ec);
    }
  }
}

size_t WriteAheadLog::get_buffer_size() const {
  std::lock_guard<std::mutex> lock(buffer_mutex_);
  return buffer_.size();
}

void WriteAheadLog::clear_all() {
  std::lock_guard<std::mutex> lock(buffer_mutex_);

  // ftruncate 截断当前 WAL 文件，保持同一 inode，GCM 的 fd 不失效
  if (wal_fd_ >= 0) {
    if (::ftruncate(wal_fd_, 0) != 0) {
      throw std::runtime_error("clear_all: ftruncate failed: " +
                               std::string(std::strerror(errno)));
    }
  }

  buffer_.clear();
}

std::string WriteAheadLog::serialize_entry(const LogEntry &entry) {
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

  std::string data;
  // 预留空间减少 reallocation
  // 格式: entry_size(4B) + op(1B) + key_len(4B) + key + value_len(4B) + value +
  // timestamp(8B) + lsn(8B) + checksum(4B)
  data.reserve(4 + 1 + 4 + entry.key.size() + 4 + entry.value.size() + 8 + 8 + 4);

  // 预留空间用于条目大小（稍后回填）
  size_t size_offset = data.size();
  data.append(4, '\0');

  // 写入操作类型
  data.push_back(static_cast<char>(static_cast<uint8_t>(entry.op)));

  // 安全转换：已经检查过大小限制
  uint32_t key_len = static_cast<uint32_t>(entry.key.size());
  uint32_t value_len = static_cast<uint32_t>(entry.value.size());

  // 写入 key
  data.append(reinterpret_cast<const char *>(&key_len), sizeof(key_len));
  data.append(entry.key);

  // 写入 value
  data.append(reinterpret_cast<const char *>(&value_len), sizeof(value_len));
  data.append(entry.value);

  // 写入时间戳
  data.append(reinterpret_cast<const char *>(&entry.timestamp_ms),
              sizeof(entry.timestamp_ms));

  // 写入 LSN
  data.append(reinterpret_cast<const char *>(&entry.lsn), sizeof(entry.lsn));

  // 写入校验和
  uint32_t checksum = entry.compute_checksum();
  data.append(reinterpret_cast<const char *>(&checksum), sizeof(checksum));

  // 回填条目大小（不包括大小字段本身）
  uint32_t entry_size = static_cast<uint32_t>(data.size() - 4);
  std::memcpy(&data[size_offset], &entry_size, 4);

  return data;
}

void WriteAheadLog::reopen_wal_file_locked(const std::string &filepath) {
  if (wal_fd_ >= 0) {
    ::close(wal_fd_);
    wal_fd_ = -1;
  }

  wal_file_ = filepath;
  wal_fd_ = ::open(wal_file_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
  if (wal_fd_ < 0) {
    throw std::runtime_error(std::string("Failed to open WAL file: ") + wal_file_ + " (" +
                             std::strerror(errno) + ")");
  }
}

std::optional<LogEntry> WriteAheadLog::deserialize_entry(const uint8_t *data,
                                                         size_t size) {
  LogEntry entry;
  size_t offset = 0;

  auto require = [&](size_t bytes) -> bool {
    return offset <= size && bytes <= size - offset;
  };

  auto read_bytes = [&](void *dst, size_t bytes) -> bool {
    if (!require(bytes)) {
      return false;
    }
    std::memcpy(dst, data + offset, bytes);
    offset += bytes;
    return true;
  };

  auto read_string = [&](std::string &out, size_t min_remaining_after) -> bool {
    uint32_t len = 0;
    if (!read_bytes(&len, sizeof(len)) || !require(min_remaining_after) ||
        len > size - offset - min_remaining_after) {
      return false;
    }
    out.assign(reinterpret_cast<const char *>(data + offset), len);
    offset += len;
    return true;
  };

  uint8_t op = 0;
  if (!read_bytes(&op, sizeof(op))) {
    return std::nullopt;
  }
  if (op != LogEntry::PUT && op != LogEntry::DELETE && op != LogEntry::SNAPSHOT) {
    return std::nullopt;
  }
  entry.op = static_cast<LogEntry::OpType>(op);

  constexpr size_t kTimestampSize = sizeof(entry.timestamp_ms);
  constexpr size_t kLsnSize = sizeof(entry.lsn);
  constexpr size_t kChecksumSize = sizeof(uint32_t);
  constexpr size_t kValueLenSize = sizeof(uint32_t);

  if (!read_string(entry.key,
                   kValueLenSize + kTimestampSize + kLsnSize + kChecksumSize) ||
      !read_string(entry.value, kTimestampSize + kLsnSize + kChecksumSize) ||
      !read_bytes(&entry.timestamp_ms, sizeof(entry.timestamp_ms)) ||
      !read_bytes(&entry.lsn, sizeof(entry.lsn))) {
    return std::nullopt;
  }

  uint32_t stored_checksum = 0;
  if (!read_bytes(&stored_checksum, sizeof(stored_checksum)) || offset != size) {
    return std::nullopt;
  }

  uint32_t computed_checksum = entry.compute_checksum();
  if (stored_checksum != computed_checksum) {
    return std::nullopt; // 条目损坏，通知调用方截断
  }

  return entry;
}

} // namespace db
} // namespace minkv
