#pragma once

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include "../base/serializer.h"

namespace minkv {
namespace db {

/**
 * @brief WAL (Write-Ahead Log) 日志条目
 *
 * 每个日志条目记录一次数据库操作（PUT、DELETE、SNAPSHOT）。
 * 格式：[EntrySize(4B)][OpType(1B)][KeyLen(4B)][Key][ValueLen(4B)][Value][Timestamp(8B)][LSN(8B)][Checksum(4B)]
 * 其中 EntrySize 不包含自身的 4 字节，Checksum 覆盖 key+value（LSN
 * 不参与校验）。
 */
struct LogEntry {
  enum OpType : uint8_t { PUT = 1, DELETE = 2, SNAPSHOT = 3 };

  OpType op;            // 操作类型
  std::string key;      // 键
  std::string value;    // 值（DELETE 时为空）
  int64_t timestamp_ms; // 时间戳（毫秒，保留用于调试/审计）
  uint64_t lsn {0}; // Log Sequence Number，严格单调递增，由 ShardedCache::next_lsn() 分配

  // 计算校验和（key + value，lsn 不参与）
  uint32_t compute_checksum() const;
};

/**
 * @brief Write-Ahead Log 实现
 *
 * 核心功能：
 * 1. 异步日志写入：日志先写到内存缓冲，后台线程定期 fsync
 * 2. Checkpoint 协作：支持按 snapshot_lsn 过滤重放和清理旧 WAL
 * 3. 宕机恢复：按 WAL 文件序号顺序重放日志
 *
 * 线程安全：append/flush/rotate 的共享缓冲访问由内部锁保护
 */
class WriteAheadLog {
public:
  /**
   * @brief 构造函数
   *
   * @param data_dir 数据目录（存放 wal_<index>.log 和 snapshots/）
   * @param buffer_size 日志缓冲区大小（字节），默认 1MB
   * @param fsync_interval_ms 后台 fsync 间隔（毫秒），默认 1000ms
   */
  explicit WriteAheadLog(const std::string &data_dir,
                         size_t buffer_size = 1024 * 1024,
                         int64_t fsync_interval_ms = 1000);

  ~WriteAheadLog();

  // 禁止拷贝
  WriteAheadLog(const WriteAheadLog &) = delete;
  WriteAheadLog &operator=(const WriteAheadLog &) = delete;

  /**
   * @brief 追加日志条目
   *
   * 将日志条目追加到内存缓冲区。
   * 有三种触发 fsync 的时机：
   *   1. 缓冲区满时，自动 fsync 后继续写入
   *   2. 后台线程按 fsync_interval_ms_ 定期 fsync
   *   3. fsync_interval_ms_ == 0 时为同步模式，每次 append 立即 fsync
   *
   * @param entry 日志条目
   * @return 是否成功
   */
  bool append(const LogEntry &entry);

  /**
   * @brief 读取所有日志条目
   *
   * 从 WAL 文件读取全部条目，遇到校验和不匹配的损坏条目时截断。
   * 用于宕机恢复时重放日志，通常配合 read_after_snapshot() 使用。
   *
   * @return 日志条目列表
   */
  std::vector<LogEntry> read_all();

  /**
   * @brief 读取指定 WAL 文件中的全部日志条目
   */
  std::vector<LogEntry> read_file(const std::string &filepath) const;

  /**
   * @brief 读取快照之后的日志（按 LSN 过滤）
   *
   * @param snapshot_lsn 快照时刻的 LSN（来自 SnapshotHeader::wal_lsn）
   * @return 所有 entry.lsn > snapshot_lsn 的条目，顺序与文件一致
   */
  std::vector<LogEntry> read_after_snapshot(uint64_t snapshot_lsn);

  /**
   * @brief 轮转到新的 WAL 文件
   *
   * 调用方应保证没有并发 append/flush 正在进行。
   *
   * @return 轮转前的 WAL 文件路径
   */
  std::string rotate();

  /**
   * @brief 强制 fsync 到磁盘
   *
   * 通常不需要调用，后台线程会自动 fsync。
   * fsync_interval_ms == 0 的同步模式不会启动后台线程，每次 append 会直接 fsync。
   * 用于关键操作后确保数据持久化。
   *
   * @return 是否成功
   */
  bool flush();

  /**
   * @brief 启动后台 fsync 线程
   *
   * 后台线程定期将缓冲区数据 fsync 到磁盘。
   * fsync_interval_ms == 0 时直接返回；重复调用安全。
   */
  void start_background_fsync();

  /**
   * @brief 停止后台 fsync 线程
   *
   * 调用此方法会等待后台线程退出；重复调用安全。
   */
  void stop_background_fsync();

  /**
   * @brief 获取当前日志大小（字节）
   */
  size_t get_log_size() const;

  /**
   * @brief 获取当前活动 WAL 文件路径
   */
  std::string current_wal_path() const;

  /**
   * @brief 列出数据目录下所有 WAL 文件（按 WAL 序号升序）
   */
  std::vector<std::string> list_wal_files() const;

  /**
   * @brief 删除所有已完全被 snapshot_lsn 覆盖的旧 WAL 文件
   */
  void cleanup_logs_up_to(uint64_t snapshot_lsn);

  /**
   * @brief 序列化日志条目为二进制字符串
   *
   * 输出格式与 WAL 文件一致，供 GroupCommitManager 在 hot path 直接写入。
   * 返回 std::string 而非 std::vector<uint8_t>，与 GCM 的 CommitRequest::data 类型一致，
   * 避免类型转换拷贝。
   *
   * @param entry 日志条目
   * @return 二进制序列化结果
   */
  static std::string serialize_entry(const LogEntry &entry);
  // 返回 nullopt 表示校验和不匹配（条目损坏）
  static std::optional<LogEntry> deserialize_entry(const uint8_t *data, size_t size);

  /**
   * @brief 获取缓冲区中待 fsync 的字节数
   */
  size_t get_buffer_size() const;

  /**
   * @brief 清空当前 WAL 文件和内存缓冲区
   *
   * 谨慎使用！这会丢弃当前活动 WAL 文件中的持久化数据。
   * 不会删除历史 WAL 文件或快照文件。
   */
  void clear_all();

private:
  std::string data_dir_;     // 数据目录
  std::string wal_file_;     // WAL 文件路径
  std::string snapshot_dir_; // 快照目录
  uint64_t wal_file_index_ {1};

  size_t buffer_size_;        // 缓冲区大小
  int64_t fsync_interval_ms_; // fsync 间隔

  std::vector<uint8_t> buffer_;     // 内存缓冲区
  mutable std::mutex buffer_mutex_; // 缓冲区互斥锁

  int wal_fd_ {-1}; // WAL 文件描述符（用于 write + fsync）

  // 后台 fsync 线程
  std::thread fsync_thread_;
  mutable std::atomic<bool> fsync_running_ {false};
  std::mutex fsync_lifecycle_mutex_;
  std::condition_variable fsync_cv_;
  std::mutex fsync_cv_mutex_;

  // 辅助函数
  static int64_t current_time_ms();
  static std::string wal_filename_for_index(uint64_t index);
  static bool parse_wal_index(const std::string &filename, uint64_t &index);
  void fsync_thread_main();
  bool write_to_buffer(const LogEntry &entry);
  bool flush_buffer_to_disk();
  void reopen_wal_file_locked(const std::string &filepath);
};


} // namespace db
} // namespace minkv
