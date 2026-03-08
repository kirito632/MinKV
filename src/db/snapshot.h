#pragma once

#include <string>
#include <functional>
#include <memory>
#include <atomic>
#include <thread>
#include <mutex>
#include <unordered_map>
#include <sys/types.h>

namespace minkv {
namespace db {

// 快照管理器
// 使用 fork() + COW (Copy-On-Write) 实现数据快照
class SnapshotManager {
public:
    // 快照回调函数类型
    // 参数：key, value, expiration_time
    using SnapshotCallback = std::function<void(const std::string&, const std::string&, uint64_t)>;
    
    // 快照完成回调
    using CompletionCallback = std::function<void(bool success, const std::string& error)>;
    
    explicit SnapshotManager(const std::string& snapshotDir = "./snapshots");
    ~SnapshotManager();
    
    // 禁止拷贝
    SnapshotManager(const SnapshotManager&) = delete;
    SnapshotManager& operator=(const SnapshotManager&) = delete;
    
    // 创建快照
    // dataCallback: 遍历数据的回调函数
    // completionCallback: 快照完成后的回调
    bool createSnapshot(const std::string& filename,
                       std::function<void(SnapshotCallback)> dataProvider,
                       CompletionCallback completionCallback = nullptr);
    
    // 同步创建快照（阻塞直到完成）
    bool createSnapshotSync(const std::string& filename,
                           std::function<void(SnapshotCallback)> dataProvider);
    
    // 加载快照
    bool loadSnapshot(const std::string& filename,
                     std::function<void(const std::string&, const std::string&, uint64_t)> loadCallback);
    
    // 获取快照信息
    struct SnapshotInfo {
        std::string filename;
        size_t fileSize;
        uint64_t timestamp;
        uint32_t recordCount;
        bool isValid;
    };
    
    SnapshotInfo getSnapshotInfo(const std::string& filename) const;
    
    // 清理旧快照
    void cleanupOldSnapshots(int keepCount = 3);
    
    // 获取统计信息
    struct Stats {
        uint64_t totalSnapshots;
        uint64_t successfulSnapshots;
        uint64_t failedSnapshots;
        uint64_t totalRecords;
        uint64_t totalBytes;
        std::chrono::milliseconds avgDuration;
    };
    
    Stats getStats() const;
    
private:
    // 子进程中执行快照写入
    void childSnapshotProcess(const std::string& filepath,
                             std::function<void(SnapshotCallback)> dataProvider);
    
    // 父进程等待子进程完成
    void waitForChild(pid_t childPid, 
                     CompletionCallback completionCallback);
    
    // 快照文件格式操作
    bool writeSnapshotHeader(int fd, uint32_t recordCount);
    bool writeSnapshotRecord(int fd, const std::string& key, 
                           const std::string& value, uint64_t expiration);
    bool readSnapshotHeader(int fd, uint32_t& recordCount) const;
    bool readSnapshotRecord(int fd, std::string& key, 
                          std::string& value, uint64_t& expiration) const;
    
    std::string snapshotDir_;
    
    // 统计信息
    mutable std::mutex statsMutex_;
    uint64_t totalSnapshots_;
    uint64_t successfulSnapshots_;
    uint64_t failedSnapshots_;
    uint64_t totalRecords_;
    uint64_t totalBytes_;
    std::chrono::steady_clock::time_point startTime_;
    
    // 当前正在进行的快照
    std::atomic<bool> snapshotInProgress_;
    std::thread waitThread_;
};

// 快照文件格式：
// [Header: 8 bytes]
//   - Magic Number: 4 bytes ("MKVS")
//   - Record Count: 4 bytes
// [Records: variable length]
//   - Key Length: 4 bytes
//   - Value Length: 4 bytes  
//   - Expiration: 8 bytes
//   - Key Data: variable
//   - Value Data: variable

} // namespace db
} // namespace minkv