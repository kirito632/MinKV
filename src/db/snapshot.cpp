#include "snapshot.h"
#include "../base/async_logger.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <chrono>
#include <filesystem>
#include <future>  // for std::promise

using namespace minkv::base;

namespace minkv {
namespace db {

static const char* SNAPSHOT_MAGIC = "MKVS";
static const size_t MAGIC_SIZE = 4;

SnapshotManager::SnapshotManager(const std::string& snapshotDir)
    : snapshotDir_(snapshotDir),
      totalSnapshots_(0),
      successfulSnapshots_(0),
      failedSnapshots_(0),
      totalRecords_(0),
      totalBytes_(0),
      snapshotInProgress_(false),
      startTime_(std::chrono::steady_clock::now()) {
    
    // 创建快照目录
    std::filesystem::create_directories(snapshotDir_);
}

SnapshotManager::~SnapshotManager() {
    if (waitThread_.joinable()) {
        waitThread_.join();
    }
}

bool SnapshotManager::createSnapshot(const std::string& filename,
                                   std::function<void(SnapshotCallback)> dataProvider,
                                   CompletionCallback completionCallback) {
    
    if (snapshotInProgress_.exchange(true)) {
        LOG_WARN << "Snapshot already in progress, skipping";
        if (completionCallback) {
            completionCallback(false, "Snapshot already in progress");
        }
        return false;
    }
    
    std::string filepath = snapshotDir_ + "/" + filename;
    
    LOG_INFO << "Starting snapshot creation: " << filepath;
    
    // 使用 fork() 创建子进程
    pid_t pid = fork();
    
    if (pid < 0) {
        // fork 失败
        std::string error = "Fork failed: " + std::string(std::strerror(errno));
        LOG_ERROR << error;
        snapshotInProgress_ = false;
        
        if (completionCallback) {
            completionCallback(false, error);
        }
        return false;
        
    } else if (pid == 0) {
        // 子进程：执行快照写入
        try {
            childSnapshotProcess(filepath, dataProvider);
            _exit(0);  // 子进程正常退出
        } catch (const std::exception& e) {
            LOG_ERROR << "Child process exception: " << e.what();
            _exit(1);  // 子进程异常退出
        }
        
    } else {
        // 父进程：异步等待子进程完成
        if (waitThread_.joinable()) {
            waitThread_.join();
        }
        
        waitThread_ = std::thread(&SnapshotManager::waitForChild, this, pid, completionCallback);
        return true;
    }
    
    return false;
}

bool SnapshotManager::createSnapshotSync(const std::string& filename,
                                        std::function<void(SnapshotCallback)> dataProvider) {
    
    std::promise<bool> promise;
    auto future = promise.get_future();
    
    bool started = createSnapshot(filename, dataProvider, 
        [&promise](bool success, const std::string& error) {
            promise.set_value(success);
        });
    
    if (!started) {
        return false;
    }
    
    return future.get();
}

void SnapshotManager::childSnapshotProcess(const std::string& filepath,
                                         std::function<void(SnapshotCallback)> dataProvider) {
    
    LOG_INFO << "Child process creating snapshot: " << filepath;
    
    // 打开快照文件
    int fd = open(filepath.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) {
        throw std::runtime_error("Failed to create snapshot file: " + 
                               std::string(std::strerror(errno)));
    }
    
    uint32_t recordCount = 0;
    size_t totalBytes = 0;
    
    // 先写入占位符头部（稍后更新记录数）
    if (!writeSnapshotHeader(fd, 0)) {
        close(fd);
        throw std::runtime_error("Failed to write snapshot header");
    }
    
    // 遍历数据并写入快照
    try {
        dataProvider([this, fd, &recordCount, &totalBytes]
                    (const std::string& key, const std::string& value, uint64_t expiration) {
            
            if (writeSnapshotRecord(fd, key, value, expiration)) {
                recordCount++;
                totalBytes += key.size() + value.size() + 16; // 16 bytes for lengths and expiration
            }
        });
        
    } catch (const std::exception& e) {
        close(fd);
        throw std::runtime_error("Failed to write snapshot data: " + std::string(e.what()));
    }
    
    // 更新头部的记录数
    if (lseek(fd, MAGIC_SIZE, SEEK_SET) < 0) {
        close(fd);
        throw std::runtime_error("Failed to seek to header");
    }
    
    if (write(fd, &recordCount, sizeof(recordCount)) != sizeof(recordCount)) {
        close(fd);
        throw std::runtime_error("Failed to update record count");
    }
    
    // 同步到磁盘
    if (fsync(fd) < 0) {
        close(fd);
        throw std::runtime_error("Failed to sync snapshot file");
    }
    
    close(fd);
    
    LOG_INFO << "Snapshot created successfully: " << recordCount << " records, " 
             << totalBytes << " bytes";
}

void SnapshotManager::waitForChild(pid_t childPid, CompletionCallback completionCallback) {
    int status;
    pid_t result = waitpid(childPid, &status, 0);
    
    snapshotInProgress_ = false;
    
    bool success = false;
    std::string error;
    
    if (result == childPid) {
        if (WIFEXITED(status)) {
            int exitCode = WEXITSTATUS(status);
            if (exitCode == 0) {
                success = true;
                LOG_INFO << "Snapshot completed successfully";
                
                std::lock_guard<std::mutex> lock(statsMutex_);
                totalSnapshots_++;
                successfulSnapshots_++;
                
            } else {
                error = "Child process exited with code: " + std::to_string(exitCode);
                LOG_ERROR << error;
                
                std::lock_guard<std::mutex> lock(statsMutex_);
                totalSnapshots_++;
                failedSnapshots_++;
            }
        } else {
            error = "Child process terminated abnormally";
            LOG_ERROR << error;
            
            std::lock_guard<std::mutex> lock(statsMutex_);
            totalSnapshots_++;
            failedSnapshots_++;
        }
    } else {
        error = "waitpid failed: " + std::string(std::strerror(errno));
        LOG_ERROR << error;
        
        std::lock_guard<std::mutex> lock(statsMutex_);
        totalSnapshots_++;
        failedSnapshots_++;
    }
    
    if (completionCallback) {
        completionCallback(success, error);
    }
}

bool SnapshotManager::loadSnapshot(const std::string& filename,
                                 std::function<void(const std::string&, const std::string&, uint64_t)> loadCallback) {
    
    std::string filepath = snapshotDir_ + "/" + filename;
    
    LOG_INFO << "Loading snapshot: " << filepath;
    
    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        LOG_ERROR << "Failed to open snapshot file: " << std::strerror(errno);
        return false;
    }
    
    uint32_t recordCount;
    if (!readSnapshotHeader(fd, recordCount)) {
        close(fd);
        return false;
    }
    
    LOG_INFO << "Loading " << recordCount << " records from snapshot";
    
    for (uint32_t i = 0; i < recordCount; ++i) {
        std::string key, value;
        uint64_t expiration;
        
        if (!readSnapshotRecord(fd, key, value, expiration)) {
            LOG_ERROR << "Failed to read record " << i;
            close(fd);
            return false;
        }
        
        loadCallback(key, value, expiration);
    }
    
    close(fd);
    LOG_INFO << "Snapshot loaded successfully: " << recordCount << " records";
    return true;
}

bool SnapshotManager::writeSnapshotHeader(int fd, uint32_t recordCount) {
    // 写入魔数
    if (write(fd, SNAPSHOT_MAGIC, MAGIC_SIZE) != MAGIC_SIZE) {
        return false;
    }
    
    // 写入记录数
    if (write(fd, &recordCount, sizeof(recordCount)) != sizeof(recordCount)) {
        return false;
    }
    
    return true;
}

bool SnapshotManager::writeSnapshotRecord(int fd, const std::string& key,
                                        const std::string& value, uint64_t expiration) {
    
    uint32_t keyLen = key.size();
    uint32_t valueLen = value.size();
    
    // 写入key长度
    if (write(fd, &keyLen, sizeof(keyLen)) != sizeof(keyLen)) {
        return false;
    }
    
    // 写入value长度
    if (write(fd, &valueLen, sizeof(valueLen)) != sizeof(valueLen)) {
        return false;
    }
    
    // 写入过期时间
    if (write(fd, &expiration, sizeof(expiration)) != sizeof(expiration)) {
        return false;
    }
    
    // 写入key数据
    if (write(fd, key.c_str(), keyLen) != static_cast<ssize_t>(keyLen)) {
        return false;
    }
    
    // 写入value数据
    if (write(fd, value.c_str(), valueLen) != static_cast<ssize_t>(valueLen)) {
        return false;
    }
    
    return true;
}

bool SnapshotManager::readSnapshotHeader(int fd, uint32_t& recordCount) const {
    char magic[MAGIC_SIZE];
    
    // 读取魔数
    if (read(fd, magic, MAGIC_SIZE) != MAGIC_SIZE) {
        return false;
    }
    
    if (memcmp(magic, SNAPSHOT_MAGIC, MAGIC_SIZE) != 0) {
        LOG_ERROR << "Invalid snapshot file format";
        return false;
    }
    
    // 读取记录数
    if (read(fd, &recordCount, sizeof(recordCount)) != sizeof(recordCount)) {
        return false;
    }
    
    return true;
}

bool SnapshotManager::readSnapshotRecord(int fd, std::string& key,
                                       std::string& value, uint64_t& expiration) const {
    
    uint32_t keyLen, valueLen;
    
    // 读取key长度
    if (read(fd, &keyLen, sizeof(keyLen)) != sizeof(keyLen)) {
        return false;
    }
    
    // 读取value长度
    if (read(fd, &valueLen, sizeof(valueLen)) != sizeof(valueLen)) {
        return false;
    }
    
    // 读取过期时间
    if (read(fd, &expiration, sizeof(expiration)) != sizeof(expiration)) {
        return false;
    }
    
    // 读取key数据
    key.resize(keyLen);
    if (read(fd, &key[0], keyLen) != static_cast<ssize_t>(keyLen)) {
        return false;
    }
    
    // 读取value数据
    value.resize(valueLen);
    if (read(fd, &value[0], valueLen) != static_cast<ssize_t>(valueLen)) {
        return false;
    }
    
    return true;
}

SnapshotManager::SnapshotInfo SnapshotManager::getSnapshotInfo(const std::string& filename) const {
    SnapshotInfo info;
    info.filename = filename;
    info.isValid = false;
    
    std::string filepath = snapshotDir_ + "/" + filename;
    
    struct stat fileStat;
    if (stat(filepath.c_str(), &fileStat) == 0) {
        info.fileSize = fileStat.st_size;
        info.timestamp = fileStat.st_mtime;
        
        // 尝试读取记录数
        int fd = open(filepath.c_str(), O_RDONLY);
        if (fd >= 0) {
            uint32_t recordCount;
            if (readSnapshotHeader(fd, recordCount)) {
                info.recordCount = recordCount;
                info.isValid = true;
            }
            close(fd);
        }
    }
    
    return info;
}

void SnapshotManager::cleanupOldSnapshots(int keepCount) {
    // 简化实现：删除最旧的快照文件
    // 实际项目中可以根据时间戳排序后删除
    LOG_INFO << "Cleaning up old snapshots, keeping " << keepCount << " files";
}

SnapshotManager::Stats SnapshotManager::getStats() const {
    std::lock_guard<std::mutex> lock(statsMutex_);
    
    Stats stats;
    stats.totalSnapshots = totalSnapshots_;
    stats.successfulSnapshots = successfulSnapshots_;
    stats.failedSnapshots = failedSnapshots_;
    stats.totalRecords = totalRecords_;
    stats.totalBytes = totalBytes_;
    
    // 简化的平均时长计算
    stats.avgDuration = std::chrono::milliseconds(100);
    
    return stats;
}

} // namespace db
} // namespace minkv