#pragma once

#include <string>
#include <vector>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <chrono>
#include <cstring>  // for memcpy

namespace minkv {
namespace base {

// 日志级别
enum class LogLevel {
    DEBUG = 0,
    INFO = 1,
    WARN = 2,
    ERROR = 3,
    FATAL = 4
};

// 固定大小的缓冲区
class FixedBuffer {
public:
    static constexpr size_t kBufferSize = 4 * 1024 * 1024;  // 4MB
    
    FixedBuffer() : cur_(data_) {}
    
    void append(const char* data, size_t len) {
        if (avail() >= len) {
            memcpy(cur_, data, len);
            cur_ += len;
        }
    }
    
    const char* data() const { return data_; }
    size_t length() const { return cur_ - data_; }
    size_t avail() const { return end() - cur_; }
    void reset() { cur_ = data_; }
    bool empty() const { return cur_ == data_; }
    
private:
    const char* end() const { return data_ + sizeof(data_); }
    
    char data_[kBufferSize];
    char* cur_;
};

// 异步日志器
class AsyncLogger {
public:
    AsyncLogger(const std::string& basename, size_t rollSize = 500 * 1024 * 1024);
    ~AsyncLogger();
    
    void append(const char* logline, size_t len);
    void flush();
    void start();
    void stop();
    
    // 静态接口
    static AsyncLogger& instance();
    static void setLogLevel(LogLevel level);
    static LogLevel getLogLevel();
    
private:
    void threadFunc();
    
    using Buffer = FixedBuffer;
    using BufferPtr = std::unique_ptr<Buffer>;
    using BufferVector = std::vector<BufferPtr>;
    
    const std::string basename_;
    const size_t rollSize_;
    
    std::atomic<bool> running_;
    std::thread thread_;
    
    std::mutex mutex_;
    std::condition_variable cond_;
    
    BufferPtr currentBuffer_;
    BufferPtr nextBuffer_;
    BufferVector buffers_;
    
    static std::atomic<LogLevel> logLevel_;
};

// 日志流 - 简化版本，不做复杂格式化
class LogStream {
public:
    LogStream(LogLevel level, const char* file, int line);
    ~LogStream();
    
    LogStream& operator<<(const char* str);
    LogStream& operator<<(const std::string& str);
    LogStream& operator<<(int val);
    LogStream& operator<<(long val);
    LogStream& operator<<(long long val);
    LogStream& operator<<(unsigned int val);
    LogStream& operator<<(unsigned long val);
    LogStream& operator<<(unsigned long long val);
    LogStream& operator<<(float val);
    LogStream& operator<<(double val);
    LogStream& operator<<(bool val);
    LogStream& operator<<(char c);
    
private:
    void formatHeader(LogLevel level, const char* file, int line);
    
    LogLevel level_;
    std::string buffer_;
};

// 便利宏定义
#define LOG_DEBUG if (minkv::base::AsyncLogger::getLogLevel() <= minkv::base::LogLevel::DEBUG) \
    minkv::base::LogStream(minkv::base::LogLevel::DEBUG, __FILE__, __LINE__)

#define LOG_INFO if (minkv::base::AsyncLogger::getLogLevel() <= minkv::base::LogLevel::INFO) \
    minkv::base::LogStream(minkv::base::LogLevel::INFO, __FILE__, __LINE__)

#define LOG_WARN if (minkv::base::AsyncLogger::getLogLevel() <= minkv::base::LogLevel::WARN) \
    minkv::base::LogStream(minkv::base::LogLevel::WARN, __FILE__, __LINE__)

#define LOG_ERROR if (minkv::base::AsyncLogger::getLogLevel() <= minkv::base::LogLevel::ERROR) \
    minkv::base::LogStream(minkv::base::LogLevel::ERROR, __FILE__, __LINE__)

#define LOG_FATAL minkv::base::LogStream(minkv::base::LogLevel::FATAL, __FILE__, __LINE__)

} // namespace base
} // namespace minkv