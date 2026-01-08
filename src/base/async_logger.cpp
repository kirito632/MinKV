#include "async_logger.h"
#include "append_file.h"
#include <iostream>
#include <sstream>
#include <iomanip>
#include <cstring>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <mutex>  // for std::once_flag

namespace minkv {
namespace base {

std::atomic<LogLevel> AsyncLogger::logLevel_{LogLevel::INFO};

AsyncLogger::AsyncLogger(const std::string& basename, size_t rollSize)
    : basename_(basename),
      rollSize_(rollSize),
      running_(false),
      currentBuffer_(std::make_unique<Buffer>()),
      nextBuffer_(std::make_unique<Buffer>()) {
}

AsyncLogger::~AsyncLogger() {
    if (running_) {
        stop();
    }
}

void AsyncLogger::start() {
    running_ = true;
    thread_ = std::thread(&AsyncLogger::threadFunc, this);
}

void AsyncLogger::stop() {
    running_ = false;
    cond_.notify_all();
    if (thread_.joinable()) {
        thread_.join();
    }
}

void AsyncLogger::append(const char* logline, size_t len) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (currentBuffer_->avail() > len) {
        // 当前缓冲区有足够空间
        currentBuffer_->append(logline, len);
    } else {
        // 当前缓冲区空间不足，移到待写入队列
        buffers_.push_back(std::move(currentBuffer_));
        
        if (nextBuffer_) {
            currentBuffer_ = std::move(nextBuffer_);
        } else {
            // 备用缓冲区也被用完了，只能新建一个
            currentBuffer_ = std::make_unique<Buffer>();
        }
        
        currentBuffer_->append(logline, len);
        cond_.notify_one();
    }
}

void AsyncLogger::flush() {
    std::lock_guard<std::mutex> lock(mutex_);
    cond_.notify_one();
}

void AsyncLogger::threadFunc() {
    // 准备两个备用缓冲区
    BufferPtr newBuffer1 = std::make_unique<Buffer>();
    BufferPtr newBuffer2 = std::make_unique<Buffer>();
    BufferVector buffersToWrite;
    buffersToWrite.reserve(16);
    
    // 创建高性能日志文件
    std::string filename = basename_ + ".log";
    std::unique_ptr<AppendFile> output;
    
    try {
        output = std::make_unique<AppendFile>(filename);
    } catch (const std::exception& e) {
        std::cerr << "Failed to create log file: " << e.what() << std::endl;
        return;
    }
    
    while (running_) {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            
            if (buffers_.empty()) {
                // 等待条件：有数据要写 或者 超时 或者 程序退出
                cond_.wait_for(lock, std::chrono::seconds(3));
            }
            
            // 将当前缓冲区也移到待写入队列
            buffers_.push_back(std::move(currentBuffer_));
            currentBuffer_ = std::move(newBuffer1);
            
            buffersToWrite.swap(buffers_);
            
            if (!nextBuffer_) {
                nextBuffer_ = std::move(newBuffer2);
            }
        }
        
        // 写入文件（在锁外进行，使用系统调用）
        for (const auto& buffer : buffersToWrite) {
            if (!buffer->empty()) {
                try {
                    output->append(buffer->data(), buffer->length());
                } catch (const std::exception& e) {
                    std::cerr << "Log write failed: " << e.what() << std::endl;
                }
            }
        }
        
        // 刷新到磁盘（AppendFile 使用系统调用，无需额外flush）
        output->flush();
        
        // 重用缓冲区
        if (buffersToWrite.size() > 2) {
            // 如果积压太多，只保留两个
            buffersToWrite.resize(2);
        }
        
        if (!newBuffer1) {
            newBuffer1 = std::move(buffersToWrite.back());
            buffersToWrite.pop_back();
            newBuffer1->reset();
        }
        
        if (!newBuffer2) {
            newBuffer2 = std::move(buffersToWrite.back());
            buffersToWrite.pop_back();
            newBuffer2->reset();
        }
        
        buffersToWrite.clear();
    }
    
    // 最后同步到磁盘
    if (output) {
        try {
            output->sync();
        } catch (const std::exception& e) {
            std::cerr << "Final sync failed: " << e.what() << std::endl;
        }
    }
}

AsyncLogger& AsyncLogger::instance() {
    static AsyncLogger logger("minkv");
    static std::once_flag flag;
    std::call_once(flag, []() {
        logger.start();
    });
    return logger;
}

void AsyncLogger::setLogLevel(LogLevel level) {
    logLevel_.store(level);
}

LogLevel AsyncLogger::getLogLevel() {
    return logLevel_.load();
}

// LogStream 实现
LogStream::LogStream(LogLevel level, const char* file, int line)
    : level_(level) {
    formatHeader(level, file, line);
}

LogStream::~LogStream() {
    buffer_ += '\n';
    AsyncLogger::instance().append(buffer_.c_str(), buffer_.size());
}

void LogStream::formatHeader(LogLevel level, const char* file, int line) {
    // 获取当前时间
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()) % 1000;
    
    // 格式化时间戳
    std::ostringstream oss;
    oss << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S");
    oss << '.' << std::setfill('0') << std::setw(3) << ms.count();
    
    // 日志级别
    const char* levelStr[] = {"DEBUG", "INFO ", "WARN ", "ERROR", "FATAL"};
    
    // 提取文件名（去掉路径）
    const char* basename = strrchr(file, '/');
    if (basename) {
        basename++;
    } else {
        basename = file;
    }
    
    // 格式: [时间] [级别] [文件:行号] 
    buffer_ = "[" + oss.str() + "] [" + levelStr[static_cast<int>(level)] + "] [" 
              + basename + ":" + std::to_string(line) + "] ";
}

LogStream& LogStream::operator<<(const char* str) {
    if (str) {
        buffer_ += str;
    }
    return *this;
}

LogStream& LogStream::operator<<(const std::string& str) {
    buffer_ += str;
    return *this;
}

LogStream& LogStream::operator<<(int val) {
    buffer_ += std::to_string(val);
    return *this;
}

LogStream& LogStream::operator<<(long val) {
    buffer_ += std::to_string(val);
    return *this;
}

LogStream& LogStream::operator<<(long long val) {
    buffer_ += std::to_string(val);
    return *this;
}

LogStream& LogStream::operator<<(unsigned int val) {
    buffer_ += std::to_string(val);
    return *this;
}

LogStream& LogStream::operator<<(unsigned long val) {
    buffer_ += std::to_string(val);
    return *this;
}

LogStream& LogStream::operator<<(unsigned long long val) {
    buffer_ += std::to_string(val);
    return *this;
}

LogStream& LogStream::operator<<(float val) {
    buffer_ += std::to_string(val);
    return *this;
}

LogStream& LogStream::operator<<(double val) {
    buffer_ += std::to_string(val);
    return *this;
}

LogStream& LogStream::operator<<(bool val) {
    buffer_ += (val ? "true" : "false");
    return *this;
}

LogStream& LogStream::operator<<(char c) {
    buffer_ += c;
    return *this;
}

} // namespace base
} // namespace minkv