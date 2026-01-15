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

// [原子操作] 全局日志级别，默认INFO级别，多线程安全
std::atomic<LogLevel> AsyncLogger::logLevel_{LogLevel::INFO};

AsyncLogger::AsyncLogger(const std::string& basename, size_t rollSize)
    : basename_(basename),
      rollSize_(rollSize),
      running_(false),
      // [核心优化] 预分配两个缓冲区，避免运行时动态分配造成的性能损失
      currentBuffer_(std::make_unique<Buffer>()),
      nextBuffer_(std::make_unique<Buffer>()) {
}

AsyncLogger::~AsyncLogger() {
    // [RAII] 析构时确保后台线程正确停止，避免资源泄漏
    if (running_) {
        stop();
    }
}

void AsyncLogger::start() {
    running_ = true;
    // 启动后台I/O线程，使用成员函数指针和this指针
    thread_ = std::thread(&AsyncLogger::threadFunc, this);
}

void AsyncLogger::stop() {
    // [线程同步] 设置停止标志并通知所有等待的线程
    running_ = false;
    cond_.notify_all();  // 唤醒可能在等待的后台线程
    
    // [异常安全] 等待后台线程完成所有工作后再退出
    if (thread_.joinable()) {
        thread_.join();
    }
}

void AsyncLogger::append(const char* logline, size_t len) {
    // [分片锁] 使用RAII的lock_guard确保异常安全，自动释放锁
    std::lock_guard<std::mutex> lock(mutex_);
    
    // [核心优化] 检查当前缓冲区是否有足够空间
    if (currentBuffer_->avail() > len) {
        // 当前缓冲区有足够空间，直接写入（快速路径）
        currentBuffer_->append(logline, len);
    } else {
        // [双缓冲机制] 当前缓冲区空间不足，切换缓冲区
        // 将满的缓冲区移到待写入队列，供后台线程处理
        buffers_.push_back(std::move(currentBuffer_));
        
        if (nextBuffer_) {
            // 使用预分配的备用缓冲区，避免动态分配
            currentBuffer_ = std::move(nextBuffer_);
        } else {
            // [异常情况] 备用缓冲区也被用完了，只能新建一个
            // 这种情况说明日志产生速度超过了写入速度
            currentBuffer_ = std::make_unique<Buffer>();
        }
        
        // 将日志写入新的当前缓冲区
        currentBuffer_->append(logline, len);
        
        // [生产者-消费者] 通知后台线程有新数据需要处理
        cond_.notify_one();  // 只需要唤醒一个线程，因为只有一个后台线程
    }
}

void AsyncLogger::flush() {
    // [强制刷新] 获取锁并通知后台线程立即处理当前缓冲区
    std::lock_guard<std::mutex> lock(mutex_);
    cond_.notify_one();
}

void AsyncLogger::threadFunc() {
    // [对象池模式] 准备两个备用缓冲区，避免频繁的内存分配/释放
    BufferPtr newBuffer1 = std::make_unique<Buffer>();
    BufferPtr newBuffer2 = std::make_unique<Buffer>();
    BufferVector buffersToWrite;
    buffersToWrite.reserve(16);  // 预分配空间，避免vector扩容
    
    // [系统调用优化] 创建高性能日志文件，使用AppendFile封装系统调用
    std::string filename = basename_ + ".log";
    std::unique_ptr<AppendFile> output;
    
    try {
        output = std::make_unique<AppendFile>(filename);
    } catch (const std::exception& e) {
        std::cerr << "Failed to create log file: " << e.what() << std::endl;
        return;
    }
    
    // [生产者-消费者] 后台线程主循环
    while (running_) {
        {
            // [临界区] 获取锁，处理缓冲区队列
            std::unique_lock<std::mutex> lock(mutex_);
            
            if (buffers_.empty()) {
                // [条件变量] 没有数据时等待，最多等待3秒后自动唤醒
                // 这样可以定期刷新日志，即使没有新日志产生
                cond_.wait_for(lock, std::chrono::seconds(3));
            }
            
            // [双缓冲机制] 将当前缓冲区也移到待写入队列
            // 即使当前缓冲区没满，也要定期写入，避免日志延迟过大
            buffers_.push_back(std::move(currentBuffer_));
            currentBuffer_ = std::move(newBuffer1);
            
            // [零拷贝] 使用swap避免数据拷贝，提升性能
            buffersToWrite.swap(buffers_);
            
            // 如果备用缓冲区被用掉了，补充一个新的
            if (!nextBuffer_) {
                nextBuffer_ = std::move(newBuffer2);
            }
        }
        // [关键设计] 锁的作用域结束，释放锁，在锁外进行I/O操作
        
        // [批量I/O] 批量写入文件，减少系统调用次数
        for (const auto& buffer : buffersToWrite) {
            if (!buffer->empty()) {
                try {
                    // 使用AppendFile的高性能写入，直接调用系统调用
                    output->append(buffer->data(), buffer->length());
                } catch (const std::exception& e) {
                    std::cerr << "Log write failed: " << e.what() << std::endl;
                }
            }
        }
        
        // [系统调用] 刷新到磁盘，AppendFile内部使用fsync确保数据持久化
        output->flush();
        
        // [内存管理] 重用缓冲区，避免频繁分配/释放
        if (buffersToWrite.size() > 2) {
            // 如果积压太多缓冲区，只保留两个，其余的释放内存
            // 这样可以在日志爆发时控制内存使用
            buffersToWrite.resize(2);
        }
        
        // [对象池] 回收缓冲区供下次使用
        if (!newBuffer1) {
            newBuffer1 = std::move(buffersToWrite.back());
            buffersToWrite.pop_back();
            newBuffer1->reset();  // 重置缓冲区状态
        }
        
        if (!newBuffer2) {
            newBuffer2 = std::move(buffersToWrite.back());
            buffersToWrite.pop_back();
            newBuffer2->reset();
        }
        
        buffersToWrite.clear();  // 清空待写入队列
    }
    
    // [异常安全] 程序退出时，确保所有日志都写入磁盘
    if (output) {
        try {
            output->sync();  // 最后一次同步到磁盘
        } catch (const std::exception& e) {
            std::cerr << "Final sync failed: " << e.what() << std::endl;
        }
    }
}

AsyncLogger& AsyncLogger::instance() {
    // [单例模式] 使用Meyer's Singleton，C++11保证静态局部变量的线程安全初始化
    static AsyncLogger logger("minkv");
    static std::once_flag flag;
    
    // [线程安全] 使用std::call_once确保start()只被调用一次
    std::call_once(flag, []() {
        logger.start();
    });
    return logger;
}

void AsyncLogger::setLogLevel(LogLevel level) {
    // [原子操作] 使用atomic的store操作，保证多线程环境下的安全性
    // memory_order_seq_cst是默认的内存序，提供最强的同步保证
    logLevel_.store(level);
}

LogLevel AsyncLogger::getLogLevel() {
    // [原子操作] 使用atomic的load操作读取当前日志级别
    return logLevel_.load();
}

// ==================== LogStream 实现 ====================

LogStream::LogStream(LogLevel level, const char* file, int line)
    : level_(level) {
    // 构造时立即格式化日志头部，包含时间戳、级别、文件位置
    formatHeader(level, file, line);
}

LogStream::~LogStream() {
    // [RAII] 析构时自动提交日志内容到AsyncLogger
    buffer_ += '\n';  // 添加换行符
    AsyncLogger::instance().append(buffer_.c_str(), buffer_.size());
}

void LogStream::formatHeader(LogLevel level, const char* file, int line) {
    // [时间戳生成] 获取高精度时间戳，包含毫秒信息
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()) % 1000;
    
    // [字符串格式化] 使用ostringstream进行高效的字符串拼接
    std::ostringstream oss;
    oss << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S");
    oss << '.' << std::setfill('0') << std::setw(3) << ms.count();
    
    // [日志级别映射] 将枚举值转换为可读的字符串
    const char* levelStr[] = {"DEBUG", "INFO ", "WARN ", "ERROR", "FATAL"};
    
    // [文件名提取] 只保留文件名，去掉完整路径，减少日志长度
    const char* basename = strrchr(file, '/');
    if (basename) {
        basename++;  // 跳过'/'字符
    } else {
        basename = file;  // 没有路径分隔符，直接使用原文件名
    }
    
    // [日志格式] 标准格式：[时间戳] [级别] [文件名:行号] 消息内容
    buffer_ = "[" + oss.str() + "] [" + levelStr[static_cast<int>(level)] + "] [" 
              + basename + ":" + std::to_string(line) + "] ";
}

// ==================== 流式操作符重载实现 ====================
// [性能优化] 所有操作符重载都直接操作string，避免额外的内存分配

LogStream& LogStream::operator<<(const char* str) {
    if (str) {  // 空指针检查，避免程序崩溃
        buffer_ += str;
    }
    return *this;  // 返回自身引用，支持链式调用
}

LogStream& LogStream::operator<<(const std::string& str) {
    buffer_ += str;  // string的+=操作符已经优化过，直接使用
    return *this;
}

// [数值转换] 使用std::to_string进行高效的数值到字符串转换
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
    // 布尔值转换为可读的字符串
    buffer_ += (val ? "true" : "false");
    return *this;
}

LogStream& LogStream::operator<<(char c) {
    buffer_ += c;  // 单个字符直接追加
    return *this;
}

} // namespace base
} // namespace minkv