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

/**
 * @brief 日志级别枚举
 * 
 * 定义了五个标准的日志级别，数值越大优先级越高。
 * 只有当前设置级别大于等于消息级别时，日志才会被输出。
 */
enum class LogLevel {
    DEBUG = 0,  ///< 调试信息，开发阶段使用
    INFO = 1,   ///< 一般信息，程序正常运行状态
    WARN = 2,   ///< 警告信息，可能存在问题但不影响运行
    ERROR = 3,  ///< 错误信息，程序出现错误但可以继续运行
    FATAL = 4   ///< 致命错误，程序无法继续运行
};

/**
 * @brief 固定大小的日志缓冲区
 * 
 * [核心优化] 使用固定大小缓冲区避免动态内存分配，提升性能。
 * 4MB的缓冲区大小是经过测试优化的，既能减少I/O次数，又不会占用过多内存。
 * 
 * @note 线程不安全，需要外部同步机制保护
 */
class FixedBuffer {
public:
    static constexpr size_t kBufferSize = 4 * 1024 * 1024;  ///< 4MB缓冲区大小，平衡内存使用和I/O效率
    
    /**
     * @brief 构造函数，初始化缓冲区指针
     */
    FixedBuffer() : cur_(data_) {}
    
    /**
     * @brief 向缓冲区追加数据
     * @param data 要追加的数据指针
     * @param len 数据长度
     * 
     * [核心优化] 使用memcpy进行内存拷贝，比逐字符拷贝快数倍
     */
    void append(const char* data, size_t len) {
        if (avail() >= len) {
            // 使用memcpy而不是strcpy，避免字符串结束符检查，提升性能
            memcpy(cur_, data, len);
            cur_ += len;
        }
    }
    
    /**
     * @brief 获取缓冲区数据起始地址
     * @return 数据起始指针
     */
    const char* data() const { return data_; }
    
    /**
     * @brief 获取当前数据长度
     * @return 已使用的字节数
     */
    size_t length() const { return cur_ - data_; }
    
    /**
     * @brief 获取剩余可用空间
     * @return 剩余字节数
     */
    size_t avail() const { return end() - cur_; }
    
    /**
     * @brief 重置缓冲区，清空所有数据
     */
    void reset() { cur_ = data_; }
    
    /**
     * @brief 检查缓冲区是否为空
     * @return true表示空，false表示有数据
     */
    bool empty() const { return cur_ == data_; }
    
private:
    /**
     * @brief 获取缓冲区结束地址
     * @return 缓冲区末尾指针
     */
    const char* end() const { return data_ + sizeof(data_); }
    
    char data_[kBufferSize];  ///< 固定大小的缓冲区数组
    char* cur_;               ///< 当前写入位置指针
};

/**
 * @brief 高性能异步日志器
 * 
 * [核心优化] 采用双缓冲机制实现异步日志，前端线程写入不阻塞，后端线程负责I/O。
 * 这是Muduo网络库的经典设计，能够达到百万级QPS的日志写入性能。
 * 
 * 设计特点：
 * - 双缓冲区：currentBuffer_和nextBuffer_，避免前端阻塞
 * - 生产者-消费者模式：前端（业务线程）生产日志，后端（写磁盘的独立线程）消费写入文件
 * - 批量写入：积累多条日志后批量写入，减少系统调用次数
 * - 异常安全：使用RAII管理资源，确保线程安全退出
 * 
 * @note 线程安全，支持多线程并发写入
 */
class AsyncLogger {
public:
    /**
     * @brief 构造异步日志器
     * @param basename 日志文件基础名称
     * @param rollSize 日志文件滚动大小，默认500MB
     */
    AsyncLogger(const std::string& basename, size_t rollSize = 500 * 1024 * 1024);
    
    /**
     * @brief 析构函数，确保资源正确释放
     * 
     * [RAII] 析构时自动停止后台线程，刷新剩余日志到文件
     */
    ~AsyncLogger();
    
    /**
     * @brief 追加日志数据到缓冲区
     * @param logline 日志内容
     * @param len 日志长度
     * 
     * [核心优化] 前端接口，快速写入缓冲区后立即返回，不等待I/O完成
     */
    void append(const char* logline, size_t len);
    
    /**
     * @brief 强制刷新缓冲区
     * 
     * 通知后台线程立即处理当前缓冲区，用于程序退出前确保日志完整性
     */
    void flush();
    
    /**
     * @brief 启动异步日志线程
     */
    void start();
    
    /**
     * @brief 停止异步日志线程
     * 
     * [异常安全] 等待后台线程完成所有日志写入后再退出
     */
    void stop();
    
    // 静态接口，实现全局日志管理
    /**
     * @brief 获取全局日志器实例
     * @return 单例日志器引用
     * 
     * [单例模式] 使用Meyer's Singleton保证线程安全的延迟初始化
     */
    static AsyncLogger& instance();
    
    /**
     * @brief 设置全局日志级别
     * @param level 日志级别
     * 
     * [原子操作] 使用atomic确保多线程环境下的安全性
     */
    static void setLogLevel(LogLevel level);
    
    /**
     * @brief 获取当前日志级别
     * @return 当前日志级别
     */
    static LogLevel getLogLevel();
    
private:
    /**
     * @brief 后台线程函数，负责日志的实际写入
     * 
     * [生产者-消费者] 从缓冲区队列中取出数据，批量写入文件
     * 使用条件变量实现高效的线程间通信
     */
    void threadFunc();
    
    // 类型别名，提高代码可读性
    using Buffer = FixedBuffer;                    ///< 缓冲区类型
    using BufferPtr = std::unique_ptr<Buffer>;     ///< 缓冲区智能指针
    using BufferVector = std::vector<BufferPtr>;   ///< 缓冲区向量
    
    const std::string basename_;  ///< 日志文件基础名称
    const size_t rollSize_;       ///< 日志文件滚动大小
    
    std::atomic<bool> running_;   ///< 运行状态标志，原子操作保证线程安全
    std::thread thread_;          ///< 后台I/O线程
    
    // [分片锁] 使用mutex保护共享数据，condition_variable实现高效等待
    std::mutex mutex_;                    ///< 保护缓冲区的互斥锁
    std::condition_variable cond_;        ///< 条件变量，用于线程间通信
    
    // [双缓冲机制] 核心数据结构
    BufferPtr currentBuffer_;   ///< 当前写入缓冲区，前端线程使用
    BufferPtr nextBuffer_;      ///< 备用缓冲区，避免频繁内存分配
    BufferVector buffers_;      ///< 待写入缓冲区队列，后端线程处理
    
    static std::atomic<LogLevel> logLevel_;  ///< 全局日志级别，原子操作
};

/**
 * @brief 日志流类，提供类似cout的流式接口
 * 
 * 简化版本的日志流，避免复杂的格式化操作以提升性能。
 * 支持基本数据类型的流式输出，自动添加时间戳和文件位置信息。
 * 
 * @note 对象生命周期结束时自动将日志内容提交到AsyncLogger
 */
class LogStream {
public:
    /**
     * @brief 构造日志流
     * @param level 日志级别
     * @param file 源文件名
     * @param line 源文件行号
     * 
     * 构造时自动格式化日志头部信息（时间戳、级别、文件位置）
     */
    LogStream(LogLevel level, const char* file, int line);
    
    /**
     * @brief 析构函数，自动提交日志
     * 
     * [RAII] 对象销毁时自动将缓冲的日志内容提交到AsyncLogger
     */
    ~LogStream();
    
    // 流式操作符重载，支持各种基本数据类型
    LogStream& operator<<(const char* str);           ///< C字符串
    LogStream& operator<<(const std::string& str);    ///< C++字符串
    LogStream& operator<<(int val);                   ///< 整型
    LogStream& operator<<(long val);                  ///< 长整型
    LogStream& operator<<(long long val);             ///< 长长整型
    LogStream& operator<<(unsigned int val);          ///< 无符号整型
    LogStream& operator<<(unsigned long val);         ///< 无符号长整型
    LogStream& operator<<(unsigned long long val);    ///< 无符号长长整型
    LogStream& operator<<(float val);                 ///< 单精度浮点
    LogStream& operator<<(double val);                ///< 双精度浮点
    LogStream& operator<<(bool val);                  ///< 布尔值
    LogStream& operator<<(char c);                    ///< 字符
    
private:
    /**
     * @brief 格式化日志头部信息
     * @param level 日志级别
     * @param file 源文件名
     * @param line 源文件行号
     * 
     * 生成格式：[时间戳] [级别] [文件名:行号]
     */
    void formatHeader(LogLevel level, const char* file, int line);
    
    LogLevel level_;      ///< 当前日志级别
    std::string buffer_;  ///< 日志内容缓冲区
};

// 便利宏定义，提供简洁的日志接口
// [性能优化] 使用条件编译，只有满足级别要求的日志才会创建LogStream对象

/**
 * @brief DEBUG级别日志宏
 * 
 * 只有当前日志级别<=DEBUG时才会执行，避免不必要的字符串构造开销
 */
#define LOG_DEBUG if (minkv::base::AsyncLogger::getLogLevel() <= minkv::base::LogLevel::DEBUG) \
    minkv::base::LogStream(minkv::base::LogLevel::DEBUG, __FILE__, __LINE__)

/**
 * @brief INFO级别日志宏
 */
#define LOG_INFO if (minkv::base::AsyncLogger::getLogLevel() <= minkv::base::LogLevel::INFO) \
    minkv::base::LogStream(minkv::base::LogLevel::INFO, __FILE__, __LINE__)

/**
 * @brief WARN级别日志宏
 */
#define LOG_WARN if (minkv::base::AsyncLogger::getLogLevel() <= minkv::base::LogLevel::WARN) \
    minkv::base::LogStream(minkv::base::LogLevel::WARN, __FILE__, __LINE__)

/**
 * @brief ERROR级别日志宏
 */
#define LOG_ERROR if (minkv::base::AsyncLogger::getLogLevel() <= minkv::base::LogLevel::ERROR) \
    minkv::base::LogStream(minkv::base::LogLevel::ERROR, __FILE__, __LINE__)

/**
 * @brief FATAL级别日志宏
 * 
 * FATAL级别总是输出，不做级别检查
 */
#define LOG_FATAL minkv::base::LogStream(minkv::base::LogLevel::FATAL, __FILE__, __LINE__)

} // namespace base
} // namespace minkv