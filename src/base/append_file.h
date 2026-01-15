#pragma once

#include <string>
#include <memory>

namespace minkv {
namespace base {

/**
 * @brief 高性能文件追加写入器
 * 
 * [核心优化] 直接使用Linux系统调用，绕过标准库的用户空间缓冲，
 * 实现最高性能的文件写入。这是数据库系统和高性能日志系统的核心组件。
 * 
 * 设计特点：
 * - 系统调用优化：直接使用open/write/fsync，避免stdio缓冲开销
 * - O_APPEND模式：确保多进程写入的原子性，防止数据交错
 * - 异常安全：RAII管理文件描述符，确保资源正确释放
 * - 错误处理：完善的错误检查和异常处理机制
 * 
 * 性能对比：
 * - 标准库fwrite：用户缓冲 + 内核缓冲，双重开销
 * - 系统调用write：直接内核缓冲，性能提升20-50%
 * - O_APPEND模式：多进程安全，无需额外锁机制
 * 
 * 应用场景：
 * - WAL日志写入：数据库事务日志的高性能写入
 * - 异步日志系统：AsyncLogger的底层I/O引擎
 * - Group Commit：批量提交的文件写入后端
 * 
 * @note 线程安全性：单个AppendFile对象不是线程安全的，
 *       需要外部同步机制保护。但O_APPEND模式保证多进程写入的原子性。
 */
class AppendFile {
public:
    /**
     * @brief 构造高性能文件写入器
     * @param filename 目标文件路径
     * 
     * [系统调用优化] 使用open()系统调用创建文件描述符：
     * - O_APPEND：每次写入自动追加到文件末尾，多进程安全
     * - O_CREAT：文件不存在时自动创建，简化使用
     * - O_WRONLY：只写模式，明确访问权限
     * - 0644权限：用户读写，组和其他用户只读
     * 
     * @throws std::runtime_error 文件打开失败时抛出异常
     */
    explicit AppendFile(const std::string& filename);
    
    /**
     * @brief 析构函数，确保资源正确释放
     * 
     * [RAII] 自动关闭文件描述符，即使发生异常也能保证资源清理
     */
    ~AppendFile();
    
    // [资源管理] 禁止拷贝和赋值，确保文件描述符的唯一所有权
    AppendFile(const AppendFile&) = delete;
    AppendFile& operator=(const AppendFile&) = delete;
    
    /**
     * @brief 追加写入数据到文件
     * @param data 要写入的数据指针
     * @param len 数据长度（字节）
     * 
     * [核心接口] 高性能写入接口，直接调用write()系统调用
     * 特点：
     * - 无用户空间缓冲：数据直接进入内核缓冲区
     * - 原子追加：O_APPEND模式保证写入位置的原子性
     * - 完整写入：处理部分写入情况，确保所有数据都被写入
     * - 信号安全：正确处理EINTR信号中断
     * 
     * @throws std::runtime_error 写入失败时抛出异常
     */
    void append(const char* data, size_t len);
    
    /**
     * @brief 刷新数据到内核缓冲区
     * 
     * [接口兼容] 为了与标准库接口保持一致而提供的方法。
     * 由于直接使用系统调用，数据已经在内核缓冲区中，
     * 此方法实际上是空操作，但保留以维护接口语义。
     */
    void flush();
    
    /**
     * @brief 强制同步数据到磁盘
     * 
     * [持久化保证] 调用fsync()系统调用，强制将内核缓冲区的数据
     * 写入到物理磁盘，确保数据持久化。这是数据库系统ACID特性中
     * 持久性(Durability)的关键实现。
     * 
     * 性能特点：
     * - fsync()是昂贵的系统调用，通常耗时1-10ms
     * - 确保数据真正写入磁盘，即使断电也不会丢失
     * - Group Commit优化：批量调用fsync减少开销
     * 
     * @throws std::runtime_error fsync失败时抛出异常
     */
    void sync();
    
    /**
     * @brief 获取已写入的总字节数
     * @return 累计写入的字节数
     * 
     * [统计接口] 用于性能监控和调试，跟踪文件写入量
     */
    off_t writtenBytes() const { return writtenBytes_; }
    
private:
    /**
     * @brief 无锁写入实现
     * @param data 数据指针
     * @param len 数据长度
     * 
     * [核心实现] 处理实际的写入逻辑：
     * - 循环写入：处理部分写入的情况
     * - 信号处理：正确处理EINTR中断
     * - 字节统计：累计写入字节数
     * - 异常处理：写入失败时抛出明确的错误信息
     */
    void writeUnlocked(const char* data, size_t len);
    
    int fd_;                    ///< 文件描述符，Linux系统调用的核心句柄
    off_t writtenBytes_;        ///< 累计写入字节数，用于统计和监控
    std::string filename_;      ///< 文件名，用于错误信息和调试
};

} // namespace base
} // namespace minkv