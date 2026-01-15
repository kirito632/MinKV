#include "append_file.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <cerrno>
#include <cstring>
#include <stdexcept>

namespace minkv {
namespace base {

AppendFile::AppendFile(const std::string& filename)
    : fd_(-1), writtenBytes_(0), filename_(filename) {
    
    // [系统调用优化] 使用open()系统调用直接创建文件描述符
    // 这是高性能I/O的关键：绕过标准库的用户空间缓冲
    
    // O_APPEND: 每次写入都自动追加到文件末尾
    //   - 原子性保证：即使多个进程同时写入，也不会出现数据交错
    //   - 无需lseek：系统自动定位到文件末尾，避免额外系统调用
    //   - 并发安全：多个AppendFile实例可以安全地写入同一文件
    
    // O_CREAT: 文件不存在时自动创建
    //   - 简化使用：无需预先创建文件
    //   - 权限设置：0644 = rw-r--r--，用户可读写，其他用户只读
    
    // O_WRONLY: 只写模式
    //   - 明确权限：只允许写入操作，防止意外读取
    //   - 性能优化：内核可以针对只写模式进行优化
    fd_ = ::open(filename.c_str(), O_APPEND | O_CREAT | O_WRONLY, 0644);
    
    // [异常安全] 文件打开失败时抛出明确的错误信息
    if (fd_ < 0) {
        // 使用strerror获取系统错误描述，提供详细的错误信息
        throw std::runtime_error("Failed to open file: " + filename + 
                                ", error: " + std::strerror(errno));
    }
}

AppendFile::~AppendFile() {
    // [RAII] 析构时自动关闭文件描述符，确保资源不泄漏
    if (fd_ >= 0) {
        // 使用::close明确调用系统调用，而不是标准库函数
        ::close(fd_);
    }
}

void AppendFile::append(const char* data, size_t len) {
    // [接口封装] 公共接口直接调用内部实现
    // 这种设计为将来可能的线程安全扩展预留空间
    writeUnlocked(data, len);
}

void AppendFile::writeUnlocked(const char* data, size_t len) {
    // [核心优化] 高性能写入实现，处理所有边界情况
    
    size_t written = 0;  // 已写入字节数，用于处理部分写入
    
    // [完整写入保证] 循环写入直到所有数据都被写入
    // write()系统调用可能只写入部分数据，特别是在以下情况：
    // - 磁盘空间不足
    // - 信号中断
    // - 网络文件系统延迟
    while (written < len) {
        // [系统调用] 直接调用write()，绕过标准库缓冲
        // 参数说明：
        // - fd_: 文件描述符
        // - data + written: 当前写入位置
        // - len - written: 剩余要写入的字节数
        ssize_t n = ::write(fd_, data + written, len - written);
        
        if (n < 0) {
            // [信号处理] 处理系统调用被信号中断的情况
            if (errno == EINTR) {
                // EINTR: 系统调用被信号中断，这是正常情况
                // 解决方案：继续重试写入，不视为错误
                continue;
            } else {
                // [错误处理] 其他错误情况，抛出异常
                // 常见错误：ENOSPC(磁盘满)、EIO(I/O错误)、EBADF(无效文件描述符)
                throw std::runtime_error("Write failed: " + std::string(std::strerror(errno)));
            }
        }
        
        // [进度跟踪] 更新已写入字节数和总计数器
        written += n;           // 本次循环的进度
        writtenBytes_ += n;     // 文件总写入量统计
    }
}

void AppendFile::flush() {
    // [接口兼容] 为了与标准库FILE*接口保持一致而提供
    // 
    // 对于直接使用系统调用的文件描述符，flush实际上是空操作：
    // - 标准库fwrite：数据先到用户缓冲区，需要fflush刷到内核
    // - 系统调用write：数据直接到内核缓冲区，无需用户空间flush
    // 
    // 但我们保留这个方法的原因：
    // 1. 接口一致性：与标准库接口保持兼容
    // 2. 语义清晰：调用者明确表达"刷新"的意图
    // 3. 扩展性：将来可能添加用户空间缓冲时，此接口仍然有效
    
    // 注意：flush()只是将数据从用户缓冲区刷到内核缓冲区
    // 要确保数据真正写入磁盘，需要调用sync()方法
}

void AppendFile::sync() {
    // [持久化保证] 强制将内核缓冲区的数据同步到物理磁盘
    // 这是数据库系统ACID特性中持久性(Durability)的关键实现
    
    // fsync()系统调用的作用：
    // 1. 将文件的所有修改过的内核缓冲区数据写入磁盘
    // 2. 将文件的元数据（如修改时间、文件大小）写入磁盘
    // 3. 等待磁盘写入完成后才返回
    
    // 性能特点：
    // - fsync()是昂贵的系统调用，通常耗时1-10ms
    // - SSD比HDD快，但仍然比内存操作慢1000倍以上
    // - 这就是为什么需要Group Commit优化：批量fsync减少调用次数
    
    if (::fsync(fd_) < 0) {
        // [错误处理] fsync失败通常表示严重的I/O问题
        // 常见错误：EIO(I/O错误)、ENOSPC(磁盘满)、EROFS(只读文件系统)
        throw std::runtime_error("fsync failed: " + std::string(std::strerror(errno)));
    }
    
    // [面试要点] fsync vs fdatasync的区别：
    // - fsync：同步数据和元数据，完全的持久化保证
    // - fdatasync：只同步数据，不同步元数据，性能更好
    // 我们选择fsync是为了最强的持久化保证
}

} // namespace base
} // namespace minkv