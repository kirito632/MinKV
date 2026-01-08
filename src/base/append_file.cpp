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
    
    // 使用 O_APPEND | O_CREAT | O_WRONLY 模式打开文件
    // O_APPEND: 每次写入都追加到文件末尾
    // O_CREAT: 如果文件不存在则创建
    // O_WRONLY: 只写模式
    fd_ = ::open(filename.c_str(), O_APPEND | O_CREAT | O_WRONLY, 0644);
    
    if (fd_ < 0) {
        throw std::runtime_error("Failed to open file: " + filename + 
                                ", error: " + std::strerror(errno));
    }
}

AppendFile::~AppendFile() {
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

void AppendFile::append(const char* data, size_t len) {
    writeUnlocked(data, len);
}

void AppendFile::writeUnlocked(const char* data, size_t len) {
    size_t written = 0;
    
    while (written < len) {
        ssize_t n = ::write(fd_, data + written, len - written);
        
        if (n < 0) {
            if (errno == EINTR) {
                // 被信号中断，继续写入
                continue;
            } else {
                throw std::runtime_error("Write failed: " + std::string(std::strerror(errno)));
            }
        }
        
        written += n;
        writtenBytes_ += n;
    }
}

void AppendFile::flush() {
    // 对于直接使用系统调用的文件描述符，flush 实际上不需要做什么
    // 因为我们没有使用用户空间缓冲
    // 但为了接口一致性，我们保留这个方法
}

void AppendFile::sync() {
    // 强制将数据同步到磁盘
    if (::fsync(fd_) < 0) {
        throw std::runtime_error("fsync failed: " + std::string(std::strerror(errno)));
    }
}

} // namespace base
} // namespace minkv