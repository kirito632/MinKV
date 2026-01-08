#pragma once

#include <string>
#include <memory>

namespace minkv {
namespace base {

// 高性能文件追加写入类
// 使用系统调用，避免标准库缓冲
class AppendFile {
public:
    explicit AppendFile(const std::string& filename);
    ~AppendFile();
    
    // 禁止拷贝
    AppendFile(const AppendFile&) = delete;
    AppendFile& operator=(const AppendFile&) = delete;
    
    // 追加写入数据
    void append(const char* data, size_t len);
    
    // 刷新到磁盘
    void flush();
    
    // 强制同步到磁盘
    void sync();
    
    // 获取已写入字节数
    off_t writtenBytes() const { return writtenBytes_; }
    
private:
    void writeUnlocked(const char* data, size_t len);
    
    int fd_;
    off_t writtenBytes_;
    std::string filename_;
};

} // namespace base
} // namespace minkv