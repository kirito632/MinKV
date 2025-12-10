#include "resp_parser.h"
#include <algorithm>
#include <charconv>

namespace minkv {
namespace server {

// 辅助函数：查找下一个 CRLF (\r\n) 的位置
// 返回值是相对于 view 开头的偏移量
static size_t find_crlf(std::string_view view) {
    return view.find("\r\n");
}

// 辅助函数：将 string_view 转为整数 (比 atoi 快且安全)
static std::optional<int> parse_int(std::string_view view) {
    int result;
    auto [ptr, ec] = std::from_chars(view.data(), view.data() + view.size(), result);
    if (ec == std::errc()) {
        return result;
    }
    return std::nullopt;
}

std::optional<RespParser::Command> RespParser::parse(std::string_view data) {
    Command args;
    size_t pos = 0; // 当前解析到的位置指针

    // 1. 检查是否为空或格式错误
    if (data.empty() || data[0] != '*') {
        return std::nullopt; // 目前只处理 Array 格式 (*3\r\n...)
    }

    // 2. 解析数组长度 (*3\r\n)
    // 跳过 '*'
    pos++; 
    
    // 找到第一个 \r\n
    size_t crlf = find_crlf(data.substr(pos));
    if (crlf == std::string_view::npos) return std::nullopt;

    // 截取数字部分 "3"
    auto count_str = data.substr(pos, crlf);
    auto count = parse_int(count_str);
    if (!count.has_value()) return std::nullopt;

    // 移动指针到 \r\n 之后
    pos += crlf + 2; // +2 是因为 \r\n 占2字节

    // 3. 循环解析每个参数
    for (int i = 0; i < count.value(); ++i) {
        // 检查剩余数据是否足够 ($len\r\nval\r\n 至少要有 $ + 数字 + \r\n)
        if (pos >= data.size() || data[pos] != '$') return std::nullopt;
        pos++; // 跳过 '$'

        // 读取字符串长度 ($3\r\n)
        crlf = find_crlf(data.substr(pos));
        if (crlf == std::string_view::npos) return std::nullopt;

        auto len_str = data.substr(pos, crlf);
        auto len = parse_int(len_str);
        if (!len.has_value()) return std::nullopt;

        pos += crlf + 2; // 跳过长度行

        // 读取实际内容 (SET\r\n)
        // 检查剩余长度是否够 (内容长度 + \r\n)
        if (pos + len.value() + 2 > data.size()) return std::nullopt;

        // 截取内容并存入 vector
        args.emplace_back(data.substr(pos, len.value()));

        // 移动指针跳过内容和 \r\n
        pos += len.value() + 2;
    }

    return args;
}

// --- 序列化函数实现 ---

std::string RespParser::serialize_simple_string(const std::string& msg) {
    // 简单字符串: +OK\r\n
    return "+" + msg + "\r\n";
}

std::string RespParser::serialize_error(const std::string& msg) {
    // 错误: -Error message\r\n
    return "-" + msg + "\r\n";
}

std::string RespParser::serialize_bulk_string(const std::string& val) {
    // 批量字符串: $5\r\nvalue\r\n
    return "$" + std::to_string(val.length()) + "\r\n" + val + "\r\n";
}

std::string RespParser::serialize_null() {
    // 空值 (nil): $-1\r\n
    return "$-1\r\n";
}

} // namespace server
} // namespace minkv
