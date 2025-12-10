#pragma once

#include <string>
#include <vector>
#include <string_view>
#include <optional>

namespace minkv {
namespace server {

/**
 * @brief RESP (Redis Serialization Protocol) 解析器
 * 
 * 负责将客户端发送的字节流解析为命令参数列表。
 * 目前仅支持解析 RESP 数组（Array）格式，因为这是客户端发送命令的标准格式。
 * 例如: "*3\r\n$3\r\nSET\r\n..." -> {"SET", "key", "value"}
 */
class RespParser {
public:
    // 解析结果类型：命令参数列表
    using Command = std::vector<std::string>;

    /**
     * @brief 解析一段完整的 RESP 消息
     * 
     * @param data 接收到的原始数据
     * @return 如果解析成功，返回参数列表；如果数据不完整或格式错误，返回 nullopt
     */
    static std::optional<Command> parse(std::string_view data);

    /**
     * @brief 将结果序列化为 RESP 格式
     * 
     * @param msg 简单的字符串消息
     * @return "+msg\r\n"
     */
    static std::string serialize_simple_string(const std::string& msg);

    /**
     * @brief 将错误序列化为 RESP 格式
     * 
     * @param msg 错误消息
     * @return "-msg\r\n"
     */
    static std::string serialize_error(const std::string& msg);

    /**
     * @brief 将批量字符串序列化为 RESP 格式 (用于 GET 返回)
     * 
     * @param val 具体的值
     * @return "$len\r\nval\r\n"
     */
    static std::string serialize_bulk_string(const std::string& val);

    /**
     * @brief 序列化空值 (用于 GET 没找到时)
     * 
     * @return "$-1\r\n"
     */
    static std::string serialize_null();
};

} // namespace server
} // namespace minkv
