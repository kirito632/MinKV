#include "graph_serializer.h"
#include <cstdint>
#include <cstring>

namespace minkv {
namespace graph {

// ══════════════════════════════════════════════════════════════════════════════
// 内部工具函数
// ══════════════════════════════════════════════════════════════════════════════

/**
 * 把一个 32 位无符号整数以小端序（低字节在前）追加到字符串 buf 末尾。
 * 小端序是 x86/x64 的原生字节序，也是本项目的统一约定。
 *
 * 例：val = 0x00000005 -> buf 追加 [0x05, 0x00, 0x00, 0x00]
 */
void GraphSerializer::AppendUint32LE(std::string &buf, uint32_t val) {
  char bytes[4];
  bytes[0] = static_cast<char>(val & 0xFF); // 最低字节
  bytes[1] = static_cast<char>((val >> 8) & 0xFF);
  bytes[2] = static_cast<char>((val >> 16) & 0xFF);
  bytes[3] = static_cast<char>((val >> 24) & 0xFF); // 最高字节
  buf.append(bytes, 4);
}

/**
 * 从 buf 的 offset 位置读取 4 字节，按小端序还原为 uint32_t。
 * 如果剩余字节不足 4 个，抛出 runtime_error。
 */
uint32_t GraphSerializer::ReadUint32LE(const std::string &buf, size_t offset) {
  if (offset + 4 > buf.size()) {
    throw std::runtime_error(
        "GraphSerializer: buffer too short reading uint32");
  }
  // 用 unsigned char* 避免有符号扩展问题
  const unsigned char *p =
      reinterpret_cast<const unsigned char *>(buf.data() + offset);
  return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
         (static_cast<uint32_t>(p[2]) << 16) |
         (static_cast<uint32_t>(p[3]) << 24);
}

/**
 * 从 buf 的 offset 位置读取一个"长度前缀字符串"：
 *   先读 4 字节得到字符串长度 len，再读 len 字节得到字符串内容。
 * offset 会被更新到读完之后的位置，方便连续调用。
 */
std::string GraphSerializer::ReadString(const std::string &buf,
                                        size_t &offset) {
  uint32_t len = ReadUint32LE(buf, offset);
  offset += 4; // 跳过长度字段
  if (offset + len > buf.size()) {
    throw std::runtime_error(
        "GraphSerializer: buffer too short reading string");
  }
  std::string s(buf.data() + offset, len);
  offset += len; // 跳过字符串内容
  return s;
}

// ══════════════════════════════════════════════════════════════════════════════
// Node 序列化 / 反序列化
//
// 二进制布局（设计文档 3.1 节）：
//   [4B node_id_len][node_id 字节][4B props_len][properties_json 字节]
// ══════════════════════════════════════════════════════════════════════════════

std::string GraphSerializer::SerializeNode(const Node &node) {
  std::string buf;
  // 预分配内存，避免多次 realloc（8 = 两个 4 字节长度字段）
  buf.reserve(8 + node.node_id.size() + node.properties_json.size());

  AppendUint32LE(buf, static_cast<uint32_t>(node.node_id.size()));
  buf.append(node.node_id);

  AppendUint32LE(buf, static_cast<uint32_t>(node.properties_json.size()));
  buf.append(node.properties_json);

  return buf;
}

Node GraphSerializer::DeserializeNode(const std::string &data) {
  size_t offset = 0;
  Node node;
  node.node_id = ReadString(data, offset);         // 读 node_id
  node.properties_json = ReadString(data, offset); // 读 properties_json
  return node;
}

// ══════════════════════════════════════════════════════════════════════════════
// Edge 序列化 / 反序列化
//
// 二进制布局（设计文档 3.2 节）：
//   [4B src_len][src][4B dst_len][dst][4B label_len][label]
//   [4B float weight][4B props_len][properties_json]
// ══════════════════════════════════════════════════════════════════════════════

std::string GraphSerializer::SerializeEdge(const Edge &edge) {
  std::string buf;
  // 20 = 4个长度字段(4B each) + 1个float(4B)
  buf.reserve(20 + edge.src_id.size() + edge.dst_id.size() + edge.label.size() +
              edge.properties_json.size());

  AppendUint32LE(buf, static_cast<uint32_t>(edge.src_id.size()));
  buf.append(edge.src_id);

  AppendUint32LE(buf, static_cast<uint32_t>(edge.dst_id.size()));
  buf.append(edge.dst_id);

  AppendUint32LE(buf, static_cast<uint32_t>(edge.label.size()));
  buf.append(edge.label);

  // weight 是 IEEE 754 单精度浮点，直接把内存字节拷贝进去（4 字节）
  // 用 memcpy 而不是强制转换，避免未定义行为
  char wbytes[4];
  std::memcpy(wbytes, &edge.weight, 4);
  buf.append(wbytes, 4);

  AppendUint32LE(buf, static_cast<uint32_t>(edge.properties_json.size()));
  buf.append(edge.properties_json);

  return buf;
}

Edge GraphSerializer::DeserializeEdge(const std::string &data) {
  size_t offset = 0;
  Edge edge;

  edge.src_id = ReadString(data, offset);
  edge.dst_id = ReadString(data, offset);
  edge.label = ReadString(data, offset);

  // 读取 4 字节 float weight
  if (offset + 4 > data.size()) {
    throw std::runtime_error(
        "GraphSerializer: buffer too short reading weight");
  }
  std::memcpy(&edge.weight, data.data() + offset, 4);
  offset += 4;

  edge.properties_json = ReadString(data, offset);
  return edge;
}

// ══════════════════════════════════════════════════════════════════════════════
// 邻接表序列化 / 反序列化
//
// 优化：从 JSON 格式改为紧凑二进制格式，消除 JSON 解析开销。
//
// 二进制布局：
//   [4B count]  元素个数（uint32_t 小端序）
//   对每个元素：
//     [4B len][len bytes]  字符串长度前缀 + 内容
//
// 对比 JSON 的优势：
//   1. 序列化：直接 memcpy，无需字符转义，O(N) 且常数极小
//   2. 反序列化：直接按偏移读取，无状态机，无分支预测失败
//   3. 内存连续：整块 buffer 一次分配，CPU cache line 预取友好
//   4. Hub Node 场景：出度 1000 的节点，JSON 约 15KB，二进制约 12KB，
//      但序列化速度提升 3~5x（消除了 jsonEscape 和字符串拼接）
// ══════════════════════════════════════════════════════════════════════════════

std::string
GraphSerializer::SerializeAdjList(const std::vector<std::string> &ids) {
  // 预计算总大小，一次性分配内存，避免 realloc
  // 总大小 = 4（count）+ sum(4 + len_i)
  size_t total = 4;
  for (const auto &id : ids)
    total += 4 + id.size();

  std::string buf;
  buf.reserve(total);

  // 写入元素个数
  AppendUint32LE(buf, static_cast<uint32_t>(ids.size()));

  // 写入每个字符串：[4B len][bytes]
  for (const auto &id : ids) {
    AppendUint32LE(buf, static_cast<uint32_t>(id.size()));
    buf.append(id);
  }

  return buf;
}

std::vector<std::string>
GraphSerializer::DeserializeAdjList(const std::string &data) {
  if (data.empty())
    return {};

  size_t offset = 0;
  uint32_t count = ReadUint32LE(data, offset);
  offset += 4;

  std::vector<std::string> result;
  result.reserve(count); // 预分配，避免 vector 扩容

  for (uint32_t i = 0; i < count; ++i) {
    result.push_back(ReadString(data, offset));
  }

  return result;
}

} // namespace graph
} // namespace minkv
