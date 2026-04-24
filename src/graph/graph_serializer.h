#pragma once

#include "graph_types.h"
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace minkv {
namespace graph {

/**
 * GraphSerializer — 负责 Node / Edge / 邻接表 的序列化与反序列化
 *
 * 设计原则：
 *   - GraphStore 只管存取字符串，所有"结构体 <-> 字节流"的转换都在这里完成
 *   - 不依赖 nlohmann/json 等第三方库，手写二进制/JSON 格式，减少依赖
 *   - 反序列化失败时抛出 std::runtime_error，由调用方决定如何处理
 *
 * 二进制格式（小端序）：
 *   Node:  [4B 长度][node_id 字节][4B 长度][properties_json 字节]
 *   Edge:  [4B][src][4B][dst][4B][label][4B float weight][4B][props]
 *   AdjList: JSON 数组字符串，例如 ["id1","id2"]
 */
class GraphSerializer {
public:
  // ── Node ──────────────────────────────────────────────────────────────────

  /** 将 Node 结构体序列化为二进制字符串，写入 KV 时使用 */
  static std::string SerializeNode(const Node &node);

  /** 从二进制字符串还原 Node 结构体，读取 KV 后使用；数据损坏时抛异常 */
  static Node DeserializeNode(const std::string &data);

  // ── Edge ──────────────────────────────────────────────────────────────────

  /** 将 Edge 结构体序列化为二进制字符串 */
  static std::string SerializeEdge(const Edge &edge);

  /** 从二进制字符串还原 Edge 结构体；数据损坏时抛异常 */
  static Edge DeserializeEdge(const std::string &data);

  // ── Adjacency List ────────────────────────────────────────────────────────

  /**
   * 将邻居 ID 列表序列化为 JSON 数组字符串
   * 例如：{"id1", "id2"} -> ["id1","id2"]
   * 选用 JSON 格式是为了方便人工调试时直接读懂内容
   */
  static std::string SerializeAdjList(const std::vector<std::string> &ids);

  /**
   * 将 JSON 数组字符串解析回邻居 ID 列表
   * 例如：["id1","id2"] -> {"id1", "id2"}
   * 格式不合法时抛出 std::runtime_error
   */
  static std::vector<std::string> DeserializeAdjList(const std::string &json);

private:
  // ── 内部工具函数 ──────────────────────────────────────────────────────────

  /** 向 buf 末尾追加一个 uint32_t（小端序，4 字节） */
  static void AppendUint32LE(std::string &buf, uint32_t val);

  /** 从 buf 的 offset 位置读取一个 uint32_t（小端序） */
  static uint32_t ReadUint32LE(const std::string &buf, size_t offset);

  /**
   * 从 buf 的 offset 位置读取一个"长度前缀字符串"
   * 格式：[4B uint32 长度][字符串内容]
   * 读完后 offset 会自动向后移动
   */
  static std::string ReadString(const std::string &buf, size_t &offset);
};

} // namespace graph
} // namespace minkv
