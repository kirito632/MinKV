#pragma once

#include <string>

namespace minkv {
namespace graph {

/**
 * 图节点
 *
 * 存储在 KV 中的 Key 格式：n:{node_id}
 * properties_json 是任意 JSON 字符串，例如 {"name":"Alice","age":30}
 * 上层调用方负责解析 JSON，GraphStore 内部只把它当普通字符串存储。
 */
struct Node {
  std::string node_id;         // 节点唯一 ID，全局不重复
  std::string properties_json; // 节点属性，JSON 格式字符串

  // 用于测试断言：两个 Node 所有字段都相等才算相等
  bool operator==(const Node &other) const {
    return node_id == other.node_id && properties_json == other.properties_json;
  }
};

/**
 * 图中的有向边
 *
 * 一条边由三元组 (src_id, dst_id, label) 唯一确定。
 * 同一对节点之间可以有多条不同 label 的边（多重边）。
 *
 * 存储在 KV 中的 Key 格式：e:{src_id}:{dst_id}:{label}
 * 注意：如果 id 或 label 本身含有 ':'，GraphStore 会先做转义再拼 Key。
 */
struct Edge {
  std::string src_id;  // 起点节点 ID
  std::string dst_id;  // 终点节点 ID
  std::string label;   // 边的类型/标签，例如 "KNOWS"、"WORKS_AT"
  float weight = 1.0f; // 边的权重，默认 1.0
  std::string properties_json; // 边的附加属性，JSON 格式字符串

  // 用于测试断言：所有字段完全相等才算相等
  bool operator==(const Edge &other) const {
    return src_id == other.src_id && dst_id == other.dst_id &&
           label == other.label && weight == other.weight &&
           properties_json == other.properties_json;
  }
};

} // namespace graph
} // namespace minkv
