/**
 * GraphSerializer Property-Based Tests (rapidcheck)
 *
 * Property 2: Node 序列化往返 — DeserializeNode(SerializeNode(n)) == n
 *   Validates: Requirements 2.6, 2.7
 *
 * Property 3: Edge 序列化往返 — DeserializeEdge(SerializeEdge(e)) == e
 *   Validates: Requirements 3.5, 3.6
 *
 * Property 4: 邻接表序列化往返 — 反序列化结果与原列表元素集合相同
 *   Validates: Requirements 4.7, 4.8
 */

#include "graph/graph_serializer.h"
#include "graph/graph_types.h"

#include <rapidcheck.h>

#include <algorithm>
#include <cmath>
#include <iostream>
#include <string>
#include <vector>

using namespace minkv::graph;

// ── Arbitrary 生成器
// ──────────────────────────────────────────────────────────

namespace rc {

// 为 Node 实现 Arbitrary 生成器
// node_id 和 properties_json 可以包含任意字符（包括 ':'）
template <> struct Arbitrary<Node> {
  static Gen<Node> arbitrary() {
    return gen::build<Node>(
        gen::set(&Node::node_id, gen::arbitrary<std::string>()),
        gen::set(&Node::properties_json, gen::arbitrary<std::string>()));
  }
};

// 为 Edge 实现 Arbitrary 生成器
// weight 过滤掉 NaN（NaN != NaN，无法做往返相等断言）
template <> struct Arbitrary<Edge> {
  static Gen<Edge> arbitrary() {
    // 生成有限浮点数（过滤 NaN 和 Inf）
    auto finite_float = gen::suchThat(gen::arbitrary<float>(),
                                      [](float f) { return std::isfinite(f); });

    return gen::build<Edge>(
        gen::set(&Edge::src_id, gen::arbitrary<std::string>()),
        gen::set(&Edge::dst_id, gen::arbitrary<std::string>()),
        gen::set(&Edge::label, gen::arbitrary<std::string>()),
        gen::set(&Edge::weight, finite_float),
        gen::set(&Edge::properties_json, gen::arbitrary<std::string>()));
  }
};

} // namespace rc

// ── 测试主函数
// ────────────────────────────────────────────────────────────────

int main() {
  int failed = 0;

  // ── Property 2: Node 序列化往返 ──────────────────────────────────────────
  // Feature: graph-on-kv, Property 2: Node 序列化往返
  // Validates: Requirements 2.6, 2.7
  bool p2 = rc::check(
      "Property 2: Node 序列化往返 — DeserializeNode(SerializeNode(n)) == n",
      [](const Node &n) {
        auto serialized = GraphSerializer::SerializeNode(n);
        auto deserialized = GraphSerializer::DeserializeNode(serialized);
        RC_ASSERT(deserialized == n);
      });
  if (!p2)
    ++failed;

  // ── Property 3: Edge 序列化往返 ──────────────────────────────────────────
  // Feature: graph-on-kv, Property 3: Edge 序列化往返
  // Validates: Requirements 3.5, 3.6
  bool p3 = rc::check(
      "Property 3: Edge 序列化往返 — DeserializeEdge(SerializeEdge(e)) == e",
      [](const Edge &e) {
        auto serialized = GraphSerializer::SerializeEdge(e);
        auto deserialized = GraphSerializer::DeserializeEdge(serialized);
        RC_ASSERT(deserialized == e);
      });
  if (!p3)
    ++failed;

  // ── Property 4: 邻接表序列化往返 ─────────────────────────────────────────
  // Feature: graph-on-kv, Property 4: 邻接表序列化往返
  // Validates: Requirements 4.7, 4.8
  // 注意：AdjList 反序列化保持顺序，因此直接比较列表相等即可
  bool p4 = rc::check(
      "Property 4: 邻接表序列化往返 — 反序列化结果与原列表元素集合相同",
      [](const std::vector<std::string> &ids) {
        auto json = GraphSerializer::SerializeAdjList(ids);
        auto deserialized = GraphSerializer::DeserializeAdjList(json);

        // 验证元素集合相同（顺序可以不同，用排序后比较）
        auto sorted_orig = ids;
        auto sorted_des = deserialized;
        std::sort(sorted_orig.begin(), sorted_orig.end());
        std::sort(sorted_des.begin(), sorted_des.end());
        RC_ASSERT(sorted_orig == sorted_des);
      });
  if (!p4)
    ++failed;

  if (failed == 0) {
    std::cout << "\n[PASS] All 3 property-based tests passed.\n";
    return 0;
  } else {
    std::cerr << "\n[FAIL] " << failed << " property test(s) failed.\n";
    return 1;
  }
}
