/**
 * Phase 1 测试：GraphStore Node/Edge CRUD
 *
 * 单元测试：
 *   - Node 序列化往返（含空字段、特殊字符）
 *   - Edge 序列化往返（含 weight、特殊字符）
 *   - AdjList 序列化往返（含空列表、含转义字符的 ID）
 *   - GraphStore Node CRUD（AddNode/GetNode/UpdateNode/DeleteNode）
 *   - GraphStore Edge CRUD（AddEdge/GetEdge/DeleteEdge）
 *   - Key 转义（node_id 含 ':' 时不影响其他节点）
 *
 * Property-Based Tests (rapidcheck)：
 *   - Property 5: Node CRUD 往返 — AddNode(n); GetNode(id) == n；未添加的 id
 * 返回 nullopt Validates: Requirements 2.1, 2.2, 2.3
 *   - Property 6: Edge CRUD 往返 — AddEdge(e); GetEdge(src,dst,label) ==
 * e；未添加的边返回 nullopt Validates: Requirements 3.1, 3.2, 3.3
 */

#include "core/sharded_cache.h"
#include "graph/graph_serializer.h"
#include "graph/graph_store.h"

#include <rapidcheck.h>

#include <cassert>
#include <cmath>
#include <iostream>
#include <string>
#include <vector>

using namespace minkv::graph;
using GraphKVStore = minkv::db::ShardedCache<std::string, std::string>;

// ── 辅助宏
// ────────────────────────────────────────────────────────────────────

#define CHECK(cond, msg)                                                       \
  do {                                                                         \
    if (!(cond)) {                                                             \
      std::cerr << "[FAIL] " << msg << "\n";                                   \
      return false;                                                            \
    }                                                                          \
  } while (0)

#define PASS(name)                                                             \
  do {                                                                         \
    std::cout << "[PASS] " << name << "\n";                                    \
  } while (0)

// ── GraphSerializer 测试
// ──────────────────────────────────────────────────────

bool test_node_serializer_roundtrip() {
  Node n;
  n.node_id = "alice";
  n.properties_json = R"({"name":"Alice","age":30})";

  auto data = GraphSerializer::SerializeNode(n);
  auto n2 = GraphSerializer::DeserializeNode(data);

  CHECK(n2 == n, "Node roundtrip: basic");
  PASS("Node serializer roundtrip (basic)");
  return true;
}

bool test_node_serializer_empty_props() {
  Node n;
  n.node_id = "empty_node";
  n.properties_json = "";

  auto n2 = GraphSerializer::DeserializeNode(GraphSerializer::SerializeNode(n));
  CHECK(n2 == n, "Node roundtrip: empty properties_json");
  PASS("Node serializer roundtrip (empty props)");
  return true;
}

bool test_node_serializer_special_chars() {
  Node n;
  n.node_id = "user:123"; // 含 ':'
  n.properties_json = R"({"key":"val:ue","x":1})";

  auto n2 = GraphSerializer::DeserializeNode(GraphSerializer::SerializeNode(n));
  CHECK(n2 == n, "Node roundtrip: special chars in id/props");
  PASS("Node serializer roundtrip (special chars)");
  return true;
}

bool test_edge_serializer_roundtrip() {
  Edge e;
  e.src_id = "alice";
  e.dst_id = "bob";
  e.label = "KNOWS";
  e.weight = 0.75f;
  e.properties_json = R"({"since":2020})";

  auto e2 = GraphSerializer::DeserializeEdge(GraphSerializer::SerializeEdge(e));
  CHECK(e2 == e, "Edge roundtrip: basic");
  PASS("Edge serializer roundtrip (basic)");
  return true;
}

bool test_edge_serializer_default_weight() {
  Edge e;
  e.src_id = "a";
  e.dst_id = "b";
  e.label = "LINK";
  // weight defaults to 1.0f
  e.properties_json = "";

  auto e2 = GraphSerializer::DeserializeEdge(GraphSerializer::SerializeEdge(e));
  CHECK(e2 == e, "Edge roundtrip: default weight");
  CHECK(std::fabs(e2.weight - 1.0f) < 1e-6f, "Edge default weight preserved");
  PASS("Edge serializer roundtrip (default weight)");
  return true;
}

bool test_adjlist_serializer_roundtrip() {
  std::vector<std::string> ids = {"alice", "bob", "charlie"};
  auto json = GraphSerializer::SerializeAdjList(ids);
  auto ids2 = GraphSerializer::DeserializeAdjList(json);
  CHECK(ids == ids2, "AdjList roundtrip: basic");
  PASS("AdjList serializer roundtrip (basic)");
  return true;
}

bool test_adjlist_serializer_empty() {
  std::vector<std::string> ids;
  auto json = GraphSerializer::SerializeAdjList(ids);
  // [二进制格式] SerializeAdjList 使用紧凑二进制格式而非 JSON，
  // 空列表序列化为 [4B count=0] 共 4 字节，而非 JSON 的 "[]"
  CHECK(json.size() == 4, "AdjList empty serializes to 4 bytes (count=0)");
  auto ids2 = GraphSerializer::DeserializeAdjList(json);
  CHECK(ids2.empty(), "AdjList empty roundtrip");
  PASS("AdjList serializer roundtrip (empty)");
  return true;
}

bool test_adjlist_serializer_special_chars() {
  // IDs with quotes and backslashes
  std::vector<std::string> ids = {R"(id"1)", R"(id\2)"};
  auto ids2 = GraphSerializer::DeserializeAdjList(
      GraphSerializer::SerializeAdjList(ids));
  CHECK(ids == ids2, "AdjList roundtrip: special chars");
  PASS("AdjList serializer roundtrip (special chars)");
  return true;
}

// ── GraphStore Node CRUD 测试
// ─────────────────────────────────────────────────

static std::shared_ptr<GraphKVStore> make_kv() {
  return std::make_shared<GraphKVStore>(1024, 4);
}

bool test_node_add_get() {
  auto kv = make_kv();
  GraphStore gs(kv);

  Node n{"node1", R"({"x":1})"};
  gs.AddNode(n);

  auto got = gs.GetNode("node1");
  CHECK(got.has_value(), "GetNode: should find added node");
  CHECK(*got == n, "GetNode: value matches");
  PASS("Node AddNode/GetNode");
  return true;
}

bool test_node_get_missing() {
  auto kv = make_kv();
  GraphStore gs(kv);

  auto got = gs.GetNode("nonexistent");
  CHECK(!got.has_value(), "GetNode: missing node returns nullopt");
  PASS("Node GetNode (missing)");
  return true;
}

bool test_node_update() {
  auto kv = make_kv();
  GraphStore gs(kv);

  Node n{"node1", R"({"x":1})"};
  gs.AddNode(n);

  Node updated{"node1", R"({"x":99})"};
  gs.UpdateNode(updated);

  auto got = gs.GetNode("node1");
  CHECK(got.has_value(), "UpdateNode: node still exists");
  CHECK(*got == updated, "UpdateNode: value updated");
  PASS("Node UpdateNode");
  return true;
}

bool test_node_delete() {
  auto kv = make_kv();
  GraphStore gs(kv);

  Node n{"node1", R"({})"};
  gs.AddNode(n);
  gs.DeleteNode("node1");

  auto got = gs.GetNode("node1");
  CHECK(!got.has_value(), "DeleteNode: node gone after delete");
  PASS("Node DeleteNode");
  return true;
}

bool test_node_key_escaping() {
  // node_id with ':' should not collide with other keys
  auto kv = make_kv();
  GraphStore gs(kv);

  Node n1{"a:b", R"({"id":"a:b"})"};
  Node n2{"a", R"({"id":"a"})"};
  gs.AddNode(n1);
  gs.AddNode(n2);

  auto got1 = gs.GetNode("a:b");
  auto got2 = gs.GetNode("a");
  CHECK(got1.has_value() && *got1 == n1, "Key escaping: a:b node correct");
  CHECK(got2.has_value() && *got2 == n2, "Key escaping: a node correct");
  PASS("Node key escaping (colon in node_id)");
  return true;
}

// ── GraphStore Edge CRUD 测试
// ─────────────────────────────────────────────────

bool test_edge_add_get() {
  auto kv = make_kv();
  GraphStore gs(kv);

  Edge e{"alice", "bob", "KNOWS", 0.9f, R"({"since":2021})"};
  gs.AddEdge(e);

  auto got = gs.GetEdge("alice", "bob", "KNOWS");
  CHECK(got.has_value(), "GetEdge: should find added edge");
  CHECK(*got == e, "GetEdge: value matches");
  PASS("Edge AddEdge/GetEdge");
  return true;
}

bool test_edge_get_missing() {
  auto kv = make_kv();
  GraphStore gs(kv);

  auto got = gs.GetEdge("x", "y", "LABEL");
  CHECK(!got.has_value(), "GetEdge: missing edge returns nullopt");
  PASS("Edge GetEdge (missing)");
  return true;
}

bool test_edge_delete() {
  auto kv = make_kv();
  GraphStore gs(kv);

  Edge e{"alice", "bob", "KNOWS", 1.0f, ""};
  gs.AddEdge(e);
  gs.DeleteEdge("alice", "bob", "KNOWS");

  auto got = gs.GetEdge("alice", "bob", "KNOWS");
  CHECK(!got.has_value(), "DeleteEdge: edge gone after delete");
  PASS("Edge DeleteEdge");
  return true;
}

bool test_edge_multi_label() {
  // Same src/dst, different labels → independent edges
  auto kv = make_kv();
  GraphStore gs(kv);

  Edge e1{"a", "b", "KNOWS", 1.0f, ""};
  Edge e2{"a", "b", "LIKES", 0.5f, ""};
  gs.AddEdge(e1);
  gs.AddEdge(e2);

  auto got1 = gs.GetEdge("a", "b", "KNOWS");
  auto got2 = gs.GetEdge("a", "b", "LIKES");
  CHECK(got1.has_value() && *got1 == e1, "Multi-label: KNOWS edge correct");
  CHECK(got2.has_value() && *got2 == e2, "Multi-label: LIKES edge correct");
  PASS("Edge multi-label independence");
  return true;
}

bool test_edge_label_escaping() {
  // label with ':' should not break key parsing
  auto kv = make_kv();
  GraphStore gs(kv);

  Edge e{"src", "dst", "TYPE:A", 1.0f, ""};
  gs.AddEdge(e);

  auto got = gs.GetEdge("src", "dst", "TYPE:A");
  CHECK(got.has_value() && *got == e, "Edge label escaping: TYPE:A");
  PASS("Edge label key escaping (colon in label)");
  return true;
}

// ── Arbitrary 生成器（供 rapidcheck 使用）────────────────────────────────────

namespace rc {

// Node 生成器：node_id 和 properties_json 可以是任意字符串（含 ':'）
template <> struct Arbitrary<Node> {
  static Gen<Node> arbitrary() {
    return gen::build<Node>(
        gen::set(&Node::node_id, gen::arbitrary<std::string>()),
        gen::set(&Node::properties_json, gen::arbitrary<std::string>()));
  }
};

// Edge 生成器：weight 过滤掉 NaN/Inf（NaN != NaN，无法做往返相等断言）
template <> struct Arbitrary<Edge> {
  static Gen<Edge> arbitrary() {
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

// ── Property-Based Tests
// ──────────────────────────────────────────────────────

/**
 * Property 5: Node CRUD 往返
 *
 * 对任意合法的 Node n：
 *   AddNode(n); GetNode(n.node_id) 应返回与 n 等价的对象
 *
 * 对任意未添加的 node_id（与 n.node_id 不同）：
 *   GetNode(other_id) 应返回 nullopt
 *
 * Validates: Requirements 2.1, 2.2, 2.3
 */
static bool run_property5() {
  // Feature: graph-on-kv, Property 5: Node CRUD 往返
  bool p5a =
      rc::check("Property 5a: AddNode(n); GetNode(id) == n", [](const Node &n) {
        auto kv = std::make_shared<GraphKVStore>(1024, 4);
        GraphStore gs(kv);
        gs.AddNode(n);
        auto got = gs.GetNode(n.node_id);
        RC_ASSERT(got.has_value());
        RC_ASSERT(*got == n);
      });

  // 未添加的 node_id 返回 nullopt
  // 生成两个不同的 node_id，只添加其中一个
  bool p5b = rc::check("Property 5b: GetNode(未添加的 id) == nullopt",
                       [](const Node &n, const std::string &other_id) {
                         RC_PRE(n.node_id !=
                                other_id); // 确保 other_id 与 n.node_id 不同
                         auto kv = std::make_shared<GraphKVStore>(1024, 4);
                         GraphStore gs(kv);
                         gs.AddNode(n);
                         auto got = gs.GetNode(other_id);
                         RC_ASSERT(!got.has_value());
                       });

  return p5a && p5b;
}

/**
 * Property 6: Edge CRUD 往返
 *
 * 对任意合法的 Edge e：
 *   AddEdge(e); GetEdge(src, dst, label) 应返回与 e 等价的对象
 *
 * 对任意未添加的边三元组（与 e 的三元组不完全相同）：
 *   GetEdge(other_src, other_dst, other_label) 应返回 nullopt
 *
 * Validates: Requirements 3.1, 3.2, 3.3
 */
static bool run_property6() {
  // Feature: graph-on-kv, Property 6: Edge CRUD 往返
  bool p6a = rc::check("Property 6a: AddEdge(e); GetEdge(src,dst,label) == e",
                       [](const Edge &e) {
                         auto kv = std::make_shared<GraphKVStore>(1024, 4);
                         GraphStore gs(kv);
                         gs.AddEdge(e);
                         auto got = gs.GetEdge(e.src_id, e.dst_id, e.label);
                         RC_ASSERT(got.has_value());
                         RC_ASSERT(*got == e);
                       });

  // 未添加的边三元组返回 nullopt
  // 通过改变 label 来保证三元组不同（最简单的方式）
  bool p6b = rc::check("Property 6b: GetEdge(未添加的三元组) == nullopt",
                       [](const Edge &e, const std::string &other_label) {
                         // 确保 other_label 与 e.label 不同，从而三元组不同
                         RC_PRE(other_label != e.label);
                         auto kv = std::make_shared<GraphKVStore>(1024, 4);
                         GraphStore gs(kv);
                         gs.AddEdge(e);
                         // 查询相同 src/dst 但不同 label 的边，应返回 nullopt
                         auto got = gs.GetEdge(e.src_id, e.dst_id, other_label);
                         RC_ASSERT(!got.has_value());
                       });

  return p6a && p6b;
}

// ── main
// ──────────────────────────────────────────────────────────────────────

int main() {
  std::cout << "=== Phase 1 Unit Tests ===\n\n";

  int passed = 0, failed = 0;

  auto run = [&](bool (*fn)(), const char *name) {
    try {
      if (fn())
        ++passed;
      else
        ++failed;
    } catch (const std::exception &ex) {
      std::cerr << "[FAIL] " << name << " threw: " << ex.what() << "\n";
      ++failed;
    }
  };

  // Serializer tests
  run(test_node_serializer_roundtrip, "node_serializer_roundtrip");
  run(test_node_serializer_empty_props, "node_serializer_empty_props");
  run(test_node_serializer_special_chars, "node_serializer_special_chars");
  run(test_edge_serializer_roundtrip, "edge_serializer_roundtrip");
  run(test_edge_serializer_default_weight, "edge_serializer_default_weight");
  run(test_adjlist_serializer_roundtrip, "adjlist_serializer_roundtrip");
  run(test_adjlist_serializer_empty, "adjlist_serializer_empty");
  run(test_adjlist_serializer_special_chars,
      "adjlist_serializer_special_chars");

  // GraphStore Node CRUD
  run(test_node_add_get, "node_add_get");
  run(test_node_get_missing, "node_get_missing");
  run(test_node_update, "node_update");
  run(test_node_delete, "node_delete");
  run(test_node_key_escaping, "node_key_escaping");

  // GraphStore Edge CRUD
  run(test_edge_add_get, "edge_add_get");
  run(test_edge_get_missing, "edge_get_missing");
  run(test_edge_delete, "edge_delete");
  run(test_edge_multi_label, "edge_multi_label");
  run(test_edge_label_escaping, "edge_label_escaping");

  std::cout << "\n=== Unit Test Results: " << passed << " passed, " << failed
            << " failed ===\n";

  // ── Property-Based Tests ──────────────────────────────────────────────────
  std::cout << "\n=== Phase 1 Property-Based Tests ===\n\n";

  int pbt_failed = 0;

  // Property 5: Node CRUD 往返 (Validates: Requirements 2.1, 2.2, 2.3)
  if (!run_property5())
    ++pbt_failed;

  // Property 6: Edge CRUD 往返 (Validates: Requirements 3.1, 3.2, 3.3)
  if (!run_property6())
    ++pbt_failed;

  if (pbt_failed == 0) {
    std::cout << "\n[PASS] All property-based tests passed.\n";
  } else {
    std::cerr << "\n[FAIL] " << pbt_failed << " property test(s) failed.\n";
  }

  int total_failed = failed + pbt_failed;
  std::cout << "\n=== Total: " << (passed) << " unit tests, "
            << (2 - pbt_failed) << "/2 PBT suites passed ===\n";
  return total_failed == 0 ? 0 : 1;
}
