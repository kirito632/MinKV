/**
 * Phase 2 测试：邻接表一致性 Property-Based Tests
 *
 * Property 7：邻接表一致性不变量
 *   任意边 (src,dst,label) 存在时：
 *     dst ∈ GetOutNeighbors(src)
 *     src ∈ GetInNeighbors(dst)
 *   DeleteEdge 后若 (src,dst) 间无其他边：
 *     dst ∉ GetOutNeighbors(src)
 *     src ∉ GetInNeighbors(dst)
 *   Validates: Requirements 4.1, 4.2, 4.3
 *
 * Property 8：DeleteNode 完整性
 *   DeleteNode 后 GetNode / GetNodeEmbedding /
 *   GetOutNeighbors / GetInNeighbors 均返回空/nullopt
 *   Validates: Requirements 2.5, 7.4
 *
 * 单元测试（边界情况）：
 *   - 多 label 边去重：(src,dst) 有两条不同 label 的边时，邻接表只记录一条 dst
 *   - 最后一条边删除后邻接表条目消失
 *   - DeleteNode 级联清理邻居的邻接表
 *   - 无出/入边节点返回空列表
 */

#include "core/sharded_cache.h"
#include "graph/graph_store.h"
#include "graph/graph_types.h"

#include <rapidcheck.h>

#include <algorithm>
#include <iostream>
#include <string>
#include <vector>

using namespace minkv::graph;
using GraphKVStore = minkv::db::ShardedCache<std::string, std::string>;

// ── 辅助
// ──────────────────────────────────────────────────────────────────────

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

static std::shared_ptr<GraphKVStore> make_kv() {
  return std::make_shared<GraphKVStore>(1024, 4);
}

static bool contains(const std::vector<std::string> &v, const std::string &s) {
  return std::find(v.begin(), v.end(), s) != v.end();
}

// ── 单元测试
// ──────────────────────────────────────────────────────────────────

// AddEdge 后邻接表包含对应条目
bool test_adj_add_edge_updates_adj() {
  auto kv = make_kv();
  GraphStore gs(kv);

  gs.AddEdge({"alice", "bob", "KNOWS", 1.0f, ""});

  CHECK(contains(gs.GetOutNeighbors("alice"), "bob"),
        "AddEdge: bob in alice's out-neighbors");
  CHECK(contains(gs.GetInNeighbors("bob"), "alice"),
        "AddEdge: alice in bob's in-neighbors");
  PASS("AddEdge updates adjacency lists");
  return true;
}

// DeleteEdge 后邻接表条目消失（单条边）
bool test_adj_delete_edge_removes_adj() {
  auto kv = make_kv();
  GraphStore gs(kv);

  gs.AddEdge({"alice", "bob", "KNOWS", 1.0f, ""});
  gs.DeleteEdge("alice", "bob", "KNOWS");

  CHECK(!contains(gs.GetOutNeighbors("alice"), "bob"),
        "DeleteEdge: bob removed from alice's out-neighbors");
  CHECK(!contains(gs.GetInNeighbors("bob"), "alice"),
        "DeleteEdge: alice removed from bob's in-neighbors");
  PASS("DeleteEdge removes adjacency entries");
  return true;
}

// 多 label 边：邻接表去重，只有最后一条边删除后才移除条目
bool test_adj_multi_label_dedup() {
  auto kv = make_kv();
  GraphStore gs(kv);

  gs.AddEdge({"a", "b", "KNOWS", 1.0f, ""});
  gs.AddEdge({"a", "b", "LIKES", 0.5f, ""});

  // 邻接表中 b 只出现一次
  auto out = gs.GetOutNeighbors("a");
  int count = (int)std::count(out.begin(), out.end(), "b");
  CHECK(count == 1, "Multi-label: b appears exactly once in out-neighbors");

  // 删除一条边后，另一条还在，邻接表保留
  gs.DeleteEdge("a", "b", "KNOWS");
  CHECK(contains(gs.GetOutNeighbors("a"), "b"),
        "Multi-label: b still in out-neighbors after deleting one label");

  // 删除最后一条边后，邻接表条目消失
  gs.DeleteEdge("a", "b", "LIKES");
  CHECK(!contains(gs.GetOutNeighbors("a"), "b"),
        "Multi-label: b removed after all labels deleted");
  CHECK(
      !contains(gs.GetInNeighbors("b"), "a"),
      "Multi-label: a removed from b's in-neighbors after all labels deleted");
  PASS("Multi-label edge deduplication in adjacency list");
  return true;
}

// 无出/入边节点返回空列表
bool test_adj_empty_neighbors() {
  auto kv = make_kv();
  GraphStore gs(kv);

  gs.AddNode({"isolated", "{}"});

  CHECK(gs.GetOutNeighbors("isolated").empty(),
        "Isolated node: out-neighbors is empty");
  CHECK(gs.GetInNeighbors("isolated").empty(),
        "Isolated node: in-neighbors is empty");
  PASS("Isolated node returns empty neighbor lists");
  return true;
}

// DeleteNode 级联清理：邻居的邻接表中不再包含被删节点
bool test_delete_node_cascades_adj() {
  auto kv = make_kv();
  GraphStore gs(kv);

  // alice -> bob -> charlie
  gs.AddNode({"alice", "{}"});
  gs.AddNode({"bob", "{}"});
  gs.AddNode({"charlie", "{}"});
  gs.AddEdge({"alice", "bob", "KNOWS", 1.0f, ""});
  gs.AddEdge({"bob", "charlie", "KNOWS", 1.0f, ""});

  gs.DeleteNode("bob");

  // bob 的节点数据消失
  CHECK(!gs.GetNode("bob").has_value(), "DeleteNode: bob's node data gone");
  // alice 的出边邻接表中不再有 bob
  CHECK(!contains(gs.GetOutNeighbors("alice"), "bob"),
        "DeleteNode: bob removed from alice's out-neighbors");
  // charlie 的入边邻接表中不再有 bob
  CHECK(!contains(gs.GetInNeighbors("charlie"), "bob"),
        "DeleteNode: bob removed from charlie's in-neighbors");
  PASS("DeleteNode cascades adjacency cleanup");
  return true;
}

// DeleteNode 同时删除 embedding
bool test_delete_node_removes_embedding() {
  auto kv = make_kv();
  GraphStore gs(kv);

  gs.AddNode({"node1", "{}"});
  gs.SetNodeEmbedding("node1", {1.0f, 2.0f, 3.0f});
  gs.DeleteNode("node1");

  CHECK(gs.GetNodeEmbedding("node1").empty(), "DeleteNode: embedding removed");
  PASS("DeleteNode removes embedding");
  return true;
}

// ── Arbitrary 生成器
// ──────────────────────────────────────────────────────────

namespace rc {

template <> struct Arbitrary<Node> {
  static Gen<Node> arbitrary() {
    return gen::build<Node>(
        gen::set(&Node::node_id, gen::arbitrary<std::string>()),
        gen::set(&Node::properties_json, gen::arbitrary<std::string>()));
  }
};

template <> struct Arbitrary<Edge> {
  static Gen<Edge> arbitrary() {
    return gen::build<Edge>(
        gen::set(&Edge::src_id, gen::arbitrary<std::string>()),
        gen::set(&Edge::dst_id, gen::arbitrary<std::string>()),
        gen::set(&Edge::label, gen::arbitrary<std::string>()),
        gen::set(&Edge::weight, gen::just(1.0f)),
        gen::set(&Edge::properties_json, gen::just(std::string(""))));
  }
};

} // namespace rc

// ── Property-Based Tests
// ──────────────────────────────────────────────────────

/**
 * Property 7：邻接表一致性不变量
 *
 * 对任意边 (src, dst, label)：
 *   AddEdge 后：dst ∈ GetOutNeighbors(src) 且 src ∈ GetInNeighbors(dst)
 *   DeleteEdge 后（无其他边）：dst ∉ GetOutNeighbors(src) 且 src ∉
 * GetInNeighbors(dst)
 *
 * Validates: Requirements 4.1, 4.2, 4.3
 */
static bool run_property7() {
  // Feature: graph-on-kv, Property 7: 邻接表一致性不变量
  bool p7a =
      rc::check("Property 7a: AddEdge 后邻接表包含对应条目", [](const Edge &e) {
        auto kv = std::make_shared<GraphKVStore>(1024, 4);
        GraphStore gs(kv);
        gs.AddEdge(e);

        auto out = gs.GetOutNeighbors(e.src_id);
        auto in = gs.GetInNeighbors(e.dst_id);

        RC_ASSERT(std::find(out.begin(), out.end(), e.dst_id) != out.end());
        RC_ASSERT(std::find(in.begin(), in.end(), e.src_id) != in.end());
      });

  // DeleteEdge 后（单条边），邻接表条目消失
  bool p7b = rc::check(
      "Property 7b: DeleteEdge 后（无其他边）邻接表条目消失",
      [](const Edge &e) {
        auto kv = std::make_shared<GraphKVStore>(1024, 4);
        GraphStore gs(kv);
        gs.AddEdge(e);
        gs.DeleteEdge(e.src_id, e.dst_id, e.label);

        auto out = gs.GetOutNeighbors(e.src_id);
        auto in = gs.GetInNeighbors(e.dst_id);

        RC_ASSERT(std::find(out.begin(), out.end(), e.dst_id) == out.end());
        RC_ASSERT(std::find(in.begin(), in.end(), e.src_id) == in.end());
      });

  // 多条边：只要 (src,dst) 间还有其他 label 的边，邻接表条目保留
  bool p7c = rc::check(
      "Property 7c: 多 label 边删一条后邻接表条目保留",
      [](const Edge &e, const std::string &other_label) {
        RC_PRE(other_label != e.label); // 确保两条边 label 不同

        auto kv = std::make_shared<GraphKVStore>(1024, 4);
        GraphStore gs(kv);

        Edge e2 = e;
        e2.label = other_label;

        gs.AddEdge(e);
        gs.AddEdge(e2);

        // 删除其中一条
        gs.DeleteEdge(e.src_id, e.dst_id, e.label);

        // 另一条还在，邻接表应保留
        auto out = gs.GetOutNeighbors(e.src_id);
        auto in = gs.GetInNeighbors(e.dst_id);

        RC_ASSERT(std::find(out.begin(), out.end(), e.dst_id) != out.end());
        RC_ASSERT(std::find(in.begin(), in.end(), e.src_id) != in.end());
      });

  return p7a && p7b && p7c;
}

/**
 * Property 8：DeleteNode 完整性
 *
 * 对任意已存在的节点（含邻接表和 embedding），DeleteNode 后：
 *   GetNode            → nullopt
 *   GetNodeEmbedding   → 空 vector
 *   GetOutNeighbors    → 空列表
 *   GetInNeighbors     → 空列表
 *
 * Validates: Requirements 2.5, 7.4
 */
static bool run_property8() {
  // Feature: graph-on-kv, Property 8: DeleteNode 完整性
  bool p8a = rc::check("Property 8a: DeleteNode 后 GetNode 返回 nullopt",
                       [](const Node &n) {
                         auto kv = std::make_shared<GraphKVStore>(1024, 4);
                         GraphStore gs(kv);
                         gs.AddNode(n);
                         gs.DeleteNode(n.node_id);
                         RC_ASSERT(!gs.GetNode(n.node_id).has_value());
                       });

  bool p8b = rc::check("Property 8b: DeleteNode 后 GetNodeEmbedding 返回空",
                       [](const Node &n) {
                         auto kv = std::make_shared<GraphKVStore>(1024, 4);
                         GraphStore gs(kv);
                         gs.AddNode(n);
                         gs.SetNodeEmbedding(n.node_id, {1.0f, 2.0f, 3.0f});
                         gs.DeleteNode(n.node_id);
                         RC_ASSERT(gs.GetNodeEmbedding(n.node_id).empty());
                       });

  bool p8c = rc::check(
      "Property 8c: DeleteNode 后 GetOutNeighbors/GetInNeighbors 返回空",
      [](const Node &n, const std::string &neighbor_id) {
        RC_PRE(n.node_id != neighbor_id);

        auto kv = std::make_shared<GraphKVStore>(1024, 4);
        GraphStore gs(kv);

        gs.AddNode(n);
        gs.AddNode({neighbor_id, "{}"});

        // n -> neighbor (out-edge) and neighbor -> n (in-edge)
        gs.AddEdge({n.node_id, neighbor_id, "LINK", 1.0f, ""});
        gs.AddEdge({neighbor_id, n.node_id, "BACK", 1.0f, ""});

        gs.DeleteNode(n.node_id);

        RC_ASSERT(gs.GetOutNeighbors(n.node_id).empty());
        RC_ASSERT(gs.GetInNeighbors(n.node_id).empty());

        // 邻居的邻接表中也不再包含被删节点
        auto neighbor_out = gs.GetOutNeighbors(neighbor_id);
        auto neighbor_in = gs.GetInNeighbors(neighbor_id);
        RC_ASSERT(std::find(neighbor_out.begin(), neighbor_out.end(),
                            n.node_id) == neighbor_out.end());
        RC_ASSERT(std::find(neighbor_in.begin(), neighbor_in.end(),
                            n.node_id) == neighbor_in.end());
      });

  return p8a && p8b && p8c;
}

// ── Property 14: RebuildAdjacencyList 修复一致性 ─────────────────────────────

/**
 * Property 14：RebuildAdjacencyList 修复一致性
 *
 * 随机删除部分邻接表条目后调用 RebuildAdjacencyList()，Property 7 重新成立：
 *   对所有存在的边 (src, dst, label)：
 *     dst ∈ GetOutNeighbors(src)
 *     src ∈ GetInNeighbors(dst)
 *
 * Feature: graph-on-kv, Property 14: RebuildAdjacencyList 修复一致性
 * Validates: Requirement 10.4
 */
static bool run_property14() {
  // Feature: graph-on-kv, Property 14: RebuildAdjacencyList 修复一致性
  // Validates: Requirement 10.4
  return rc::check(
      "Property 14: RebuildAdjacencyList 后 Property 7 重新成立",
      [](const std::vector<Edge> &edges,
         const std::vector<bool> &corrupt_flags) {
        RC_PRE(!edges.empty());

        auto kv = std::make_shared<GraphKVStore>(1024, 4);
        GraphStore gs(kv);

        // Step 1: 添加所有边（同时建立邻接表）
        for (const auto &e : edges) {
          gs.AddEdge(e);
        }

        // Step 2: 随机破坏部分邻接表条目
        // 收集所有涉及的节点 ID，然后按 corrupt_flags 决定是否删除其邻接表
        std::vector<std::string> node_ids;
        for (const auto &e : edges) {
          node_ids.push_back(e.src_id);
          node_ids.push_back(e.dst_id);
        }
        // 去重
        std::sort(node_ids.begin(), node_ids.end());
        node_ids.erase(std::unique(node_ids.begin(), node_ids.end()),
                       node_ids.end());

        // 通过共享的 kv 指针直接删除 adj:out: 和 adj:in: 键来模拟崩溃后的不一致
        for (size_t i = 0; i < node_ids.size(); ++i) {
          bool should_corrupt = corrupt_flags.empty()
                                    ? (i % 2 == 0)
                                    : corrupt_flags[i % corrupt_flags.size()];

          if (should_corrupt) {
            // 直接通过 kv 删除邻接表键，模拟崩溃导致的邻接表丢失
            kv->remove("adj:out:" + node_ids[i]);
            kv->remove("adj:in:" + node_ids[i]);
          }
        }

        // Step 3: 调用 RebuildAdjacencyList 修复
        gs.RebuildAdjacencyList();

        // Step 4: 验证 Property 7 重新成立
        // 对所有添加的边，邻接表一致性不变量必须成立
        for (const auto &e : edges) {
          // 边数据仍然存在（RebuildAdjacencyList 不删边）
          auto got = gs.GetEdge(e.src_id, e.dst_id, e.label);
          RC_ASSERT(got.has_value());

          auto out = gs.GetOutNeighbors(e.src_id);
          auto in = gs.GetInNeighbors(e.dst_id);

          RC_ASSERT(std::find(out.begin(), out.end(), e.dst_id) != out.end());
          RC_ASSERT(std::find(in.begin(), in.end(), e.src_id) != in.end());
        }
      });
}

// ── main
// ──────────────────────────────────────────────────────────────────────

int main() {
  std::cout << "=== Phase 2 Unit Tests ===\n\n";

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

  run(test_adj_add_edge_updates_adj, "adj_add_edge_updates_adj");
  run(test_adj_delete_edge_removes_adj, "adj_delete_edge_removes_adj");
  run(test_adj_multi_label_dedup, "adj_multi_label_dedup");
  run(test_adj_empty_neighbors, "adj_empty_neighbors");
  run(test_delete_node_cascades_adj, "delete_node_cascades_adj");
  run(test_delete_node_removes_embedding, "delete_node_removes_embedding");

  std::cout << "\n=== Unit Test Results: " << passed << " passed, " << failed
            << " failed ===\n";

  // ── Property-Based Tests ──────────────────────────────────────────────────
  std::cout << "\n=== Phase 2 Property-Based Tests ===\n\n";

  int pbt_failed = 0;

  // Property 7: 邻接表一致性不变量 (Validates: Requirements 4.1, 4.2, 4.3)
  if (!run_property7())
    ++pbt_failed;

  // Property 8: DeleteNode 完整性 (Validates: Requirements 2.5, 7.4)
  if (!run_property8())
    ++pbt_failed;

  // Property 14: RebuildAdjacencyList 修复一致性 (Validates: Requirement 10.4)
  if (!run_property14())
    ++pbt_failed;

  if (pbt_failed == 0) {
    std::cout << "\n[PASS] All property-based tests passed.\n";
  } else {
    std::cerr << "\n[FAIL] " << pbt_failed
              << " property test suite(s) failed.\n";
  }

  int total_failed = failed + pbt_failed;
  std::cout << "\n=== Total: " << passed << " unit tests passed, "
            << (3 - pbt_failed) << "/3 PBT suites passed ===\n";
  return total_failed == 0 ? 0 : 1;
}
