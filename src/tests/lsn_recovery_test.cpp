/**
 * @file lsn_recovery_test.cpp
 * @brief WAL LSN 改造集成测试
 *
 * 覆盖范围：
 * 1. LSN 单调递增验证
 * 2. WAL 序列化/反序列化往返（lsn 字段）
 * 3. read_after_snapshot 按 LSN 过滤
 * 4. 崩溃恢复流程：写入 → checkpoint → 再写入 → recover → 数据完整
 * 5. 回归：基础 put/get/remove 不受影响
 * 6. stop_background_fsync 快速退出（条件变量）
 */

#include "../core/sharded_cache.h"
#include "../persistence/checkpoint_manager.h"
#include <cassert>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>

namespace fs = std::filesystem;
using Cache = minkv::db::ShardedCache<std::string, std::string>;
using CheckpointMgr =
    minkv::db::SimpleCheckpointManager<std::string, std::string>;

// ─── 工具 ────────────────────────────────────────────────────────────────────

static void cleanup_dir(const std::string &dir) { fs::remove_all(dir); }

static void pass(const std::string &name) {
  std::cout << "  [PASS] " << name << "\n";
}

static void fail(const std::string &name, const std::string &reason) {
  std::cerr << "  [FAIL] " << name << ": " << reason << "\n";
  std::exit(1);
}

// ─── Test 1: LSN 单调递增
// ─────────────────────────────────────────────────────

void test_lsn_monotonic() {
  std::cout << "\n[Test 1] LSN 单调递增\n";
  const std::string dir = "/tmp/minkv_test_lsn_mono";
  cleanup_dir(dir);

  Cache cache(100);
  cache.enable_persistence(dir, 0); // 同步刷盘

  uint64_t prev = 0;
  for (int i = 0; i < 100; ++i) {
    cache.put("k" + std::to_string(i), "v" + std::to_string(i));
    uint64_t cur = cache.current_lsn();
    if (cur <= prev && i > 0) {
      fail("lsn_monotonic", "LSN 未单调递增: prev=" + std::to_string(prev) +
                                " cur=" + std::to_string(cur));
    }
    prev = cur;
  }

  // 读取 WAL 验证文件中 lsn 也单调递增
  auto entries = cache.read_wal_after_lsn(0);
  if (entries.size() != 100)
    fail("lsn_monotonic", "WAL 条目数不对: " + std::to_string(entries.size()));

  uint64_t last_lsn = 0;
  for (const auto &e : entries) {
    if (e.lsn <= last_lsn)
      fail("lsn_monotonic", "WAL 中 lsn 未单调递增");
    last_lsn = e.lsn;
  }

  cleanup_dir(dir);
  pass("LSN 单调递增");
}

// ─── Test 2: 序列化/反序列化往返
// ──────────────────────────────────────────────

void test_serialization_roundtrip() {
  std::cout << "\n[Test 2] 序列化/反序列化往返\n";
  const std::string dir = "/tmp/minkv_test_ser";
  cleanup_dir(dir);

  Cache cache(100);
  cache.enable_persistence(dir, 0);

  // 写入几条带特殊字符的 key/value
  cache.put("hello", "world");
  cache.put("key with spaces", "value\nwith\nnewlines");
  cache.put("empty_val", "");
  cache.remove("hello");

  auto entries = cache.read_wal_after_lsn(0);
  if (entries.size() != 4)
    fail("serialization_roundtrip",
         "条目数不对: " + std::to_string(entries.size()));

  // 验证 lsn 字段被正确读回
  for (const auto &e : entries) {
    if (e.lsn == 0)
      fail("serialization_roundtrip", "lsn 读回为 0");
  }

  // 验证 key/value 内容
  if (entries[0].key != "hello" || entries[0].value != "world")
    fail("serialization_roundtrip", "第1条 key/value 不匹配");
  if (entries[2].key != "empty_val" || entries[2].value != "")
    fail("serialization_roundtrip", "空 value 不匹配");
  if (entries[3].op != minkv::db::LogEntry::DELETE)
    fail("serialization_roundtrip", "DELETE 操作类型不对");

  cleanup_dir(dir);
  pass("序列化/反序列化往返");
}

// ─── Test 3: read_after_snapshot 按 LSN 过滤 ─────────────────────────────────

void test_read_after_snapshot_filter() {
  std::cout << "\n[Test 3] read_after_snapshot 按 LSN 过滤\n";
  const std::string dir = "/tmp/minkv_test_filter";
  cleanup_dir(dir);

  Cache cache(100);
  cache.enable_persistence(dir, 0);

  // 写入 10 条
  for (int i = 0; i < 10; ++i)
    cache.put("k" + std::to_string(i), "v" + std::to_string(i));

  auto all = cache.read_wal_after_lsn(0);
  if (all.size() != 10)
    fail("read_after_snapshot_filter", "全量读取数量不对");

  // 取第 5 条的 lsn 作为 snapshot_lsn
  uint64_t snap_lsn = all[4].lsn;
  auto after = cache.read_wal_after_lsn(snap_lsn);

  if (after.size() != 5)
    fail("read_after_snapshot_filter",
         "过滤后数量不对: expected=5 got=" + std::to_string(after.size()));

  for (const auto &e : after) {
    if (e.lsn <= snap_lsn)
      fail("read_after_snapshot_filter", "返回了 lsn <= snap_lsn 的条目");
  }

  cleanup_dir(dir);
  pass("read_after_snapshot 按 LSN 过滤");
}

// ─── Test 4: 崩溃恢复流程 ────────────────────────────────────────────────────

void test_crash_recovery() {
  std::cout << "\n[Test 4] 崩溃恢复流程\n";
  const std::string dir = "/tmp/minkv_test_recovery";
  cleanup_dir(dir);

  // Phase A：写入 50 条，做 checkpoint，再写入 50 条
  {
    Cache cache(1000);
    cache.enable_persistence(dir, 0);

    CheckpointMgr::CheckpointConfig cfg;
    cfg.data_dir = dir;
    CheckpointMgr ckpt(&cache, cfg);

    for (int i = 0; i < 50; ++i)
      cache.put("k" + std::to_string(i), "before_ckpt_" + std::to_string(i));

    if (!ckpt.checkpoint_now())
      fail("crash_recovery", "checkpoint_now 失败");

    for (int i = 50; i < 100; ++i)
      cache.put("k" + std::to_string(i), "after_ckpt_" + std::to_string(i));

    // 模拟崩溃：不调用析构前的 flush，直接让 cache 析构
    // （enable_persistence 的析构会 flush，这里测的是 recover 逻辑正确性）
  }

  // Phase B：新建 cache，从磁盘恢复
  {
    Cache cache(1000);
    cache.enable_persistence(dir, 0);

    CheckpointMgr::CheckpointConfig cfg;
    cfg.data_dir = dir;
    CheckpointMgr ckpt(&cache, cfg);

    if (!ckpt.recover_from_disk())
      fail("crash_recovery", "recover_from_disk 失败");

    // 验证 100 条全部恢复
    for (int i = 0; i < 50; ++i) {
      auto v = cache.get("k" + std::to_string(i));
      if (!v.has_value())
        fail("crash_recovery", "快照数据丢失: k" + std::to_string(i));
      if (v.value() != "before_ckpt_" + std::to_string(i))
        fail("crash_recovery", "快照数据值错误: k" + std::to_string(i));
    }
    for (int i = 50; i < 100; ++i) {
      auto v = cache.get("k" + std::to_string(i));
      if (!v.has_value())
        fail("crash_recovery", "WAL 重放数据丢失: k" + std::to_string(i));
      if (v.value() != "after_ckpt_" + std::to_string(i))
        fail("crash_recovery", "WAL 重放数据值错误: k" + std::to_string(i));
    }

    // 验证 reset_lsn 生效：恢复后新写入的 lsn 应大于所有已恢复条目
    uint64_t lsn_before = cache.current_lsn();
    cache.put("new_key", "new_val");
    uint64_t lsn_after = cache.current_lsn();
    if (lsn_after <= lsn_before)
      fail("crash_recovery", "恢复后 LSN 未正确递增");
  }

  cleanup_dir(dir);
  pass("崩溃恢复流程（快照 + WAL 重放 + reset_lsn）");
}

// ─── Test 5: 回归 - 基础 put/get/remove ──────────────────────────────────────

void test_basic_regression() {
  std::cout << "\n[Test 5] 回归：基础 put/get/remove\n";
  const std::string dir = "/tmp/minkv_test_regression";
  cleanup_dir(dir);

  Cache cache(100);
  cache.enable_persistence(dir, 0);

  cache.put("a", "1");
  cache.put("b", "2");
  cache.put("c", "3");

  auto va = cache.get("a");
  if (!va || va.value() != "1")
    fail("basic_regression", "get a 失败");

  cache.put("a", "updated");
  auto va2 = cache.get("a");
  if (!va2 || va2.value() != "updated")
    fail("basic_regression", "update a 失败");

  bool removed = cache.remove("b");
  if (!removed)
    fail("basic_regression", "remove b 失败");
  auto vb = cache.get("b");
  if (vb.has_value())
    fail("basic_regression", "remove 后仍能 get b");

  if (cache.size() != 2)
    fail("basic_regression", "size 不对: " + std::to_string(cache.size()));

  cleanup_dir(dir);
  pass("基础 put/get/remove 回归");
}

// ─── Test 6: stop_background_fsync 快速退出 ──────────────────────────────────

void test_fast_stop() {
  std::cout << "\n[Test 6] stop_background_fsync 快速退出\n";
  const std::string dir = "/tmp/minkv_test_fast_stop";
  cleanup_dir(dir);

  Cache cache(100);
  // fsync_interval = 10000ms（10秒），验证 stop 不会等 10 秒
  cache.enable_persistence(dir, 10000);

  cache.put("x", "y");

  auto t0 = std::chrono::steady_clock::now();
  cache.disable_persistence(); // 内部调用 stop_background_fsync
  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - t0)
                     .count();

  if (elapsed > 500)
    fail("fast_stop",
         "stop 耗时过长: " + std::to_string(elapsed) + "ms (应 < 500ms)");

  cleanup_dir(dir);
  pass("stop_background_fsync 快速退出（" + std::to_string(elapsed) + "ms）");
}

// ─── Test 7: 无快照时全量 WAL 恢复 ───────────────────────────────────────────

void test_recovery_no_snapshot() {
  std::cout << "\n[Test 7] 无快照时全量 WAL 恢复\n";
  const std::string dir = "/tmp/minkv_test_no_snap";
  cleanup_dir(dir);

  {
    Cache cache(100);
    cache.enable_persistence(dir, 0);
    for (int i = 0; i < 20; ++i)
      cache.put("k" + std::to_string(i), "v" + std::to_string(i));
    // 不做 checkpoint，直接析构
  }

  {
    Cache cache(100);
    cache.enable_persistence(dir, 0);

    CheckpointMgr::CheckpointConfig cfg;
    cfg.data_dir = dir;
    CheckpointMgr ckpt(&cache, cfg);

    if (!ckpt.recover_from_disk())
      fail("recovery_no_snapshot", "recover_from_disk 失败");

    for (int i = 0; i < 20; ++i) {
      auto v = cache.get("k" + std::to_string(i));
      if (!v.has_value())
        fail("recovery_no_snapshot", "数据丢失: k" + std::to_string(i));
    }
  }

  cleanup_dir(dir);
  pass("无快照时全量 WAL 恢复");
}

// ─── main
// ─────────────────────────────────────────────────────────────────────

int main() {
  std::cout << "========================================\n";
  std::cout << "  WAL LSN Recovery Integration Tests\n";
  std::cout << "========================================\n";

  test_lsn_monotonic();
  test_serialization_roundtrip();
  test_read_after_snapshot_filter();
  test_crash_recovery();
  test_basic_regression();
  test_fast_stop();
  test_recovery_no_snapshot();

  std::cout << "\n========================================\n";
  std::cout << "  All tests PASSED\n";
  std::cout << "========================================\n";
  return 0;
}
