/**
 * @file lsn_simple_test.cpp
 * @brief LSN机制简化测试（不依赖WAL和持久化）
 *
 * 测试目标：
 * 1. LSN单调递增性
 * 2. LSN并发安全性
 * 3. current_lsn() vs next_lsn()
 */

#include <atomic>
#include <cassert>
#include <chrono>
#include <iostream>
#include <set>
#include <thread>
#include <vector>

// 简化的LSN测试类
class LSNCounter {
private:
  std::atomic<uint64_t> global_lsn_{1}; // 从1开始

public:
  uint64_t next_lsn() {
    return global_lsn_.fetch_add(1, std::memory_order_relaxed);
  }

  uint64_t current_lsn() const {
    uint64_t next = global_lsn_.load(std::memory_order_relaxed);
    return (next > 0) ? (next - 1) : 0;
  }
};

// 测试结果统计
struct TestResult {
  bool passed = true;
  std::string error_message;

  void fail(const std::string &msg) {
    passed = false;
    error_message = msg;
  }
};

/**
 * @brief 测试1: LSN基本单调递增性
 */
TestResult test_lsn_monotonic() {
  TestResult result;
  std::cout << "\n[Test 1] LSN Monotonic Increment Test" << std::endl;

  try {
    LSNCounter counter;

    // 获取100个LSN，验证单调递增
    std::vector<uint64_t> lsns;
    for (int i = 0; i < 100; ++i) {
      uint64_t lsn = counter.next_lsn();
      lsns.push_back(lsn);
    }

    // 验证单调递增
    for (size_t i = 1; i < lsns.size(); ++i) {
      if (lsns[i] <= lsns[i - 1]) {
        result.fail("LSN not monotonic: lsn[" + std::to_string(i - 1) +
                    "]=" + std::to_string(lsns[i - 1]) + ", lsn[" +
                    std::to_string(i) + "]=" + std::to_string(lsns[i]));
        return result;
      }
    }

    // 验证连续性（应该是1, 2, 3, ...）
    for (size_t i = 0; i < lsns.size(); ++i) {
      if (lsns[i] != i + 1) {
        result.fail("LSN not continuous: expected " + std::to_string(i + 1) +
                    ", got " + std::to_string(lsns[i]));
        return result;
      }
    }

    std::cout << "✅ LSN monotonic increment test PASSED" << std::endl;
    std::cout << "   Generated LSNs: 1 to " << lsns.back() << std::endl;

  } catch (const std::exception &e) {
    result.fail(std::string("Exception: ") + e.what());
  }

  return result;
}

/**
 * @brief 测试2: LSN并发安全性
 */
TestResult test_lsn_concurrent() {
  TestResult result;
  std::cout << "\n[Test 2] LSN Concurrent Safety Test" << std::endl;

  try {
    LSNCounter counter;

    const int num_threads = 10;
    const int lsns_per_thread = 1000;

    std::vector<std::thread> threads;
    std::vector<std::vector<uint64_t>> thread_lsns(num_threads);

    // 多线程并发获取LSN
    for (int t = 0; t < num_threads; ++t) {
      threads.emplace_back([&counter, &thread_lsns, t, lsns_per_thread]() {
        for (int i = 0; i < lsns_per_thread; ++i) {
          uint64_t lsn = counter.next_lsn();
          thread_lsns[t].push_back(lsn);
        }
      });
    }

    // 等待所有线程完成
    for (auto &thread : threads) {
      thread.join();
    }

    // 收集所有LSN到一个集合中
    std::set<uint64_t> all_lsns;
    for (const auto &lsns : thread_lsns) {
      for (uint64_t lsn : lsns) {
        all_lsns.insert(lsn);
      }
    }

    // 验证：所有LSN应该是唯一的
    size_t total_lsns = num_threads * lsns_per_thread;
    if (all_lsns.size() != total_lsns) {
      result.fail("LSN collision detected: expected " +
                  std::to_string(total_lsns) + " unique LSNs, got " +
                  std::to_string(all_lsns.size()));
      return result;
    }

    // 验证：LSN应该是连续的 1 到 total_lsns
    uint64_t expected_lsn = 1;
    for (uint64_t lsn : all_lsns) {
      if (lsn != expected_lsn) {
        result.fail("LSN gap detected: expected " +
                    std::to_string(expected_lsn) + ", got " +
                    std::to_string(lsn));
        return result;
      }
      expected_lsn++;
    }

    std::cout << "✅ LSN concurrent safety test PASSED" << std::endl;
    std::cout << "   " << num_threads << " threads generated " << total_lsns
              << " unique LSNs without collision" << std::endl;

  } catch (const std::exception &e) {
    result.fail(std::string("Exception: ") + e.what());
  }

  return result;
}

/**
 * @brief 测试3: LSN vs 时间戳对比测试
 */
TestResult test_lsn_vs_timestamp() {
  TestResult result;
  std::cout << "\n[Test 3] LSN vs Timestamp Comparison Test" << std::endl;

  try {
    LSNCounter counter;

    // 快速连续获取LSN和时间戳
    std::vector<uint64_t> lsns;
    std::vector<int64_t> timestamps;

    for (int i = 0; i < 1000; ++i) {
      lsns.push_back(counter.next_lsn());

      auto now = std::chrono::high_resolution_clock::now();
      auto duration = now.time_since_epoch();
      timestamps.push_back(
          std::chrono::duration_cast<std::chrono::milliseconds>(duration)
              .count());
    }

    // 统计LSN重复
    std::set<uint64_t> unique_lsns(lsns.begin(), lsns.end());
    int lsn_duplicates = lsns.size() - unique_lsns.size();

    // 统计时间戳重复
    std::set<int64_t> unique_timestamps(timestamps.begin(), timestamps.end());
    int timestamp_duplicates = timestamps.size() - unique_timestamps.size();

    std::cout << "   LSN duplicates: " << lsn_duplicates << " / " << lsns.size()
              << std::endl;
    std::cout << "   Timestamp duplicates: " << timestamp_duplicates << " / "
              << timestamps.size() << std::endl;

    // LSN应该没有重复
    if (lsn_duplicates > 0) {
      result.fail("LSN has duplicates: " + std::to_string(lsn_duplicates));
      return result;
    }

    // 时间戳可能有重复（这是正常的）
    if (timestamp_duplicates > 0) {
      std::cout << "   ⚠️  Timestamp has duplicates (this is expected in fast "
                   "operations)"
                << std::endl;
    }

    std::cout << "✅ LSN vs Timestamp comparison test PASSED" << std::endl;
    std::cout << "   LSN provides better uniqueness guarantee than timestamp"
              << std::endl;

  } catch (const std::exception &e) {
    result.fail(std::string("Exception: ") + e.what());
  }

  return result;
}

/**
 * @brief 测试4: current_lsn() 不递增测试
 */
TestResult test_current_lsn_no_increment() {
  TestResult result;
  std::cout << "\n[Test 4] current_lsn() No Increment Test" << std::endl;

  try {
    LSNCounter counter;

    // 分配一些LSN
    for (int i = 0; i < 10; ++i) {
      counter.next_lsn();
    }

    // 多次调用current_lsn()，应该返回相同的值
    uint64_t lsn1 = counter.current_lsn();
    uint64_t lsn2 = counter.current_lsn();
    uint64_t lsn3 = counter.current_lsn();

    if (lsn1 != lsn2 || lsn2 != lsn3) {
      result.fail("current_lsn() is incrementing: " + std::to_string(lsn1) +
                  ", " + std::to_string(lsn2) + ", " + std::to_string(lsn3));
      return result;
    }

    // 分配一个新LSN
    uint64_t new_lsn = counter.next_lsn();

    // current_lsn()应该返回新值
    uint64_t current = counter.current_lsn();

    if (current != new_lsn) {
      result.fail("current_lsn() not updated after next_lsn(): " +
                  std::to_string(current) + " != " + std::to_string(new_lsn));
      return result;
    }

    std::cout << "✅ current_lsn() no increment test PASSED" << std::endl;
    std::cout << "   current_lsn() correctly returns current value without "
                 "incrementing"
              << std::endl;

  } catch (const std::exception &e) {
    result.fail(std::string("Exception: ") + e.what());
  }

  return result;
}

/**
 * @brief 测试5: LSN性能测试
 */
TestResult test_lsn_performance() {
  TestResult result;
  std::cout << "\n[Test 5] LSN Performance Test" << std::endl;

  try {
    LSNCounter counter;

    const int iterations = 1000000;

    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < iterations; ++i) {
      counter.next_lsn();
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start);

    double avg_time_us = static_cast<double>(duration.count()) / iterations;

    std::cout << "   " << iterations << " LSN allocations: " << duration.count()
              << " μs" << std::endl;
    std::cout << "   Average: " << avg_time_us << " μs per LSN" << std::endl;

    // 性能要求：平均每次LSN分配应该 < 0.1μs
    if (avg_time_us > 0.1) {
      result.fail("LSN allocation too slow: " + std::to_string(avg_time_us) +
                  " μs per LSN");
      return result;
    }

    std::cout << "✅ LSN performance test PASSED" << std::endl;
    std::cout << "   Performance: " << (1.0 / avg_time_us) << " M ops/sec"
              << std::endl;

  } catch (const std::exception &e) {
    result.fail(std::string("Exception: ") + e.what());
  }

  return result;
}

int main() {
  std::cout << "========================================" << std::endl;
  std::cout << "LSN Simple Test Suite" << std::endl;
  std::cout << "========================================" << std::endl;

  std::vector<TestResult> results;

  // 运行所有测试
  results.push_back(test_lsn_monotonic());
  results.push_back(test_lsn_concurrent());
  results.push_back(test_lsn_vs_timestamp());
  results.push_back(test_current_lsn_no_increment());
  results.push_back(test_lsn_performance());

  // 统计结果
  int passed = 0;
  int failed = 0;

  std::cout << "\n========================================" << std::endl;
  std::cout << "Test Results Summary" << std::endl;
  std::cout << "========================================" << std::endl;

  for (size_t i = 0; i < results.size(); ++i) {
    if (results[i].passed) {
      passed++;
      std::cout << "Test " << (i + 1) << ": ✅ PASSED" << std::endl;
    } else {
      failed++;
      std::cout << "Test " << (i + 1) << ": ❌ FAILED - "
                << results[i].error_message << std::endl;
    }
  }

  std::cout << "\n========================================" << std::endl;
  std::cout << "Total: " << results.size() << " tests" << std::endl;
  std::cout << "Passed: " << passed << std::endl;
  std::cout << "Failed: " << failed << std::endl;
  std::cout << "========================================" << std::endl;

  if (failed == 0) {
    std::cout << "\n🎉 All tests passed! LSN implementation is correct."
              << std::endl;
  }

  return (failed == 0) ? 0 : 1;
}
