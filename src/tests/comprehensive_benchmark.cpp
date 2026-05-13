#include <algorithm>
#include <atomic>
#include <chrono>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <sstream>
#include <thread>
#include <vector>

#include "../core/sharded_cache.h"

using namespace minkv::db;
using Cache = ShardedCache<std::string, std::string>;

// 获取当前时间字符串
std::string get_current_time() {
  auto now = std::chrono::system_clock::now();
  auto time_t_now = std::chrono::system_clock::to_time_t(now);
  std::tm tm_now;
  localtime_r(&time_t_now, &tm_now);

  char buffer[100];
  std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &tm_now);
  return std::string(buffer);
}

// 格式化持续时间
std::string format_duration(double seconds) {
  int hours = static_cast<int>(seconds / 3600);
  int minutes = static_cast<int>((seconds - hours * 3600) / 60);
  int secs = static_cast<int>(seconds - hours * 3600 - minutes * 60);

  std::ostringstream oss;
  if (hours > 0) {
    oss << hours << "h " << minutes << "m " << secs << "s";
  } else if (minutes > 0) {
    oss << minutes << "m " << secs << "s";
  } else {
    oss << secs << "s";
  }
  return oss.str();
}

namespace {

// 测试结果结构
struct BenchmarkResult {
  std::string test_name;
  std::string workload_type; // "miss-heavy", "hit-heavy", "noop", "vector"
  int thread_count;
  int64_t total_ops;
  double duration_ms;
  double qps;
  double avg_latency_us;
  double p50_latency_us;
  double p95_latency_us;
  double p99_latency_us;
  size_t cache_hit_rate;
  int preload_count;
  int key_range;
};

// 无锁延迟统计（Thread-Local）
class LatencyStats {
private:
  std::vector<std::vector<double>> thread_local_latencies_;

public:
  explicit LatencyStats(int thread_count)
      : thread_local_latencies_(thread_count) {
    for (auto &vec : thread_local_latencies_) {
      vec.reserve(10000);
    }
  }

  void record(int thread_id, double latency_us) {
    thread_local_latencies_[thread_id].push_back(latency_us);
  }

  void get_percentiles(double &p50, double &p95, double &p99) {
    std::vector<double> merged;
    size_t total_size = 0;
    for (const auto &vec : thread_local_latencies_) {
      total_size += vec.size();
    }
    merged.reserve(total_size);

    for (const auto &vec : thread_local_latencies_) {
      merged.insert(merged.end(), vec.begin(), vec.end());
    }

    if (merged.empty()) {
      p50 = p95 = p99 = 0;
      return;
    }

    std::sort(merged.begin(), merged.end());
    size_t n = merged.size();
    p50 = merged[n * 50 / 100];
    p95 = merged[n * 95 / 100];
    p99 = merged[n * 99 / 100];
  }
};

// 生成随机字符串
std::string random_string(size_t length) {
  static const char charset[] =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  static std::random_device rd;
  static std::mt19937 gen(rd());
  static std::uniform_int_distribution<> dis(0, sizeof(charset) - 2);

  std::string result;
  result.reserve(length);
  for (size_t i = 0; i < length; ++i) {
    result += charset[dis(gen)];
  }
  return result;
}

// ============================================================
//  Benchmark 1: 并发读写 (支持不同命中率配置)
// ============================================================
// 参数:
//   preload_count: 预填充的 key 数量
//   key_range:     随机 key 的范围 (0 ~ key_range-1)
//   当 preload_count == key_range 时 ≈ 100% hit
//   当 preload_count << key_range 时 ≈ miss-heavy
// ============================================================
BenchmarkResult benchmark_concurrent_rw(int thread_count, int ops_per_thread,
                                        int read_ratio,
                                        int preload_count = 100000,
                                        int key_range = 1000000) {
  Cache cache(10000, 32);

  std::cout << "  预填充 " << preload_count << " 条数据..." << std::flush;
  for (int i = 0; i < preload_count; ++i) {
    cache.put("key_" + std::to_string(i), "val");
  }
  std::cout << " 完成！" << std::endl;

  // 🔥 FIX: 使用 thread-local 计数器，消除全局 atomic 竞争
  std::vector<int64_t> thread_local_ops(thread_count, 0);
  LatencyStats latency_stats(thread_count);

  auto worker = [&](int thread_id) {
    std::mt19937 gen(thread_id);
    std::uniform_int_distribution<> key_dis(0, key_range - 1);
    std::uniform_int_distribution<> op_dis(0, 99);

    int64_t local_ops = 0;

    for (int i = 0; i < ops_per_thread; ++i) {
      std::string key = "key_" + std::to_string(key_dis(gen));

      auto start = std::chrono::steady_clock::now();

      if (op_dis(gen) < read_ratio) {
        cache.get(key);
      } else {
        cache.put(key, "val");
      }

      if (i % 100 == 0) {
        auto end = std::chrono::steady_clock::now();
        double latency_us =
            std::chrono::duration<double, std::micro>(end - start).count();
        latency_stats.record(thread_id, latency_us);
      }

      local_ops++;
    }

    // 最后一次性写入 thread-local 数组，无竞争
    thread_local_ops[thread_id] = local_ops;
  };

  auto start_time = std::chrono::high_resolution_clock::now();

  std::vector<std::thread> threads;
  for (int i = 0; i < thread_count; ++i) {
    threads.emplace_back(worker, i);
  }

  for (auto &t : threads) {
    t.join();
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  double duration_ms =
      std::chrono::duration<double, std::milli>(end_time - start_time).count();

  // Merge thread-local counters — 无竞争操作
  int64_t total_ops = 0;
  for (auto ops : thread_local_ops) {
    total_ops += ops;
  }

  BenchmarkResult result;
  result.test_name = "Concurrent_R" + std::to_string(read_ratio) + "W" +
                     std::to_string(100 - read_ratio);
  result.thread_count = thread_count;
  result.total_ops = total_ops;
  result.duration_ms = duration_ms;
  result.qps = (total_ops * 1000.0) / duration_ms;
  result.avg_latency_us = duration_ms * 1000.0 / total_ops;
  result.preload_count = preload_count;
  result.key_range = key_range;

  // 根据命中率标记 workload 类型
  double expected_hit_rate =
      static_cast<double>(preload_count) / key_range * 100.0;
  if (expected_hit_rate > 90.0) {
    result.workload_type = "hit-heavy";
  } else {
    result.workload_type = "miss-heavy";
  }

  latency_stats.get_percentiles(result.p50_latency_us, result.p95_latency_us,
                                result.p99_latency_us);

  auto stats = cache.getStats();
  result.cache_hit_rate = (stats.hits * 100) / (stats.hits + stats.misses + 1);

  return result;
}

// ============================================================
//  Benchmark 2: 向量检索
// ============================================================
BenchmarkResult benchmark_vector_search(int thread_count,
                                        int searches_per_thread) {
  Cache cache(10000, 32);

  std::cout << "  预填充向量数据（10万条）..." << std::flush;
  for (int i = 0; i < 100000; ++i) {
    std::vector<float> vec(128);
    for (int j = 0; j < 128; ++j) {
      vec[j] = static_cast<float>(rand()) / RAND_MAX;
    }
    cache.vectorPut("vec_" + std::to_string(i), vec);

    if (i % 10000 == 0 && i > 0) {
      std::cout << i << "..." << std::flush;
    }
  }
  std::cout << " 完成！" << std::endl;

  // 🔥 FIX: thread-local 计数器
  std::vector<int64_t> thread_local_ops(thread_count, 0);
  LatencyStats latency_stats(thread_count);

  auto worker = [&](int thread_id) {
    std::mt19937 gen(thread_id);
    std::uniform_real_distribution<float> dis(0.0f, 1.0f);

    int64_t local_ops = 0;

    for (int i = 0; i < searches_per_thread; ++i) {
      std::vector<float> query(128);
      for (int j = 0; j < 128; ++j) {
        query[j] = dis(gen);
      }

      auto start = std::chrono::steady_clock::now();
      cache.vectorSearch(query, 10);
      auto end = std::chrono::steady_clock::now();

      double latency_us =
          std::chrono::duration<double, std::micro>(end - start).count();
      latency_stats.record(thread_id, latency_us);

      local_ops++;
    }

    thread_local_ops[thread_id] = local_ops;
  };

  auto start_time = std::chrono::high_resolution_clock::now();

  std::vector<std::thread> threads;
  for (int i = 0; i < thread_count; ++i) {
    threads.emplace_back(worker, i);
  }

  for (auto &t : threads) {
    t.join();
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  double duration_ms =
      std::chrono::duration<double, std::milli>(end_time - start_time).count();

  int64_t total_ops = 0;
  for (auto ops : thread_local_ops) {
    total_ops += ops;
  }

  BenchmarkResult result;
  result.test_name = "Vector_Search";
  result.workload_type = "vector";
  result.thread_count = thread_count;
  result.total_ops = total_ops;
  result.duration_ms = duration_ms;
  result.qps = (total_ops * 1000.0) / duration_ms;
  result.avg_latency_us = duration_ms * 1000.0 / total_ops;
  result.preload_count = 100000;
  result.key_range = 0;

  latency_stats.get_percentiles(result.p50_latency_us, result.p95_latency_us,
                                result.p99_latency_us);
  result.cache_hit_rate = 0;

  return result;
}

// ============================================================
//  Benchmark 3: Noop 基线测试 (测量 benchmark framework overhead)
// ============================================================
// 这个测试用空操作替换 cache.get/put，用来测量 benchmark 框架本身的
// 线程扩展性。如果 noop benchmark 在 8 线程时还能线性扩展，
// 说明问题确实在 cache 上；否则说明 benchmark 框架自身有瓶颈。
// ============================================================
BenchmarkResult benchmark_noop(int thread_count, int ops_per_thread) {
  // 🔥 FIX: thread-local 计数器
  std::vector<int64_t> thread_local_ops(thread_count, 0);
  LatencyStats latency_stats(thread_count);

  auto worker = [&](int thread_id) {
    std::mt19937 gen(thread_id);
    std::uniform_int_distribution<> key_dis(0, 999999);
    std::uniform_int_distribution<> op_dis(0, 99);

    int64_t local_ops = 0;

    for (int i = 0; i < ops_per_thread; ++i) {
      // 生成 key 模拟真实开销，但不访问 cache
      volatile std::string key = "key_" + std::to_string(key_dis(gen));
      volatile int op = op_dis(gen);

      auto start = std::chrono::steady_clock::now();

      // NOOP: 什么都不做，只测量循环 + 随机数 + 计时开销
      (void)key;
      (void)op;

      if (i % 100 == 0) {
        auto end = std::chrono::steady_clock::now();
        double latency_us =
            std::chrono::duration<double, std::micro>(end - start).count();
        latency_stats.record(thread_id, latency_us);
      }

      local_ops++;
    }

    thread_local_ops[thread_id] = local_ops;
  };

  auto start_time = std::chrono::high_resolution_clock::now();

  std::vector<std::thread> threads;
  for (int i = 0; i < thread_count; ++i) {
    threads.emplace_back(worker, i);
  }

  for (auto &t : threads) {
    t.join();
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  double duration_ms =
      std::chrono::duration<double, std::milli>(end_time - start_time).count();

  int64_t total_ops = 0;
  for (auto ops : thread_local_ops) {
    total_ops += ops;
  }

  BenchmarkResult result;
  result.test_name = "Noop_Baseline";
  result.workload_type = "noop";
  result.thread_count = thread_count;
  result.total_ops = total_ops;
  result.duration_ms = duration_ms;
  result.qps = (total_ops * 1000.0) / duration_ms;
  result.avg_latency_us = duration_ms * 1000.0 / total_ops;
  result.preload_count = 0;
  result.key_range = 1000000;

  latency_stats.get_percentiles(result.p50_latency_us, result.p95_latency_us,
                                result.p99_latency_us);
  result.cache_hit_rate = 0;

  return result;
}

// 保存结果到CSV（带时间戳）
void save_to_csv(const std::vector<BenchmarkResult> &results,
                 const std::string &filename, const std::string &start_time,
                 const std::string &end_time, double total_duration) {
  std::ofstream file(filename);

  // 添加测试元数据
  file << "# MinKV Comprehensive Benchmark Report\n";
  file << "# Test Start Time: " << start_time << "\n";
  file << "# Test End Time: " << end_time << "\n";
  file << "# Total Duration: " << format_duration(total_duration) << " ("
       << std::fixed << std::setprecision(2) << total_duration << "s)\n";
  file << "# Optimization Version: v3.0\n";
  file << "# Key Improvements:\n";
  file << "#   - Removed global atomic total_ops (thread-local counters)\n";
  file << "#   - Added hit-heavy workload (100% hit) vs miss-heavy (10% hit)\n";
  file << "#   - Added noop baseline to measure benchmark framework overhead\n";
  file << "#   - Added preload_count and key_range metadata\n";
  file << "#\n";

  // CSV头部
  file << "Test,WorkloadType,Threads,TotalOps,Duration(ms),QPS,AvgLatency(us),"
          "P50(us),P95(us),P99(us),HitRate(%),PreloadCount,KeyRange\n";

  // 数据行
  for (const auto &r : results) {
    file << r.test_name << "," << r.workload_type << "," << r.thread_count
         << "," << r.total_ops << "," << std::fixed << std::setprecision(2)
         << r.duration_ms << "," << std::fixed << std::setprecision(0) << r.qps
         << "," << std::fixed << std::setprecision(2) << r.avg_latency_us << ","
         << std::fixed << std::setprecision(2) << r.p50_latency_us << ","
         << std::fixed << std::setprecision(2) << r.p95_latency_us << ","
         << std::fixed << std::setprecision(2) << r.p99_latency_us << ","
         << r.cache_hit_rate << "," << r.preload_count << "," << r.key_range
         << "\n";
  }

  file.close();
  std::cout << "结果已保存到: " << filename << std::endl;
}

// 打印结果表格
void print_results(const std::vector<BenchmarkResult> &results) {
  std::cout << "\n╔════════════════════════════════════════════════════════════"
               "══════════════════════════════════════════╗\n";
  std::cout << "║                    MinKV 综合压力测试报告 v3.0               "
               "                                  ║\n";
  std::cout << "╚══════════════════════════════════════════════════════════════"
               "══════════════════════════════════════════╝\n\n";

  std::cout << std::left << std::setw(22) << "测试场景" << std::left
            << std::setw(14) << "Workload" << std::right << std::setw(8)
            << "线程数" << std::setw(12) << "QPS" << std::setw(12) << "P50(us)"
            << std::setw(12) << "P95(us)" << std::setw(12) << "P99(us)"
            << std::setw(10) << "命中率" << "\n";
  std::cout << std::string(102, '-') << "\n";

  for (const auto &r : results) {
    std::cout << std::left << std::setw(22) << r.test_name << std::left
              << std::setw(14) << r.workload_type << std::right << std::setw(8)
              << r.thread_count << std::setw(12) << std::fixed
              << std::setprecision(0) << r.qps << std::setw(12) << std::fixed
              << std::setprecision(2) << r.p50_latency_us << std::setw(12)
              << std::fixed << std::setprecision(2) << r.p95_latency_us
              << std::setw(12) << std::fixed << std::setprecision(2)
              << r.p99_latency_us << std::setw(9) << r.cache_hit_rate << "%\n";
  }
  std::cout << "\n";
}

} // anonymous namespace

int main() {
  auto test_start_time = std::chrono::system_clock::now();
  std::string start_time_str = get_current_time();

  std::cout << "╔══════════════════════════════════════════════════════════════"
               "══════════════════════════════════════╗\n";
  std::cout << "║              MinKV 综合压力测试套件 v3.0                     "
               "                                  ║\n";
  std::cout
      << "║        Comprehensive Benchmark Suite (Methodology Cleanup)    "
         "                                  ║\n";
  std::cout << "║                                                              "
               "                                  ║\n";
  std::cout << "║  本次改进：                                                  "
               "                                  ║\n";
  std::cout << "║  1. 移除全局 atomic total_ops → thread-local 计数器          "
               "                                  ║\n";
  std::cout << "║     消除 benchmark framework 自身的 cacheline contention     "
               "                                  ║\n";
  std::cout << "║  2. 新增 hit-heavy workload (100% hit) 对比实验              "
               "                                  ║\n";
  std::cout << "║     区分 concurrent hashmap scalability vs cache scalability "
               "                                  ║\n";
  std::cout << "║  3. 新增 noop baseline 测试                                  "
               "                                  ║\n";
  std::cout << "║     测量 benchmark framework 自身的线程扩展性                "
               "                                  ║\n";
  std::cout << "║  4. CSV 输出增加 workload_type / preload_count / key_range   "
               "                                  ║\n";
  std::cout << "╚══════════════════════════════════════════════════════════════"
               "══════════════════════════════════════╝\n\n";

  std::cout << "⏰ 测试开始时间: " << start_time_str << "\n\n";

  std::vector<BenchmarkResult> results;

  // ================================================================
  // 实验 A: Noop 基线 — 测量 benchmark framework 自身的线程扩展性
  // ================================================================
  std::cout << "[实验 A] Noop 基线测试（测量 benchmark framework overhead）\n";
  for (int threads : {1, 2, 4, 8, 16}) {
    std::cout << "  - " << threads << " 线程:\n";
    auto result = benchmark_noop(threads, 100000);
    results.push_back(result);
    std::cout << "    QPS: " << std::fixed << std::setprecision(0) << result.qps
              << ", P99: " << std::fixed << std::setprecision(2)
              << result.p99_latency_us << "μs\n";
  }

  // ================================================================
  // 实验 B: Hit-heavy workload (100% hit)
  //   preload_count = 100K, key_range = 100K  →  ≈ 100% hit
  // ================================================================
  std::cout << "\n[实验 B] Hit-heavy workload（100% 命中率，90%读）\n";
  std::cout << "  配置: preload=100K, key_range=100K\n";
  for (int threads : {1, 2, 4, 8, 16}) {
    std::cout << "  - " << threads << " 线程:\n";
    auto result =
        benchmark_concurrent_rw(threads, 100000, 90,
                                100000,  // preload_count
                                100000); // key_range == preload → 100% hit
    results.push_back(result);
    std::cout << "    QPS: " << std::fixed << std::setprecision(0) << result.qps
              << ", P99: " << std::fixed << std::setprecision(2)
              << result.p99_latency_us << "μs"
              << ", 命中率: " << result.cache_hit_rate << "%\n";
  }

  // ================================================================
  // 实验 C: Miss-heavy workload (10% hit)
  //   preload_count = 100K, key_range = 1M  →  ≈ 10% hit
  // ================================================================
  std::cout << "\n[实验 C] Miss-heavy workload（10% 命中率，90%读）\n";
  std::cout << "  配置: preload=100K, key_range=1M\n";
  for (int threads : {1, 2, 4, 8, 16}) {
    std::cout << "  - " << threads << " 线程:\n";
    auto result =
        benchmark_concurrent_rw(threads, 100000, 90,
                                100000,   // preload_count
                                1000000); // key_range >> preload → 10% hit
    results.push_back(result);
    std::cout << "    QPS: " << std::fixed << std::setprecision(0) << result.qps
              << ", P99: " << std::fixed << std::setprecision(2)
              << result.p99_latency_us << "μs"
              << ", 命中率: " << result.cache_hit_rate << "%\n";
  }

  // ================================================================
  // 实验 D: 不同读写比例 (8线程, miss-heavy)
  // ================================================================
  std::cout << "\n[实验 D] 不同读写比例（8线程，miss-heavy）\n";
  for (int read_ratio : {50, 70, 90, 95, 99}) {
    std::cout << "  - R" << read_ratio << "/W" << (100 - read_ratio) << ":\n";
    auto result =
        benchmark_concurrent_rw(8, 100000, read_ratio, 100000, 1000000);
    results.push_back(result);
    std::cout << "    QPS: " << std::fixed << std::setprecision(0) << result.qps
              << ", P99: " << std::fixed << std::setprecision(2)
              << result.p99_latency_us << "μs\n";
  }

  // ================================================================
  // 实验 E: 向量检索性能
  // ================================================================
  std::cout << "\n[实验 E] 向量检索性能（10万向量，SIMD优化）\n";
  for (int threads : {1, 2, 4, 8}) {
    std::cout << "  - " << threads << " 线程:\n";
    auto result = benchmark_vector_search(threads, 500);
    results.push_back(result);
    std::cout << "    QPS: " << std::fixed << std::setprecision(0) << result.qps
              << ", P99: " << std::fixed << std::setprecision(2)
              << result.p99_latency_us << "μs\n";
  }

  auto test_end_time = std::chrono::system_clock::now();
  std::string end_time_str = get_current_time();
  double total_duration =
      std::chrono::duration<double>(test_end_time - test_start_time).count();

  // 打印结果
  std::cout << "\n生成测试报告...\n";
  print_results(results);

  // 保存到CSV
  std::cout << "保存数据文件...\n";
  save_to_csv(results, "benchmark_results.csv", start_time_str, end_time_str,
              total_duration);

  std::cout << "\n╔════════════════════════════════════════════════════════════"
               "══════════════════════════════════════════╗\n";
  std::cout << "║                          ✓ 压力测试完成！                    "
               "                                  ║\n";
  std::cout << "╚══════════════════════════════════════════════════════════════"
               "══════════════════════════════════════════╝\n\n";

  std::cout << "⏰ 测试时间统计:\n";
  std::cout << "  开始时间: " << start_time_str << "\n";
  std::cout << "  结束时间: " << end_time_str << "\n";
  std::cout << "  总耗时:   " << format_duration(total_duration) << " ("
            << std::fixed << std::setprecision(2) << total_duration << "s)\n\n";

  std::cout << "📊 数据文件:\n";
  std::cout << "  - 查看详细数据: benchmark_results.csv\n";
  std::cout << "  - 生成可视化图表: python3 visualize_benchmark.py\n\n";

  return 0;
}
