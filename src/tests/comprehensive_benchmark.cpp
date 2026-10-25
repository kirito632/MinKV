#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
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

constexpr uint32_t kBenchmarkSeed = 42;
constexpr int kLatencySampleInterval = 100;
constexpr size_t kDefaultTotalCapacity = 100000;
constexpr int kWarmupOps = 50000;

size_t capacity_per_shard(size_t total_capacity, int shards) {
  return (total_capacity + static_cast<size_t>(shards) - 1) / static_cast<size_t>(shards);
}

class StartGate {
private:
  std::mutex mutex_;
  std::condition_variable cv_;
  int ready_count_ = 0;
  const int expected_count_;
  bool start_ = false;

public:
  explicit StartGate(int expected_count) : expected_count_(expected_count) {}

  void wait_ready() {
    std::unique_lock<std::mutex> lock(mutex_);
    ++ready_count_;
    cv_.notify_all();
    cv_.wait(lock, [&] { return start_; });
  }

  void release_when_ready() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [&] { return ready_count_ == expected_count_; });
    start_ = true;
    cv_.notify_all();
  }
};

// 测试结果结构
struct BenchmarkResult {
  std::string test_name;
  std::string workload_type; // "miss-heavy", "hit-heavy", "noop", "vector"
  int thread_count;
  int ops_per_thread;
  int64_t planned_total_ops;
  int64_t total_ops;
  double duration_ms;
  double qps;
  double avg_latency_us;
  double p50_latency_us;
  double p95_latency_us;
  double p99_latency_us;
  size_t cache_hit_rate;
  double actual_hit_rate_pct;
  int preload_count;
  int key_range;
  int shard_count;
  size_t total_capacity;
  size_t capacity_per_shard;
  uint32_t seed;
  int latency_sample_interval;
  int warmup_ops;
  size_t latency_sample_count;
  bool wal_enabled = false; // WAL 持久化是否开启

  // 详细统计（用于命中率漂移分析）
  uint64_t stats_hits = 0;
  uint64_t stats_misses = 0;
  uint64_t stats_puts = 0;
  uint64_t stats_evictions = 0;
  size_t current_size = 0;
};

// WAL A/B 对比结果（paired comparison）
struct WalComparisonResult {
  int threads;
  double qps_off;
  double qps_on;
  double p99_off;
  double p99_on;
  double p50_off;
  double p50_on;
  double qps_drop_pct;     // (off - on) / off * 100
  double p99_increase_pct; // (on - off) / off * 100
};

// 无锁延迟统计（Thread-Local）
class LatencyStats {
private:
  std::vector<std::vector<double>> thread_local_latencies_;

public:
  explicit LatencyStats(int thread_count) : thread_local_latencies_(thread_count) {
    for (auto &vec : thread_local_latencies_) {
      vec.reserve(10000);
    }
  }

  void record(int thread_id, double latency_us) {
    thread_local_latencies_[thread_id].push_back(latency_us);
  }

  size_t sample_count() const {
    size_t total_size = 0;
    for (const auto &vec : thread_local_latencies_) {
      total_size += vec.size();
    }
    return total_size;
  }

  void get_percentiles(double &p50, double &p95, double &p99) {
    std::vector<double> merged;
    size_t total_size = sample_count();
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
    auto percentile = [&](double pct) {
      size_t rank = static_cast<size_t>(std::ceil((pct / 100.0) * n));
      rank = std::max<size_t>(rank, 1) - 1;
      return merged[std::min(rank, n - 1)];
    };
    p50 = percentile(50.0);
    p95 = percentile(95.0);
    p99 = percentile(99.0);
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
//  Benchmark 1: 并发读写 (支持不同命中率配置 & 分片数)
// ============================================================
// 参数:
//   preload_count: 预填充的 key 数量
//   key_range:     随机 key 的范围 (0 ~ key_range-1)
//   当 preload_count == key_range 时 ≈ 100% hit
//   当 preload_count << key_range 时 ≈ miss-heavy
//   shards:        分片数，用于测试分片锁扩展性
// ============================================================
BenchmarkResult benchmark_concurrent_rw(int thread_count,
                                        int ops_per_thread,
                                        int read_ratio,
                                        int preload_count = 100000,
                                        int key_range = 1000000,
                                        int shards = 128,
                                        bool enable_wal = false,
                                        size_t total_capacity = kDefaultTotalCapacity,
                                        uint32_t seed = kBenchmarkSeed) {
  // 如果启用 WAL，先清理旧数据目录
  if (enable_wal) {
    std::filesystem::remove_all("./test_wal_data");
  }

  const size_t per_shard_capacity = capacity_per_shard(total_capacity, shards);
  Cache cache(per_shard_capacity, shards);

  // 如果启用 WAL 持久化，配置 Group Commit（10ms 刷盘间隔）
  if (enable_wal) {
    cache.enable_persistence("./test_wal_data", 10);
  }

  std::cout << "  预填充 " << preload_count << " 条数据..." << std::flush;
  for (int i = 0; i < preload_count; ++i) {
    cache.put("key_" + std::to_string(i), "val");
  }
  std::cout << " 完成！" << std::endl;

  // --- Warmup phase: stabilize LRU/allocator/WAL before timed stats ---
  {
    std::mt19937 warmup_gen(seed + 9999);
    std::uniform_int_distribution<> warmup_key_dis(0, key_range - 1);
    std::uniform_int_distribution<> warmup_op_dis(0, 99);
    for (int i = 0; i < kWarmupOps; ++i) {
      std::string key = "key_" + std::to_string(warmup_key_dis(warmup_gen));
      if (warmup_op_dis(warmup_gen) < read_ratio) {
        cache.get(key);
      } else {
        cache.put(key, "val");
      }
    }
  }

  // --- Reset stats after warmup so CSV reflects only the timed run ---
  cache.resetStats();
  auto stats_before = cache.getStats();

  // 🔥 FIX: 使用 thread-local 计数器，消除全局 atomic 竞争
  std::vector<int64_t> thread_local_ops(thread_count, 0);
  LatencyStats latency_stats(thread_count);
  StartGate gate(thread_count);

  auto worker = [&](int thread_id) {
    std::mt19937 gen(seed + static_cast<uint32_t>(thread_id));
    std::uniform_int_distribution<> key_dis(0, key_range - 1);
    std::uniform_int_distribution<> op_dis(0, 99);

    int64_t local_ops = 0;
    gate.wait_ready();

    for (int i = 0; i < ops_per_thread; ++i) {
      std::string key = "key_" + std::to_string(key_dis(gen));

      auto start = std::chrono::steady_clock::now();

      if (op_dis(gen) < read_ratio) {
        cache.get(key);
      } else {
        cache.put(key, "val");
      }

      if (i % kLatencySampleInterval == 0) {
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

  std::vector<std::thread> threads;
  for (int i = 0; i < thread_count; ++i) {
    threads.emplace_back(worker, i);
  }

  auto start_time = std::chrono::high_resolution_clock::now();
  gate.release_when_ready();

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
  result.ops_per_thread = ops_per_thread;
  result.planned_total_ops = static_cast<int64_t>(thread_count) * ops_per_thread;
  result.total_ops = total_ops;
  result.duration_ms = duration_ms;
  result.qps = (total_ops * 1000.0) / duration_ms;
  result.avg_latency_us = duration_ms * 1000.0 / total_ops;
  result.preload_count = preload_count;
  result.key_range = key_range;
  result.shard_count = shards;
  result.total_capacity = total_capacity;
  result.capacity_per_shard = per_shard_capacity;
  result.seed = seed;
  result.latency_sample_interval = kLatencySampleInterval;
  result.warmup_ops = kWarmupOps;
  result.latency_sample_count = latency_stats.sample_count();
  result.wal_enabled = enable_wal;

  // 根据命中率标记 workload 类型
  double expected_hit_rate = static_cast<double>(preload_count) / key_range * 100.0;
  if (expected_hit_rate > 90.0) {
    result.workload_type = "hit-heavy";
  } else {
    result.workload_type = "miss-heavy";
  }

  latency_stats.get_percentiles(
      result.p50_latency_us, result.p95_latency_us, result.p99_latency_us);

  auto stats_after = cache.getStats();
  uint64_t delta_hits = stats_after.hits - stats_before.hits;
  uint64_t delta_misses = stats_after.misses - stats_before.misses;
  result.stats_hits = delta_hits;
  result.stats_misses = delta_misses;
  result.stats_puts = stats_after.puts - stats_before.puts;
  result.stats_evictions = stats_after.evictions - stats_before.evictions;
  result.current_size = cache.size();
  const uint64_t measured_gets = delta_hits + delta_misses;
  result.actual_hit_rate_pct =
      measured_gets > 0 ? (static_cast<double>(delta_hits) * 100.0) / measured_gets : 0.0;
  result.cache_hit_rate = static_cast<size_t>(std::lround(result.actual_hit_rate_pct));

  // DIAG: print lock contention summary after each benchmark run
  {
    // Read /proc/self/status for voluntary/involuntary context switches
    std::ifstream proc_self("/proc/self/status");
    std::string line;
    while (std::getline(proc_self, line)) {
      if (line.find("voluntary_ctxt_switches") != std::string::npos ||
          line.find("nonvoluntary_ctxt_switches") != std::string::npos) {
        std::cout << "  [DIAG-PROC] " << line << std::endl;
      }
    }
  }

  return result;
}

// ============================================================
//  Benchmark 2: 向量检索
// ============================================================
// NOTE: this section uses uniform random 128d vectors, which is adversarial
// for any graph index: distance concentration erases neighbourhood structure
// and recall collapses regardless of implementation quality. Keep it as the
// historical brute-force baseline, but evaluate HNSW with benchmark_hnsw,
// which measures both a clustered (realistic) and a uniform (stress) set.
BenchmarkResult benchmark_vector_search(int thread_count,
                                        int searches_per_thread,
                                        uint32_t seed = kBenchmarkSeed) {
  constexpr int kVectorCount = 100000;
  constexpr int kVectorShards = 32;
  const size_t per_shard_capacity = capacity_per_shard(kVectorCount, kVectorShards);
  Cache cache(per_shard_capacity, kVectorShards);

  std::mt19937 preload_gen(seed);
  std::uniform_real_distribution<float> preload_dis(0.0f, 1.0f);

  std::cout << "  预填充向量数据（10万条）..." << std::flush;
  for (int i = 0; i < kVectorCount; ++i) {
    std::vector<float> vec(128);
    for (int j = 0; j < 128; ++j) {
      vec[j] = preload_dis(preload_gen);
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
  StartGate gate(thread_count);

  auto worker = [&](int thread_id) {
    std::mt19937 gen(seed + static_cast<uint32_t>(thread_id));
    std::uniform_real_distribution<float> dis(0.0f, 1.0f);

    int64_t local_ops = 0;
    gate.wait_ready();

    for (int i = 0; i < searches_per_thread; ++i) {
      std::vector<float> query(128);
      for (int j = 0; j < 128; ++j) {
        query[j] = dis(gen);
      }

      auto start = std::chrono::steady_clock::now();
      cache.vectorSearch(query, 10);
      auto end = std::chrono::steady_clock::now();

      double latency_us = std::chrono::duration<double, std::micro>(end - start).count();
      latency_stats.record(thread_id, latency_us);

      local_ops++;
    }

    thread_local_ops[thread_id] = local_ops;
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < thread_count; ++i) {
    threads.emplace_back(worker, i);
  }

  auto start_time = std::chrono::high_resolution_clock::now();
  gate.release_when_ready();

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
  result.ops_per_thread = searches_per_thread;
  result.planned_total_ops = static_cast<int64_t>(thread_count) * searches_per_thread;
  result.total_ops = total_ops;
  result.duration_ms = duration_ms;
  result.qps = (total_ops * 1000.0) / duration_ms;
  result.avg_latency_us = duration_ms * 1000.0 / total_ops;
  result.preload_count = 100000;
  result.key_range = 0;
  result.shard_count = kVectorShards;
  result.total_capacity = kVectorCount;
  result.capacity_per_shard = per_shard_capacity;
  result.seed = seed;
  result.latency_sample_interval = 1;
  result.warmup_ops = 0;
  result.latency_sample_count = latency_stats.sample_count();

  latency_stats.get_percentiles(
      result.p50_latency_us, result.p95_latency_us, result.p99_latency_us);
  result.cache_hit_rate = 0;
  result.actual_hit_rate_pct = 0.0;

  return result;
}

// ============================================================
//  Benchmark 3: Noop 基线测试 (测量 benchmark framework overhead)
// ============================================================
// 这个测试用空操作替换 cache.get/put，用来测量 benchmark 框架本身的
// 线程扩展性。如果 noop benchmark 在 8 线程时还能线性扩展，
// 说明问题确实在 cache 上；否则说明 benchmark 框架自身有瓶颈。
// ============================================================
BenchmarkResult benchmark_noop(int thread_count,
                               int ops_per_thread,
                               uint32_t seed = kBenchmarkSeed) {
  // 🔥 FIX: thread-local 计数器
  std::vector<int64_t> thread_local_ops(thread_count, 0);
  LatencyStats latency_stats(thread_count);
  StartGate gate(thread_count);

  auto worker = [&](int thread_id) {
    std::mt19937 gen(seed + static_cast<uint32_t>(thread_id));
    std::uniform_int_distribution<> key_dis(0, 999999);
    std::uniform_int_distribution<> op_dis(0, 99);

    int64_t local_ops = 0;
    gate.wait_ready();

    for (int i = 0; i < ops_per_thread; ++i) {
      // 生成 key 模拟真实开销，但不访问 cache
      volatile std::string key = "key_" + std::to_string(key_dis(gen));
      volatile int op = op_dis(gen);

      auto start = std::chrono::steady_clock::now();

      // NOOP: 什么都不做，只测量循环 + 随机数 + 计时开销
      (void)key;
      (void)op;

      if (i % kLatencySampleInterval == 0) {
        auto end = std::chrono::steady_clock::now();
        double latency_us =
            std::chrono::duration<double, std::micro>(end - start).count();
        latency_stats.record(thread_id, latency_us);
      }

      local_ops++;
    }

    thread_local_ops[thread_id] = local_ops;
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < thread_count; ++i) {
    threads.emplace_back(worker, i);
  }

  auto start_time = std::chrono::high_resolution_clock::now();
  gate.release_when_ready();

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
  result.ops_per_thread = ops_per_thread;
  result.planned_total_ops = static_cast<int64_t>(thread_count) * ops_per_thread;
  result.total_ops = total_ops;
  result.duration_ms = duration_ms;
  result.qps = (total_ops * 1000.0) / duration_ms;
  result.avg_latency_us = duration_ms * 1000.0 / total_ops;
  result.preload_count = 0;
  result.key_range = 1000000;
  result.shard_count = 0;
  result.total_capacity = 0;
  result.capacity_per_shard = 0;
  result.seed = seed;
  result.latency_sample_interval = kLatencySampleInterval;
  result.warmup_ops = 0;
  result.latency_sample_count = latency_stats.sample_count();

  latency_stats.get_percentiles(
      result.p50_latency_us, result.p95_latency_us, result.p99_latency_us);
  result.cache_hit_rate = 0;
  result.actual_hit_rate_pct = 0.0;

  return result;
}

// 保存结果到CSV（带时间戳）
void save_to_csv(const std::vector<BenchmarkResult> &results,
                 const std::string &filename,
                 const std::string &start_time,
                 const std::string &end_time,
                 double total_duration) {
  std::ofstream file(filename);

  // 添加测试元数据
  file << "# MinKV Comprehensive Benchmark Report\n";
  file << "# Test Start Time: " << start_time << "\n";
  file << "# Test End Time: " << end_time << "\n";
  file << "# Total Duration: " << format_duration(total_duration) << " (" << std::fixed
       << std::setprecision(2) << total_duration << "s)\n";
  file << "# Optimization Version: v4.0\n";
  file << "# Key Improvements:\n";
  file << "#   - Removed global atomic total_ops (thread-local counters)\n";
  file << "#   - Added hit-heavy workload (100% hit) vs miss-heavy (10% hit)\n";
  file << "#   - Added noop baseline to measure benchmark framework overhead\n";
  file << "#   - Added preload_count and key_range metadata\n";
  file << "#   - Added shard scalability test (experiment F)\n";
  file << "#   - Added WAL A/B test (experiment G)\n";
  file << "#   - Added start barrier, fixed total capacity, fixed seed, and nearest-rank "
          "percentiles\n";
  file << "#   - Default shards: 128, ops_per_thread: 500K\n";
  file << "#   - Warmup phase (50K ops) before timed run\n";
  file << "#   - Detailed stats: hits/misses/puts/evictions/current_size via pre/post "
          "snapshot delta\n";
  file << "#\n";

  // CSV头部
  file << "Test,WorkloadType,Threads,OpsPerThread,PlannedTotalOps,ActualTotalOps,"
          "Duration(ms),QPS,AvgLatency(us),P50(us),P95(us),P99(us),"
          "HitRate(%),PreloadCount,KeyRange,Shards,TotalCapacity,CapacityPerShard,"
          "Seed,WarmupOps,LatencySampleInterval,LatencySamples,WAL,StatsHits,"
          "StatsMisses,StatsPuts,StatsEvictions,CurrentSize\n";

  // 数据行
  for (const auto &r : results) {
    file << r.test_name << "," << r.workload_type << "," << r.thread_count << ","
         << r.ops_per_thread << "," << r.planned_total_ops << "," << r.total_ops << ","
         << std::fixed << std::setprecision(2) << r.duration_ms << "," << std::fixed
         << std::setprecision(0) << r.qps << "," << std::fixed << std::setprecision(2)
         << r.avg_latency_us << "," << std::fixed << std::setprecision(2)
         << r.p50_latency_us << "," << std::fixed << std::setprecision(2)
         << r.p95_latency_us << "," << std::fixed << std::setprecision(2)
         << r.p99_latency_us << "," << std::fixed << std::setprecision(2)
         << r.actual_hit_rate_pct << "," << r.preload_count << "," << r.key_range << ","
         << r.shard_count << "," << r.total_capacity << "," << r.capacity_per_shard << ","
         << r.seed << "," << r.warmup_ops << "," << r.latency_sample_interval << ","
         << r.latency_sample_count << "," << (r.wal_enabled ? "ON" : "OFF") << ","
         << r.stats_hits << "," << r.stats_misses << "," << r.stats_puts << ","
         << r.stats_evictions << "," << r.current_size << "\n";
  }

  file.close();
  std::cout << "结果已保存到: " << filename << std::endl;
}

// 打印结果表格
void print_results(const std::vector<BenchmarkResult> &results) {
  std::cout << "\n╔════════════════════════════════════════════════════════════"
               "══════════════════════════════════════════╗\n";
  std::cout << "║                    MinKV 综合压力测试报告 v4.0               "
               "                                  ║\n";
  std::cout << "╚══════════════════════════════════════════════════════════════"
               "══════════════════════════════════════════╝\n\n";

  std::cout << std::left << std::setw(22) << "测试场景" << std::left << std::setw(14)
            << "Workload" << std::right << std::setw(8) << "线程数" << std::setw(12)
            << "QPS" << std::setw(12) << "P50(us)" << std::setw(12) << "P95(us)"
            << std::setw(12) << "P99(us)" << std::setw(10) << "命中率" << std::setw(8)
            << "分片数" << std::setw(6) << "WAL" << "\n";
  std::cout << std::string(116, '-') << "\n";

  for (const auto &r : results) {
    std::cout << std::left << std::setw(22) << r.test_name << std::left << std::setw(14)
              << r.workload_type << std::right << std::setw(8) << r.thread_count
              << std::setw(12) << std::fixed << std::setprecision(0) << r.qps
              << std::setw(12) << std::fixed << std::setprecision(2) << r.p50_latency_us
              << std::setw(12) << std::fixed << std::setprecision(2) << r.p95_latency_us
              << std::setw(12) << std::fixed << std::setprecision(2) << r.p99_latency_us
              << std::setw(9) << r.cache_hit_rate << "%" << std::setw(8) << r.shard_count
              << std::setw(6) << (r.wal_enabled ? "ON" : "OFF") << "\n";
  }
  std::cout << "\n";
}

// 打印 WAL A/B 对比分析报告（论文级表格）
void print_wal_comparison(const std::vector<WalComparisonResult> &comparisons) {
  std::cout << "\n";
  std::cout << "╔══════════════════════════════════════════════════════════════"
               "══════════"
               "══════════════════════════╗\n";
  std::cout << "║                        WAL Durability Overhead Analysis (Paired "
               "Comparison)                     ║\n";
  std::cout << "╚══════════════════════════════════════════════════════════════"
               "══════════"
               "══════════════════════════╝\n\n";

  // ── Table 1: Throughput Impact ──
  std::cout << "Table 1: Throughput Impact (QPS)\n";
  std::cout << "══════════════════════════════════════════════════════════════════\n";
  std::cout << std::left << std::setw(10) << "Threads" << std::right << std::setw(14)
            << "QPS_OFF" << std::setw(14) << "QPS_ON" << std::setw(18)
            << "Throughput_Drop" << "\n";
  std::cout << std::string(56, '-') << "\n";

  for (const auto &c : comparisons) {
    std::cout << std::left << std::setw(10) << c.threads << std::right << std::setw(14)
              << std::fixed << std::setprecision(2) << c.qps_off / 1000000.0 << "M"
              << std::setw(14) << std::fixed << std::setprecision(2)
              << c.qps_on / 1000000.0 << "M" << std::setw(16) << std::fixed
              << std::setprecision(1) << c.qps_drop_pct << "%" << "\n";
  }
  std::cout << "\n";

  // ── Table 2: Latency Impact ──
  std::cout << "Table 2: P99 Latency Impact\n";
  std::cout << "══════════════════════════════════════════════════════════════════\n";
  std::cout << std::left << std::setw(10) << "Threads" << std::right << std::setw(14)
            << "P99_OFF" << std::setw(14) << "P99_ON" << std::setw(18) << "P99_Increase"
            << "\n";
  std::cout << std::string(56, '-') << "\n";

  for (const auto &c : comparisons) {
    std::cout << std::left << std::setw(10) << c.threads << std::right << std::setw(14)
              << std::fixed << std::setprecision(2) << c.p99_off << "us" << std::setw(14)
              << std::fixed << std::setprecision(2) << c.p99_on << "us" << std::setw(16)
              << std::fixed << std::setprecision(1) << c.p99_increase_pct << "%" << "\n";
  }
  std::cout << "\n";

  // ── Table 3: P50 Latency Impact ──
  std::cout << "Table 3: P50 (Median) Latency Impact\n";
  std::cout << "══════════════════════════════════════════════════════════════════\n";
  std::cout << std::left << std::setw(10) << "Threads" << std::right << std::setw(14)
            << "P50_OFF" << std::setw(14) << "P50_ON" << std::setw(18) << "P50_Increase"
            << "\n";
  std::cout << std::string(56, '-') << "\n";

  for (const auto &c : comparisons) {
    double p50_inc = (c.p50_on - c.p50_off) / c.p50_off * 100.0;
    std::cout << std::left << std::setw(10) << c.threads << std::right << std::setw(14)
              << std::fixed << std::setprecision(2) << c.p50_off << "us" << std::setw(14)
              << std::fixed << std::setprecision(2) << c.p50_on << "us" << std::setw(16)
              << std::fixed << std::setprecision(1) << p50_inc << "%" << "\n";
  }
  std::cout << "\n";

  // ── Key Findings ──
  std::cout << "Key Findings:\n";
  std::cout << "══════════════════════════════════════════════════════════════════\n";

  // 计算平均 QPS drop
  double avg_drop = 0;
  for (const auto &c : comparisons) {
    avg_drop += c.qps_drop_pct;
  }
  avg_drop /= comparisons.size();

  // 检查 drop 是否稳定（max - min）
  double min_drop = 100, max_drop = 0;
  for (const auto &c : comparisons) {
    if (c.qps_drop_pct < min_drop)
      min_drop = c.qps_drop_pct;
    if (c.qps_drop_pct > max_drop)
      max_drop = c.qps_drop_pct;
  }
  double drop_spread = max_drop - min_drop;

  std::cout << "  • Average WAL throughput overhead: " << std::fixed
            << std::setprecision(1) << avg_drop << "%\n";
  std::cout << "  • Overhead stability (max-min spread): " << std::fixed
            << std::setprecision(1) << drop_spread << "%\n";

  if (drop_spread < 10.0) {
    std::cout << "  • Observation: WAL throughput overhead is relatively stable across "
                 "thread counts\n";
    std::cout << "    -> This run does not by itself prove the dominant cause; use "
                 "profiling counters to distinguish fsync, serialization, and locks\n";
  } else {
    std::cout << "  • Observation: WAL throughput overhead varies with thread count\n";
    std::cout << "    -> This may indicate scaling effects, but needs profiling before "
                 "attributing it to lock contention\n";
  }

  // 检查 P99 趋势
  double p99_first = comparisons.front().p99_increase_pct;
  double p99_last = comparisons.back().p99_increase_pct;
  std::cout << "  • P99 latency impact: " << std::fixed << std::setprecision(1)
            << p99_first << "% (1 thread) → " << p99_last << "% ("
            << comparisons.back().threads << " threads)\n";

  std::cout << "\n";
}

} // anonymous namespace

int main() {
  auto test_start_time = std::chrono::system_clock::now();
  std::string start_time_str = get_current_time();

  std::cout << "╔══════════════════════════════════════════════════════════════"
               "══════════════════════════════════════╗\n";
  std::cout << "║              MinKV 综合压力测试套件 v4.0                     "
               "                                  ║\n";
  std::cout << "║        Comprehensive Benchmark Suite (Methodology Cleanup)    "
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
  std::cout << "║  5. 新增分片数 (Shard) 扩展性测试                            "
               "                                  ║\n";
  std::cout << "║  6. 新增 WAL A/B 对比测试                                    "
               "                                  ║\n";
  std::cout << "║  7. 加入 StartGate 同步启动、固定总容量、固定种子            "
               "                                  ║\n";
  std::cout << "║  8. 新增 warmup 阶段 (50K ops)、pre/post 统计快照            "
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
    auto result = benchmark_noop(threads, 500000);
    results.push_back(result);
    std::cout << "    QPS: " << std::fixed << std::setprecision(0) << result.qps
              << ", P99: " << std::fixed << std::setprecision(2) << result.p99_latency_us
              << "μs\n";
  }

  // ================================================================
  // 实验 B: Hit-heavy workload (100% hit)
  //   preload_count = 100K, key_range = 100K  →  ≈ 100% hit
  // ================================================================
  std::cout << "\n[实验 B] Hit-heavy workload（100% 命中率，90%读）\n";
  std::cout << "  配置: preload=100K, key_range=100K\n";
  for (int threads : {1, 2, 4, 8, 16}) {
    std::cout << "  - " << threads << " 线程:\n";
    auto result = benchmark_concurrent_rw(threads,
                                          500000,
                                          90,
                                          100000,  // preload_count
                                          100000); // key_range == preload → 100% hit
    results.push_back(result);
    std::cout << "    QPS: " << std::fixed << std::setprecision(0) << result.qps
              << ", P99: " << std::fixed << std::setprecision(2) << result.p99_latency_us
              << "μs" << ", 命中率: " << result.cache_hit_rate << "%\n";
  }

  // ================================================================
  // 实验 C: Miss-heavy workload (10% hit)
  //   preload_count = 100K, key_range = 1M  →  ≈ 10% hit
  // ================================================================
  std::cout << "\n[实验 C] Miss-heavy workload（10% 命中率，90%读）\n";
  std::cout << "  配置: preload=100K, key_range=1M\n";
  for (int threads : {1, 2, 4, 8, 16}) {
    std::cout << "  - " << threads << " 线程:\n";
    auto result = benchmark_concurrent_rw(threads,
                                          500000,
                                          90,
                                          100000,   // preload_count
                                          1000000); // key_range >> preload → 10% hit
    results.push_back(result);
    std::cout << "    QPS: " << std::fixed << std::setprecision(0) << result.qps
              << ", P99: " << std::fixed << std::setprecision(2) << result.p99_latency_us
              << "μs" << ", 命中率: " << result.cache_hit_rate << "%\n";
  }

  // ================================================================
  // 实验 D: 不同读写比例 (8线程, miss-heavy)
  // ================================================================
  std::cout << "\n[实验 D] 不同读写比例（8线程，miss-heavy）\n";
  for (int read_ratio : {50, 70, 90, 95, 99}) {
    std::cout << "  - R" << read_ratio << "/W" << (100 - read_ratio) << ":\n";
    auto result = benchmark_concurrent_rw(8, 500000, read_ratio, 100000, 1000000, 128);
    results.push_back(result);
    std::cout << "    QPS: " << std::fixed << std::setprecision(0) << result.qps
              << ", P99: " << std::fixed << std::setprecision(2) << result.p99_latency_us
              << "μs, 命中率: " << result.cache_hit_rate << "%\n";
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
              << ", P99: " << std::fixed << std::setprecision(2) << result.p99_latency_us
              << "μs\n";
  }

  // ================================================================
  // 实验 F1: 分片数 (Shard) 扩展性 — Hit-heavy
  // key_range=preload=100K → 100% 命中
  // 测纯锁竞争：所有操作都在热点路径，分片越多锁冲突越低
  // ================================================================
  std::cout << "\n[实验 F1] 分片扩展 — Hit-heavy（8线程，100%命中，90%读）\n";
  for (int shards : {1, 4, 16, 32, 64, 128, 256}) {
    std::cout << "  - " << shards << " 分片:\n";
    auto result = benchmark_concurrent_rw(8, 500000, 90, 100000, 100000, shards);
    result.test_name = "ShardScaling_F1_HitHeavy_R90W10";
    results.push_back(result);
    std::cout << "    QPS: " << std::fixed << std::setprecision(0) << result.qps
              << ", P99: " << std::fixed << std::setprecision(2) << result.p99_latency_us
              << "μs, 命中率: " << result.cache_hit_rate << "%\n";
  }

  // ================================================================
  // 实验 F2: 分片数 (Shard) 扩展性 — Miss-heavy
  // key_range=1M, preload=100K → ~10% 命中
  // 测真实缓存：miss 开销 + 缓存淘汰 + 写放大
  // ================================================================
  std::cout << "\n[实验 F2] 分片扩展 — Miss-heavy（8线程，10%命中，90%读）\n";
  for (int shards : {1, 4, 16, 32, 64, 128, 256}) {
    std::cout << "  - " << shards << " 分片:\n";
    auto result = benchmark_concurrent_rw(8, 500000, 90, 100000, 1000000, shards);
    result.test_name = "ShardScaling_F2_MissHeavy_R90W10";
    results.push_back(result);
    std::cout << "    QPS: " << std::fixed << std::setprecision(0) << result.qps
              << ", P99: " << std::fixed << std::setprecision(2) << result.p99_latency_us
              << "μs, 命中率: " << result.cache_hit_rate << "%\n";
  }

  // ================================================================
  // 实验 G: WAL 开启/关闭 A/B 对比测试（最佳分片配置）
  // 固定 miss-heavy workload（10%命中，90%读），使用 128 分片（实验 F2
  // 确认的最佳配置） 对比 WAL ON/OFF，测量纯 WAL 开销（排除分片锁瓶颈干扰）
  // ================================================================
  std::cout << "\n[实验 G] WAL 开启/关闭 A/B "
               "对比测试（128分片，miss-heavy，90%读）\n";
  std::cout << "  配置: preload=100K, key_range=1M, shards=128 (最佳分片配置)\n";

  std::vector<WalComparisonResult> wal_comparisons;

  for (int threads : {1, 2, 4, 8, 16}) {
    // WAL OFF
    std::cout << "  - " << threads << " 线程 (WAL OFF):\n";
    auto result_off =
        benchmark_concurrent_rw(threads, 500000, 90, 100000, 1000000, 128, false);
    results.push_back(result_off);
    std::cout << "    QPS: " << std::fixed << std::setprecision(0) << result_off.qps
              << ", P99: " << std::fixed << std::setprecision(2)
              << result_off.p99_latency_us << "μs, 命中率: " << result_off.cache_hit_rate
              << "%\n";

    // WAL ON (Group Commit, 10ms 刷盘间隔)
    std::cout << "  - " << threads << " 线程 (WAL ON):\n";
    auto result_on =
        benchmark_concurrent_rw(threads, 500000, 90, 100000, 1000000, 128, true);
    results.push_back(result_on);
    std::cout << "    QPS: " << std::fixed << std::setprecision(0) << result_on.qps
              << ", P99: " << std::fixed << std::setprecision(2)
              << result_on.p99_latency_us << "μs, 命中率: " << result_on.cache_hit_rate
              << "%\n";

    // 计算 WAL 性能损耗
    double loss_pct = (result_off.qps - result_on.qps) / result_off.qps * 100.0;
    std::cout << "    → WAL 性能损耗: " << std::fixed << std::setprecision(1) << loss_pct
              << "%\n";

    // 收集 paired comparison 数据
    WalComparisonResult cmp;
    cmp.threads = threads;
    cmp.qps_off = result_off.qps;
    cmp.qps_on = result_on.qps;
    cmp.p99_off = result_off.p99_latency_us;
    cmp.p99_on = result_on.p99_latency_us;
    cmp.p50_off = result_off.p50_latency_us;
    cmp.p50_on = result_on.p50_latency_us;
    cmp.qps_drop_pct = loss_pct;
    cmp.p99_increase_pct = (result_on.p99_latency_us - result_off.p99_latency_us) /
                           result_off.p99_latency_us * 100.0;
    wal_comparisons.push_back(cmp);
  }

  // 输出论文级 WAL Overhead Analysis
  print_wal_comparison(wal_comparisons);

  auto test_end_time = std::chrono::system_clock::now();
  std::string end_time_str = get_current_time();
  double total_duration =
      std::chrono::duration<double>(test_end_time - test_start_time).count();

  // 打印结果
  std::cout << "\n生成测试报告...\n";
  print_results(results);

  // 保存到CSV
  std::cout << "保存数据文件...\n";
  save_to_csv(
      results, "benchmark_results.csv", start_time_str, end_time_str, total_duration);

  std::cout << "\n╔════════════════════════════════════════════════════════════"
               "══════════════════════════════════════════╗\n";
  std::cout << "║                          ✓ 压力测试完成！                    "
               "                                  ║\n";
  std::cout << "╚══════════════════════════════════════════════════════════════"
               "══════════════════════════════════════════╝\n\n";

  std::cout << "⏰ 测试时间统计:\n";
  std::cout << "  开始时间: " << start_time_str << "\n";
  std::cout << "  结束时间: " << end_time_str << "\n";
  std::cout << "  总耗时:   " << format_duration(total_duration) << " (" << std::fixed
            << std::setprecision(2) << total_duration << "s)\n\n";

  std::cout << "📊 数据文件:\n";
  std::cout << "  - 查看详细数据: benchmark_results.csv\n";
  std::cout << "  - 生成可视化图表: python3 visualize_benchmark.py\n\n";

  return 0;
}
