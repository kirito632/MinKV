#include "core/minkv.h"
#include <algorithm>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <numeric>
#include <random>
#include <thread>
#include <unordered_map>
#include <vector>

// 测试配置
const int NUM_OPERATIONS = 1000000;
const int PREFILL_SIZE = 10000;

// 延迟统计
struct LatencyStats {
  std::vector<double> latencies;
  std::mutex mutex;

  void record(double latency_us) {
    std::lock_guard<std::mutex> lock(mutex);
    latencies.push_back(latency_us);
  }

  void report(const std::string &prefix = "") {
    if (latencies.empty())
      return;

    std::sort(latencies.begin(), latencies.end());

    double avg = std::accumulate(latencies.begin(), latencies.end(), 0.0) /
                 latencies.size();
    double p50 = latencies[latencies.size() * 0.50];
    double p95 = latencies[latencies.size() * 0.95];
    double p99 = latencies[latencies.size() * 0.99];
    double p999 = latencies[latencies.size() * 0.999];

    std::cout << prefix << "Avg: " << std::fixed << std::setprecision(2) << avg
              << "μs" << std::endl;
    std::cout << prefix << "P50: " << p50 << "μs" << std::endl;
    std::cout << prefix << "P95: " << p95 << "μs" << std::endl;
    std::cout << prefix << "P99: " << p99 << "μs ⭐" << std::endl;
    std::cout << prefix << "P999: " << p999 << "μs" << std::endl;
  }
};

// Baseline: std::unordered_map + std::mutex
class StdMapBaseline {
  std::unordered_map<std::string, std::string> map_;
  mutable std::mutex mutex_;

public:
  void put(const std::string &key, const std::string &value) {
    std::lock_guard<std::mutex> lock(mutex_);
    map_[key] = value;
  }

  bool get(const std::string &key, std::string &value) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = map_.find(key);
    if (it != map_.end()) {
      value = it->second;
      return true;
    }
    return false;
  }
};

// 1. 基础读写性能测试
template <typename DB>
void benchmark_basic_rw(DB &db, const std::string &name, int num_threads) {
  std::cout << "\n=== " << name << " - 基础读写性能 (线程数: " << num_threads
            << ") ===" << std::endl;

  // 预热数据
  for (int i = 0; i < PREFILL_SIZE; ++i) {
    db.put("key_" + std::to_string(i), "value_data_" + std::to_string(i));
  }

  // 混合读写 (90% 读, 10% 写)
  auto worker = [&](int ops_per_thread) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, PREFILL_SIZE - 1);

    for (int i = 0; i < ops_per_thread; ++i) {
      if (i % 10 < 9) {
        std::string value;
        db.get("key_" + std::to_string(dis(gen)), value);
      } else {
        db.put("key_" + std::to_string(dis(gen)), "new_value");
      }
    }
  };

  auto start = std::chrono::high_resolution_clock::now();

  std::vector<std::thread> threads;
  int ops_per_thread = NUM_OPERATIONS / num_threads;
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back(worker, ops_per_thread);
  }
  for (auto &t : threads)
    t.join();

  auto end = std::chrono::high_resolution_clock::now();
  auto duration_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
          .count();

  double qps = NUM_OPERATIONS * 1000.0 / duration_ms;

  std::cout << "总操作数: " << NUM_OPERATIONS << std::endl;
  std::cout << "耗时: " << duration_ms << " ms" << std::endl;
  std::cout << "QPS: " << std::fixed << std::setprecision(0) << qps << " ("
            << qps / 10000 << "万)" << std::endl;
}

// 2. 并发扩展性测试
template <typename DB>
void benchmark_scalability(DB &db, const std::string &name) {
  std::cout << "\n=== " << name << " - 并发扩展性测试 ===" << std::endl;
  std::cout << std::left << std::setw(8) << "线程数" << std::setw(15) << "QPS"
            << std::setw(15) << "QPS(万)" << std::setw(12) << "扩展效率"
            << std::endl;
  std::cout << std::string(50, '-') << std::endl;

  std::vector<int> thread_counts = {1, 2, 4, 8, 16};
  double baseline_qps = 0;

  for (int num_threads : thread_counts) {
    // 重新初始化数据
    for (int i = 0; i < PREFILL_SIZE; ++i) {
      db.put("key_" + std::to_string(i), "value_" + std::to_string(i));
    }

    auto worker = [&](int ops) {
      std::random_device rd;
      std::mt19937 gen(rd());
      std::uniform_int_distribution<> dis(0, PREFILL_SIZE - 1);

      for (int i = 0; i < ops; ++i) {
        std::string value;
        db.get("key_" + std::to_string(dis(gen)), value);
      }
    };

    auto start = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> threads;
    int ops_per_thread = NUM_OPERATIONS / num_threads;
    for (int i = 0; i < num_threads; ++i) {
      threads.emplace_back(worker, ops_per_thread);
    }
    for (auto &t : threads)
      t.join();

    auto end = std::chrono::high_resolution_clock::now();
    auto duration_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
            .count();

    double qps = NUM_OPERATIONS * 1000.0 / duration_ms;

    if (num_threads == 1)
      baseline_qps = qps;
    double efficiency = (qps / baseline_qps) / num_threads * 100;

    std::cout << std::left << std::setw(8) << num_threads << std::setw(15)
              << std::fixed << std::setprecision(0) << qps << std::setw(15)
              << std::setprecision(1) << (qps / 10000) << std::setw(12)
              << std::setprecision(1) << efficiency << "%" << std::endl;
  }
}

// 3. 延迟分布测试
template <typename DB>
void benchmark_latency(DB &db, const std::string &name, int num_threads) {
  std::cout << "\n=== " << name << " - 延迟分布测试 (线程数: " << num_threads
            << ") ===" << std::endl;

  // 预热
  for (int i = 0; i < PREFILL_SIZE; ++i) {
    db.put("key_" + std::to_string(i), "value_" + std::to_string(i));
  }

  LatencyStats combined;

  auto worker = [&](int ops) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, PREFILL_SIZE - 1);

    for (int i = 0; i < ops; ++i) {
      auto start = std::chrono::high_resolution_clock::now();

      std::string value;
      db.get("key_" + std::to_string(dis(gen)), value);

      auto end = std::chrono::high_resolution_clock::now();
      auto latency_us =
          std::chrono::duration_cast<std::chrono::microseconds>(end - start)
              .count();

      combined.record(latency_us);
    }
  };

  std::vector<std::thread> threads;
  int ops_per_thread = NUM_OPERATIONS / num_threads;
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back(worker, ops_per_thread);
  }
  for (auto &t : threads)
    t.join();

  combined.report("  ");
}

// 4. 对比测试报告
void generate_comparison_report() {
  std::cout << "\n" << std::string(80, '=') << std::endl;
  std::cout << "MinKV vs StdMap 性能对比报告" << std::endl;
  std::cout << std::string(80, '=') << std::endl;

  std::cout << "\n【测试场景】混合读写 (90% Get, 10% Put)" << std::endl;
  std::cout << "【数据规模】10,000 条预填充数据" << std::endl;
  std::cout << "【总操作数】1,000,000 次操作" << std::endl;

  std::cout << "\n"
            << std::left << std::setw(12) << "线程数" << std::setw(18)
            << "StdMap QPS(万)" << std::setw(18) << "MinKV QPS(万)"
            << std::setw(15) << "提升倍数" << std::setw(15) << "MinKV P99"
            << std::endl;
  std::cout << std::string(80, '-') << std::endl;

  // 这里需要实际运行测试后填入数据
  std::cout << "（运行完整测试后自动生成）" << std::endl;
}

int main() {
  std::cout << "========================================" << std::endl;
  std::cout << "MinKV 完整性能压测" << std::endl;
  std::cout << "========================================" << std::endl;
  std::cout << "测试配置:" << std::endl;
  std::cout << "  - 总操作数: " << NUM_OPERATIONS << std::endl;
  std::cout << "  - 预填充数据: " << PREFILL_SIZE << " 条" << std::endl;
  std::cout << "  - 读写比例: 90% 读, 10% 写" << std::endl;
  std::cout << "  - MinKV 分片数: 16" << std::endl;
  std::cout << "========================================" << std::endl;

  // 初始化数据库
  MinKV<std::string, std::string> minkv(PREFILL_SIZE, 16);
  StdMapBaseline stdmap;

  // ========== 第一部分：基础读写性能 ==========
  std::cout << "\n\n【第一部分：基础读写性能测试】" << std::endl;

  benchmark_basic_rw(stdmap, "StdMap (Baseline)", 1);
  benchmark_basic_rw(minkv, "MinKV", 1);

  benchmark_basic_rw(stdmap, "StdMap (Baseline)", 8);
  benchmark_basic_rw(minkv, "MinKV", 8);

  // ========== 第二部分：并发扩展性 ==========
  std::cout << "\n\n【第二部分：并发扩展性测试】" << std::endl;

  benchmark_scalability(stdmap, "StdMap (Baseline)");
  benchmark_scalability(minkv, "MinKV");

  // ========== 第三部分：延迟分布 ==========
  std::cout << "\n\n【第三部分：延迟分布测试】" << std::endl;

  benchmark_latency(stdmap, "StdMap (Baseline)", 8);
  benchmark_latency(minkv, "MinKV", 8);

  benchmark_latency(stdmap, "StdMap (Baseline)", 16);
  benchmark_latency(minkv, "MinKV", 16);

  // ========== 生成对比报告 ==========
  generate_comparison_report();

  std::cout << "\n========================================" << std::endl;
  std::cout << "测试完成！" << std::endl;
  std::cout << "========================================" << std::endl;

  return 0;
}
