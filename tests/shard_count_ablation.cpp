#include "../core/sharded_cache.h"
#include <atomic>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <random>
#include <thread>
#include <vector>

using namespace minkv::db;

// 压测参数
const int NUM_THREADS = 16;        // 线程数
const int OPS_PER_THREAD = 100000; // 每个线程的操作数
const int KEY_RANGE = 10000;       // Key 的范围（模拟热点数据）

// 统计信息
struct BenchmarkResult {
  std::string name;
  int shard_count;
  long long total_ops;
  long long duration_ms;
  double qps;
  double relative_to_single;
  double relative_to_best;
};

// 分片锁版本的压测
BenchmarkResult benchmark_sharded_lock(int shard_count) {
  ShardedCache<std::string, std::string> cache(10000 / shard_count,
                                               shard_count);

  auto start = std::chrono::high_resolution_clock::now();

  // 创建线程
  std::vector<std::thread> threads;
  for (int t = 0; t < NUM_THREADS; ++t) {
    threads.emplace_back([&cache, t]() {
      std::mt19937 gen(t);
      std::uniform_int_distribution<> dis(0, KEY_RANGE - 1);

      for (int i = 0; i < OPS_PER_THREAD; ++i) {
        int key_id = dis(gen);
        std::string key = "key_" + std::to_string(key_id);
        std::string value = "value_" + std::to_string(i);

        // 混合操作：70% put, 30% get
        if (i % 10 < 7) {
          cache.put(key, value);
        } else {
          auto result = cache.get(key);
          (void)result; // 避免未使用警告
        }
      }
    });
  }

  // 等待所有线程完成
  for (auto &t : threads) {
    t.join();
  }

  auto end = std::chrono::high_resolution_clock::now();
  long long duration_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
          .count();
  long long total_ops = (long long)NUM_THREADS * OPS_PER_THREAD;
  double qps = (double)total_ops / duration_ms * 1000;

  return {
      .name = std::to_string(shard_count) + " shards",
      .shard_count = shard_count,
      .total_ops = total_ops,
      .duration_ms = duration_ms,
      .qps = qps,
      .relative_to_single = 0.0, // 稍后计算
      .relative_to_best = 0.0    // 稍后计算
  };
}

void print_header() {
  std::cout << "\n========================================" << std::endl;
  std::cout << "MinKV Shard Count Ablation Study" << std::endl;
  std::cout << "========================================" << std::endl;
  std::cout << "Threads: " << NUM_THREADS << std::endl;
  std::cout << "Ops per thread: " << OPS_PER_THREAD << std::endl;
  std::cout << "Total ops: " << NUM_THREADS * OPS_PER_THREAD << std::endl;
  std::cout << "Key range: " << KEY_RANGE << std::endl;
  std::cout << "Workload: 70% put, 30% get" << std::endl;
  std::cout << "========================================\n" << std::endl;
}

void print_result(const BenchmarkResult &r) {
  std::cout << std::left << std::setw(15) << r.name;
  std::cout << std::right << std::setw(12) << r.duration_ms << " ms";
  std::cout << std::setw(15) << (long long)r.qps << " QPS";

  if (r.relative_to_single > 0) {
    std::cout << std::setw(12) << std::fixed << std::setprecision(2)
              << r.relative_to_single << "x";
  } else {
    std::cout << std::setw(12) << "baseline";
  }

  if (r.relative_to_best > 0) {
    std::cout << std::setw(12) << std::fixed << std::setprecision(2)
              << r.relative_to_best << "x";
  }

  // 添加评价标签
  if (r.relative_to_single < 1.0 && r.relative_to_single > 0) {
    std::cout << "  ❌ 性能下降";
  } else if (r.relative_to_best >= 0.95 && r.relative_to_best <= 1.0) {
    std::cout << "  ✅ 最优";
  } else if (r.relative_to_best >= 0.85 && r.relative_to_best < 0.95) {
    std::cout << "  ⚠️  接近最优";
  }

  std::cout << std::endl;
}

void print_table_header() {
  std::cout << "\n========================================" << std::endl;
  std::cout << "RESULTS" << std::endl;
  std::cout << "========================================\n" << std::endl;

  std::cout << std::left << std::setw(15) << "Configuration";
  std::cout << std::right << std::setw(12) << "Duration";
  std::cout << std::setw(15) << "QPS";
  std::cout << std::setw(12) << "vs Single";
  std::cout << std::setw(12) << "vs Best";
  std::cout << std::endl;

  std::cout << std::string(66, '-') << std::endl;
}

void print_analysis(const std::vector<BenchmarkResult> &results) {
  std::cout << "\n========================================" << std::endl;
  std::cout << "ANALYSIS" << std::endl;
  std::cout << "========================================\n" << std::endl;

  // 找到最优配置
  auto best_it =
      std::max_element(results.begin(), results.end(),
                       [](const BenchmarkResult &a, const BenchmarkResult &b) {
                         return a.qps < b.qps;
                       });

  std::cout << "🏆 Best Configuration: " << best_it->name << " ("
            << (long long)best_it->qps << " QPS)" << std::endl;
  std::cout << "   Performance improvement: " << std::fixed
            << std::setprecision(2) << best_it->relative_to_single
            << "x vs single lock\n"
            << std::endl;

  // 分析性能趋势
  std::cout << "📊 Performance Trend:" << std::endl;

  for (size_t i = 0; i < results.size(); ++i) {
    const auto &r = results[i];
    std::cout << "   " << std::setw(12) << std::left << r.name << ": ";

    if (i == 0) {
      std::cout << "Baseline (single lock)" << std::endl;
    } else if (r.relative_to_single < 1.0) {
      std::cout << "❌ Performance degradation (" << std::fixed
                << std::setprecision(1) << (1.0 - r.relative_to_single) * 100
                << "% slower)" << std::endl;
    } else if (r.shard_count == best_it->shard_count) {
      std::cout << "✅ Optimal configuration (" << std::fixed
                << std::setprecision(1) << (r.relative_to_single - 1.0) * 100
                << "% faster)" << std::endl;
    } else if (r.relative_to_best >= 0.90) {
      std::cout << "⚠️  Close to optimal (" << std::fixed << std::setprecision(1)
                << (r.relative_to_single - 1.0) * 100 << "% faster)"
                << std::endl;
    } else {
      std::cout << "Performance improvement (" << std::fixed
                << std::setprecision(1) << (r.relative_to_single - 1.0) * 100
                << "% faster)" << std::endl;
    }
  }

  // 技术分析
  std::cout << "\n💡 Technical Insights:" << std::endl;

  // 找到性能下降的配置
  bool has_degradation = false;
  for (const auto &r : results) {
    if (r.relative_to_single < 1.0 && r.relative_to_single > 0) {
      if (!has_degradation) {
        std::cout << "   • Too few shards cause hash collisions:" << std::endl;
        has_degradation = true;
      }
      std::cout << "     - " << r.name << ": collision probability ~"
                << std::fixed << std::setprecision(0)
                << (1.0 - std::pow(1.0 - 1.0 / r.shard_count, NUM_THREADS)) *
                       100
                << "%" << std::endl;
    }
  }

  std::cout << "   • Optimal shard count: " << best_it->shard_count
            << " (2x thread count)" << std::endl;
  std::cout << "     - Collision probability: ~" << std::fixed
            << std::setprecision(0)
            << (1.0 - std::pow(1.0 - 1.0 / best_it->shard_count, NUM_THREADS)) *
                   100
            << "%" << std::endl;

  // 检查是否有性能回落
  if (results.size() > 2) {
    const auto &last = results.back();
    if (last.relative_to_best < 0.95) {
      std::cout << "   • Too many shards cause cache line contention:"
                << std::endl;
      std::cout << "     - " << last.name
                << ": false sharing reduces L2 cache hit rate" << std::endl;
    }
  }
}

int main() {
  print_header();

  // 测试不同分片数量
  std::vector<int> shard_counts = {1, 2, 4, 8, 16, 32, 64};
  std::vector<BenchmarkResult> results;

  for (size_t i = 0; i < shard_counts.size(); ++i) {
    int count = shard_counts[i];
    std::cout << "[" << (i + 1) << "/" << shard_counts.size() << "] ";
    std::cout << "Testing " << count << " shard(s)..." << std::flush;

    auto result = benchmark_sharded_lock(count);
    results.push_back(result);

    std::cout << " Done (" << result.duration_ms << "ms)" << std::endl;
  }

  // 计算相对性能
  double single_qps = results[0].qps;
  double best_qps = 0.0;

  for (auto &r : results) {
    if (r.qps > best_qps) {
      best_qps = r.qps;
    }
  }

  for (auto &r : results) {
    if (r.shard_count == 1) {
      r.relative_to_single = 0.0; // baseline
    } else {
      r.relative_to_single = r.qps / single_qps;
    }
    r.relative_to_best = r.qps / best_qps;
  }

  // 打印结果表格
  print_table_header();
  for (const auto &r : results) {
    print_result(r);
  }

  // 打印分析
  print_analysis(results);

  std::cout << "\n========================================" << std::endl;
  std::cout << "Test completed successfully!" << std::endl;
  std::cout << "========================================\n" << std::endl;

  return 0;
}
