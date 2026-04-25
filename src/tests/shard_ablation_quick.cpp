#include "../core/sharded_cache.h"
#include <atomic>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <random>
#include <thread>
#include <vector>

using namespace minkv::db;

// 快速测试单个分片配置
struct TestResult {
  size_t shard_count;
  int thread_count;
  double duration_ms;
  double qps;
  double relative_to_best;
};

TestResult test_shard_count(size_t shard_count, int thread_count,
                            int ops_per_thread) {
  // 创建指定分片数的缓存
  ShardedCache<std::string, std::string> cache(10000, shard_count);

  // 预填充数据
  for (int i = 0; i < 10000; ++i) {
    cache.put("key_" + std::to_string(i), "value");
  }

  std::atomic<int64_t> total_ops{0};

  auto worker = [&](int thread_id) {
    std::mt19937 gen(thread_id);
    std::uniform_int_distribution<> key_dis(0, 99999);
    std::uniform_int_distribution<> op_dis(0, 99);

    for (int i = 0; i < ops_per_thread; ++i) {
      std::string key = "key_" + std::to_string(key_dis(gen));

      if (op_dis(gen) < 70) {
        // 70% PUT
        cache.put(key, "val");
      } else {
        // 30% GET
        cache.get(key);
      }

      total_ops++;
    }
  };

  // 开始测试
  auto start = std::chrono::high_resolution_clock::now();

  std::vector<std::thread> threads;
  for (int i = 0; i < thread_count; ++i) {
    threads.emplace_back(worker, i);
  }

  for (auto &t : threads) {
    t.join();
  }

  auto end = std::chrono::high_resolution_clock::now();
  double duration_ms =
      std::chrono::duration<double, std::milli>(end - start).count();

  TestResult result;
  result.shard_count = shard_count;
  result.thread_count = thread_count;
  result.duration_ms = duration_ms;
  result.qps = (total_ops * 1000.0) / duration_ms;
  result.relative_to_best = 0; // 稍后计算

  return result;
}

void print_results(std::vector<TestResult> &results) {
  // 找到最佳QPS
  double best_qps = 0;
  for (const auto &r : results) {
    if (r.qps > best_qps) {
      best_qps = r.qps;
    }
  }

  // 计算相对性能
  for (auto &r : results) {
    r.relative_to_best = r.qps / best_qps;
  }

  std::cout << "\n╔════════════════════════════════════════════════════════════"
               "════════════════╗\n";
  std::cout << "║                    分片数量消融测试结果                      "
               "              ║\n";
  std::cout << "╚══════════════════════════════════════════════════════════════"
               "══════════════╝\n\n";

  std::cout << std::left << std::setw(12) << "分片数" << std::right
            << std::setw(12) << "线程数" << std::setw(15) << "QPS"
            << std::setw(15) << "耗时(ms)" << std::setw(15) << "相对性能"
            << std::setw(10) << "评价" << "\n";
  std::cout << std::string(78, '-') << "\n";

  for (const auto &r : results) {
    std::cout << std::left << std::setw(12) << r.shard_count << std::right
              << std::setw(12) << r.thread_count << std::setw(15) << std::fixed
              << std::setprecision(0) << r.qps << std::setw(15) << std::fixed
              << std::setprecision(2) << r.duration_ms << std::setw(14)
              << std::fixed << std::setprecision(2) << r.relative_to_best
              << "x";

    if (r.relative_to_best >= 0.98) {
      std::cout << std::setw(10) << "✅ 最优";
    } else if (r.relative_to_best >= 0.90) {
      std::cout << std::setw(10) << "⚠️ 接近";
    } else {
      std::cout << std::setw(10) << "❌ 较差";
    }
    std::cout << "\n";
  }

  std::cout << "\n";

  // 找到最优配置
  size_t best_shard_count = 0;
  for (const auto &r : results) {
    if (r.qps == best_qps) {
      best_shard_count = r.shard_count;
      break;
    }
  }

  std::cout << "🎯 最优配置: " << best_shard_count << " 分片\n";
  std::cout << "📊 峰值性能: " << std::fixed << std::setprecision(2)
            << best_qps / 1000000.0 << "M QPS\n\n";
}

int main() {
  std::cout << "╔══════════════════════════════════════════════════════════════"
               "══════════════╗\n";
  std::cout << "║                    MinKV 分片数量快速消融测试                "
               "              ║\n";
  std::cout << "║                  Shard Count Ablation Study (Quick)          "
               "              ║\n";
  std::cout << "╚══════════════════════════════════════════════════════════════"
               "══════════════╝\n\n";

  // 获取系统信息
  int hardware_threads = std::thread::hardware_concurrency();
  std::cout << "系统信息:\n";
  std::cout << "  硬件线程数: " << hardware_threads << "\n\n";

  // 测试配置
  int test_thread_count = 8;  // 固定8线程测试
  int ops_per_thread = 50000; // 每线程5万次操作

  std::vector<size_t> shard_counts = {1, 2, 4, 8, 16, 32, 64};

  std::cout << "测试配置:\n";
  std::cout << "  测试线程数: " << test_thread_count << "\n";
  std::cout << "  每线程操作数: " << ops_per_thread << "\n";
  std::cout << "  总操作数: " << test_thread_count * ops_per_thread << "\n";
  std::cout << "  测试分片数: ";
  for (size_t s : shard_counts) {
    std::cout << s << " ";
  }
  std::cout << "\n\n";

  std::vector<TestResult> results;

  std::cout << "开始测试...\n\n";

  for (size_t shard_count : shard_counts) {
    std::cout << "  测试 " << shard_count << " 分片..." << std::flush;

    auto result =
        test_shard_count(shard_count, test_thread_count, ops_per_thread);
    results.push_back(result);

    std::cout << " QPS: " << std::fixed << std::setprecision(0) << result.qps
              << "\n";
  }

  // 打印结果
  print_results(results);

  // 保存到CSV
  std::ofstream csv("shard_ablation_quick_results.csv");
  csv << "ShardCount,ThreadCount,QPS,Duration(ms),RelativeToBest\n";
  for (const auto &r : results) {
    csv << r.shard_count << "," << r.thread_count << "," << std::fixed
        << std::setprecision(0) << r.qps << "," << std::fixed
        << std::setprecision(2) << r.duration_ms << "," << std::fixed
        << std::setprecision(3) << r.relative_to_best << "\n";
  }
  csv.close();

  std::cout << "结果已保存到: shard_ablation_quick_results.csv\n\n";

  std::cout << "╔══════════════════════════════════════════════════════════════"
               "══════════════╗\n";
  std::cout << "║                          ✓ 测试完成！                        "
               "              ║\n";
  std::cout << "╚══════════════════════════════════════════════════════════════"
               "══════════════╝\n\n";

  return 0;
}
