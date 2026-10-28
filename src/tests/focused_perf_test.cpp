#include <algorithm>
#include <atomic>
#include <chrono>
#include <iostream>
#include <random>
#include <thread>
#include <vector>

#include "../core/sharded_cache.h"

using namespace minkv::db;
using Cache = ShardedCache<std::string, std::string>;

int main() {
  const int thread_count = 1;
  const int ops_per_thread = 500000;
  const int preload_count = 100000;
  const int key_range = 100000; // 100% hit
  const int shards = 32;
  const int read_ratio = 90;

  Cache cache(10000, shards);

  // Preload
  for (int i = 0; i < preload_count; ++i) {
    cache.put("key_" + std::to_string(i), "val");
  }

  std::vector<int64_t> thread_local_ops(thread_count, 0);

  auto worker = [&](int thread_id) {
    std::mt19937 gen(thread_id);
    std::uniform_int_distribution<> key_dis(0, key_range - 1);
    std::uniform_int_distribution<> op_dis(0, 99);

    int64_t local_ops = 0;

    for (int i = 0; i < ops_per_thread; ++i) {
      std::string key = "key_" + std::to_string(key_dis(gen));

      if (op_dis(gen) < read_ratio) {
        cache.get(key);
      } else {
        cache.put(key, "val");
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

  double qps = (total_ops * 1000.0) / duration_ms;
  double avg_latency_us = duration_ms * 1000.0 / total_ops;

  std::cout << "Threads: " << thread_count << " Shards: " << shards << std::endl;
  std::cout << "Total ops: " << total_ops << " Duration: " << duration_ms << "ms"
            << std::endl;
  std::cout << "QPS: " << qps << " AvgLatency: " << avg_latency_us << "us" << std::endl;

  auto stats = cache.getStats();
  std::cout << "Hit rate: " << (stats.hits * 100.0 / (stats.hits + stats.misses + 1))
            << "%" << std::endl;

  return 0;
}
