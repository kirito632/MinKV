/**
 * @file benchmark_kv_overhead.cpp
 * @brief What enabling the vector index costs a plain KV workload.
 *
 * The index decides "is this value a vector?" by byte length (size == dim*4).
 * That guess has two consequences this benchmark measures:
 *
 *   1. every put, vector or not, touches the index bookkeeping;
 *   2. a plain value whose length happens to equal dim*4 is treated as a
 *      vector and pays a full HNSW insertion.
 *
 * Two value sizes are measured on purpose: 32B (obviously not a vector) and
 * dim*4 (indistinguishable from one by length alone).
 *
 *   cmake -S . -B build/release -DCMAKE_BUILD_TYPE=Release
 *   cmake --build build/release --target benchmark_kv_overhead -j2
 *   ./build/release/bin/benchmark_kv_overhead [threads] [ops_per_thread] [dim]
 *
 * Build in Release: a Debug build inflates everything by roughly 7x and makes
 * the comparison meaningless.
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "core/sharded_cache.h"

namespace {

using Cache = minkv::db::ShardedCache<std::string, std::string>;
using Clock = std::chrono::steady_clock;

constexpr size_t kShards = 32;

struct Latency {
  double p50_us = 0.0;
  double p95_us = 0.0;
  double p99_us = 0.0;
};

double quantile(const std::vector<double> &sorted, double q) {
  if (sorted.empty()) {
    return 0.0;
  }
  return sorted[static_cast<size_t>(q * static_cast<double>(sorted.size() - 1))];
}

Latency summarise_us(std::vector<double> &samples) {
  Latency latency;
  if (samples.empty()) {
    return latency;
  }
  std::sort(samples.begin(), samples.end());
  latency.p50_us = quantile(samples, 0.50);
  latency.p95_us = quantile(samples, 0.95);
  latency.p99_us = quantile(samples, 0.99);
  return latency;
}

struct Result {
  Latency put;
  Latency get;
  double seconds = 0.0;
  double ops_per_second = 0.0;
};

/**
 * @param value_size bytes per value
 * @param ann_dim dimension the index is configured for (independent of the
 *                value size, so a plain workload can be measured against an
 *                index that does not happen to match its values)
 */
Result run_workload(size_t threads,
                    size_t ops_per_thread,
                    size_t value_size,
                    size_t key_range,
                    bool enable_ann,
                    size_t ann_dim) {
  const size_t total_ops = threads * ops_per_thread;
  Cache cache(total_ops / kShards + key_range / kShards + 1024, kShards);

  const std::string value(value_size, 'x');
  std::mt19937 preload(11u);
  for (size_t i = 0; i < key_range; ++i) {
    cache.put("k" + std::to_string(i), value);
  }

  if (enable_ann) {
    Cache::AnnConfig config;
    config.dim = ann_dim;
    if (!cache.enable_ann_index(config)) {
      std::printf("enable_ann_index failed\n");
      std::exit(1);
    }
  }

  std::vector<std::vector<double>> put_samples(threads);
  std::vector<std::vector<double>> get_samples(threads);
  std::atomic<size_t> ready {0};

  const auto started = Clock::now();
  std::vector<std::thread> workers;
  workers.reserve(threads);
  for (size_t worker = 0; worker < threads; ++worker) {
    workers.emplace_back([&, worker]() {
      std::mt19937 gen(static_cast<unsigned>(worker) * 7919u + 17u);
      std::uniform_int_distribution<size_t> key_pick(0, key_range - 1);
      std::uniform_int_distribution<int> op_pick(0, 99);
      put_samples[worker].reserve(ops_per_thread / 10 + 16);
      get_samples[worker].reserve(ops_per_thread + 16);

      ready.fetch_add(1);
      while (ready.load() < threads) {
        std::this_thread::yield();
      }

      for (size_t i = 0; i < ops_per_thread; ++i) {
        const std::string key = "k" + std::to_string(key_pick(gen));
        if (op_pick(gen) < 10) {
          const auto start = Clock::now();
          cache.put(key, value);
          put_samples[worker].push_back(
              std::chrono::duration<double, std::micro>(Clock::now() - start).count());
        } else {
          const auto start = Clock::now();
          cache.get(key);
          get_samples[worker].push_back(
              std::chrono::duration<double, std::micro>(Clock::now() - start).count());
        }
      }
    });
  }
  for (std::thread &worker : workers) {
    worker.join();
  }
  const double seconds = std::chrono::duration<double>(Clock::now() - started).count();

  std::vector<double> puts;
  std::vector<double> gets;
  for (size_t worker = 0; worker < threads; ++worker) {
    puts.insert(puts.end(), put_samples[worker].begin(), put_samples[worker].end());
    gets.insert(gets.end(), get_samples[worker].begin(), get_samples[worker].end());
  }

  Result result;
  result.put = summarise_us(puts);
  result.get = summarise_us(gets);
  result.seconds = seconds;
  result.ops_per_second = static_cast<double>(total_ops) / seconds;
  return result;
}

void print_row(const char *label, size_t value_size, bool announce, const Result &result) {
  std::printf("%-22s %5zu  %-3s | %8.2f %8.2f %8.2f | %7.2f %7.2f | %9.0f\n",
              label,
              value_size,
              announce ? "on" : "off",
              result.put.p50_us,
              result.put.p95_us,
              result.put.p99_us,
              result.get.p50_us,
              result.get.p99_us,
              result.ops_per_second);
}

} // namespace

int main(int argc, char **argv) {
  const size_t threads = argc > 1 ? static_cast<size_t>(std::stoul(argv[1])) : 4;
  const size_t ops_per_thread =
      argc > 2 ? static_cast<size_t>(std::stoul(argv[2])) : 20000;
  const size_t dim = argc > 3 ? static_cast<size_t>(std::stoul(argv[3])) : 128;
  const size_t key_range = 50000;

  std::printf(
      "MinKV KV-write overhead with the vector index, threads=%zu ops=%zu key_range=%zu "
      "dim=%zu\n",
      threads,
      ops_per_thread,
      key_range,
      dim);
  std::printf("%-22s %5s  %-3s | %8s %8s %8s | %7s %7s | %9s\n",
              "case",
              "bytes",
              "ann",
              "put50",
              "put95",
              "put99",
              "get50",
              "get99",
              "ops/s");

  struct Case {
    const char *label;
    size_t value_size;
  };

  // 32B cannot be mistaken for a vector; dim*4 can, by length alone.
  const Case cases[] = {{"small-value", 32}, {"vector-sized-value", dim * sizeof(float)}};

  for (const Case &item : cases) {
    const Result off =
        run_workload(threads, ops_per_thread, item.value_size, key_range, false, dim);
    print_row(item.label, item.value_size, false, off);
    std::fflush(stdout);

    const Result on =
        run_workload(threads, ops_per_thread, item.value_size, key_range, true, dim);
    print_row(item.label, item.value_size, true, on);
    std::fflush(stdout);

    std::printf("%-22s %5s  %-3s | put x%.1f slower, throughput x%.2f\n\n",
                "",
                "",
                "gap",
                off.put.p50_us > 0 ? on.put.p50_us / off.put.p50_us : 0.0,
                off.ops_per_second > 0 ? off.ops_per_second / on.ops_per_second : 0.0);
    std::fflush(stdout);
  }

  std::printf("note: values are plain bytes; the index sees only their length.\n");
  return 0;
}
