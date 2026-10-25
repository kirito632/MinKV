/**
 * @file benchmark_hnsw.cpp
 * @brief Reproducible A/B benchmark: exact scan versus HNSW.
 *
 * Every number in docs/HNSW_DESIGN.md should be reachable from this binary.
 * Build it in Release; a Debug build inflates both columns by roughly 7x.
 *
 *   cmake -S . -B build/release -DCMAKE_BUILD_TYPE=Release
 *   cmake --build build/release --target benchmark_hnsw -j2
 *   ./build/release/bin/benchmark_hnsw
 *
 * Both paths run through ShardedCache, so the ANN column includes the
 * post-search existence re-check and the exact column includes the same
 * shard-copy overhead the production path has.
 *
 * Two data distributions are measured on purpose:
 *   - clustered: what real embeddings look like, and the only realistic
 *     basis for a recall claim;
 *   - uniform: adversarial, distance concentration leaves HNSW almost no
 *     neighbourhood structure. Recall here says more about the data than
 *     about the index, which is exactly why the repository's older benchmark
 *     (uniform 128d) must not be used to judge the index.
 */

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <numeric>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "core/sharded_cache.h"

namespace {

using Cache = minkv::db::ShardedCache<std::string, std::string>;
using Clock = std::chrono::steady_clock;

struct Latency {
  double p50_ms = 0.0;
  double p95_ms = 0.0;
  double p99_ms = 0.0;
  double avg_ms = 0.0;
};

double quantile(const std::vector<double> &sorted, double q) {
  if (sorted.empty()) {
    return 0.0;
  }
  const size_t index = static_cast<size_t>(q * static_cast<double>(sorted.size() - 1));
  return sorted[index];
}

Latency summarise(std::vector<double> samples_ms) {
  Latency latency;
  if (samples_ms.empty()) {
    return latency;
  }
  latency.avg_ms =
      std::accumulate(samples_ms.begin(), samples_ms.end(), 0.0) / samples_ms.size();
  std::sort(samples_ms.begin(), samples_ms.end());
  latency.p50_ms = quantile(samples_ms, 0.50);
  latency.p95_ms = quantile(samples_ms, 0.95);
  latency.p99_ms = quantile(samples_ms, 0.99);
  return latency;
}

/// Clustered vectors plus queries drawn from the same mixture.
void make_clustered(size_t count, size_t dim, size_t clusters, std::mt19937 &gen,
                    std::vector<float> &flat, std::vector<std::vector<float>> &queries) {
  std::normal_distribution<float> center(0.0f, 3.0f);
  std::normal_distribution<float> noise(0.0f, 0.5f);
  std::vector<std::vector<float>> centres(clusters, std::vector<float>(dim));
  for (auto &centre : centres) {
    for (float &value : centre) {
      value = center(gen);
    }
  }
  std::uniform_int_distribution<size_t> pick(0, clusters - 1);
  flat.assign(count * dim, 0.0f);
  for (size_t i = 0; i < count; ++i) {
    const auto &centre = centres[pick(gen)];
    for (size_t d = 0; d < dim; ++d) {
      flat[i * dim + d] = centre[d] + noise(gen);
    }
  }
  for (auto &query : queries) {
    const auto &centre = centres[pick(gen)];
    for (size_t d = 0; d < dim; ++d) {
      query[d] = centre[d] + noise(gen);
    }
  }
}

/// Uniform noise in [0, 1)^dim: the adversarial case.
void make_uniform(size_t count, size_t dim, std::mt19937 &gen, std::vector<float> &flat,
                  std::vector<std::vector<float>> &queries) {
  std::uniform_real_distribution<float> dist(0.0f, 1.0f);
  flat.assign(count * dim, 0.0f);
  for (float &value : flat) {
    value = dist(gen);
  }
  for (auto &query : queries) {
    for (float &value : query) {
      value = dist(gen);
    }
  }
}

struct Row {
  std::string dataset;
  size_t count = 0;
  size_t dim = 0;
  double build_s = 0.0;
  Latency exact;
  Latency approximate;
  double recall = 0.0;
  size_t live = 0;
  size_t index_bytes = 0;
};

Row run_case(const char *dataset, size_t count, size_t dim, bool clustered,
             size_t query_count, size_t k, size_t ef) {
  std::mt19937 gen(20261025u);
  std::vector<float> flat;
  std::vector<std::vector<float>> queries(query_count, std::vector<float>(dim));
  if (clustered) {
    make_clustered(count, dim, 200, gen, flat, queries);
  } else {
    make_uniform(count, dim, gen, flat, queries);
  }

  const size_t shards = 32;
  Cache cache(count / shards + 64, shards);

  std::vector<float> vec(dim);
  for (size_t i = 0; i < count; ++i) {
    std::copy(flat.begin() + static_cast<long>(i * dim),
              flat.begin() + static_cast<long>((i + 1) * dim), vec.begin());
    cache.vectorPut("v" + std::to_string(i), vec);
  }

  Cache::AnnConfig config;
  config.dim = dim;
  config.ef_search = ef;
  const auto build_start = Clock::now();
  cache.enable_ann_index(config);
  const double build_s =
      std::chrono::duration<double>(Clock::now() - build_start).count();

  Row row;
  row.dataset = dataset;
  row.count = count;
  row.dim = dim;
  row.build_s = build_s;

  std::vector<double> exact_ms;
  std::vector<double> approximate_ms;
  double recall_sum = 0.0;

  for (const auto &query : queries) {
    auto start = Clock::now();
    const std::vector<std::string> exact = cache.vectorSearchExact(query, static_cast<int>(k));
    exact_ms.push_back(
        std::chrono::duration<double, std::milli>(Clock::now() - start).count());

    start = Clock::now();
    const auto approximate = cache.vectorSearchScored(query, k, ef);
    approximate_ms.push_back(
        std::chrono::duration<double, std::milli>(Clock::now() - start).count());

    const std::set<std::string> truth(exact.begin(), exact.end());
    size_t hits = 0;
    for (const auto &match : approximate) {
      if (truth.count(match.first) != 0) {
        ++hits;
      }
    }
    recall_sum += truth.empty() ? 1.0 : static_cast<double>(hits) / truth.size();
  }

  row.exact = summarise(exact_ms);
  row.approximate = summarise(approximate_ms);
  row.recall = recall_sum / static_cast<double>(queries.size());
  const auto stats = cache.ann_stats();
  row.live = stats.empty() ? 0 : stats[0].live;
  row.index_bytes = stats.empty() ? 0 : stats[0].memory_bytes;
  return row;
}

void print_row(const Row &row, size_t k) {
  const double speedup = row.approximate.p50_ms > 0.0
                             ? row.exact.p50_ms / row.approximate.p50_ms
                             : 0.0;
  std::printf("%-10s %7zu %4zu | %8.2f | %9.3f %9.3f %9.3f | %8.3f %8.3f %8.3f | %7.0fx | "
              "%.4f | %7zu %6.1f\n",
              row.dataset.c_str(), row.count, row.dim, row.build_s, row.exact.p50_ms,
              row.exact.p95_ms, row.exact.p99_ms, row.approximate.p50_ms,
              row.approximate.p95_ms, row.approximate.p99_ms, speedup, row.recall, row.live,
              static_cast<double>(row.index_bytes) / (1024.0 * 1024.0));
  (void)k;
}

} // namespace

int main(int argc, char **argv) {
  const size_t k = argc > 1 ? static_cast<size_t>(std::stoul(argv[1])) : 10;
  const size_t ef = argc > 2 ? static_cast<size_t>(std::stoul(argv[2])) : 64;
  const size_t queries = argc > 3 ? static_cast<size_t>(std::stoul(argv[3])) : 20;

  const auto started = Clock::now();
  std::printf("MinKV HNSW benchmark: exact scan versus HNSW\n");
  std::printf("k=%zu ef=%zu queries=%zu threads=%u\n", k, ef, queries,
              std::thread::hardware_concurrency());
  std::printf(
      "%-10s %7s %4s | %8s | %9s %9s %9s | %8s %8s %8s | %8s | %6s | %7s %6s\n", "dataset",
      "N", "dim", "build_s", "exact50", "exact95", "exact99", "ann50", "ann95", "ann99",
      "speedup", "recall", "live", "MB");

  std::vector<Row> rows;
  rows.push_back(run_case("clustered", 20000, 128, true, queries, k, ef));
  rows.push_back(run_case("uniform", 20000, 128, false, queries, k, ef));
  rows.push_back(run_case("clustered", 100000, 128, true, queries, k, ef));
  rows.push_back(run_case("uniform", 100000, 128, false, queries, k, ef));
  rows.push_back(run_case("clustered", 10000, 768, true, queries, k, ef));

  for (const Row &row : rows) {
    print_row(row, k);
    std::fflush(stdout);
  }

  const double total_s = std::chrono::duration<double>(Clock::now() - started).count();
  std::printf("\ntotal %.1f s\n", total_s);
  return 0;
}
