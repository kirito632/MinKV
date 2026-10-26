/**
 * @file benchmark_hnsw.cpp
 * @brief Reproducible A/B benchmark and ef sweep for the HNSW index.
 *
 * Two modes:
 *
 *   benchmark_hnsw                 # fixed suite, one row per dataset
 *   benchmark_hnsw <k> <ef> <q>    # same suite with custom parameters
 *   benchmark_hnsw sweep <clustered|uniform> <N> <dim> <M> <ef,ef,...>
 *
 * Every number in docs/HNSW_DESIGN.md should be reachable from this binary.
 * Build it in Release; a Debug build inflates both columns by roughly 7x.
 *
 * The `dense` column exists because the ShardedCache exact path is not a fair
 * reference for the algorithm: it copies every shard's map per query, so most
 * of its time is allocation rather than distance arithmetic. `dense` scans the
 * same vectors as one contiguous array - the cost the algorithm actually has
 * to beat. Reporting only the cache path would inflate every speedup.
 *
 * The sweep also reports distance evaluations per query, which only exist when
 * the build sets -DHNSW_TRACE_DISTANCE (off by default, since counting must not
 * perturb the latency measurement):
 *
 *   cmake -S . -B build/trace -DCMAKE_BUILD_TYPE=Release -DHNSW_TRACE_DISTANCE=ON
 *
 * Two data distributions are measured on purpose:
 *   - clustered: what real embeddings look like, and the only realistic basis
 *     for a recall claim;
 *   - uniform: adversarial, distance concentration leaves HNSW almost no
 *     neighbourhood structure.
 */

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <numeric>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "core/sharded_cache.h"
#include "vector/vector_ops.h"

#ifdef HNSW_TRACE_DISTANCE
#include "vector/hnsw_index.h"
#endif

namespace {

using Cache = minkv::db::ShardedCache<std::string, std::string>;
using Clock = std::chrono::steady_clock;

constexpr size_t kShards = 32;

struct Latency {
  double p50_ms = 0.0;
  double p95_ms = 0.0;
  double p99_ms = 0.0;
};

double quantile(const std::vector<double> &sorted, double q) {
  if (sorted.empty()) {
    return 0.0;
  }
  return sorted[static_cast<size_t>(q * static_cast<double>(sorted.size() - 1))];
}

Latency summarise(std::vector<double> samples_ms) {
  Latency latency;
  if (samples_ms.empty()) {
    return latency;
  }
  std::sort(samples_ms.begin(), samples_ms.end());
  latency.p50_ms = quantile(samples_ms, 0.50);
  latency.p95_ms = quantile(samples_ms, 0.95);
  latency.p99_ms = quantile(samples_ms, 0.99);
  return latency;
}

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

void fill_cache(Cache &cache, const std::vector<float> &flat, size_t count, size_t dim) {
  std::vector<float> vec(dim);
  for (size_t i = 0; i < count; ++i) {
    std::copy(flat.begin() + static_cast<long>(i * dim),
              flat.begin() + static_cast<long>((i + 1) * dim), vec.begin());
    cache.vectorPut("v" + std::to_string(i), vec);
  }
}

/// Lower bound on what any exact search must cost: one contiguous scan.
Latency dense_exact(const std::vector<float> &flat, size_t count, size_t dim,
                    const std::vector<std::vector<float>> &queries, size_t k) {
  std::vector<double> samples;
  samples.reserve(queries.size());
  std::vector<std::pair<float, uint32_t>> scored(count);
  for (const auto &query : queries) {
    const auto start = Clock::now();
    for (size_t i = 0; i < count; ++i) {
      scored[i] = {VectorOps::L2DistanceSquare(query.data(), flat.data() + i * dim, dim),
                   static_cast<uint32_t>(i)};
    }
    std::partial_sort(scored.begin(), scored.begin() + static_cast<long>(k), scored.end());
    samples.push_back(
        std::chrono::duration<double, std::milli>(Clock::now() - start).count());
  }
  return summarise(samples);
}

struct Row {
  std::string dataset;
  size_t count = 0;
  size_t dim = 0;
  double build_s = 0.0;
  Latency dense;
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

  Cache cache(count / kShards + 64, kShards);
  fill_cache(cache, flat, count, dim);

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
  row.dense = dense_exact(flat, count, dim, queries, k);

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

void print_row(const Row &row) {
  const double vs_cache = row.approximate.p50_ms > 0.0
                              ? row.exact.p50_ms / row.approximate.p50_ms
                              : 0.0;
  const double vs_dense = row.approximate.p50_ms > 0.0
                              ? row.dense.p50_ms / row.approximate.p50_ms
                              : 0.0;
  std::printf("%-10s %7zu %4zu | %6.1f | %8.2f %7.2f | %8.2f %7.2f | %8.3f %7.3f | %6.1fx "
              "%5.1fx | %.4f | %5zu %5.1f\n",
              row.dataset.c_str(), row.count, row.dim, row.build_s, row.dense.p50_ms,
              row.dense.p99_ms, row.exact.p50_ms, row.exact.p99_ms, row.approximate.p50_ms,
              row.approximate.p99_ms, vs_cache, vs_dense, row.recall, row.live,
              static_cast<double>(row.index_bytes) / (1024.0 * 1024.0));
}

std::vector<size_t> parse_list(const char *text) {
  std::vector<size_t> values;
  std::string current;
  for (const char *cursor = text; *cursor != '\0'; ++cursor) {
    if (*cursor == ',') {
      if (!current.empty()) {
        values.push_back(static_cast<size_t>(std::stoul(current)));
        current.clear();
      }
    } else {
      current.push_back(*cursor);
    }
  }
  if (!current.empty()) {
    values.push_back(static_cast<size_t>(std::stoul(current)));
  }
  std::sort(values.begin(), values.end());
  return values;
}

int run_sweep(bool clustered, size_t count, size_t dim, size_t M,
              const std::vector<size_t> &ef_list, size_t query_count, size_t k) {
  const char *name = clustered ? "clustered" : "uniform";
  std::mt19937 gen(20261025u);
  std::vector<float> flat;
  std::vector<std::vector<float>> queries(query_count, std::vector<float>(dim));
  if (clustered) {
    make_clustered(count, dim, 200, gen, flat, queries);
  } else {
    make_uniform(count, dim, gen, flat, queries);
  }

  Cache cache(count / kShards + 64, kShards);
  fill_cache(cache, flat, count, dim);

  // Exact answers do not depend on M or ef: measure once.
  std::vector<std::vector<std::string>> truth;
  std::vector<double> exact_ms;
  truth.reserve(queries.size());
  for (const auto &query : queries) {
    const auto start = Clock::now();
    truth.push_back(cache.vectorSearchExact(query, static_cast<int>(k)));
    exact_ms.push_back(
        std::chrono::duration<double, std::milli>(Clock::now() - start).count());
  }
  const Latency exact = summarise(exact_ms);
  const Latency dense = dense_exact(flat, count, dim, queries, k);

  Cache::AnnConfig config;
  config.dim = dim;
  config.M = M;
  config.M0 = M * 2;
  const auto build_start = Clock::now();
  if (!cache.enable_ann_index(config)) {
    std::printf("enable_ann_index failed\n");
    return 1;
  }
  const double build_s = std::chrono::duration<double>(Clock::now() - build_start).count();

  std::printf("\n%s N=%zu dim=%zu M=%zu k=%zu queries=%zu build=%.2fs\n", name, count, dim,
              M, k, query_count, build_s);
  std::printf("  exact(cache) P50=%.3f P99=%.3f ms | exact(dense) P50=%.3f P99=%.3f ms\n",
              exact.p50_ms, exact.p99_ms, dense.p50_ms, dense.p99_ms);
  std::printf("%5s | %9s | %8s %8s %8s | %10s | %8s %8s | %9s %9s\n", "ef", "recall@k", "P50",
              "P95", "P99", "dist/query", "vsCache", "vsDense", "d50", "d99");

  for (size_t ef : ef_list) {
    std::vector<double> ann_ms;
    double recall_sum = 0.0;
    ann_ms.reserve(queries.size());
#ifdef HNSW_TRACE_DISTANCE
    // Reset once for the whole batch, then divide: resetting per query and
    // reading the counter afterwards only ever reported the last query.
    minkv::vector::trace_reset_distance_calls();
#endif
    for (size_t q = 0; q < queries.size(); ++q) {
      const auto start = Clock::now();
      const auto hits = cache.vectorSearchScored(queries[q], k, ef);
      ann_ms.push_back(
          std::chrono::duration<double, std::milli>(Clock::now() - start).count());

      const std::set<std::string> expected(truth[q].begin(), truth[q].end());
      size_t matched = 0;
      for (const auto &hit : hits) {
        if (expected.count(hit.first) != 0) {
          ++matched;
        }
      }
      recall_sum += expected.empty() ? 1.0 : static_cast<double>(matched) / expected.size();
    }

    const Latency ann = summarise(ann_ms);
    const double recall = recall_sum / static_cast<double>(queries.size());
#ifdef HNSW_TRACE_DISTANCE
    char distance_text[32];
    std::snprintf(distance_text, sizeof(distance_text), "%.0f",
                  static_cast<double>(minkv::vector::trace_distance_calls()) /
                      static_cast<double>(queries.size()));
#else
    const char *distance_text = "n/a";
#endif
    std::printf("%5zu | %9.4f | %8.3f %8.3f %8.3f | %10s | %7.1fx %7.1fx | %8.1fx %8.1fx\n",
                ef, recall, ann.p50_ms, ann.p95_ms, ann.p99_ms, distance_text,
                ann.p50_ms > 0.0 ? exact.p50_ms / ann.p50_ms : 0.0,
                ann.p50_ms > 0.0 ? dense.p50_ms / ann.p50_ms : 0.0,
                ann.p99_ms > 0.0 ? exact.p99_ms / ann.p99_ms : 0.0,
                ann.p99_ms > 0.0 ? dense.p99_ms / ann.p99_ms : 0.0);
    std::fflush(stdout);
  }
  return 0;
}

/// Concurrent query throughput: does the read path actually scale?
int run_concurrent(bool clustered, size_t count, size_t dim, size_t M, size_t ef,
                   size_t threads, size_t queries_per_thread, size_t k) {
  const char *name = clustered ? "clustered" : "uniform";
  std::mt19937 gen(20261025u);
  std::vector<float> flat;
  std::vector<std::vector<float>> queries(threads * queries_per_thread,
                                          std::vector<float>(dim));
  if (clustered) {
    make_clustered(count, dim, 200, gen, flat, queries);
  } else {
    make_uniform(count, dim, gen, flat, queries);
  }

  Cache cache(count / kShards + 64, kShards);
  fill_cache(cache, flat, count, dim);

  Cache::AnnConfig config;
  config.dim = dim;
  config.M = M;
  config.M0 = M * 2;
  config.ef_search = ef;
  if (!cache.enable_ann_index(config)) {
    std::printf("enable_ann_index failed\n");
    return 1;
  }

  std::vector<std::vector<double>> per_thread(threads);
  std::atomic<size_t> ready {0};
  const auto started = Clock::now();
  std::vector<std::thread> workers;
  workers.reserve(threads);
  for (size_t t = 0; t < threads; ++t) {
    workers.emplace_back([&, t]() {
      per_thread[t].reserve(queries_per_thread);
      ready.fetch_add(1);
      while (ready.load() < threads) {
        std::this_thread::yield();
      }
      for (size_t i = 0; i < queries_per_thread; ++i) {
        const auto &query = queries[t * queries_per_thread + i];
        const auto start = Clock::now();
        const auto hits = cache.vectorSearchScored(query, k, ef);
        per_thread[t].push_back(
            std::chrono::duration<double, std::milli>(Clock::now() - start).count());
        if (hits.empty()) {
          std::printf("  [warn] empty result\n");
        }
      }
    });
  }
  for (std::thread &worker : workers) {
    worker.join();
  }
  const double seconds = std::chrono::duration<double>(Clock::now() - started).count();

  std::vector<double> all;
  for (auto &samples : per_thread) {
    all.insert(all.end(), samples.begin(), samples.end());
  }
  const Latency latency = summarise(all);
  const double total = static_cast<double>(threads * queries_per_thread);
  std::printf("%-10s N=%zu dim=%zu M=%zu ef=%zu | threads=%-2zu | P50 %7.1f us  P99 %8.1f us | "
              "%8.0f QPS\n",
              name, count, dim, M, ef, threads, latency.p50_ms * 1000.0,
              latency.p99_ms * 1000.0, total / seconds);
  std::fflush(stdout);
  return 0;
}

// ---------------------------------------------------------------------
// Real-data mode: reads <int32 rows><int32 dim><float32 payload> files.
// ---------------------------------------------------------------------

struct RawData {
  size_t count = 0;
  size_t dim = 0;
  std::vector<float> flat;
};

bool load_raw(const char *path, RawData &out, size_t max_rows) {
  std::ifstream input(path, std::ios::binary);
  if (!input) {
    std::printf("cannot open %s\n", path);
    return false;
  }
  int32_t rows = 0;
  int32_t dim = 0;
  input.read(reinterpret_cast<char *>(&rows), sizeof(rows));
  input.read(reinterpret_cast<char *>(&dim), sizeof(dim));
  if (!input || rows <= 0 || dim <= 0) {
    std::printf("bad header in %s\n", path);
    return false;
  }
  const size_t keep =
      max_rows == 0 ? static_cast<size_t>(rows) : std::min<size_t>(max_rows, rows);
  out.count = keep;
  out.dim = static_cast<size_t>(dim);
  out.flat.assign(keep * out.dim, 0.0f);
  input.read(reinterpret_cast<char *>(out.flat.data()),
             static_cast<std::streamsize>(keep * out.dim * sizeof(float)));
  return static_cast<size_t>(input.gcount()) == keep * out.dim * sizeof(float);
}

int run_real(const char *train_path, const char *query_path, size_t M,
             const std::vector<size_t> &ef_list, size_t max_train, size_t max_queries,
             size_t k) {
  RawData train;
  RawData queries;
  if (!load_raw(train_path, train, max_train)) {
    return 1;
  }
  if (!load_raw(query_path, queries, max_queries)) {
    return 1;
  }
  if (train.dim != queries.dim) {
    std::printf("dimension mismatch: %zu vs %zu\n", train.dim, queries.dim);
    return 1;
  }

  Cache cache(train.count / kShards + 64, kShards);
  fill_cache(cache, train.flat, train.count, train.dim);

  // Ground truth from one contiguous scan; also the honest latency rival.
  std::vector<std::vector<uint32_t>> truth;
  std::vector<double> kth_distance;
  truth.reserve(queries.count);
  kth_distance.reserve(queries.count);
  std::vector<double> dense_ms;
  std::vector<std::pair<float, uint32_t>> scored(train.count);
  for (size_t q = 0; q < queries.count; ++q) {
    const float *query = queries.flat.data() + q * train.dim;
    const auto start = Clock::now();
    for (size_t i = 0; i < train.count; ++i) {
      scored[i] = {VectorOps::L2DistanceSquare(query, train.flat.data() + i * train.dim,
                                               train.dim),
                   static_cast<uint32_t>(i)};
    }
    std::partial_sort(scored.begin(), scored.begin() + static_cast<long>(k), scored.end());
    dense_ms.push_back(
        std::chrono::duration<double, std::milli>(Clock::now() - start).count());
    std::vector<uint32_t> ids;
    ids.reserve(k);
    for (size_t i = 0; i < k; ++i) {
      ids.push_back(scored[i].second);
    }
    truth.push_back(std::move(ids));
    // Distance of the k-th exact neighbour. Overlapping text chunks and
    // duplicate images produce exact ties, and comparing id sets would count
    // those ties as misses, so the tolerance recall below uses this bound.
    kth_distance.push_back(static_cast<double>(scored[k - 1].first));
  }
  const Latency dense = summarise(dense_ms);

  Cache::AnnConfig config;
  config.dim = train.dim;
  config.M = M;
  config.M0 = M * 2;
  const auto build_start = Clock::now();
  if (!cache.enable_ann_index(config)) {
    std::printf("enable_ann_index failed\n");
    return 1;
  }
  const double build_s =
      std::chrono::duration<double>(Clock::now() - build_start).count();

  std::printf("\nreal data: %s N=%zu dim=%zu queries=%zu M=%zu k=%zu build=%.2fs\n",
              train_path, train.count, train.dim, queries.count, M, k, build_s);
  std::printf("dense exact P50=%.3f P99=%.3f ms\n", dense.p50_ms, dense.p99_ms);
  std::printf("%5s | %9s %9s | %8s %8s %8s | %10s | %8s %8s\n", "ef", "recall@k", "tol@1%",
              "P50", "P95", "P99", "dist/query", "vsDense", "dP99");

  for (size_t ef : ef_list) {
    std::vector<double> ann_ms;
    double recall_sum = 0.0;
    double tolerance_sum = 0.0;
#ifdef HNSW_TRACE_DISTANCE
    minkv::vector::trace_reset_distance_calls();
#endif
    for (size_t q = 0; q < queries.count; ++q) {
      const float *query = queries.flat.data() + q * train.dim;
      const auto start = Clock::now();
      const auto hits = cache.vectorSearchScored(
          std::vector<float>(query, query + train.dim), k, ef);
      ann_ms.push_back(
          std::chrono::duration<double, std::milli>(Clock::now() - start).count());

      const std::set<uint32_t> expected(truth[q].begin(), truth[q].end());
      size_t matched = 0;
      size_t within_tolerance = 0;
      const double bound = kth_distance[q] * 1.01 + 1e-6;
      for (const auto &hit : hits) {
        if (expected.count(static_cast<uint32_t>(std::stoul(hit.first.substr(1)))) != 0) {
          ++matched;
        }
        // A hit is equally correct if its distance matches the k-th exact one.
        if (static_cast<double>(hit.second) <= bound) {
          ++within_tolerance;
        }
      }
      recall_sum += expected.empty() ? 1.0 : static_cast<double>(matched) / expected.size();
      tolerance_sum += static_cast<double>(within_tolerance) / static_cast<double>(k);
    }

    const Latency ann = summarise(ann_ms);
    const double recall = recall_sum / static_cast<double>(queries.count);
    const double tolerance_recall = tolerance_sum / static_cast<double>(queries.count);
#ifdef HNSW_TRACE_DISTANCE
    char distance_text[32];
    std::snprintf(distance_text, sizeof(distance_text), "%.0f",
                  static_cast<double>(minkv::vector::trace_distance_calls()) /
                      static_cast<double>(queries.count));
#else
    const char *distance_text = "n/a";
#endif
    std::printf("%5zu | %9.4f %9.4f | %8.3f %8.3f %8.3f | %10s | %7.1fx %7.1fx\n", ef, recall,
                tolerance_recall, ann.p50_ms, ann.p95_ms, ann.p99_ms, distance_text,
                ann.p50_ms > 0.0 ? dense.p50_ms / ann.p50_ms : 0.0,
                ann.p99_ms > 0.0 ? dense.p99_ms / ann.p99_ms : 0.0);
    std::fflush(stdout);
  }
  return 0;
}

} // namespace

int main(int argc, char **argv) {
  if (argc > 1 && std::string(argv[1]) == "real") {
    // benchmark_hnsw real <train.bin> <query.bin> <M> <ef,ef,...> [max_train] [max_q]
    if (argc < 6) {
      std::printf("usage: %s real <train.bin> <query.bin> <M> <ef,ef,...> [max_train] [max_q]\n",
                  argv[0]);
      return 2;
    }
    const size_t max_train = argc > 6 ? static_cast<size_t>(std::stoul(argv[6])) : 0;
    const size_t max_queries = argc > 7 ? static_cast<size_t>(std::stoul(argv[7])) : 20;
    return run_real(argv[2], argv[3], static_cast<size_t>(std::stoul(argv[4])),
                    parse_list(argv[5]), max_train, max_queries, 10);
  }

  if (argc > 1 && std::string(argv[1]) == "concurrent") {
    // benchmark_hnsw concurrent <clustered|uniform> <N> <dim> <M> <ef> <threads> <q>
    if (argc < 9) {
      std::printf("usage: %s concurrent <clustered|uniform> <N> <dim> <M> <ef> <threads> <q>\n",
                  argv[0]);
      return 2;
    }
    const bool clustered = std::string(argv[2]) == "clustered";
    return run_concurrent(clustered, static_cast<size_t>(std::stoul(argv[3])),
                          static_cast<size_t>(std::stoul(argv[4])),
                          static_cast<size_t>(std::stoul(argv[5])),
                          static_cast<size_t>(std::stoul(argv[6])),
                          static_cast<size_t>(std::stoul(argv[7])),
                          static_cast<size_t>(std::stoul(argv[8])), 10);
  }

  if (argc > 1 && std::string(argv[1]) == "sweep") {
    if (argc < 7) {
      std::printf("usage: %s sweep <clustered|uniform> <N> <dim> <M> <ef,ef,...>\n",
                  argv[0]);
      return 2;
    }
    const bool clustered = std::string(argv[2]) == "clustered";
    const size_t count = static_cast<size_t>(std::stoul(argv[3]));
    const size_t dim = static_cast<size_t>(std::stoul(argv[4]));
    const size_t M = static_cast<size_t>(std::stoul(argv[5]));
    const std::vector<size_t> ef_list = parse_list(argv[6]);
    return run_sweep(clustered, count, dim, M, ef_list, 20, 10);
  }

  const size_t k = argc > 1 ? static_cast<size_t>(std::stoul(argv[1])) : 10;
  const size_t ef = argc > 2 ? static_cast<size_t>(std::stoul(argv[2])) : 64;
  const size_t queries = argc > 3 ? static_cast<size_t>(std::stoul(argv[3])) : 20;

  const auto started = Clock::now();
  std::printf("MinKV HNSW benchmark: exact scan versus HNSW\n");
  std::printf("k=%zu ef=%zu queries=%zu threads=%u\n", k, ef, queries,
              std::thread::hardware_concurrency());
  std::printf("%-10s %7s %4s | %6s | %8s %7s | %8s %7s | %8s %7s | %6s %5s | %6s | %5s %5s\n",
              "dataset", "N", "dim", "build", "dense50", "dense99", "cache50", "cache99",
              "ann50", "ann99", "vsCach", "vsDens", "recall", "live", "MB");

  std::vector<Row> rows;
  rows.push_back(run_case("clustered", 20000, 128, true, queries, k, ef));
  rows.push_back(run_case("uniform", 20000, 128, false, queries, k, ef));
  rows.push_back(run_case("clustered", 100000, 128, true, queries, k, ef));
  rows.push_back(run_case("uniform", 100000, 128, false, queries, k, ef));
  rows.push_back(run_case("clustered", 10000, 768, true, queries, k, ef));

  for (const Row &row : rows) {
    print_row(row);
    std::fflush(stdout);
  }

  const double total_s = std::chrono::duration<double>(Clock::now() - started).count();
  std::printf("\ntotal %.1f s\n", total_s);
  std::printf("vsCach: speedup over the ShardedCache exact path (allocation bound)\n");
  std::printf("vsDens: speedup over one contiguous exact scan (the algorithm's real rival)\n");
  return 0;
}
