/**
 * @file test_ann_consistency.cpp
 * @brief ShardedCache + HNSW consistency tests.
 *
 * This is the layer where mistakes are easiest to make: the index has to follow
 * every KV mutation, and three of the removal paths (capacity eviction, TTL
 * expiry, explicit remove) never go through vectorPut. Missing any one of them
 * makes the search return ghost keys that the cache no longer holds.
 *
 * assert-based like the rest of the repository, so do not build with -DNDEBUG.
 */

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "core/sharded_cache.h"

namespace {

using Cache = minkv::db::ShardedCache<std::string, std::string>;

std::vector<float> random_vector(std::mt19937 &gen, size_t dim, float spread) {
  std::normal_distribution<float> dist(0.0f, spread);
  std::vector<float> vec(dim);
  for (float &value : vec) {
    value = dist(gen);
  }
  return vec;
}

/// Every returned key must still be in the cache with a vector of query size.
void assert_no_ghost(Cache &cache, const std::vector<float> &query, size_t k) {
  for (const auto &hit : cache.vectorSearchScored(query, k, 64)) {
    const std::vector<float> stored = cache.vectorGet(hit.first);
    assert(stored.size() == query.size());
  }
}

} // namespace

int main() {
  std::printf("=== ShardedCache + HNSW consistency tests ===\n");
  std::mt19937 gen(20261025u);

  // ---------------------------------------------------------------
  // 1. Recall against the exact scan once the index is enabled.
  // ---------------------------------------------------------------
  {
    constexpr size_t kDim = 16;
    constexpr size_t kCount = 800;

    Cache cache(256, 8);
    Cache::AnnConfig config;
    config.dim = kDim;
    assert(cache.enable_ann_index(config));
    assert(cache.ann_enabled());

    std::vector<std::vector<float>> database;
    database.reserve(kCount);
    for (size_t i = 0; i < kCount; ++i) {
      database.push_back(random_vector(gen, kDim, 1.0f));
      cache.vectorPut("k" + std::to_string(i), database.back());
    }

    double recall_sum = 0.0;
    constexpr size_t kQueries = 20;
    for (size_t q = 0; q < kQueries; ++q) {
      std::vector<float> query = database[(q * 37) % kCount];
      for (float &value : query) {
        value += 0.05f;
      }
      const std::vector<std::string> exact = cache.vectorSearchExact(query, 10);
      assert(exact.size() == 10);
      const std::set<std::string> truth(exact.begin(), exact.end());

      const auto approximate = cache.vectorSearchScored(query, 10, 64);
      size_t hits = 0;
      for (const auto &match : approximate) {
        if (truth.count(match.first) != 0) {
          ++hits;
        }
      }
      recall_sum += static_cast<double>(hits) / truth.size();
      assert_no_ghost(cache, query, 10);
    }
    const double recall = recall_sum / kQueries;
    const auto stats = cache.ann_stats();
    std::printf("%-22s recall@10=%.4f live=%zu tombstones=%zu\n", "ann-recall", recall,
                stats.empty() ? 0 : stats[0].live, stats.empty() ? 0 : stats[0].tombstones);
    assert(recall >= 0.95);
    assert(!stats.empty());
    assert(stats[0].live == kCount);
  }

  // ---------------------------------------------------------------
  // 2. Capacity eviction: keys pushed out by LRU must leave the index too.
  // ---------------------------------------------------------------
  {
    constexpr size_t kDim = 8;
    constexpr size_t kCapacity = 64; // 16 shards x 4 entries

    Cache cache(4, 16);
    Cache::AnnConfig config;
    config.dim = kDim;
    assert(cache.enable_ann_index(config));

    for (size_t i = 0; i < 400; ++i) {
      cache.vectorPut("e" + std::to_string(i), random_vector(gen, kDim, 1.0f));
    }
    assert(cache.size() <= kCapacity);

    for (size_t q = 0; q < 30; ++q) {
      assert_no_ghost(cache, random_vector(gen, kDim, 1.0f), 10);
    }
    const auto stats = cache.ann_stats();
    std::printf("%-22s cache=%zu ann_live=%zu tombstones=%zu\n", "eviction", cache.size(),
                stats.empty() ? 0 : stats[0].live, stats.empty() ? 0 : stats[0].tombstones);
    // The index must not hold far more live entries than the cache can store.
    assert(stats[0].live <= cache.size() + 8);
  }

  // ---------------------------------------------------------------
  // 3. TTL expiry
  // ---------------------------------------------------------------
  {
    constexpr size_t kDim = 8;
    Cache cache(64, 4);
    Cache::AnnConfig config;
    config.dim = kDim;
    assert(cache.enable_ann_index(config));

    for (size_t i = 0; i < 40; ++i) {
      cache.vectorPut("t" + std::to_string(i), random_vector(gen, kDim, 1.0f), 1);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(30));

    const std::vector<float> query = random_vector(gen, kDim, 1.0f);
    const auto results = cache.vectorSearchScored(query, 10, 64);
    std::printf("%-22s expired results=%zu\n", "ttl-expiry", results.size());
    assert(results.empty()); // everything expired: nothing may be returned
  }

  // ---------------------------------------------------------------
  // 4. Explicit remove
  // ---------------------------------------------------------------
  {
    constexpr size_t kDim = 8;
    Cache cache(128, 4);
    Cache::AnnConfig config;
    config.dim = kDim;
    assert(cache.enable_ann_index(config));

    for (size_t i = 0; i < 60; ++i) {
      cache.vectorPut("r" + std::to_string(i), random_vector(gen, kDim, 1.0f));
    }
    for (size_t i = 0; i < 60; i += 2) {
      assert(cache.remove("r" + std::to_string(i)));
    }
    for (size_t q = 0; q < 20; ++q) {
      const std::vector<float> query = random_vector(gen, kDim, 1.0f);
      for (const auto &hit : cache.vectorSearchScored(query, 10, 64)) {
        const size_t id = static_cast<size_t>(std::stoul(hit.first.substr(1)));
        assert(id % 2 != 0); // only the odd ids survive
      }
      assert_no_ghost(cache, query, 10);
    }
    std::printf("%-22s removed half, no ghost\n", "remove");
  }

  // ---------------------------------------------------------------
  // 5. Dimension mismatch must fall back to the exact scan, not return less.
  // ---------------------------------------------------------------
  {
    constexpr size_t kIndexedDim = 8;
    constexpr size_t kOtherDim = 12;

    Cache cache(128, 4);
    Cache::AnnConfig config;
    config.dim = kIndexedDim;
    assert(cache.enable_ann_index(config));

    for (size_t i = 0; i < 30; ++i) {
      cache.vectorPut("a" + std::to_string(i), random_vector(gen, kIndexedDim, 1.0f));
      cache.vectorPut("b" + std::to_string(i), random_vector(gen, kOtherDim, 1.0f));
    }

    const std::vector<float> other_query = random_vector(gen, kOtherDim, 1.0f);
    const auto approximate = cache.vectorSearchScored(other_query, 10, 64);
    const std::vector<std::string> exact = cache.vectorSearchExact(other_query, 10);
    std::printf("%-22s indexed_dim=8 query_dim=12 -> %zu results (exact=%zu)\n",
                "dimension-fallback", approximate.size(), exact.size());
    assert(approximate.size() == 10);
    assert(approximate.size() == exact.size());
  }

  // ---------------------------------------------------------------
  // 6. Disabling the index returns to the exact path.
  // ---------------------------------------------------------------
  {
    constexpr size_t kDim = 8;
    Cache cache(128, 4);
    Cache::AnnConfig config;
    config.dim = kDim;
    assert(cache.enable_ann_index(config));
    for (size_t i = 0; i < 40; ++i) {
      cache.vectorPut("d" + std::to_string(i), random_vector(gen, kDim, 1.0f));
    }
    cache.disable_ann_index();
    assert(!cache.ann_enabled());
    assert(cache.ann_stats().empty());

    const std::vector<float> query = random_vector(gen, kDim, 1.0f);
    const auto results = cache.vectorSearchScored(query, 10, 64);
    assert(results.size() == 10); // still correct, just brute force now
    std::printf("%-22s disabled, exact path returned %zu\n", "disable", results.size());
  }

  std::printf("=== all consistency tests passed ===\n");
  return 0;
}

