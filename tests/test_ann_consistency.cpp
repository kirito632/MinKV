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
#include <cstdlib>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "core/sharded_cache.h"

// CHECK() vanishes under -DNDEBUG, and these tests establish their
// preconditions by calling into the cache inside it. CHECK always evaluates, so
// a Release build exercises the same paths as a Debug build instead of
// dereferencing whatever the skipped call was supposed to set up.
#define CHECK(cond)                                                                  \
  do {                                                                               \
    if (!(cond)) {                                                                   \
      std::fprintf(stderr, "CHECK failed: %s (%s:%d)\n", #cond, __FILE__, __LINE__); \
      std::abort();                                                                  \
    }                                                                                \
  } while (false)

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
    CHECK(stored.size() == query.size());
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
    CHECK(cache.enable_ann_index(config));
    CHECK(cache.ann_enabled());

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
      CHECK(exact.size() == 10);
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
    std::printf("%-22s recall@10=%.4f live=%zu tombstones=%zu\n",
                "ann-recall",
                recall,
                stats.empty() ? 0 : stats[0].live,
                stats.empty() ? 0 : stats[0].tombstones);
    CHECK(recall >= 0.95);
    CHECK(!stats.empty());
    CHECK(stats[0].live == kCount);
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
    CHECK(cache.enable_ann_index(config));

    for (size_t i = 0; i < 400; ++i) {
      cache.vectorPut("e" + std::to_string(i), random_vector(gen, kDim, 1.0f));
    }
    CHECK(cache.size() <= kCapacity);

    for (size_t q = 0; q < 30; ++q) {
      assert_no_ghost(cache, random_vector(gen, kDim, 1.0f), 10);
    }
    const auto stats = cache.ann_stats();
    std::printf("%-22s cache=%zu ann_live=%zu tombstones=%zu\n",
                "eviction",
                cache.size(),
                stats.empty() ? 0 : stats[0].live,
                stats.empty() ? 0 : stats[0].tombstones);
    // The index must not hold far more live entries than the cache can store.
    CHECK(stats[0].live <= cache.size() + 8);
  }

  // ---------------------------------------------------------------
  // 3. TTL expiry
  // ---------------------------------------------------------------
  {
    constexpr size_t kDim = 8;
    Cache cache(64, 4);
    Cache::AnnConfig config;
    config.dim = kDim;
    CHECK(cache.enable_ann_index(config));

    for (size_t i = 0; i < 40; ++i) {
      cache.vectorPut("t" + std::to_string(i), random_vector(gen, kDim, 1.0f), 1);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(30));

    const std::vector<float> query = random_vector(gen, kDim, 1.0f);
    const auto results = cache.vectorSearchScored(query, 10, 64);
    std::printf("%-22s expired results=%zu\n", "ttl-expiry", results.size());
    CHECK(results.empty()); // everything expired: nothing may be returned
  }

  // ---------------------------------------------------------------
  // 4. Explicit remove
  // ---------------------------------------------------------------
  {
    constexpr size_t kDim = 8;
    Cache cache(128, 4);
    Cache::AnnConfig config;
    config.dim = kDim;
    CHECK(cache.enable_ann_index(config));

    for (size_t i = 0; i < 60; ++i) {
      cache.vectorPut("r" + std::to_string(i), random_vector(gen, kDim, 1.0f));
    }
    for (size_t i = 0; i < 60; i += 2) {
      CHECK(cache.remove("r" + std::to_string(i)));
    }
    for (size_t q = 0; q < 20; ++q) {
      const std::vector<float> query = random_vector(gen, kDim, 1.0f);
      for (const auto &hit : cache.vectorSearchScored(query, 10, 64)) {
        const size_t id = static_cast<size_t>(std::stoul(hit.first.substr(1)));
        CHECK(id % 2 != 0); // only the odd ids survive
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
    CHECK(cache.enable_ann_index(config));

    for (size_t i = 0; i < 30; ++i) {
      cache.vectorPut("a" + std::to_string(i), random_vector(gen, kIndexedDim, 1.0f));
      cache.vectorPut("b" + std::to_string(i), random_vector(gen, kOtherDim, 1.0f));
    }

    const std::vector<float> other_query = random_vector(gen, kOtherDim, 1.0f);
    const auto approximate = cache.vectorSearchScored(other_query, 10, 64);
    const std::vector<std::string> exact = cache.vectorSearchExact(other_query, 10);
    std::printf("%-22s indexed_dim=8 query_dim=12 -> %zu results (exact=%zu)\n",
                "dimension-fallback",
                approximate.size(),
                exact.size());
    CHECK(approximate.size() == 10);
    CHECK(approximate.size() == exact.size());
  }

  // ---------------------------------------------------------------
  // 6. Disabling the index returns to the exact path.
  // ---------------------------------------------------------------
  {
    constexpr size_t kDim = 8;
    Cache cache(128, 4);
    Cache::AnnConfig config;
    config.dim = kDim;
    CHECK(cache.enable_ann_index(config));
    for (size_t i = 0; i < 40; ++i) {
      cache.vectorPut("d" + std::to_string(i), random_vector(gen, kDim, 1.0f));
    }
    cache.disable_ann_index();
    CHECK(!cache.ann_enabled());
    CHECK(cache.ann_stats().empty());

    const std::vector<float> query = random_vector(gen, kDim, 1.0f);
    const auto results = cache.vectorSearchScored(query, 10, 64);
    CHECK(results.size() == 10); // still correct, just brute force now
    std::printf("%-22s disabled, exact path returned %zu\n", "disable", results.size());
  }

  // ---------------------------------------------------------------
  // 7. Restart rebuild honours the configured key prefix.
  //
  // Without a prefix the startup scan can only judge by length, so a plain
  // blob of the right size is indexed once (documented gap, needs persisted
  // type metadata to close). With a prefix the rule becomes coherent: only
  // keys under that namespace are rebuilt.
  // ---------------------------------------------------------------
  {
    constexpr size_t kDim = 8;
    const std::string blob(kDim * sizeof(float), 'x');

    Cache cache(128, 4);
    for (size_t i = 0; i < 40; ++i) {
      cache.put("plain:" + std::to_string(i), blob);
      cache.put("emb:" + std::to_string(i), blob);
    }

    Cache::AnnConfig config;
    config.dim = kDim;
    config.key_prefix = "emb:";
    CHECK(cache.enable_ann_index(config));

    const auto stats = cache.ann_stats();
    CHECK(!stats.empty());
    std::printf("%-22s live=%zu (expected 40: only the prefixed keys)\n",
                "prefix-rebuild",
                stats[0].live);
    CHECK(stats[0].live == 40);

    // A new prefixed key may still enter through a plain put; an unprefixed one
    // may not.
    cache.put("emb:new", blob);
    cache.put("plain:new", blob);
    const auto after = cache.ann_stats();
    std::printf("%-22s live=%zu after one prefixed and one plain put\n",
                "prefix-write",
                after[0].live);
    CHECK(after[0].live == 41);
  }

  // ---------------------------------------------------------------
  // 8. Mixed writers and readers.
  //
  // The index protects reads with a shared lock and writes with the instance
  // lock, and ShardedCache re-checks candidates against the cache after
  // releasing it. Under concurrent mutation a reader must still never receive
  // a key the cache no longer holds.
  // ---------------------------------------------------------------
  {
    constexpr size_t kDim = 16;
    constexpr size_t kReadThreads = 2;
    constexpr size_t kWriteThreads = 2;
    constexpr size_t kOpsPerWriter = 300;

    Cache cache(512, 8);
    Cache::AnnConfig config;
    config.dim = kDim;
    CHECK(cache.enable_ann_index(config));

    std::vector<std::vector<float>> seeds;
    for (size_t i = 0; i < 200; ++i) {
      seeds.push_back(random_vector(gen, kDim, 1.0f));
      cache.vectorPut("seed" + std::to_string(i), seeds.back());
    }

    std::atomic<bool> stop {false};
    std::atomic<size_t> reads {0};
    std::atomic<size_t> writes {0};
    std::atomic<size_t> hits {0};
    std::atomic<size_t> vanished {0};
    std::vector<std::thread> readers;
    std::vector<std::thread> writers;

    for (size_t t = 0; t < kReadThreads; ++t) {
      readers.emplace_back([&, t]() {
        std::mt19937 local_gen(static_cast<unsigned>(t) * 104729u + 3u);
        std::uniform_int_distribution<size_t> pick(0, seeds.size() - 1);
        while (!stop.load()) {
          const auto &query = seeds[pick(local_gen)];
          for (const auto &hit : cache.vectorSearchScored(query, 5, 64)) {
            const auto value = cache.vectorGet(hit.first);
            hits.fetch_add(1);
            if (value.empty()) {
              // The search re-checks existence at its own moment; a writer can
              // still remove the key between that moment and this read. Counting
              // the window is honest and the rate is bounded after the join: if
              // filtering regressed, nearly every hit would vanish.
              vanished.fetch_add(1);
              continue;
            }
            CHECK(value.size() == kDim);

            // Only the immutable half admits an exact check. For a rewritten
            // key, the value read here may legitimately differ from the one
            // the search scored, so asserting equality would test the test.
            const size_t seed_index =
                hit.first.rfind("seed", 0) == 0
                    ? static_cast<size_t>(std::stoul(hit.first.substr(4)))
                    : 200; // non-seed keys: no assertion either way
            if (seed_index >= 100) {
              const float *current = value.data();
              double expected = 0.0;
              for (size_t d = 0; d < kDim; ++d) {
                const double delta = static_cast<double>(query[d]) - current[d];
                expected += delta * delta;
              }
              const double tolerance = 1e-3 * std::max(1.0, expected);
              CHECK(std::fabs(static_cast<double>(hit.second) - expected) <= tolerance);
            }
          }
          reads.fetch_add(1);
        }
      });
    }

    for (size_t t = 0; t < kWriteThreads; ++t) {
      writers.emplace_back([&, t]() {
        std::mt19937 local_gen(static_cast<unsigned>(t) * 7919u + 5u);
        std::normal_distribution<float> dist(0.0f, 1.0f);
        std::vector<float> vec(kDim);
        for (size_t i = 0; i < kOpsPerWriter; ++i) {
          for (float &value : vec) {
            value = dist(local_gen);
          }
          // Half vector writes, half plain values (which must not be indexed
          // but must not break anything either).
          if (i % 3 == 0) {
            // Rewrite only the lower half of the seeds. The upper half stays
            // immutable, which is what lets the readers assert an exact score
            // for it: comparing against a value that another thread is allowed
            // to replace cannot be sound.
            cache.vectorPut("seed" + std::to_string(local_gen() % 100), vec);
          } else if (i % 2 == 0) {
            cache.vectorPut("w" + std::to_string(t) + "-" + std::to_string(i), vec);
          } else {
            cache.put("p" + std::to_string(t) + "-" + std::to_string(i), "plain-value");
          }
          if (i % 50 == 0) {
            cache.remove("seed" + std::to_string(local_gen() % 200));
          }
          writes.fetch_add(1);
        }
      });
    }

    // Stop the readers once the writers are done.
    for (std::thread &writer : writers) {
      writer.join();
    }
    stop.store(true);
    for (std::thread &reader : readers) {
      reader.join();
    }

    std::printf("%-22s %zu reads / %zu writes interleaved, no ghost\n",
                "mixed-concurrency",
                reads.load(),
                writes.load());
    std::printf("%-22s %zu/%zu hits vanished after the re-check\n", "ghost-window",
                vanished.load(), hits.load());
    CHECK(hits.load() > 0);
    // The residual window is inherent to a lock-free re-check; a ghost storm is
    // not. Three orders of magnitude below "filtering is broken".
    CHECK(vanished.load() * 1000 <= hits.load());
    const auto stats = cache.ann_stats();
    CHECK(!stats.empty());
    // Every indexed key must still be resident: the removal paths keep up.
    CHECK(stats[0].live <= cache.size());
  }

  // ---------------------------------------------------------------
  // 9. Enabling under write load must refuse, not publish a partial index.
  //
  // The rebuild snapshots the cache and then spends seconds building. A write
  // that lands inside that window is invisible to the build, so publishing the
  // result would leave a silently incomplete index. The contract is therefore
  // "enable while quiescent"; the implementation enforces it instead of
  // trusting callers.
  // ---------------------------------------------------------------
  {
    constexpr size_t kDim = 32;
    Cache cache(1024, 8);
    for (size_t i = 0; i < 4000; ++i) {
      cache.vectorPut("v" + std::to_string(i), random_vector(gen, kDim, 1.0f));
    }

    std::atomic<bool> stop {false};
    std::atomic<size_t> writes {0};
    std::thread writer([&]() {
      while (!stop.load()) {
        cache.put("noise", "one-byte-value");
        cache.remove("noise");
        writes.fetch_add(1);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    });

    Cache::AnnConfig config;
    config.dim = kDim;
    const bool enabled = cache.enable_ann_index(config);

    stop.store(true);
    writer.join();

    std::printf("%-22s enable returned %s after %zu concurrent writes\n",
                "enable-race",
                enabled ? "true" : "false",
                writes.load());
    CHECK(!enabled);                  // refused, rather than silently partial
    CHECK(!cache.ann_enabled());      // nothing was published
    CHECK(cache.ann_stats().empty()); // and no half-built instance is visible
    // The cache still answers, through the exact path.
    CHECK(cache.vectorSearchScored(random_vector(gen, kDim, 1.0f), 5, 0).size() == 5);
  }

  std::printf("=== all consistency tests passed ===\n");
  return 0;
}
