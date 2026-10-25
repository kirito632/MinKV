/**
 * @file test_vector_ann_index.cpp
 * @brief VectorAnnIndex（HNSW 的 key 映射层）的正确性测试
 *
 * 覆盖：基本检索与召回、同一 key 更新语义、参数拒绝、删除、
 * Tombstone 自动重建、双度量实例并存、并发读写。
 *
 * 按仓库惯例用 assert，因此不要加 -DNDEBUG 编译。
 */

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cmath>
#include <cstdio>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "vector/vector_ann_index.h"
#include "vector/vector_ops.h"

using minkv::vector::AnnMatch;
using minkv::vector::AnnMetric;
using minkv::vector::HnswConfig;
using minkv::vector::VectorAnnIndex;

namespace {

float point_distance(AnnMetric metric, const float *a, const float *b, size_t dim) {
  if (metric == AnnMetric::COSINE) {
    return 1.0f - VectorOps::CosineSimilarity_AVX2(a, b, dim);
  }
  return VectorOps::L2DistanceSquare(a, b, dim);
}

std::vector<std::string> brute_force(const std::vector<std::vector<float>> &database,
                                     const std::vector<float> &query, size_t k,
                                     AnnMetric metric) {
  std::vector<std::pair<float, std::string>> scored;
  scored.reserve(database.size());
  for (size_t i = 0; i < database.size(); ++i) {
    scored.emplace_back(
        point_distance(metric, query.data(), database[i].data(), query.size()),
        "k" + std::to_string(i));
  }
  const size_t take = std::min(k, scored.size());
  std::partial_sort(scored.begin(), scored.begin() + static_cast<long>(take), scored.end());
  std::vector<std::string> keys;
  keys.reserve(take);
  for (size_t i = 0; i < take; ++i) {
    keys.push_back(scored[i].second);
  }
  return keys;
}

double recall_at_k(const std::vector<AnnMatch> &got, const std::vector<std::string> &exact) {
  if (exact.empty()) {
    return 1.0;
  }
  std::unordered_set<std::string> truth(exact.begin(), exact.end());
  size_t hits = 0;
  for (const AnnMatch &match : got) {
    if (truth.count(match.key) != 0) {
      ++hits;
    }
  }
  return static_cast<double>(hits) / static_cast<double>(exact.size());
}

std::vector<float> random_vector(std::mt19937 &gen, size_t dim, float center, float spread) {
  std::normal_distribution<float> dist(center, spread);
  std::vector<float> vec(dim);
  for (float &value : vec) {
    value = dist(gen);
  }
  return vec;
}

} // namespace

int main() {
  std::printf("=== VectorAnnIndex tests ===\n");

  // ---------------------------------------------------------------
  // 1. 基本检索：聚类数据的 Recall@10
  // ---------------------------------------------------------------
  constexpr size_t kDim = 64;
  constexpr size_t kCount = 2000;
  constexpr size_t kClusters = 20;

  std::mt19937 gen(20261025u);
  std::vector<std::vector<float>> centers;
  for (size_t c = 0; c < kClusters; ++c) {
    centers.push_back(random_vector(gen, kDim, 0.0f, 4.0f));
  }
  std::uniform_int_distribution<size_t> pick(0, kClusters - 1);
  std::vector<std::vector<float>> database(kCount, std::vector<float>(kDim));
  for (auto &vec : database) {
    const std::vector<float> &center = centers[pick(gen)];
    const std::vector<float> noise = random_vector(gen, kDim, 0.0f, 0.35f);
    for (size_t d = 0; d < kDim; ++d) {
      vec[d] = center[d] + noise[d];
    }
  }

  {
    HnswConfig config;
    config.dim = kDim;
    VectorAnnIndex index(config);
    for (size_t i = 0; i < kCount; ++i) {
      assert(index.upsert("k" + std::to_string(i), database[i]));
    }
    assert(index.size() == kCount);

    double recall_sum = 0.0;
    constexpr size_t kQueries = 30;
    for (size_t q = 0; q < kQueries; ++q) {
      const std::vector<float> &center = centers[pick(gen)];
      const std::vector<float> noise = random_vector(gen, kDim, 0.0f, 0.35f);
      std::vector<float> query(kDim);
      for (size_t d = 0; d < kDim; ++d) {
        query[d] = center[d] + noise[d];
      }
      const std::vector<AnnMatch> got = index.search(query, 10, 64);
      assert(got.size() == 10);
      for (size_t i = 1; i < got.size(); ++i) {
        assert(got[i - 1].score <= got[i].score);
      }
      for (const AnnMatch &match : got) {
        assert(!match.key.empty());
      }
      recall_sum += recall_at_k(got, brute_force(database, query, 10, config.metric));
    }
    const double recall = recall_sum / kQueries;
    const auto stats = index.stats();
    std::printf("%-22s recall@10=%.4f live=%zu tomb=%zu mem=%.2f MB maxlevel=%d\n",
                "basic-recall", recall, stats.live, stats.tombstones,
                static_cast<double>(stats.memory_bytes) / (1024.0 * 1024.0), stats.max_level);
    assert(recall >= 0.95);
    assert(stats.tombstones == 0);
  }

  // ---------------------------------------------------------------
  // 2. 同一 key 更新：新版本生效，旧版本退役
  // ---------------------------------------------------------------
  {
    HnswConfig config;
    config.dim = kDim;
    VectorAnnIndex index(config);

    std::vector<float> before(kDim, 1.0f);
    std::vector<float> after(kDim, -1.0f);
    assert(index.upsert("same", before));
    const std::vector<AnnMatch> first_hit = index.search(before, 1, 32);
    assert(!first_hit.empty());
    assert(first_hit.front().key == "same");

    assert(index.upsert("same", after));
    assert(index.size() == 1);

    // 旧向量已退役：同一个 key 仍可能作为「唯一的存活节点」被返回，
    // 但它与 before 的距离必须很大，说明索引里留的是新向量。
    const std::vector<AnnMatch> stale = index.search(before, 3, 32);
    for (const AnnMatch &match : stale) {
      if (match.key == "same") {
        assert(match.score > 1.0f);
      }
    }
    const std::vector<AnnMatch> new_hit = index.search(after, 1, 32);
    assert(!new_hit.empty());
    assert(new_hit.front().key == "same");
    assert(new_hit.front().score < 1e-3f);
    std::printf("%-22s replaced, stale score=%.2f new score=%.4f\n", "update-semantics",
                stale.empty() ? -1.0f : stale.front().score, new_hit.front().score);
  }

  // ---------------------------------------------------------------
  // 3. 参数拒绝
  // ---------------------------------------------------------------
  {
    HnswConfig config;
    config.dim = kDim;
    VectorAnnIndex index(config);
    assert(!index.upsert("", std::vector<float>(kDim, 0.5f)));        // 空 key
    assert(!index.upsert("short", std::vector<float>(kDim - 1, 0.5f))); // 维度不符
    assert(!index.upsert("long", std::vector<float>(kDim + 1, 0.5f)));
    assert(index.size() == 0);
    assert(index.search(std::vector<float>(kDim - 1, 0.5f), 5, 32).empty()); // 查询维度不符
    assert(index.search(std::vector<float>(kDim, 0.5f), 5, 32).empty());    // 空索引
    std::printf("%-22s rejected empty key / wrong dim\n", "validation");
  }

  // ---------------------------------------------------------------
  // 4. 删除与 Tombstone 自动重建
  // ---------------------------------------------------------------
  {
    HnswConfig config;
    config.dim = kDim;
    VectorAnnIndex index(config, 0.3); // Tombstone 超过 30% 自动 compact

    for (size_t i = 0; i < kCount; ++i) {
      index.upsert("k" + std::to_string(i), database[i]);
    }
    assert(!index.erase("does-not-exist"));

    for (size_t i = 0; i < kCount; ++i) {
      if (i % 2 == 0) {
        assert(index.erase("k" + std::to_string(i)));
      }
    }
    assert(index.size() == kCount / 2);

    const auto after_erase = index.stats();
    std::printf("%-22s live=%zu tomb=%zu ratio=%.3f (threshold=0.3)\n", "auto-compact",
                after_erase.live, after_erase.tombstones, after_erase.tombstone_ratio());
    // 真正的不变式：每次 erase 之后 Tombstone 占比不超过阈值。
    // 中途会触发过若干次重建，但重建后继续删除仍会累积少量 Tombstone，
    // 所以这里不能断言「最终一定为 0」。
    assert(after_erase.live == kCount / 2);
    assert(after_erase.tombstone_ratio() <= 0.3 + 1e-9);

    // 存活 key 仍可检索，已删除 key 不再出现
    const std::vector<float> &center = centers[pick(gen)];
    const std::vector<float> noise = random_vector(gen, kDim, 0.0f, 0.35f);
    std::vector<float> query(kDim);
    for (size_t d = 0; d < kDim; ++d) {
      query[d] = center[d] + noise[d];
    }
    for (const AnnMatch &match : index.search(query, 20, 64)) {
      const size_t id = static_cast<size_t>(std::stoul(match.key.substr(1)));
      assert(id % 2 != 0); // 只剩奇数 id
    }
  }

  // ---------------------------------------------------------------
  // 5. 双实例：L2 与 COSINE 各一个，同一批 key
  // ---------------------------------------------------------------
  {
    HnswConfig l2_config;
    l2_config.dim = kDim;
    l2_config.metric = AnnMetric::L2;

    HnswConfig cosine_config = l2_config;
    cosine_config.metric = AnnMetric::COSINE;

    VectorAnnIndex l2_index(l2_config);
    VectorAnnIndex cosine_index(cosine_config);
    for (size_t i = 0; i < 200; ++i) {
      l2_index.upsert("k" + std::to_string(i), database[i]);
      cosine_index.upsert("k" + std::to_string(i), database[i]);
    }
    assert(l2_index.size() == 200);
    assert(cosine_index.size() == 200);

    // 同方向、不同长度：COSINE 距离≈0，L2 距离显著 > 0
    std::vector<float> scaled(kDim);
    for (size_t d = 0; d < kDim; ++d) {
      scaled[d] = database[7][d] * 3.5f;
    }
    const std::vector<AnnMatch> cosine_hit = cosine_index.search(scaled, 1, 32);
    const std::vector<AnnMatch> l2_hit = l2_index.search(scaled, 1, 32);
    assert(!cosine_hit.empty() && !l2_hit.empty());
    assert(cosine_hit.front().key == "k7");
    assert(cosine_hit.front().score < 1e-4f);
    assert(l2_hit.front().key == "k7");
    assert(l2_hit.front().score > 1.0f);
    std::printf("%-22s cosine=%.6f  l2=%.3f (同一 key，两种语义)\n", "dual-instance",
                cosine_hit.front().score, l2_hit.front().score);
  }

  // ---------------------------------------------------------------
  // 6. 并发写入 + 并发查询
  // ---------------------------------------------------------------
  {
    constexpr size_t kThreads = 4;
    constexpr size_t kPerThread = 250;

    HnswConfig config;
    config.dim = 32;
    VectorAnnIndex index(config);
    index.reserve(kThreads * kPerThread);

    std::atomic<size_t> ready {0};
    std::vector<std::thread> writers;
    for (size_t worker = 0; worker < kThreads; ++worker) {
      writers.emplace_back([&index, &ready, worker]() {
        std::mt19937 gen(static_cast<unsigned>(worker) * 7919u + 11u);
        std::normal_distribution<float> dist(0.0f, 1.0f);
        std::vector<float> vec(32);
        ready.fetch_add(1);
        while (ready.load() < kThreads) {
          std::this_thread::yield();
        }
        for (size_t i = 0; i < kPerThread; ++i) {
          for (float &value : vec) {
            value = dist(gen);
          }
          if (!index.upsert("w" + std::to_string(worker) + "-" + std::to_string(i), vec)) {
            std::printf("  [warn] upsert failed\n");
          }
        }
      });
    }

    std::atomic<bool> stop {false};
    std::atomic<size_t> queries {0};
    std::thread reader([&index, &stop, &queries]() {
      std::vector<float> query(32, 0.25f);
      while (!stop.load()) {
        for (const AnnMatch &match : index.search(query, 5, 32)) {
          assert(!match.key.empty());
        }
        queries.fetch_add(1);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    });

    for (std::thread &writer : writers) {
      writer.join();
    }
    stop.store(true);
    reader.join();

    assert(index.size() == kThreads * kPerThread);
    std::printf("%-22s %zu keys, %zu concurrent searches\n", "concurrency", index.size(),
                queries.load());
  }

  // ---------------------------------------------------------------
  // 7. 批量构建
  // ---------------------------------------------------------------
  {
    constexpr size_t kBuildCount = 3000;
    HnswConfig config;
    config.dim = kDim;

    std::vector<std::string> keys;
    std::vector<float> flat(kBuildCount * kDim);
    keys.reserve(kBuildCount);
    for (size_t i = 0; i < kBuildCount; ++i) {
      keys.push_back("b" + std::to_string(i));
      for (size_t d = 0; d < kDim; ++d) {
        flat[i * kDim + d] = database[i % database.size()][d];
      }
    }

    VectorAnnIndex index(config);
    const size_t built = index.build(keys, flat.data(), kBuildCount, 4);
    assert(built == kBuildCount);
    assert(index.size() == kBuildCount);
    // 已非空时再 build 应被拒绝
    assert(index.build(keys, flat.data(), kBuildCount, 4) == 0);

    const std::vector<AnnMatch> hits = index.search(database[3], 1, 32);
    assert(!hits.empty());
    std::printf("%-22s built=%zu, search ok (top=%s)\n", "bulk-build", built,
                hits.front().key.c_str());

    index.clear();
    assert(index.size() == 0);
    assert(index.search(database[3], 1, 32).empty());
    std::printf("%-22s cleared, search empty\n", "clear");
  }

  std::printf("=== all VectorAnnIndex tests passed ===\n");
  return 0;
}
