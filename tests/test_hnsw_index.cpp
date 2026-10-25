/**
 * @file test_hnsw_index.cpp
 * @brief HnswIndex 的正确性与召回率测试
 *
 * 验收口径：
 * 1. 随机均匀数据、聚类数据上的 Recall@10 对暴力搜索达标；
 * 2. 删除后结果里不出现已删除 id，图重建后召回恢复；
 * 3. 同 seed 可复现；
 * 4. 余弦度量下「同方向向量」距离接近 0。
 *
 * 按仓库惯例用 assert，因此不要加 -DNDEBUG 编译。
 */

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <random>
#include <stdexcept>
#include <unordered_set>
#include <vector>

#include "vector/hnsw_index.h"
#include "vector/vector_ops.h"

using minkv::vector::AnnHit;
using minkv::vector::AnnMetric;
using minkv::vector::HnswConfig;
using minkv::vector::HnswIndex;

namespace {

using Clock = std::chrono::steady_clock;

std::vector<float> random_vector(std::mt19937 &gen, size_t dim, float center = 0.0f,
                                 float spread = 1.0f) {
  std::normal_distribution<float> dist(center, spread);
  std::vector<float> vec(dim);
  for (size_t i = 0; i < dim; ++i) {
    vec[i] = dist(gen);
  }
  return vec;
}

float point_distance(AnnMetric metric, const float *a, const float *b, size_t dim) {
  if (metric == AnnMetric::COSINE) {
    return 1.0f - VectorOps::CosineSimilarity_AVX2(a, b, dim);
  }
  return VectorOps::L2DistanceSquare(a, b, dim);
}

/// 精确答案：只扫存活向量，结果按距离升序
std::vector<uint32_t> brute_force(const std::vector<std::vector<float>> &database,
                                  const std::vector<uint8_t> &deleted,
                                  const std::vector<float> &query, size_t k,
                                  AnnMetric metric) {
  std::vector<std::pair<float, uint32_t>> scored;
  scored.reserve(database.size());
  for (uint32_t id = 0; id < database.size(); ++id) {
    if (!deleted.empty() && deleted[id] != 0) {
      continue;
    }
    scored.emplace_back(point_distance(metric, query.data(), database[id].data(),
                                       query.size()),
                        id);
  }
  const size_t take = std::min(k, scored.size());
  std::partial_sort(scored.begin(), scored.begin() + static_cast<long>(take), scored.end());
  std::vector<uint32_t> ids;
  ids.reserve(take);
  for (size_t i = 0; i < take; ++i) {
    ids.push_back(scored[i].second);
  }
  return ids;
}

double recall_at_k(const std::vector<uint32_t> &approximate,
                   const std::vector<uint32_t> &exact) {
  if (exact.empty()) {
    return 1.0;
  }
  std::unordered_set<uint32_t> truth(exact.begin(), exact.end());
  size_t hits = 0;
  for (uint32_t id : approximate) {
    if (truth.count(id) != 0) {
      ++hits;
    }
  }
  return static_cast<double>(hits) / static_cast<double>(exact.size());
}

void report(const char *name, double recall, double build_ms, double query_us,
            size_t live, size_t tombstones, size_t memory_bytes) {
  std::printf("%-22s recall@10=%.4f  build=%7.1f ms  query=%8.1f us  live=%-6zu tomb=%-6zu mem=%.1f MB\n",
              name, recall, build_ms, query_us, live, tombstones,
              static_cast<double>(memory_bytes) / (1024.0 * 1024.0));
  std::fflush(stdout);
}

} // namespace

int main() {
  std::printf("=== HnswIndex tests ===\n");

  // ---------------------------------------------------------------
  // 1. 随机均匀数据：ANN 最不友好的分布
  // ---------------------------------------------------------------
  {
    constexpr size_t kCount = 3000;
    constexpr size_t kDim = 48;
    constexpr size_t kK = 10;
    constexpr size_t kEf = 80;

    std::mt19937 gen(20261025u);
    std::vector<std::vector<float>> database;
    database.reserve(kCount);
    for (size_t i = 0; i < kCount; ++i) {
      database.push_back(random_vector(gen, kDim));
    }

    HnswConfig config;
    config.dim = kDim;
    HnswIndex index(config);

    const auto build_start = Clock::now();
    for (const auto &vec : database) {
      assert(index.insert(vec.data()) != HnswIndex::kInvalidId);
    }
    const double build_ms =
        std::chrono::duration<double, std::milli>(Clock::now() - build_start).count();
    assert(index.size() == kCount);

    double recall_sum = 0.0;
    double query_us_sum = 0.0;
    constexpr size_t kQueries = 50;
    for (size_t q = 0; q < kQueries; ++q) {
      const std::vector<float> query = random_vector(gen, kDim);
      const auto start = Clock::now();
      const std::vector<AnnHit> hits = index.knn(query.data(), kK, kEf);
      query_us_sum += std::chrono::duration<double, std::micro>(Clock::now() - start).count();
      assert(hits.size() == kK);
      for (size_t i = 1; i < hits.size(); ++i) {
        assert(hits[i - 1].score <= hits[i].score); // 结果必须有序
      }
      std::vector<uint32_t> got;
      got.reserve(hits.size());
      for (const AnnHit &hit : hits) {
        got.push_back(hit.id);
      }
      recall_sum += recall_at_k(got, brute_force(database, {}, query, kK, config.metric));
    }
    const double recall = recall_sum / static_cast<double>(kQueries);
    report("uniform-random", recall, build_ms, query_us_sum / kQueries, index.size(),
           index.tombstone_count(), index.memory_bytes());
    assert(recall >= 0.90);
  }

  // ---------------------------------------------------------------
  // 2. 聚类数据：更接近真实 embedding 的分布
  // ---------------------------------------------------------------
  {
    constexpr size_t kClusters = 20;
    constexpr size_t kCount = 3000;
    constexpr size_t kDim = 48;
    constexpr size_t kK = 10;
    constexpr size_t kEf = 64;

    std::mt19937 gen(7u);
    std::vector<std::vector<float>> centers;
    for (size_t c = 0; c < kClusters; ++c) {
      centers.push_back(random_vector(gen, kDim, 0.0f, 4.0f));
    }

    std::uniform_int_distribution<size_t> pick(0, kClusters - 1);
    std::vector<std::vector<float>> database;
    database.reserve(kCount);
    for (size_t i = 0; i < kCount; ++i) {
      std::vector<float> vec = centers[pick(gen)];
      const std::vector<float> noise = random_vector(gen, kDim, 0.0f, 0.35f);
      for (size_t d = 0; d < kDim; ++d) {
        vec[d] += noise[d];
      }
      database.push_back(std::move(vec));
    }

    HnswConfig config;
    config.dim = kDim;
    HnswIndex index(config);
    const auto build_start = Clock::now();
    for (const auto &vec : database) {
      index.insert(vec.data());
    }
    const double build_ms =
        std::chrono::duration<double, std::milli>(Clock::now() - build_start).count();

    double recall_sum = 0.0;
    double query_us_sum = 0.0;
    constexpr size_t kQueries = 50;
    for (size_t q = 0; q < kQueries; ++q) {
      std::vector<float> query = centers[pick(gen)];
      const std::vector<float> noise = random_vector(gen, kDim, 0.0f, 0.35f);
      for (size_t d = 0; d < kDim; ++d) {
        query[d] += noise[d];
      }
      const auto start = Clock::now();
      const std::vector<AnnHit> hits = index.knn(query.data(), kK, kEf);
      query_us_sum += std::chrono::duration<double, std::micro>(Clock::now() - start).count();
      std::vector<uint32_t> got;
      for (const AnnHit &hit : hits) {
        got.push_back(hit.id);
      }
      recall_sum += recall_at_k(got, brute_force(database, {}, query, kK, config.metric));
    }
    const double recall = recall_sum / static_cast<double>(kQueries);
    report("clustered", recall, build_ms, query_us_sum / kQueries, index.size(),
           index.tombstone_count(), index.memory_bytes());
    assert(recall >= 0.95);
  }

  // ---------------------------------------------------------------
  // 3. 删除 + 4. 重建
  // ---------------------------------------------------------------
  {
    constexpr size_t kCount = 3000;
    constexpr size_t kDim = 48;
    constexpr size_t kK = 10;

    std::mt19937 gen(99u);
    std::vector<std::vector<float>> database;
    database.reserve(kCount);
    for (size_t i = 0; i < kCount; ++i) {
      database.push_back(random_vector(gen, kDim));
    }

    HnswConfig config;
    config.dim = kDim;
    HnswIndex index(config);
    for (const auto &vec : database) {
      index.insert(vec.data());
    }

    // 删掉 40%，模拟 LRU 淘汰与 TTL 过期把向量从缓存里拿走
    std::vector<uint8_t> deleted(kCount, 0);
    std::mt19937 kill_gen(4242u);
    std::uniform_int_distribution<size_t> pick(0, kCount - 1);
    size_t killed = 0;
    uint32_t first_killed = HnswIndex::kInvalidId;
    while (killed < kCount * 4 / 10) {
      const size_t id = pick(kill_gen);
      if (deleted[id] != 0) {
        continue;
      }
      assert(index.erase(static_cast<uint32_t>(id)));
      deleted[id] = 1;
      if (first_killed == HnswIndex::kInvalidId) {
        first_killed = static_cast<uint32_t>(id);
      }
      ++killed;
    }
    assert(first_killed != HnswIndex::kInvalidId);
    assert(!index.erase(first_killed)); // 重复删除必须返回 false
    assert(!index.erase(static_cast<uint32_t>(kCount + 5))); // 越界 id 必须返回 false
    assert(index.size() == kCount - killed);

    auto measure = [&](const char *name, size_t ef) {
      double recall_sum = 0.0;
      constexpr size_t kQueries = 40;
      for (size_t q = 0; q < kQueries; ++q) {
        const std::vector<float> query = random_vector(gen, kDim);
        const std::vector<AnnHit> hits = index.knn(query.data(), kK, ef);
        std::vector<uint32_t> got;
        for (const AnnHit &hit : hits) {
          assert(deleted[hit.id] == 0); // 绝不允许返回已删除的 id
          got.push_back(hit.id);
        }
        recall_sum += recall_at_k(got, brute_force(database, deleted, query, kK,
                                                   config.metric));
      }
      const double recall = recall_sum / static_cast<double>(kQueries);
      report(name, recall, 0.0, 0.0, index.size(), index.tombstone_count(),
             index.memory_bytes());
      return recall;
    };

    const double before = measure("after-40%-delete", 64);
    assert(before >= 0.90); // Tombstone 会污染导航，容许下降

    index.compact();
    const double after = measure("after-compact", 64);
    assert(after >= 0.95);
    // 注意：compact 不保证提升 recall。被删节点仍作为导航路标留在图里，
    // 有时反而比「只留存活节点」的图更容易路由；这里只要求重建后质量不塌。
    assert(after + 0.05 >= before);
  }

  // ---------------------------------------------------------------
  // 5. 可复现性
  // ---------------------------------------------------------------
  {
    constexpr size_t kCount = 500;
    constexpr size_t kDim = 16;
    std::mt19937 gen(11u);
    std::vector<std::vector<float>> database;
    for (size_t i = 0; i < kCount; ++i) {
      database.push_back(random_vector(gen, kDim));
    }

    HnswConfig config;
    config.dim = kDim;
    HnswIndex a(config);
    HnswIndex b(config);
    for (const auto &vec : database) {
      a.insert(vec.data());
      b.insert(vec.data());
    }
    assert(a.max_level() == b.max_level());

    const std::vector<float> query = random_vector(gen, kDim);
    const std::vector<AnnHit> hits_a = a.knn(query.data(), 10, 32);
    const std::vector<AnnHit> hits_b = b.knn(query.data(), 10, 32);
    assert(hits_a.size() == hits_b.size());
    for (size_t i = 0; i < hits_a.size(); ++i) {
      assert(hits_a[i].id == hits_b[i].id);
      assert(hits_a[i].score == hits_b[i].score);
    }
    std::printf("%-22s reproducible (level=%d)\n", "determinism", a.max_level());
  }

  // ---------------------------------------------------------------
  // 6. 余弦度量
  // ---------------------------------------------------------------
  {
    constexpr size_t kDim = 32;
    std::mt19937 gen(5u);

    HnswConfig config;
    config.dim = kDim;
    config.metric = AnnMetric::COSINE;
    HnswIndex index(config);

    std::vector<std::vector<float>> database;
    for (size_t i = 0; i < 200; ++i) {
      database.push_back(random_vector(gen, kDim));
      index.insert(database.back().data());
    }

    // 同方向、不同长度：余弦距离应≈0，且排第一
    std::vector<float> query(kDim);
    for (size_t d = 0; d < kDim; ++d) {
      query[d] = database[17][d] * 3.5f;
    }
    const std::vector<AnnHit> hits = index.knn(query.data(), 1, 32);
    assert(!hits.empty());
    assert(hits.front().id == 17u);
    assert(hits.front().score < 1e-4f);
    std::printf("%-22s self-cosine distance=%.6f\n", "cosine-metric", hits.front().score);

    // L2 度量下同一个查询不会给出 0 距离（长度差异被计入）
    HnswConfig l2 = config;
    l2.metric = AnnMetric::L2;
    HnswIndex l2_index(l2);
    for (const auto &vec : database) {
      l2_index.insert(vec.data());
    }
    const std::vector<AnnHit> l2_hits = l2_index.knn(query.data(), 1, 32);
    assert(!l2_hits.empty());
    assert(l2_hits.front().score > 1e-3f);
    std::printf("%-22s self-l2 distance=%.6f (长度差异被计入)\n", "l2-metric",
                l2_hits.front().score);
  }

  // ---------------------------------------------------------------
  // 7. 非法配置
  // ---------------------------------------------------------------
  {
    bool threw = false;
    try {
      HnswConfig bad;
      bad.dim = 0;
      HnswIndex index(bad);
    } catch (const std::invalid_argument &) {
      threw = true;
    }
    assert(threw);
    std::printf("%-22s rejected dim=0\n", "config-validation");
  }

  std::printf("=== all HnswIndex tests passed ===\n");
  return 0;
}
