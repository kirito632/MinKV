#include "hnsw_index.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <queue>
#include <stdexcept>

#include "vector_ops.h"

namespace minkv {
namespace vector {
namespace {

/// 最近优先的候选队列（堆顶是距离最小的元素）
struct NearestFirst {
  bool operator()(const AnnHit &lhs, const AnnHit &rhs) const {
    return lhs.score > rhs.score;
  }
};

/// 最远优先的结果队列（堆顶是当前最差的那个，便于 O(log ef) 淘汰）
struct FarthestFirst {
  bool operator()(const AnnHit &lhs, const AnnHit &rhs) const {
    return lhs.score < rhs.score;
  }
};

/**
 * @brief 每次 search_layer 的访问标记
 *
 * 按节点数分配的 vector<uint8_t> 会让每次查询付出 O(N) 的清零成本，
 * 在 10 万节点上就是每次查询多 100KB 的 memset。这里用「时间戳 + 按需扩容」，
 * 使一次查询的标记成本降到 O(实际访问节点数)。
 *
 * thread_local 让并发查询（共享锁下的只读搜索）互不干扰。
 */
struct VisitScratch {
  std::vector<uint32_t> stamp;
  uint32_t epoch = 0;
};

VisitScratch &acquire_scratch(size_t nodes) {
  static thread_local VisitScratch scratch;
  if (scratch.stamp.size() < nodes) {
    // 新扩容的元素填 0，而 epoch 从 1 开始，因此天然是「未访问」
    scratch.stamp.resize(nodes, 0);
  }
  if (++scratch.epoch == 0) { // 溢出回绕：清空后从 1 重新开始
    std::fill(scratch.stamp.begin(), scratch.stamp.end(), 0);
    scratch.epoch = 1;
  }
  return scratch;
}

bool mark_visited(VisitScratch &scratch, uint32_t id) {
  if (scratch.stamp[id] == scratch.epoch) {
    return false;
  }
  scratch.stamp[id] = scratch.epoch;
  return true;
}

float point_distance(AnnMetric metric, const float *a, const float *b, size_t dim) {
  if (metric == AnnMetric::COSINE) {
    return 1.0f - ::VectorOps::CosineSimilarity_AVX2(a, b, dim);
  }
  return ::VectorOps::L2DistanceSquare(a, b, dim);
}

} // namespace

HnswIndex::HnswIndex(const HnswConfig &config)
    : config_(config), rng_state_(config.seed == 0 ? 1 : config.seed) {
  if (config_.dim == 0) {
    throw std::invalid_argument("HnswIndex: dim must be greater than 0");
  }
  if (config_.M == 0) {
    config_.M = 16;
  }
  if (config_.M0 == 0) {
    config_.M0 = config_.M * 2;
  }
  if (config_.ef_construction == 0) {
    config_.ef_construction = 200;
  }
  level_multiplier_ = 1.0 / std::log(static_cast<double>(config_.M));
}

float HnswIndex::distance(const float *a, const float *b) const {
  return point_distance(config_.metric, a, b, config_.dim);
}

float HnswIndex::distance_to(uint32_t id, const float *query) const {
  return distance(data_.data() + static_cast<size_t>(id) * config_.dim, query);
}

const float *HnswIndex::vector_of(uint32_t id) const {
  return data_.data() + static_cast<size_t>(id) * config_.dim;
}

int HnswIndex::random_level() {
  // splitmix64：没有全局状态，给定 seed 可完全复现
  uint64_t z = (rng_state_ += 0x9e3779b97f4a7c15ull);
  z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ull;
  z = (z ^ (z >> 27)) * 0x94d049bb133111ebull;
  z ^= z >> 31;
  // 映射到 (0, 1]，避免 log(0)
  const double u = static_cast<double>((z >> 11) + 1) / 9007199254740994.0;
  return static_cast<int>(-std::log(u) * level_multiplier_);
}

uint32_t HnswIndex::greedy_descend(uint32_t entry, const float *query, int level) const {
  uint32_t current = entry;
  float best = distance_to(current, query);
  bool improved = true;
  while (improved) {
    improved = false;
    if (static_cast<size_t>(level) >= links_[current].size()) {
      break;
    }
    for (uint32_t neighbor : links_[current][static_cast<size_t>(level)]) {
      const float candidate = distance_to(neighbor, query);
      if (candidate < best) {
        best = candidate;
        current = neighbor;
        improved = true;
      }
    }
  }
  return current;
}

std::vector<AnnHit> HnswIndex::search_layer(const float *query, uint32_t entry, size_t ef,
                                            int level) const {
  std::vector<AnnHit> results;
  if (entry == kInvalidId || ef == 0) {
    return results;
  }

  VisitScratch &scratch = acquire_scratch(levels_.size());
  std::priority_queue<AnnHit, std::vector<AnnHit>, NearestFirst> candidates;
  std::priority_queue<AnnHit, std::vector<AnnHit>, FarthestFirst> nearest;

  const float entry_distance = distance_to(entry, query);
  candidates.push({entry, entry_distance});
  if (alive(entry)) {
    nearest.push({entry, entry_distance});
  }
  mark_visited(scratch, entry);

  const float infinity = std::numeric_limits<float>::max();
  while (!candidates.empty()) {
    const AnnHit current = candidates.top();
    const float worst = nearest.size() < ef ? infinity : nearest.top().score;
    if (current.score > worst) {
      break; // 最近的候选都比已知结果里最差的还远，可以收工
    }
    candidates.pop();

    if (static_cast<size_t>(level) >= links_[current.id].size()) {
      continue;
    }
    for (uint32_t neighbor : links_[current.id][static_cast<size_t>(level)]) {
      if (!mark_visited(scratch, neighbor)) {
        continue;
      }
      const float distance = distance_to(neighbor, query);
      const float bound = nearest.size() < ef ? infinity : nearest.top().score;
      if (distance > bound) {
        continue;
      }
      candidates.push({neighbor, distance});
      if (alive(neighbor)) {
        nearest.push({neighbor, distance});
        if (nearest.size() > ef) {
          nearest.pop();
        }
      }
    }
  }

  results.reserve(nearest.size());
  while (!nearest.empty()) {
    results.push_back(nearest.top());
    nearest.pop();
  }
  std::sort(results.begin(), results.end(), [](const AnnHit &lhs, const AnnHit &rhs) {
    return lhs.score < rhs.score;
  });
  return results;
}

std::vector<uint32_t> HnswIndex::select_neighbors(const std::vector<AnnHit> &candidates,
                                                  size_t m) const {
  // HNSW 论文 Algorithm 4：优先保留「与已选邻居也不近」的候选，
  // 让每个方向的边都留得住。直接取最近的 m 个会让图在聚类边界上断裂。
  std::vector<uint32_t> chosen;
  chosen.reserve(std::min(m, candidates.size()));
  for (const AnnHit &candidate : candidates) {
    if (chosen.size() >= m) {
      break;
    }
    bool diverse = true;
    for (uint32_t picked : chosen) {
      const float between = distance_to(picked, vector_of(candidate.id));
      if (between < candidate.score) {
        diverse = false;
        break;
      }
    }
    if (diverse) {
      chosen.push_back(candidate.id);
    }
  }
  // 启发式可能选不满，用最近的候选补足，保证连通性
  for (const AnnHit &candidate : candidates) {
    if (chosen.size() >= m) {
      break;
    }
    if (std::find(chosen.begin(), chosen.end(), candidate.id) == chosen.end()) {
      chosen.push_back(candidate.id);
    }
  }
  return chosen;
}

void HnswIndex::prune(uint32_t owner, int level) {
  auto &neighbors = links_[owner][static_cast<size_t>(level)];
  const size_t limit = max_degree(level);
  if (neighbors.size() <= limit) {
    return;
  }

  // 这里必须复用建图时的启发式，而不能只保留最近的 limit 个邻居。
  // 只按距离裁剪会把「跨簇的桥接边」在加入列表的瞬间就挤掉：稠密簇内部
  // 永远有更近的候选，于是整张图会碎成若干互不连通的簇，表现为
  // ef 调到多大 recall 都不涨、部分查询完全落错簇。
  const float *owner_vector = vector_of(owner);
  std::vector<AnnHit> candidates;
  candidates.reserve(neighbors.size());
  for (uint32_t neighbor : neighbors) {
    candidates.push_back({neighbor, distance_to(neighbor, owner_vector)});
  }
  std::sort(candidates.begin(), candidates.end(),
            [](const AnnHit &lhs, const AnnHit &rhs) { return lhs.score < rhs.score; });
  neighbors = select_neighbors(candidates, limit);
}

void HnswIndex::link_node(uint32_t id, const float *vec) {
  const int node_level = levels_[id];

  uint32_t current = entry_point_;
  for (int level = max_level_; level > node_level; --level) {
    current = greedy_descend(current, vec, level);
  }

  for (int level = std::min(node_level, max_level_); level >= 0; --level) {
    const std::vector<AnnHit> candidates = search_layer(vec, current, config_.ef_construction,
                                                        level);
    const size_t limit = max_degree(level);
    const std::vector<uint32_t> chosen = select_neighbors(candidates, limit);
    links_[id][static_cast<size_t>(level)] = chosen;

    for (uint32_t neighbor : chosen) {
      auto &back_links = links_[neighbor][static_cast<size_t>(level)];
      if (std::find(back_links.begin(), back_links.end(), id) == back_links.end()) {
        back_links.push_back(id);
      }
      if (back_links.size() > limit) {
        prune(neighbor, level);
      }
    }
    if (!candidates.empty()) {
      current = candidates.front().id;
    }
  }
}

uint32_t HnswIndex::insert(const float *vec) {
  if (vec == nullptr) {
    return kInvalidId;
  }

  const uint32_t id = static_cast<uint32_t>(levels_.size());
  data_.insert(data_.end(), vec, vec + config_.dim);
  levels_.push_back(random_level());
  deleted_.push_back(0);
  links_.emplace_back();
  links_.back().resize(static_cast<size_t>(levels_.back()) + 1);
  ++live_;

  if (entry_point_ == kInvalidId) {
    entry_point_ = id;
    max_level_ = levels_.back();
    return id;
  }

  link_node(id, vector_of(id));

  if (levels_[id] > max_level_) {
    max_level_ = levels_[id];
    entry_point_ = id;
  }
  return id;
}

bool HnswIndex::erase(uint32_t id) {
  if (id >= levels_.size() || deleted_[id] != 0) {
    return false;
  }
  deleted_[id] = 1;
  --live_;

  if (id == entry_point_) {
    // 入口带 Tombstone 会让后续插入从死节点开始，这里换成最高层的存活节点
    entry_point_ = kInvalidId;
    max_level_ = -1;
    for (uint32_t candidate = 0; candidate < levels_.size(); ++candidate) {
      if (deleted_[candidate] != 0) {
        continue;
      }
      if (levels_[candidate] > max_level_) {
        max_level_ = levels_[candidate];
        entry_point_ = candidate;
      }
    }
  }
  return true;
}

std::vector<AnnHit> HnswIndex::knn(const float *query, size_t k, size_t ef) const {
  std::vector<AnnHit> results;
  if (query == nullptr || k == 0 || live_ == 0 || entry_point_ == kInvalidId) {
    return results;
  }

  uint32_t current = entry_point_;
  for (int level = max_level_; level >= 1; --level) {
    current = greedy_descend(current, query, level);
  }

  results = search_layer(query, current, std::max(ef, k), 0);
  if (results.size() > k) {
    results.resize(k);
  }
  return results;
}

void HnswIndex::reset_graph() {
  for (uint32_t id = 0; id < levels_.size(); ++id) {
    links_[id].clear();
    if (deleted_[id] == 0) {
      links_[id].resize(static_cast<size_t>(levels_[id]) + 1);
    }
  }
  entry_point_ = kInvalidId;
  max_level_ = -1;
}

void HnswIndex::compact() {
  if (live_ == 0) {
    reset_graph();
    return;
  }
  reset_graph();

  for (uint32_t id = 0; id < levels_.size(); ++id) {
    if (deleted_[id] != 0) {
      continue;
    }
    if (entry_point_ == kInvalidId) {
      entry_point_ = id;
      max_level_ = levels_[id];
      continue;
    }
    link_node(id, vector_of(id));
    if (levels_[id] > max_level_) {
      max_level_ = levels_[id];
      entry_point_ = id;
    }
  }
}

size_t HnswIndex::reachable_count() const {
  if (entry_point_ == kInvalidId) {
    return 0;
  }
  std::vector<uint8_t> seen(levels_.size(), 0);
  std::vector<uint32_t> pending;
  pending.push_back(entry_point_);
  seen[entry_point_] = 1;
  size_t count = 1;
  while (!pending.empty()) {
    const uint32_t current = pending.back();
    pending.pop_back();
    if (links_[current].empty()) {
      continue;
    }
    for (uint32_t neighbor : links_[current][0]) {
      if (seen[neighbor] == 0) {
        seen[neighbor] = 1;
        ++count;
        pending.push_back(neighbor);
      }
    }
  }
  return count;
}

size_t HnswIndex::memory_bytes() const {
  size_t bytes = data_.capacity() * sizeof(float);
  bytes += levels_.capacity() * sizeof(int);
  bytes += deleted_.capacity() * sizeof(uint8_t);
  bytes += links_.capacity() * sizeof(NodeLinks);
  for (const NodeLinks &node : links_) {
    bytes += node.capacity() * sizeof(std::vector<uint32_t>);
    for (const std::vector<uint32_t> &level_links : node) {
      bytes += level_links.capacity() * sizeof(uint32_t);
    }
  }
  return bytes;
}

} // namespace vector
} // namespace minkv
