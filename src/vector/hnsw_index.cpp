#include "hnsw_index.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <queue>
#include <stdexcept>
#include <thread>

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
 */
struct VisitScratch {
  std::vector<uint32_t> stamp;
  uint32_t epoch = 0;
};

VisitScratch &acquire_scratch(size_t nodes) {
  static thread_local VisitScratch scratch;
  if (scratch.stamp.size() < nodes) {
    scratch.stamp.resize(nodes, 0); // 新元素是 0，而 epoch 从 1 开始，天然「未访问」
  }
  if (++scratch.epoch == 0) {
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

#ifdef HNSW_TRACE_DISTANCE
// Counting is per thread on purpose: graph construction runs on several
// threads, but the sweep only reads the counter around single-threaded
// queries, so a shared counter would both race and mix both phases.
thread_local uint64_t g_distance_calls = 0;
#endif

float point_distance(AnnMetric metric, const float *a, const float *b, size_t dim) {
  if (metric == AnnMetric::COSINE) {
    return 1.0f - ::VectorOps::CosineSimilarity_AVX2(a, b, dim);
  }
  return ::VectorOps::L2DistanceSquare(a, b, dim);
}

} // namespace

#ifdef HNSW_TRACE_DISTANCE
uint64_t trace_distance_calls() { return g_distance_calls; }
void trace_reset_distance_calls() { g_distance_calls = 0; }
#endif

HnswIndex::HnswIndex(const HnswConfig &config)
    : config_(config),
      stripes_(new std::mutex[kStripeCount]),
      rng_state_(config.seed == 0 ? 1 : config.seed) {
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
#ifdef HNSW_TRACE_DISTANCE
  ++g_distance_calls;
#endif
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
  const double u = static_cast<double>((z >> 11) + 1) / 9007199254740994.0;
  return static_cast<int>(-std::log(u) * level_multiplier_);
}

void HnswIndex::ensure_capacity_locked(size_t needed) {
  if (needed <= capacity_) {
    return;
  }
  const size_t cap = std::max<size_t>(std::max<size_t>(needed, capacity_ * 2), 64);

  std::vector<float> grown(cap * config_.dim, 0.0f);
  std::copy(data_.begin(), data_.end(), grown.begin());
  data_.swap(grown);
  levels_.resize(cap, 0);
  deleted_.resize(cap, 0);
  upper_links_.resize(cap);

  std::unique_ptr<std::atomic<uint32_t>[]> slots(
      new std::atomic<uint32_t>[cap * config_.M0]);
  std::unique_ptr<std::atomic<uint32_t>[]> counts(new std::atomic<uint32_t>[cap]);
  for (size_t i = 0; i < cap * config_.M0; ++i) {
    slots[i].store(kInvalidNeighbor, std::memory_order_relaxed);
  }
  for (size_t i = 0; i < cap; ++i) {
    counts[i].store(0, std::memory_order_relaxed);
  }
  for (size_t node = 0; node < capacity_; ++node) {
    const uint32_t count = l0_counts_[node].load(std::memory_order_relaxed);
    counts[node].store(count, std::memory_order_relaxed);
    for (uint32_t i = 0; i < count; ++i) {
      slots[node * config_.M0 + i].store(
          l0_slots_[node * config_.M0 + i].load(std::memory_order_relaxed),
          std::memory_order_relaxed);
    }
  }
  l0_slots_ = std::move(slots);
  l0_counts_ = std::move(counts);
  capacity_ = cap;
}

void HnswIndex::write_node_locked(uint32_t id, const float *vec, int level) {
  std::copy(vec, vec + config_.dim,
            data_.begin() + static_cast<std::ptrdiff_t>(static_cast<size_t>(id) * config_.dim));
  levels_[id] = level;
  deleted_[id] = 0;
  l0_counts_[id].store(0, std::memory_order_relaxed);
}

std::vector<uint32_t> HnswIndex::read_links(uint32_t node, int level) const {
  std::vector<uint32_t> neighbors;
  if (level == 0) {
    const uint32_t count = l0_counts_[node].load(std::memory_order_acquire);
    neighbors.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
      const uint32_t neighbor =
          l0_slots_[static_cast<size_t>(node) * config_.M0 + i].load(std::memory_order_relaxed);
      if (neighbor != kInvalidNeighbor) {
        neighbors.push_back(neighbor);
      }
    }
    return neighbors;
  }
  std::lock_guard<std::mutex> guard(stripe_for(node));
  const size_t index = static_cast<size_t>(level - 1);
  if (index < upper_links_[node].size()) {
    neighbors = upper_links_[node][index];
  }
  return neighbors;
}

void HnswIndex::write_links_locked(uint32_t node, int level,
                                   const std::vector<uint32_t> &ids) {
  if (level == 0) {
    const size_t limit = std::min(ids.size(), config_.M0);
    for (size_t i = 0; i < limit; ++i) {
      l0_slots_[static_cast<size_t>(node) * config_.M0 + i].store(ids[i],
                                                                 std::memory_order_relaxed);
    }
    l0_counts_[node].store(static_cast<uint32_t>(limit), std::memory_order_release);
    return;
  }
  auto &node_levels = upper_links_[node];
  if (node_levels.size() < static_cast<size_t>(level)) {
    node_levels.resize(static_cast<size_t>(level));
  }
  node_levels[static_cast<size_t>(level) - 1] = ids;
}

uint32_t HnswIndex::greedy_descend(uint32_t entry, const float *query, int level) const {
  uint32_t current = entry;
  float best = distance_to(current, query);
  bool improved = true;
  while (improved) {
    improved = false;
    for (uint32_t neighbor : read_links(current, level)) {
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
  const size_t nodes = next_id_.load(std::memory_order_relaxed);
  VisitScratch &scratch = acquire_scratch(nodes);

  std::priority_queue<AnnHit, std::vector<AnnHit>, NearestFirst> candidates;
  std::priority_queue<AnnHit, std::vector<AnnHit>, FarthestFirst> nearest;
  const float infinity = std::numeric_limits<float>::max();

  const auto consider = [&](uint32_t neighbor) {
    if (neighbor == kInvalidNeighbor || neighbor >= nodes || !mark_visited(scratch, neighbor)) {
      return;
    }
    const float distance = distance_to(neighbor, query);
    const float bound = nearest.size() < ef ? infinity : nearest.top().score;
    if (distance > bound) {
      return;
    }
    candidates.push({neighbor, distance});
    if (alive(neighbor)) {
      nearest.push({neighbor, distance});
      if (nearest.size() > ef) {
        nearest.pop();
      }
    }
  };

  const auto expand = [&](uint32_t node) {
    if (level == 0) {
      const uint32_t count = l0_counts_[node].load(std::memory_order_acquire);
      for (uint32_t i = 0; i < count; ++i) {
        consider(l0_slots_[static_cast<size_t>(node) * config_.M0 + i].load(
            std::memory_order_relaxed));
      }
      return;
    }
    for (uint32_t neighbor : read_links(node, level)) {
      consider(neighbor);
    }
  };

  const float entry_distance = distance_to(entry, query);
  candidates.push({entry, entry_distance});
  if (alive(entry)) {
    nearest.push({entry, entry_distance});
  }
  mark_visited(scratch, entry);
  expand(entry);

  while (!candidates.empty()) {
    const AnnHit current = candidates.top();
    const float worst = nearest.size() < ef ? infinity : nearest.top().score;
    if (current.score > worst) {
      break; // 最近的候选都比已知结果里最差的还远，可以收工
    }
    candidates.pop();
    expand(current.id);
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

void HnswIndex::prune_locked(uint32_t owner, int level,
                             std::vector<uint32_t> candidates) {
  std::sort(candidates.begin(), candidates.end());
  candidates.erase(std::unique(candidates.begin(), candidates.end()), candidates.end());

  const float *owner_vector = vector_of(owner);
  std::vector<AnnHit> scored;
  scored.reserve(candidates.size());
  for (uint32_t candidate : candidates) {
    if (candidate == owner) {
      continue;
    }
    scored.push_back({candidate, distance_to(candidate, owner_vector)});
  }
  std::sort(scored.begin(), scored.end(),
            [](const AnnHit &lhs, const AnnHit &rhs) { return lhs.score < rhs.score; });
  write_links_locked(owner, level, select_neighbors(scored, max_degree(level)));
}

void HnswIndex::add_back_link(uint32_t owner, int level, uint32_t id) {
  std::lock_guard<std::mutex> guard(stripe_for(owner));

  if (level == 0) {
    const uint32_t count = l0_counts_[owner].load(std::memory_order_relaxed);
    bool present = false;
    for (uint32_t i = 0; i < count; ++i) {
      if (l0_slots_[static_cast<size_t>(owner) * config_.M0 + i].load(
              std::memory_order_relaxed) == id) {
        present = true;
        break;
      }
    }
    if (present) {
      return;
    }
    if (count < config_.M0) {
      l0_slots_[static_cast<size_t>(owner) * config_.M0 + count].store(
          id, std::memory_order_relaxed);
      l0_counts_[owner].store(count + 1, std::memory_order_release);
      return;
    }
    std::vector<uint32_t> merged;
    merged.reserve(count + 1);
    for (uint32_t i = 0; i < count; ++i) {
      merged.push_back(
          l0_slots_[static_cast<size_t>(owner) * config_.M0 + i].load(std::memory_order_relaxed));
    }
    merged.push_back(id);
    prune_locked(owner, 0, std::move(merged));
    return;
  }

  auto &node_levels = upper_links_[owner];
  if (node_levels.size() < static_cast<size_t>(level)) {
    node_levels.resize(static_cast<size_t>(level));
  }
  auto &list = node_levels[static_cast<size_t>(level) - 1];
  if (std::find(list.begin(), list.end(), id) != list.end()) {
    return;
  }
  if (list.size() < max_degree(level)) {
    list.push_back(id);
    return;
  }
  std::vector<uint32_t> merged = list;
  merged.push_back(id);
  prune_locked(owner, level, std::move(merged));
}

bool HnswIndex::claim_entry_if_empty(uint32_t id) {
  if (entry_point_.load(std::memory_order_relaxed) != kInvalidId) {
    return false;
  }
  std::lock_guard<std::mutex> guard(entry_mu_);
  if (entry_point_.load(std::memory_order_relaxed) != kInvalidId) {
    return false;
  }
  entry_point_.store(id, std::memory_order_relaxed);
  max_level_.store(levels_[id], std::memory_order_relaxed);
  return true;
}

void HnswIndex::link_node(uint32_t id) {
  if (claim_entry_if_empty(id)) {
    return;
  }

  const float *vec = vector_of(id);
  const int node_level = levels_[id];

  uint32_t current;
  int top;
  {
    std::lock_guard<std::mutex> guard(entry_mu_);
    current = entry_point_.load(std::memory_order_relaxed);
    top = max_level_.load(std::memory_order_relaxed);
  }
  if (current == kInvalidId || current == id) {
    return;
  }

  for (int level = top; level > node_level; --level) {
    current = greedy_descend(current, vec, level);
  }

  for (int level = std::min(node_level, top); level >= 0; --level) {
    const std::vector<AnnHit> candidates =
        search_layer(vec, current, config_.ef_construction, level);
    const std::vector<uint32_t> chosen = select_neighbors(candidates, max_degree(level));
    {
      std::lock_guard<std::mutex> guard(stripe_for(id));
      write_links_locked(id, level, chosen);
    }
    for (uint32_t neighbor : chosen) {
      if (neighbor != id) {
        add_back_link(neighbor, level, id);
      }
    }
    if (!candidates.empty()) {
      current = candidates.front().id;
    }
  }

  if (node_level > top) {
    std::lock_guard<std::mutex> guard(entry_mu_);
    if (node_level > max_level_.load(std::memory_order_relaxed)) {
      max_level_.store(node_level, std::memory_order_relaxed);
      entry_point_.store(id, std::memory_order_relaxed);
    }
  }
}

uint32_t HnswIndex::insert(const float *vec) {
  if (vec == nullptr) {
    return kInvalidId;
  }

  uint32_t id = kInvalidId;
  {
    std::unique_lock<std::shared_mutex> lock(structure_mu_);
    const size_t next = next_id_.load(std::memory_order_relaxed);
    ensure_capacity_locked(next + 1);
    id = static_cast<uint32_t>(next);
    write_node_locked(id, vec, random_level());
    next_id_.store(next + 1, std::memory_order_relaxed);
    live_.fetch_add(1, std::memory_order_relaxed);
  }

  // 建边只需要结构稳定，不需要独占：并发的 insert 各自持有不同 id，
  // 邻居链表由条带锁保护。
  std::shared_lock<std::shared_mutex> read_lock(structure_mu_);
  link_node(id);
  return id;
}

bool HnswIndex::erase(uint32_t id) {
  std::unique_lock<std::shared_mutex> lock(structure_mu_);
  if (id >= next_id_.load(std::memory_order_relaxed) || deleted_[id] != 0) {
    return false;
  }
  deleted_[id] = 1;
  live_.fetch_sub(1, std::memory_order_relaxed);

  if (entry_point_.load(std::memory_order_relaxed) == id) {
    // 入口带 Tombstone 会让后续插入从死节点开始，这里换成最高层的存活节点
    std::lock_guard<std::mutex> guard(entry_mu_);
    entry_point_.store(kInvalidId, std::memory_order_relaxed);
    max_level_.store(-1, std::memory_order_relaxed);
    const size_t nodes = next_id_.load(std::memory_order_relaxed);
    for (uint32_t candidate = 0; candidate < nodes; ++candidate) {
      if (deleted_[candidate] != 0) {
        continue;
      }
      if (levels_[candidate] > max_level_.load(std::memory_order_relaxed)) {
        max_level_.store(levels_[candidate], std::memory_order_relaxed);
        entry_point_.store(candidate, std::memory_order_relaxed);
      }
    }
  }
  return true;
}

void HnswIndex::reserve(size_t node_count) {
  std::unique_lock<std::shared_mutex> lock(structure_mu_);
  ensure_capacity_locked(node_count);
}

void HnswIndex::build_parallel(const float *flat, size_t count, size_t threads) {
  if (flat == nullptr || count == 0) {
    return;
  }

  // 层级序列在单线程里一次性生成，保证与顺序构建抽到同一组层数。
  std::vector<int> levels(count);
  for (size_t i = 0; i < count; ++i) {
    levels[i] = random_level();
  }

  size_t base = 0;
  {
    std::unique_lock<std::shared_mutex> lock(structure_mu_);
    base = next_id_.load(std::memory_order_relaxed);
    ensure_capacity_locked(base + count);
    for (size_t i = 0; i < count; ++i) {
      const uint32_t id = static_cast<uint32_t>(base + i);
      std::copy(flat + i * config_.dim, flat + (i + 1) * config_.dim,
                data_.begin() + static_cast<std::ptrdiff_t>(static_cast<size_t>(id) *
                                                            config_.dim));
      levels_[id] = levels[i];
      deleted_[id] = 0;
      l0_counts_[id].store(0, std::memory_order_relaxed);
    }
    next_id_.store(base + count, std::memory_order_relaxed);
    live_.fetch_add(count, std::memory_order_relaxed);
  }

  std::shared_lock<std::shared_mutex> read_lock(structure_mu_);
  link_node(static_cast<uint32_t>(base)); // 先单线程立好入口，避免出现孤立节点
  if (count == 1) {
    return;
  }

  const size_t workers = std::max<size_t>(
      1, std::min(threads == 0 ? 1 : threads, count - 1));
  if (workers == 1) {
    for (size_t i = 1; i < count; ++i) {
      link_node(static_cast<uint32_t>(base + i));
    }
    return;
  }

  std::atomic<size_t> cursor {1};
  std::vector<std::thread> pool;
  pool.reserve(workers);
  for (size_t worker = 0; worker < workers; ++worker) {
    pool.emplace_back([this, base, count, &cursor]() {
      for (;;) {
        const size_t index = cursor.fetch_add(1, std::memory_order_relaxed);
        if (index >= count) {
          break;
        }
        link_node(static_cast<uint32_t>(base + index));
      }
    });
  }
  for (std::thread &thread : pool) {
    thread.join();
  }
}

std::vector<AnnHit> HnswIndex::knn(const float *query, size_t k, size_t ef) const {
  std::vector<AnnHit> results;
  if (query == nullptr || k == 0) {
    return results;
  }
  std::shared_lock<std::shared_mutex> lock(structure_mu_);

  const uint32_t entry = entry_point_.load(std::memory_order_relaxed);
  int top = max_level_.load(std::memory_order_relaxed);
  if (entry == kInvalidId || top < 0 || size() == 0) {
    return results;
  }

  uint32_t current = entry;
  for (int level = top; level >= 1; --level) {
    current = greedy_descend(current, query, level);
  }

  results = search_layer(query, current, std::max(ef, k), 0);
  if (results.size() > k) {
    results.resize(k);
  }
  return results;
}

size_t HnswIndex::reachable_count() const {
  std::shared_lock<std::shared_mutex> lock(structure_mu_);
  const uint32_t entry = entry_point_.load(std::memory_order_relaxed);
  if (entry == kInvalidId) {
    return 0;
  }
  const size_t nodes = next_id_.load(std::memory_order_relaxed);
  std::vector<uint8_t> seen(nodes, 0);
  std::vector<uint32_t> pending;
  pending.push_back(entry);
  seen[entry] = 1;
  size_t count = 1;
  while (!pending.empty()) {
    const uint32_t current = pending.back();
    pending.pop_back();
    const uint32_t neighbors = l0_counts_[current].load(std::memory_order_acquire);
    for (uint32_t i = 0; i < neighbors; ++i) {
      const uint32_t neighbor =
          l0_slots_[static_cast<size_t>(current) * config_.M0 + i].load(
              std::memory_order_relaxed);
      if (neighbor != kInvalidNeighbor && neighbor < nodes && seen[neighbor] == 0) {
        seen[neighbor] = 1;
        ++count;
        pending.push_back(neighbor);
      }
    }
  }
  return count;
}

void HnswIndex::compact() {
  std::unique_lock<std::shared_mutex> lock(structure_mu_);
  const size_t nodes = next_id_.load(std::memory_order_relaxed);
  entry_point_.store(kInvalidId, std::memory_order_relaxed);
  max_level_.store(-1, std::memory_order_relaxed);

  for (size_t node = 0; node < nodes; ++node) {
    l0_counts_[node].store(0, std::memory_order_relaxed);
    for (size_t i = 0; i < config_.M0; ++i) {
      l0_slots_[node * config_.M0 + i].store(kInvalidNeighbor, std::memory_order_relaxed);
    }
    upper_links_[node].clear();
    if (levels_[node] > 0) {
      upper_links_[node].resize(static_cast<size_t>(levels_[node]));
    }
  }

  for (uint32_t id = 0; id < nodes; ++id) {
    if (deleted_[id] != 0) {
      continue;
    }
    if (claim_entry_if_empty(id)) {
      continue;
    }
    link_node(id);
  }
}

size_t HnswIndex::memory_bytes() const {
  std::shared_lock<std::shared_mutex> lock(structure_mu_);
  size_t bytes = data_.capacity() * sizeof(float);
  bytes += levels_.capacity() * sizeof(int);
  bytes += deleted_.capacity() * sizeof(uint8_t);
  bytes += capacity_ * config_.M0 * sizeof(uint32_t); // level 0 槽位
  bytes += capacity_ * sizeof(uint32_t);              // level 0 计数
  bytes += upper_links_.capacity() * sizeof(std::vector<std::vector<uint32_t>>);
  // 上层链表由条带锁保护（不是结构锁），这里必须逐节点加锁读取，
  // 否则会与并发 add_back_link 的 push_back 撞上。
  for (size_t node = 0; node < capacity_; ++node) {
    if (levels_[node] <= 0) {
      continue;
    }
    std::lock_guard<std::mutex> guard(stripe_for(static_cast<uint32_t>(node)));
    bytes += upper_links_[node].capacity() * sizeof(std::vector<uint32_t>);
    for (const auto &level_links : upper_links_[node]) {
      bytes += level_links.capacity() * sizeof(uint32_t);
    }
  }
  return bytes;
}

} // namespace vector
} // namespace minkv
