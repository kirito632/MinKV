#include "vector_ann_index.h"

#include <algorithm>
#include <stdexcept>

namespace minkv {
namespace vector {

VectorAnnIndex::VectorAnnIndex(const HnswConfig &config, double compact_ratio)
    : config_(config),
      index_(std::make_shared<HnswIndex>(config)),
      compact_ratio_(compact_ratio > 0.0 ? compact_ratio : 0.3) {}

bool VectorAnnIndex::upsert(const std::string &key, const std::vector<float> &vec) {
  if (key.empty() || vec.size() != config_.dim) {
    return false;
  }

  // 取出当前这一代索引后立刻放锁：建图是这里最贵的一步，
  // 不该让检索为它等上一毫秒。在用的对象由 shared_ptr keep alive，
  // 即使期间 compact() 换了代也不会被析构。
  std::shared_ptr<HnswIndex> index;
  {
    std::shared_lock<std::shared_mutex> lock(mu_);
    index = index_;
  }

  uint32_t id = index->insert(vec.data());
  if (id == HnswIndex::kInvalidId) {
    return false;
  }

  {
    std::unique_lock<std::shared_mutex> lock(mu_);
    if (index != index_) {
      // 建图期间 compact() 重建了索引：刚分配的 id 属于旧代，
      // 必须在当前代重新写入，否则会发布一个不存在的节点。
      id = index_->insert(vec.data());
      if (id == HnswIndex::kInvalidId) {
        return false;
      }
    }

    if (keys_.size() <= static_cast<size_t>(id)) {
      keys_.resize(static_cast<size_t>(id) + 1);
    }
    keys_[id] = key; // 发布：先把 key 写好，搜索才可能返回这个 id

    uint32_t previous = HnswIndex::kInvalidId;
    const auto it = ids_.find(key);
    if (it != ids_.end()) {
      previous = it->second;
      it->second = id;
    } else {
      ids_.emplace(key, id);
    }

    if (previous != HnswIndex::kInvalidId) {
      // 与发布放在同一把锁里：跨锁调用时 id 可能已经属于另一代索引
      index_->erase(previous);
    }
  }

  // 更新型负载同样会堆 Tombstone；只在 erase 路径检查阈值的话，
  // 一个纯覆盖写的业务永远等不到重建。
  if (should_compact()) {
    compact();
  }
  return true;
}

bool VectorAnnIndex::contains(const std::string &key) const {
  std::shared_lock<std::shared_mutex> lock(mu_);
  return ids_.find(key) != ids_.end();
}

bool VectorAnnIndex::should_compact() const {
  std::shared_lock<std::shared_mutex> lock(mu_);
  const size_t live = index_->size();
  const size_t tombstones = index_->tombstone_count();
  const size_t total = live + tombstones;
  if (total == 0) {
    return false;
  }
  return static_cast<double>(tombstones) / static_cast<double>(total) > compact_ratio_;
}

bool VectorAnnIndex::erase_without_compaction(const std::string &key) {
  std::unique_lock<std::shared_mutex> lock(mu_);
  const auto it = ids_.find(key);
  if (it == ids_.end()) {
    return false;
  }
  const uint32_t id = it->second;
  ids_.erase(it);
  // keys_[id] keeps pointing at the retired key: the node is tombstoned in the
  // graph, so it can never be returned again.
  index_->erase(id);
  return true;
}

bool VectorAnnIndex::erase(const std::string &key) {
  if (!erase_without_compaction(key)) {
    return false;
  }
  if (should_compact()) {
    compact();
  }
  return true;
}

std::vector<AnnMatch> VectorAnnIndex::search(const std::vector<float> &query, size_t k,
                                             size_t ef) const {
  std::vector<AnnMatch> matches;
  if (query.size() != config_.dim || k == 0) {
    return matches;
  }

  std::shared_lock<std::shared_mutex> lock(mu_);
  for (const AnnHit &hit : index_->knn(query.data(), k, ef)) {
    if (static_cast<size_t>(hit.id) >= keys_.size() || keys_[hit.id].empty()) {
      continue; // 尚未发布（写入与 key 登记之间的窗口）
    }
    matches.push_back({keys_[hit.id], hit.score});
  }
  return matches;
}

size_t VectorAnnIndex::build(const std::vector<std::string> &keys, const float *flat,
                             size_t count, size_t threads) {
  if (flat == nullptr || count == 0 || keys.size() != count) {
    return 0;
  }

  std::unique_lock<std::shared_mutex> lock(mu_);
  if (!ids_.empty()) {
    return 0; // 只支持在空索引上批量构建
  }

  index_->reserve(count);
  index_->build_parallel(flat, count, threads);

  ids_.reserve(count);
  keys_.resize(count);
  for (size_t i = 0; i < count; ++i) {
    if (keys[i].empty()) {
      continue;
    }
    keys_[i] = keys[i];
    ids_[keys[i]] = static_cast<uint32_t>(i);
  }
  return ids_.size();
}

void VectorAnnIndex::clear() {
  std::unique_lock<std::shared_mutex> lock(mu_);
  index_ = std::make_shared<HnswIndex>(config_);
  ids_.clear();
  keys_.clear();
}

void VectorAnnIndex::reserve(size_t expected) { index_->reserve(expected); }

void VectorAnnIndex::compact() {
  std::unique_lock<std::shared_mutex> lock(mu_);

  auto fresh = std::make_shared<HnswIndex>(config_);
  fresh->reserve(ids_.size() + 1);

  std::unordered_map<std::string, uint32_t> new_ids;
  std::vector<std::string> new_keys;
  new_ids.reserve(ids_.size());
  new_keys.reserve(ids_.size());

  for (const auto &entry : ids_) {
    const uint32_t new_id = fresh->insert(index_->vector_of(entry.second));
    if (new_keys.size() <= static_cast<size_t>(new_id)) {
      new_keys.resize(static_cast<size_t>(new_id) + 1);
    }
    new_keys[new_id] = entry.first;
    new_ids.emplace(entry.first, new_id);
  }

  index_ = std::move(fresh);
  ids_ = std::move(new_ids);
  keys_ = std::move(new_keys);
}

AnnStats VectorAnnIndex::stats() const {
  std::shared_ptr<HnswIndex> index;
  size_t live = 0;
  {
    std::shared_lock<std::shared_mutex> lock(mu_);
    index = index_;
    live = ids_.size();
  }

  AnnStats stats;
  stats.live = live;
  stats.tombstones = index->tombstone_count();
  stats.memory_bytes = index->memory_bytes();
  stats.max_level = index->max_level();
  return stats;
}

size_t VectorAnnIndex::size() const {
  std::shared_lock<std::shared_mutex> lock(mu_);
  return ids_.size();
}

} // namespace vector
} // namespace minkv
