#include "vector_index.h"

#include <algorithm>
#include <mutex>

namespace minkv {
namespace vector {

bool VectorIndex::validate_dimension(const VectorValue &value) const {
  if (!value.is_valid()) {
    return false;
  }

  // If index is empty, any dimension is valid
  if (dimension_ == 0) {
    return true;
  }

  // Otherwise, dimension must match
  return value.dimension() == dimension_;
}

bool VectorIndex::insert(const std::string &key, const VectorValue &value) {
  if (!value.is_valid()) {
    return false;
  }

  std::unique_lock<std::shared_mutex> lock(mutex_);

  // Set dimension on first insert
  if (dimension_ == 0 && !vectors_.empty()) {
    // This shouldn't happen, but handle it gracefully
    dimension_ = vectors_.begin()->second.dimension();
  }

  // Validate dimension
  if (dimension_ == 0) {
    dimension_ = value.dimension();
  } else if (value.dimension() != dimension_) {
    return false; // Dimension mismatch
  }

  vectors_[key] = value;
  return true;
}

std::optional<VectorValue> VectorIndex::get(const std::string &key) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);

  auto it = vectors_.find(key);
  if (it != vectors_.end()) {
    return it->second;
  }

  return std::nullopt;
}

bool VectorIndex::remove(const std::string &key) {
  std::unique_lock<std::shared_mutex> lock(mutex_);

  auto it = vectors_.find(key);
  if (it == vectors_.end()) {
    return false;
  }

  vectors_.erase(it);

  // Reset dimension if index becomes empty
  if (vectors_.empty()) {
    dimension_ = 0;
  }

  return true;
}

std::vector<std::string> VectorIndex::list_keys(const std::string &prefix) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);

  std::vector<std::string> result;
  result.reserve(vectors_.size());

  if (prefix.empty()) {
    // Return all keys
    for (const auto &pair : vectors_) {
      result.push_back(pair.first);
    }
  } else {
    // Filter by prefix
    for (const auto &pair : vectors_) {
      if (pair.first.compare(0, prefix.size(), prefix) == 0) {
        result.push_back(pair.first);
      }
    }
  }

  return result;
}

size_t VectorIndex::size() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return vectors_.size();
}

void VectorIndex::clear() {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  vectors_.clear();
  dimension_ = 0;
}

} // namespace vector
} // namespace minkv
