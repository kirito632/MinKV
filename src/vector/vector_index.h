#pragma once

#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "vector_value.h"

namespace minkv {
namespace vector {

/**
 * @brief Thread-safe vector index for managing vector embeddings
 *
 * This class provides a thread-safe container for storing and retrieving
 * vector embeddings. It enforces dimension consistency across all vectors
 * in the index and supports prefix-based key filtering.
 */
class VectorIndex {
public:
  VectorIndex() : dimension_(0) {}

  /**
   * @brief Insert a vector into the index
   * @param key Unique identifier for the vector
   * @param value Vector value to insert
   * @return true if insertion successful, false otherwise
   *
   * The first inserted vector determines the dimension for the entire index.
   * Subsequent insertions must match this dimension.
   */
  bool insert(const std::string &key, const VectorValue &value);

  /**
   * @brief Get a vector from the index
   * @param key Vector identifier
   * @return Optional containing the vector if found, nullopt otherwise
   */
  std::optional<VectorValue> get(const std::string &key) const;

  /**
   * @brief Remove a vector from the index
   * @param key Vector identifier
   * @return true if removal successful, false if key not found
   */
  bool remove(const std::string &key);

  /**
   * @brief List all keys in the index, optionally filtered by prefix
   * @param prefix Optional prefix filter (empty string returns all keys)
   * @return Vector of matching keys
   */
  std::vector<std::string> list_keys(const std::string &prefix = "") const;

  /**
   * @brief Get the number of vectors in the index
   * @return Number of stored vectors
   */
  size_t size() const;

  /**
   * @brief Get the dimension of vectors in this index
   * @return Dimension (0 if index is empty)
   */
  size_t dimension() const { return dimension_; }

  /**
   * @brief Clear all vectors from the index
   */
  void clear();

private:
  mutable std::shared_mutex mutex_;
  std::unordered_map<std::string, VectorValue> vectors_;
  size_t dimension_; // Dimension of all vectors in this index

  /**
   * @brief Validate that a vector matches the index dimension
   * @param value Vector to validate
   * @return true if valid, false otherwise
   */
  bool validate_dimension(const VectorValue &value) const;
};

} // namespace vector
} // namespace minkv
