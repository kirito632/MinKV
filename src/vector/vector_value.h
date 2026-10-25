#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <string>
#include <vector>

namespace minkv {
namespace vector {

/**
 * @brief Vector value structure for storing embeddings with metadata
 *
 * This structure represents a vector embedding along with its associated
 * metadata and timestamp. It provides serialization/deserialization
 * capabilities for persistent storage.
 */
struct VectorValue {
  std::vector<float> embedding; // Vector embedding data
  std::string metadata;         // JSON format metadata
  uint64_t timestamp;           // Creation/update timestamp

  /**
   * @brief Default constructor
   */
  VectorValue() : timestamp(0) {}

  /**
   * @brief Constructor with parameters
   */
  VectorValue(const std::vector<float> &emb, const std::string &meta, uint64_t ts)
      : embedding(emb), metadata(meta), timestamp(ts) {}

  /**
   * @brief Get the dimension of the embedding
   * @return Size of the embedding vector
   */
  size_t dimension() const { return embedding.size(); }

  /**
   * @brief Validate the vector value
   * @return true if valid, false otherwise
   *
   * A valid vector must:
   * - Have non-empty embedding
   * - Contain only finite values (no NaN or Inf)
   */
  bool is_valid() const {
    if (embedding.empty()) {
      return false;
    }

    return std::all_of(
        embedding.begin(), embedding.end(), [](float v) { return std::isfinite(v); });
  }

  /**
   * @brief Serialize the vector value to binary format
   * @return Serialized binary string
   *
   * Format:
   * [4 bytes: dimension]
   * [dimension * 4 bytes: float data]
   * [8 bytes: timestamp]
   * [4 bytes: metadata length]
   * [metadata length bytes: metadata string]
   */
  std::string serialize() const;

  /**
   * @brief Deserialize from binary format
   * @param data Binary data string
   * @return Deserialized VectorValue
   * @throws std::runtime_error if deserialization fails
   */
  static VectorValue deserialize(const std::string &data);
};

} // namespace vector
} // namespace minkv
