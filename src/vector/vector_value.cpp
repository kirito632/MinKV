#include "vector_value.h"

#include <cstring>
#include <stdexcept>

namespace minkv {
namespace vector {

std::string VectorValue::serialize() const {
  if (!is_valid()) {
    throw std::runtime_error("Cannot serialize invalid VectorValue");
  }

  std::string result;

  // Calculate total size
  uint32_t dim = static_cast<uint32_t>(embedding.size());
  uint32_t meta_len = static_cast<uint32_t>(metadata.size());
  size_t total_size = sizeof(uint32_t) +    // dimension
                      dim * sizeof(float) + // embedding data
                      sizeof(uint64_t) +    // timestamp
                      sizeof(uint32_t) +    // metadata length
                      meta_len;             // metadata

  result.reserve(total_size);

  // Write dimension
  result.append(reinterpret_cast<const char *>(&dim), sizeof(uint32_t));

  // Write embedding data
  result.append(reinterpret_cast<const char *>(embedding.data()), dim * sizeof(float));

  // Write timestamp
  result.append(reinterpret_cast<const char *>(&timestamp), sizeof(uint64_t));

  // Write metadata length
  result.append(reinterpret_cast<const char *>(&meta_len), sizeof(uint32_t));

  // Write metadata
  if (meta_len > 0) {
    result.append(metadata);
  }

  return result;
}

VectorValue VectorValue::deserialize(const std::string &data) {
  if (data.size() < sizeof(uint32_t)) {
    throw std::runtime_error("Invalid serialized data: too short");
  }

  size_t offset = 0;
  VectorValue result;

  // Read dimension
  uint32_t dim;
  std::memcpy(&dim, data.data() + offset, sizeof(uint32_t));
  offset += sizeof(uint32_t);

  if (dim == 0 || dim > 100000) { // Sanity check
    throw std::runtime_error("Invalid dimension in serialized data");
  }

  // Check if we have enough data
  size_t required_size =
      offset + dim * sizeof(float) + sizeof(uint64_t) + sizeof(uint32_t);
  if (data.size() < required_size) {
    throw std::runtime_error("Invalid serialized data: insufficient data");
  }

  // Read embedding data
  result.embedding.resize(dim);
  std::memcpy(result.embedding.data(), data.data() + offset, dim * sizeof(float));
  offset += dim * sizeof(float);

  // Read timestamp
  std::memcpy(&result.timestamp, data.data() + offset, sizeof(uint64_t));
  offset += sizeof(uint64_t);

  // Read metadata length
  uint32_t meta_len;
  std::memcpy(&meta_len, data.data() + offset, sizeof(uint32_t));
  offset += sizeof(uint32_t);

  // Check metadata length
  if (offset + meta_len != data.size()) {
    throw std::runtime_error("Invalid serialized data: metadata length mismatch");
  }

  // Read metadata
  if (meta_len > 0) {
    result.metadata.assign(data.data() + offset, meta_len);
  }

  // Validate the result
  if (!result.is_valid()) {
    throw std::runtime_error("Deserialized VectorValue is invalid");
  }

  return result;
}

} // namespace vector
} // namespace minkv
