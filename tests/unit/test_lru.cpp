#include <cassert>
#include <chrono>
#include <iostream>
#include <random>
#include <unordered_set>
#include <thread>

#include "../../src/core/lru_cache.h"

using namespace minkv::db;

int main() {
  std::cout << "Starting LRU Cache Tests..." << std::endl;

  minkv::db::LruCache<std::string, std::string> cache(2); // 容量为 2

  // Test 1: Put and Get
  cache.put("key1", "value1");
  auto v1 = cache.get("key1");
  assert(v1.has_value() && v1.value() == "value1");
  std::cout << "[Pass] Put and Get key1" << std::endl;

  // Test 2: LRU Eviction
  cache.put("key2", "value2"); // Cache: key2, key1
  cache.put("key3", "value3"); // Cache: key3, key2 (key1 should be evicted)

  auto v1_missing = cache.get("key1");
  assert(!v1_missing.has_value());
  std::cout << "[Pass] Eviction of key1" << std::endl;

  auto v2 = cache.get("key2"); // Cache: key2, key3 (key2 becomes most recent)
  assert(v2.has_value() && v2.value() == "value2");

  cache.put("key4", "value4"); // Cache: key4, key2 (key3 should be evicted
                               // because key2 was accessed)

  auto v3_missing = cache.get("key3");
  assert(!v3_missing.has_value());
  std::cout << "[Pass] Eviction of key3 (LRU logic verified)" << std::endl;

  // Test 3: TTL (Time To Live)
  cache.put("ttl_key", "ttl_value", 100); // 100ms TTL
  auto ttl_val = cache.get("ttl_key");
  assert(ttl_val.has_value() && ttl_val.value() == "ttl_value");
  std::cout << "[Pass] TTL key exists immediately" << std::endl;

  // 等待 150ms，让 TTL 过期
  std::this_thread::sleep_for(std::chrono::milliseconds(150));
  auto ttl_expired = cache.get("ttl_key");
  assert(!ttl_expired.has_value());
  std::cout << "[Pass] TTL key expired after timeout" << std::endl;

  // Test 4: Permanent key (TTL = 0)
  cache.put("permanent", "forever", 0); // 0 = 永不过期
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  auto permanent = cache.get("permanent");
  assert(permanent.has_value() && permanent.value() == "forever");
  std::cout << "[Pass] Permanent key (TTL=0) never expires" << std::endl;

  // Test 5: Bounded sampling should not degrade into full-scan deletion
  minkv::db::LruCache<std::string, std::string> expiration_cache(32);
  for (int i = 0; i < 10; ++i) {
    expiration_cache.put("expired_" + std::to_string(i), "value", 20);
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(40));

  std::mt19937 rng(12345);
  auto sampled_keys = expiration_cache.sample_keys(3, rng);
  assert(sampled_keys.size() == 3);

  size_t sampled_removed = expiration_cache.expire_keys(sampled_keys);
  assert(sampled_removed == sampled_keys.size());

  size_t remaining_removed = expiration_cache.cleanup_expired_keys();
  assert(remaining_removed == 7);
  assert(expiration_cache.size() == 0);
  std::cout << "[Pass] Sampled expiration only removes sampled expired keys"
            << std::endl;

  // Test 6: sample_size > cache_size should return all keys once without looping
  minkv::db::LruCache<std::string, std::string> small_cache(8);
  for (int i = 0; i < 7; ++i) {
    small_cache.put("small_" + std::to_string(i), "value");
  }

  auto oversized_sample = small_cache.sample_keys(100, rng);
  assert(oversized_sample.size() == 7);
  std::unordered_set<std::string> unique_keys(oversized_sample.begin(),
                                              oversized_sample.end());
  assert(unique_keys.size() == oversized_sample.size());
  std::cout << "[Pass] sample_keys caps sample size to cache size" << std::endl;

  // Test 7: empty cache sampling / expiration should be no-op
  minkv::db::LruCache<std::string, std::string> empty_cache(4);
  auto empty_sample = empty_cache.sample_keys(10, rng);
  assert(empty_sample.empty());
  assert(empty_cache.expire_keys(empty_sample) == 0);
  assert(empty_cache.cleanup_expired_keys() == 0);
  assert(empty_cache.size() == 0);
  std::cout << "[Pass] Empty cache sampled expiration is a no-op" << std::endl;

  // Test 8: zero-capacity cache should be a safe no-op on put
  minkv::db::LruCache<std::string, std::string> zero_capacity_cache(0);
  zero_capacity_cache.put("key", "value");
  assert(zero_capacity_cache.size() == 0);
  assert(!zero_capacity_cache.get("key").has_value());
  std::cout << "[Pass] Zero-capacity cache put is a safe no-op" << std::endl;

  // Test 9: cleanup thread should be stopped by LruCache destructor.
  auto cleanup_raii_start = std::chrono::steady_clock::now();
  {
    minkv::db::LruCache<std::string, std::string> cleanup_raii_cache(4);
    cleanup_raii_cache.put("cleanup_key", "value", 10);
    cleanup_raii_cache.start_cleanup_thread(5000);
    cleanup_raii_cache.start_cleanup_thread(5000);
  }
  auto cleanup_raii_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - cleanup_raii_start);
  assert(cleanup_raii_elapsed < std::chrono::milliseconds(1000));
  std::cout << "[Pass] Cleanup thread stops automatically on destruction"
            << std::endl;

  std::cout << "All tests passed!" << std::endl;
  return 0;
}
