#include <iostream>
#include <cassert>
#include <thread>
#include <chrono>
#include "../../db/lru_cache.h"

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

    cache.put("key4", "value4"); // Cache: key4, key2 (key3 should be evicted because key2 was accessed)
    
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

    std::cout << "All tests passed!" << std::endl;
    return 0;
}

