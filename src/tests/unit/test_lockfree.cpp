#include <iostream>
#include <cassert>
#include <thread>
#include <chrono>
#include <vector>
#include "../../db/lockfree_cache.h"

using namespace minkv::db;

int main() {
    std::cout << "Starting Optimistic LRU Cache Tests..." << std::endl;

    OptimisticLruCache cache(10);

    // Test 1: Basic Put and Get
    cache.put("key1", "value1");
    auto v1 = cache.get("key1");
    assert(v1.has_value() && v1.value() == "value1");
    std::cout << "[Pass] Put and Get key1" << std::endl;

    // Test 2: Update existing key
    cache.put("key1", "updated_value1");
    auto v1_updated = cache.get("key1");
    assert(v1_updated.has_value() && v1_updated.value() == "updated_value1");
    std::cout << "[Pass] Update existing key" << std::endl;

    // Test 3: TTL
    cache.put("ttl_key", "ttl_value", 100);
    auto ttl_val = cache.get("ttl_key");
    assert(ttl_val.has_value());
    std::cout << "[Pass] TTL key exists immediately" << std::endl;

    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    auto ttl_expired = cache.get("ttl_key");
    assert(!ttl_expired.has_value());
    std::cout << "[Pass] TTL key expired" << std::endl;

    // Test 4: Concurrent reads (无锁读的优势)
    cache.put("concurrent_key", "concurrent_value");
    
    std::vector<std::thread> threads;
    std::atomic<int> read_count{0};
    
    for (int i = 0; i < 8; ++i) {
        threads.emplace_back([&cache, &read_count]() {
            for (int j = 0; j < 1000; ++j) {
                auto val = cache.get("concurrent_key");
                if (val.has_value()) {
                    read_count++;
                }
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    assert(read_count == 8000);
    std::cout << "[Pass] Concurrent reads (8 threads, 1000 ops each)" << std::endl;

    // Test 5: Remove
    cache.put("remove_key", "remove_value");
    assert(cache.get("remove_key").has_value());
    assert(cache.remove("remove_key"));
    assert(!cache.get("remove_key").has_value());
    std::cout << "[Pass] Remove key" << std::endl;

    std::cout << "All optimistic LRU cache tests passed!" << std::endl;
    return 0;
}
