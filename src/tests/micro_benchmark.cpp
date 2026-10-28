#include <atomic>
#include <chrono>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <shared_mutex>
#include <string>
#include <unordered_map>

// ========== Micro-benchmark helpers ==========
using Clock = std::chrono::high_resolution_clock;

template <typename F>
double measure_ns(F &&func, int iterations = 100000) {
  // Warmup
  for (int i = 0; i < 10000; ++i)
    func();

  auto start = Clock::now();
  for (int i = 0; i < iterations; ++i)
    func();
  auto end = Clock::now();

  double total_ns = std::chrono::duration<double, std::nano>(end - start).count();
  return total_ns / iterations;
}

// ========== Components to measure ==========

// 1. current_time_ms()
int64_t current_time_ms() {
  auto now = std::chrono::high_resolution_clock::now();
  auto duration = now.time_since_epoch();
  return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
}

// 2. std::string creation
std::mt19937 gen(42);
std::uniform_int_distribution<> key_dis(0, 99999);

// 3. unordered_map find with string key
struct Node {
  std::string key;
  std::string value;
  std::atomic<uint64_t> last_promote_ms {0};

  Node(const std::string &k, const std::string &v) : key(k), value(v) {}
};

int main() {
  std::cout << "===== Micro Benchmark: Per-Operation Cost Breakdown =====" << std::endl;
  std::cout << "Compiler: " <<
#ifdef __clang__
      "clang"
#elif __GNUC__
      "gcc"
#else
      "unknown"
#endif
            << std::endl;
  std::cout << "NDEBUG: " <<
#ifdef NDEBUG
      "defined"
#else
      "not defined"
#endif
            << std::endl;
  std::cout << std::endl;

  // 1. current_time_ms() overhead
  double clock_ns = measure_ns([] { current_time_ms(); });
  std::cout << "1. current_time_ms(): " << clock_ns << " ns" << std::endl;

  // 2. String key creation
  double str_create_ns =
      measure_ns([] { std::string key = "key_" + std::to_string(key_dis(gen)); });
  std::cout << "2. String key creation (key_ + to_string): " << str_create_ns << " ns"
            << std::endl;

  // 3. high_resolution_clock::now() alone
  double clock_now_ns = measure_ns([] {
    volatile auto t = std::chrono::high_resolution_clock::now();
    (void)t;
  });
  std::cout << "3. high_resolution_clock::now(): " << clock_now_ns << " ns" << std::endl;

  // 4. unordered_map find (hot cache, string key)
  {
    std::unordered_map<std::string, std::string> map;
    for (int i = 0; i < 100000; ++i) {
      map["key_" + std::to_string(i)] = "val";
    }

    // Warm up cache
    std::string search_key = "key_12345";
    for (int i = 0; i < 10000; ++i) {
      auto it = map.find(search_key);
      if (it != map.end()) {
        volatile const std::string &v = it->second;
        (void)v;
      }
    }

    // 4a. find with pre-created key
    double find_only_ns = measure_ns(
        [&] {
          auto it = map.find(search_key);
          for (volatile int j = 0; j < 1; ++j) {
            if (it != map.end()) {
              volatile const std::string &v = it->second;
              (void)v;
            }
          }
        },
        100000);
    std::cout << "4a. unordered_map find (pre-created key): " << find_only_ns << " ns"
              << std::endl;
  }

  // 5. String copy (SSO)
  {
    std::string src = "val";
    double str_copy_ns = measure_ns([&] {
      std::string dst = src;
      volatile const char *p = dst.c_str();
      (void)p;
    });
    std::cout << "5. std::string copy (SSO, 3 bytes): " << str_copy_ns << " ns"
              << std::endl;

    // Larger string
    std::string large_src = "this_is_a_longer_string_that_exceeds_SSO_buffer";
    double large_copy_ns = measure_ns([&] {
      std::string dst = large_src;
      volatile const char *p = dst.c_str();
      (void)p;
    });
    std::cout << "5b. std::string copy (heap, 50 bytes): " << large_copy_ns << " ns"
              << std::endl;
  }

  // 6. std::optional<std::string> return
  {
    std::string val = "val";
    double opt_return_ns = measure_ns([&] {
      std::optional<std::string> opt = val;
      volatile const char *p = opt->c_str();
      (void)p;
    });
    std::cout << "6. optional<string> return: " << opt_return_ns << " ns" << std::endl;
  }

  // 7. mutex lock/unlock (uncontended)
  {
    std::mutex mtx;
    double mutex_ns = measure_ns([&] { std::lock_guard<std::mutex> lock(mtx); });
    std::cout << "7. mutex lock/unlock (uncontended): " << mutex_ns << " ns" << std::endl;
  }

  // 8. Shared mutex shared_lock (uncontended)
  {
    std::shared_mutex smtx;
    double shared_lock_ns =
        measure_ns([&] { std::shared_lock<std::shared_mutex> lock(smtx); });
    std::cout << "8. shared_mutex shared_lock (uncontended): " << shared_lock_ns << " ns"
              << std::endl;
  }

  // 9. Combined: full LRU-like get operation (string key, map find, copy value)
  {
    std::unordered_map<std::string, std::unique_ptr<Node>> node_map;
    for (int i = 0; i < 100000; ++i) {
      auto key = "key_" + std::to_string(i);
      node_map[key] = std::make_unique<Node>(key, "val");
    }

    std::atomic<uint64_t> stats_hits_ {0};
    std::atomic<uint64_t> last_hit_time_ms_ {0};

    double combined_ns = measure_ns(
        [&] {
          uint64_t now = static_cast<uint64_t>(current_time_ms());
          auto key = "key_" + std::to_string(key_dis(gen));
          auto it = node_map.find(key);
          if (it != node_map.end()) {
            uint64_t last = it->second->last_promote_ms.load(std::memory_order_relaxed);
            if (now >= last && (now - last) > 1000) {
              it->second->last_promote_ms.store(now, std::memory_order_relaxed);
            }
            ++stats_hits_;
            last_hit_time_ms_.store(now, std::memory_order_relaxed);
            volatile const std::string &v = it->second->value;
            (void)v;
          }
        },
        50000);
    std::cout << "9. Combined LRU get simulation: " << combined_ns << " ns" << std::endl;
  }

  return 0;
}
