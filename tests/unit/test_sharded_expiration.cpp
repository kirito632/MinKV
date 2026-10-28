#include <cassert>
#include <chrono>
#include <iostream>
#include <thread>

#include "../../src/core/sharded_cache.h"

using namespace minkv::db;

int main() {
  std::cout << "Starting Sharded Expiration Regression Test..." << std::endl;

  ShardedCache<std::string, std::string> cache(128, 1);

  for (int i = 0; i < 25; ++i) {
    cache.put("expired_" + std::to_string(i), "value", 20);
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  cache.startExpirationService(std::chrono::milliseconds(10), 5);
  std::this_thread::sleep_for(std::chrono::milliseconds(40));
  cache.stopExpirationService();

  size_t remaining_after_background = cache.size();
  assert(remaining_after_background > 0);
  assert(remaining_after_background < 25);

  size_t removed_by_manual = cache.manualExpiration();
  assert(removed_by_manual == remaining_after_background);
  assert(cache.size() == 0);

  std::cout << "[Pass] Background expiration stayed bounded and manual cleanup removed "
               "the remainder"
            << std::endl;
  return 0;
}
