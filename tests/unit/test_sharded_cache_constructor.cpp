#include <cassert>
#include <iostream>
#include <stdexcept>
#include <string>

#include "core/minkv.h"
#include "core/sharded_cache.h"

int main() {
  using minkv::MinKV;
  using minkv::db::ShardedCache;

  bool sharded_cache_threw = false;
  try {
    ShardedCache<std::string, std::string> cache(10, 0);
  } catch (const std::invalid_argument &) {
    sharded_cache_threw = true;
  }
  assert(sharded_cache_threw);
  std::cout << "[Pass] ShardedCache rejects zero shard_count" << std::endl;

  bool minkv_threw = false;
  try {
    auto engine = MinKV<std::string, std::string>::create(10, 0);
    (void)engine;
  } catch (const std::invalid_argument &) {
    minkv_threw = true;
  }
  assert(minkv_threw);
  std::cout << "[Pass] MinKV rejects zero shard_count" << std::endl;

  ShardedCache<std::string, std::string> zero_capacity_cache(0, 2);
  zero_capacity_cache.put("key", "value");
  assert(zero_capacity_cache.size() == 0);
  assert(!zero_capacity_cache.get("key").has_value());
  std::cout << "[Pass] Zero capacity remains a safe no-op" << std::endl;

  std::cout << "All ShardedCache constructor tests passed!" << std::endl;
  return 0;
}
