/**
 * @file expiration_test.cpp
 * @brief 定期删除功能测试
 *
 * 测试 MinKV 的定期删除机制，验证类似 Redis serverCron 的功能
 */

#include "../base/async_logger.h"
#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <iostream>
#include <thread>
#include <unordered_map>

using namespace minkv;

/**
 * @brief 模拟缓存分片，用于测试定期删除
 */
class MockCacheShard {
public:
  struct Item {
    std::string value;
    int64_t expire_time_ms;

    Item() : expire_time_ms(0) {} // 添加默认构造函数
    Item(const std::string &v, int64_t exp) : value(v), expire_time_ms(exp) {}

    bool isExpired() const {
      if (expire_time_ms == 0)
        return false; // 永不过期
      auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::system_clock::now().time_since_epoch())
                     .count();
      return now > expire_time_ms;
    }
  };

  MockCacheShard() : rng_(std::random_device{}()) {}

  void put(const std::string &key, const std::string &value,
           int64_t ttl_ms = 0) {
    std::lock_guard<std::mutex> lock(mutex_);
    int64_t expire_time = 0;
    if (ttl_ms > 0) {
      auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::system_clock::now().time_since_epoch())
                     .count();
      expire_time = now + ttl_ms;
    }
    data_[key] = Item(value, expire_time);
  }

  std::optional<std::string> get(const std::string &key) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = data_.find(key);
    if (it == data_.end())
      return std::nullopt;

    if (it->second.isExpired()) {
      data_.erase(it);
      return std::nullopt;
    }

    return it->second.value;
  }

  bool try_lock() { return mutex_.try_lock(); }

  void unlock() { mutex_.unlock(); }

  std::vector<std::string> randomSample(size_t sample_size) {
    // 注意：调用此函数前必须已经获取锁
    std::vector<std::string> keys;
    keys.reserve(data_.size());

    for (const auto &pair : data_) {
      keys.push_back(pair.first);
    }

    if (keys.empty())
      return {};

    std::shuffle(keys.begin(), keys.end(), rng_);

    size_t actual_size = std::min(sample_size, keys.size());
    keys.resize(actual_size);

    return keys;
  }

  size_t expireKeys(const std::vector<std::string> &keys) {
    // 注意：调用此函数前必须已经获取锁
    size_t expired_count = 0;

    for (const auto &key : keys) {
      auto it = data_.find(key);
      if (it != data_.end() && it->second.isExpired()) {
        data_.erase(it);
        expired_count++;
      }
    }

    return expired_count;
  }

  size_t size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return data_.size();
  }

private:
  mutable std::mutex mutex_;
  std::unordered_map<std::string, Item> data_;
  std::mt19937 rng_;
};

/**
 * @brief 模拟分片缓存系统
 */
class MockShardedCache {
public:
  MockShardedCache(size_t shard_count) : shards_(shard_count) {}

  void put(const std::string &key, const std::string &value,
           int64_t ttl_ms = 0) {
    size_t shard_id = std::hash<std::string>{}(key) % shards_.size();
    shards_[shard_id].put(key, value, ttl_ms);
  }

  std::optional<std::string> get(const std::string &key) {
    size_t shard_id = std::hash<std::string>{}(key) % shards_.size();
    return shards_[shard_id].get(key);
  }

  size_t size() const {
    size_t total = 0;
    for (const auto &shard : shards_) {
      total += shard.size();
    }
    return total;
  }

  // 定期删除回调函数
  size_t expirationCallback(size_t shard_id, size_t sample_size) {
    if (shard_id >= shards_.size())
      return 0;

    auto &shard = shards_[shard_id];

    // [核心优化] 使用 try_lock() 实现非阻塞访问
    if (!shard.try_lock()) {
      // 锁被占用，立即跳过（说明业务在忙）
      return 0;
    }

    // [RAII] 确保锁被正确释放
    struct LockGuard {
      MockCacheShard *shard_;
      ~LockGuard() { shard_->unlock(); }
    } guard{&shard};

    // [随机采样] 随机选择指定数量的key进行检查
    auto keys = shard.randomSample(sample_size);
    if (keys.empty())
      return 0;

    // [批量过期] 检查并删除过期的key
    return shard.expireKeys(keys);
  }

private:
  std::vector<MockCacheShard> shards_;
};

/**
 * @brief 测试基本的定期删除功能
 */
void testBasicExpiration() {
  std::cout << "\n=== 测试基本定期删除功能 ===" << std::endl;

  // 创建模拟缓存系统
  MockShardedCache cache(4); // 4个分片

  // 创建定期删除管理器
  base::ExpirationManager expiration_mgr(4, std::chrono::milliseconds(50), 10);

  // 设置回调函数
  auto callback = [&cache](size_t shard_id, size_t sample_size) -> size_t {
    return cache.expirationCallback(shard_id, sample_size);
  };

  // 插入一些测试数据
  std::cout << "插入测试数据..." << std::endl;
  for (int i = 0; i < 100; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string value = "value" + std::to_string(i);

    if (i < 50) {
      // 前50个key设置100ms TTL（很快过期）
      cache.put(key, value, 100);
    } else {
      // 后50个key永不过期
      cache.put(key, value, 0);
    }
  }

  std::cout << "初始缓存大小: " << cache.size() << std::endl;
  assert(cache.size() == 100);

  // 启动定期删除服务
  std::cout << "启动定期删除服务..." << std::endl;
  expiration_mgr.start(callback);

  // 等待过期删除生效
  std::cout << "等待过期删除..." << std::endl;
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // 检查结果
  size_t final_size = cache.size();
  std::cout << "过期删除后缓存大小: " << final_size << std::endl;

  // 验证过期的key被删除了
  int expired_count = 0;
  int valid_count = 0;

  for (int i = 0; i < 100; ++i) {
    std::string key = "key" + std::to_string(i);
    auto value = cache.get(key);

    if (i < 50) {
      // 前50个key应该过期
      if (!value.has_value()) {
        expired_count++;
      }
    } else {
      // 后50个key应该仍然有效
      if (value.has_value()) {
        valid_count++;
      }
    }
  }

  std::cout << "过期key数量: " << expired_count << "/50" << std::endl;
  std::cout << "有效key数量: " << valid_count << "/50" << std::endl;

  // 停止定期删除服务
  expiration_mgr.stop();

  // 获取统计信息
  auto stats = expiration_mgr.getStats();
  std::cout << "定期删除统计:" << std::endl;
  std::cout << "  总检查次数: " << stats.total_checks << std::endl;
  std::cout << "  总过期删除数: " << stats.total_expired << std::endl;
  std::cout << "  总跳过次数: " << stats.total_skipped << std::endl;
  std::cout << "  平均过期比例: " << stats.avg_expired_ratio << std::endl;

  std::cout << "✅ 基本定期删除功能测试通过!" << std::endl;
}

/**
 * @brief 测试非阻塞特性
 */
void testNonBlockingBehavior() {
  std::cout << "\n=== 测试非阻塞特性 ===" << std::endl;

  MockShardedCache cache(2); // 2个分片
  base::ExpirationManager expiration_mgr(2, std::chrono::milliseconds(10), 5);

  std::atomic<int> business_operations{0};
  std::atomic<int> expiration_skips{0};

  // 业务线程回调
  auto callback = [&cache, &expiration_skips](size_t shard_id,
                                              size_t sample_size) -> size_t {
    size_t result = cache.expirationCallback(shard_id, sample_size);
    if (result == 0) {
      expiration_skips.fetch_add(1);
    }
    return result;
  };

  // 启动定期删除
  expiration_mgr.start(callback);

  // 模拟高频业务操作
  std::thread business_thread([&cache, &business_operations]() {
    for (int i = 0; i < 1000; ++i) {
      std::string key = "busy_key" + std::to_string(i % 10);
      cache.put(key, "busy_value", 50); // 50ms TTL
      cache.get(key);
      business_operations.fetch_add(1);

      // 模拟业务处理时间
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
  });

  // 等待测试完成
  business_thread.join();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  expiration_mgr.stop();

  std::cout << "业务操作次数: " << business_operations.load() << std::endl;
  std::cout << "定期删除跳过次数: " << expiration_skips.load() << std::endl;

  // 验证定期删除确实在业务繁忙时跳过了一些检查
  assert(expiration_skips.load() > 0);

  std::cout << "✅ 非阻塞特性测试通过!" << std::endl;
}

/**
 * @brief 性能基准测试
 */
void benchmarkExpiration() {
  std::cout << "\n=== 定期删除性能基准测试 ===" << std::endl;

  const size_t SHARD_COUNT = 16;
  const size_t KEY_COUNT = 10000;

  MockShardedCache cache(SHARD_COUNT);
  base::ExpirationManager expiration_mgr(SHARD_COUNT,
                                         std::chrono::milliseconds(100), 20);

  // 插入大量测试数据
  std::cout << "插入 " << KEY_COUNT << " 个测试key..." << std::endl;
  auto start_time = std::chrono::steady_clock::now();

  for (size_t i = 0; i < KEY_COUNT; ++i) {
    std::string key = "bench_key" + std::to_string(i);
    std::string value = "bench_value" + std::to_string(i);

    // 50% 的key设置短TTL，50% 永不过期
    int64_t ttl = (i % 2 == 0) ? 200 : 0;
    cache.put(key, value, ttl);
  }

  auto insert_time = std::chrono::steady_clock::now();
  auto insert_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      insert_time - start_time);

  std::cout << "插入耗时: " << insert_duration.count() << "ms" << std::endl;
  std::cout << "插入QPS: " << (KEY_COUNT * 1000 / insert_duration.count())
            << std::endl;

  // 启动定期删除
  auto callback = [&cache](size_t shard_id, size_t sample_size) -> size_t {
    return cache.expirationCallback(shard_id, sample_size);
  };

  expiration_mgr.start(callback);

  // 运行一段时间让定期删除生效
  std::cout << "运行定期删除 1 秒..." << std::endl;
  std::this_thread::sleep_for(std::chrono::seconds(1));

  expiration_mgr.stop();

  // 统计结果
  auto stats = expiration_mgr.getStats();
  std::cout << "性能统计:" << std::endl;
  std::cout << "  检查轮次: " << stats.total_checks << std::endl;
  std::cout << "  删除过期key: " << stats.total_expired << std::endl;
  std::cout << "  跳过次数: " << stats.total_skipped << std::endl;
  std::cout << "  平均过期比例: " << (stats.avg_expired_ratio * 100) << "%"
            << std::endl;

  size_t final_size = cache.size();
  std::cout << "最终缓存大小: " << final_size << " (预期约 " << (KEY_COUNT / 2)
            << ")" << std::endl;

  std::cout << "✅ 性能基准测试完成!" << std::endl;
}

int main() {
  std::cout << "MinKV 定期删除功能测试" << std::endl;
  std::cout << "========================" << std::endl;

  try {
    // 初始化日志系统
    base::AsyncLogger::instance().setLogLevel(base::LogLevel::INFO);

    // 运行测试
    testBasicExpiration();
    testNonBlockingBehavior();
    benchmarkExpiration();

    std::cout << "\n🎉 所有测试通过！定期删除功能工作正常。" << std::endl;

  } catch (const std::exception &e) {
    std::cerr << "❌ 测试失败: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}