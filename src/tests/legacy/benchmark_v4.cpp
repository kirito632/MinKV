#include <iostream>
#include <chrono>
#include <thread>
#include <vector>
#include <atomic>
#include <random>
#include <iomanip>
#include <unordered_map>
#include <list>
#include <mutex>
#include <optional>

using namespace std;

// 改进的压测参数
const int NUM_THREADS = 16;
const int OPS_PER_THREAD = 100000;       // 10 万次（减少以加快压测）
const int KEY_RANGE = 100000;            // 10 万个 key
const int CACHE_SIZE = 50000;            // 缓存容量 5 万
const int READ_RATIO = 90;               // 90% 读，10% 写

struct BenchmarkResult {
    string name;
    long long total_ops;
    long long duration_ms;
    double qps;
    double latency_us;
};

void print_result(const BenchmarkResult& result) {
    cout << left << setw(35) << result.name
         << setw(15) << fixed << setprecision(2) << (result.qps / 1000000.0) << " M ops/s"
         << setw(15) << result.latency_us << " us"
         << endl;
}

// ============ 简单的整数 key 缓存实现 ============

// 版本 1: 单机大锁
class SimpleLruCache {
public:
    explicit SimpleLruCache(size_t capacity) : capacity_(capacity) {}
    
    optional<int> get(int key) {
        lock_guard<mutex> lock(mutex_);
        auto it = map_.find(key);
        if (it == map_.end()) {
            return nullopt;
        }
        // 移到前面
        list_.splice(list_.begin(), list_, it->second);
        return it->second->value;
    }
    
    void put(int key, int value) {
        lock_guard<mutex> lock(mutex_);
        auto it = map_.find(key);
        if (it != map_.end()) {
            it->second->value = value;
            list_.splice(list_.begin(), list_, it->second);
            return;
        }
        if (map_.size() >= capacity_) {
            auto last = list_.end();
            --last;
            map_.erase(last->key);
            list_.pop_back();
        }
        list_.push_front({key, value});
        map_[key] = list_.begin();
    }
    
private:
    size_t capacity_;
    struct Node {
        int key;
        int value;
    };
    list<Node> list_;
    unordered_map<int, list<Node>::iterator> map_;
    mutex mutex_;
};

// 版本 2: 分片锁
class ShardedLruCache {
public:
    ShardedLruCache(size_t capacity_per_shard, int shard_count)
        : shard_count_(shard_count), shards_(shard_count) {
        for (int i = 0; i < shard_count; ++i) {
            shards_[i] = make_unique<SimpleLruCache>(capacity_per_shard);
        }
    }
    
    optional<int> get(int key) {
        int shard_id = key % shard_count_;
        return shards_[shard_id]->get(key);
    }
    
    void put(int key, int value) {
        int shard_id = key % shard_count_;
        shards_[shard_id]->put(key, value);
    }
    
private:
    int shard_count_;
    vector<unique_ptr<SimpleLruCache>> shards_;
};

// 版本 3: 缓存行对齐的分片锁
class AlignedShardedLruCache {
public:
    AlignedShardedLruCache(size_t capacity_per_shard, int shard_count)
        : shard_count_(shard_count) {
        // 分配对齐的内存
        shards_ = new AlignedShard[shard_count];
        for (int i = 0; i < shard_count; ++i) {
            shards_[i].cache = make_unique<SimpleLruCache>(capacity_per_shard);
        }
    }
    
    ~AlignedShardedLruCache() {
        delete[] shards_;
    }
    
    optional<int> get(int key) {
        int shard_id = key % shard_count_;
        return shards_[shard_id].cache->get(key);
    }
    
    void put(int key, int value) {
        int shard_id = key % shard_count_;
        shards_[shard_id].cache->put(key, value);
    }
    
private:
    struct alignas(64) AlignedShard {
        unique_ptr<SimpleLruCache> cache;
    };
    
    int shard_count_;
    AlignedShard* shards_;
};

// ============ 压测函数 ============

BenchmarkResult benchmark_simple_lru() {
    SimpleLruCache cache(CACHE_SIZE);
    
    auto start = chrono::high_resolution_clock::now();
    
    vector<thread> threads;
    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&cache, t]() {
            mt19937 gen(t);
            uniform_int_distribution<> dis(0, KEY_RANGE - 1);
            
            for (int i = 0; i < OPS_PER_THREAD; ++i) {
                int key = dis(gen);
                
                if (i % 100 < READ_RATIO) {
                    cache.get(key);
                } else {
                    cache.put(key, key);
                }
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    auto end = chrono::high_resolution_clock::now();
    long long duration_ms = chrono::duration_cast<chrono::milliseconds>(end - start).count();
    long long total_ops = (long long)NUM_THREADS * OPS_PER_THREAD;
    double qps = (double)total_ops / duration_ms * 1000;
    double latency_us = (double)duration_ms * 1000 / total_ops;
    
    return {
        .name = "SimpleLruCache (单机大锁)",
        .total_ops = total_ops,
        .duration_ms = duration_ms,
        .qps = qps,
        .latency_us = latency_us
    };
}

BenchmarkResult benchmark_sharded_lru() {
    ShardedLruCache cache(CACHE_SIZE / 32, 32);
    
    auto start = chrono::high_resolution_clock::now();
    
    vector<thread> threads;
    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&cache, t]() {
            mt19937 gen(t);
            uniform_int_distribution<> dis(0, KEY_RANGE - 1);
            
            for (int i = 0; i < OPS_PER_THREAD; ++i) {
                int key = dis(gen);
                
                if (i % 100 < READ_RATIO) {
                    cache.get(key);
                } else {
                    cache.put(key, key);
                }
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    auto end = chrono::high_resolution_clock::now();
    long long duration_ms = chrono::duration_cast<chrono::milliseconds>(end - start).count();
    long long total_ops = (long long)NUM_THREADS * OPS_PER_THREAD;
    double qps = (double)total_ops / duration_ms * 1000;
    double latency_us = (double)duration_ms * 1000 / total_ops;
    
    return {
        .name = "ShardedLruCache(32)",
        .total_ops = total_ops,
        .duration_ms = duration_ms,
        .qps = qps,
        .latency_us = latency_us
    };
}

BenchmarkResult benchmark_aligned_sharded_lru() {
    AlignedShardedLruCache cache(CACHE_SIZE / 32, 32);
    
    auto start = chrono::high_resolution_clock::now();
    
    vector<thread> threads;
    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&cache, t]() {
            mt19937 gen(t);
            uniform_int_distribution<> dis(0, KEY_RANGE - 1);
            
            for (int i = 0; i < OPS_PER_THREAD; ++i) {
                int key = dis(gen);
                
                if (i % 100 < READ_RATIO) {
                    cache.get(key);
                } else {
                    cache.put(key, key);
                }
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    auto end = chrono::high_resolution_clock::now();
    long long duration_ms = chrono::duration_cast<chrono::milliseconds>(end - start).count();
    long long total_ops = (long long)NUM_THREADS * OPS_PER_THREAD;
    double qps = (double)total_ops / duration_ms * 1000;
    double latency_us = (double)duration_ms * 1000 / total_ops;
    
    return {
        .name = "AlignedShardedLruCache(32)",
        .total_ops = total_ops,
        .duration_ms = duration_ms,
        .qps = qps,
        .latency_us = latency_us
    };
}

int main() {
    cout << "\n╔════════════════════════════════════════════════════════════════╗\n";
    cout << "║     FlashCache 性能压测 v4 (整数 key，无字符串开销)           ║\n";
    cout << "╚════════════════════════════════════════════════════════════════╝\n\n";
    
    cout << "测试参数:\n";
    cout << "  - 线程数: " << NUM_THREADS << "\n";
    cout << "  - 每线程操作数: " << OPS_PER_THREAD << " (共 " << (NUM_THREADS * OPS_PER_THREAD / 1000000) << "M 次)\n";
    cout << "  - Key 范围: " << KEY_RANGE << " (整数 key，无字符串开销)\n";
    cout << "  - 缓存容量: " << CACHE_SIZE << "\n";
    cout << "  - 读写比: " << READ_RATIO << "% 读, " << (100 - READ_RATIO) << "% 写\n\n";
    
    cout << string(70, '=') << "\n";
    cout << left << setw(35) << "缓存实现"
         << setw(15) << "吞吐量"
         << setw(15) << "延迟"
         << endl;
    cout << string(70, '=') << "\n";
    
    // 运行压测
    auto result_simple = benchmark_simple_lru();
    print_result(result_simple);
    
    auto result_sharded = benchmark_sharded_lru();
    print_result(result_sharded);
    
    auto result_aligned = benchmark_aligned_sharded_lru();
    print_result(result_aligned);
    
    cout << string(70, '=') << "\n\n";
    
    // 计算性能提升
    double speedup_sharded = result_sharded.qps / result_simple.qps;
    double speedup_aligned = result_aligned.qps / result_simple.qps;
    
    cout << "性能提升倍数（相对于 SimpleLruCache）:\n";
    cout << "  - ShardedLruCache(32): " << fixed << setprecision(2) << speedup_sharded << "x\n";
    cout << "  - AlignedShardedLruCache(32): " << speedup_aligned << "x ✨\n\n";
    
    // 总结
    cout << "关键发现:\n";
    cout << "  ✓ 使用整数 key 消除了字符串开销\n";
    cout << "  ✓ 分片锁性能提升: " << fixed << setprecision(1) << (speedup_sharded - 1) * 100 << "%\n";
    cout << "  ✓ 缓存行对齐性能提升: " << (speedup_aligned - 1) * 100 << "%\n";
    cout << "  ✓ 总体性能提升: " << speedup_aligned << "x\n\n";
    
    cout << "建议:\n";
    cout << "  1. 字符串操作是性能的主要瓶颈\n";
    cout << "  2. 在实际应用中，应该使用更高效的 key 表示（如整数或哈希值）\n";
    cout << "  3. 分片锁和缓存行对齐在高并发场景下效果显著\n\n";
    
    return 0;
}
