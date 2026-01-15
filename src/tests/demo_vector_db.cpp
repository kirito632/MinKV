#include "../db/vector_ops.h"
#include <iostream>
#include <vector>
#include <string>
#include <thread>
#include <future>
#include <queue>
#include <algorithm>
#include <random>
#include <iomanip>

// 定义一个简单的 Key-Value 结构，模拟真实存储
struct VectorEntry {
    std::string key;
    std::vector<float> data;
};

// 比较器：用于维持小顶堆 (Min-Heap)，保留最大的 Top-K
// 堆顶是目前 Top-K 里最小的（最不像的），如果新来的比堆顶大（更像/距离更小），就踢掉堆顶
// 等等，距离越小越相似。我们要找距离最小的 K 个。
// 所以我们要维护一个 大顶堆 (Max-Heap)，堆顶是目前 Top-K 里距离最大的（最差的）。
// 如果新来的距离比堆顶小（更好），就踢掉堆顶。
struct SearchResult {
    std::string key;
    float distance;

    // 大顶堆需要 less 比较：a < b
    bool operator<(const SearchResult& other) const {
        return distance < other.distance; 
    }
};

class VectorDBLite {
public:
    VectorDBLite(size_t dim, int num_shards) 
        : dim_(dim), num_shards_(num_shards) {
        shards_.resize(num_shards);
    }

    // 插入向量 (简单的 Round-Robin 分片)
    void Put(const std::string& key, const std::vector<float>& vec) {
        if (vec.size() != dim_) {
            throw std::runtime_error("Dimension mismatch");
        }
        size_t shard_idx = std::hash<std::string>{}(key) % num_shards_;
        shards_[shard_idx].push_back({key, vec});
    }

    // 并发搜索 (Map-Reduce)
    std::vector<SearchResult> Search(const std::vector<float>& query, int k) {
        std::vector<std::future<std::vector<SearchResult>>> futures;

        // 1. Map Phase: 分发任务给各个分片
        for (int i = 0; i < num_shards_; ++i) {
            futures.push_back(std::async(std::launch::async, [this, i, &query, k]() {
                return this->SearchShard(i, query, k);
            }));
        }

        // 2. Reduce Phase: 收集所有分片的结果
        std::priority_queue<SearchResult> global_heap; // 大顶堆

        for (auto& f : futures) {
            auto shard_results = f.get();
            for (const auto& res : shard_results) {
                global_heap.push(res);
                if (global_heap.size() > k) {
                    global_heap.pop();
                }
            }
        }

        // 3. 整理最终结果 (堆里是乱序的，且是反的，需要倒序输出)
        std::vector<SearchResult> final_results;
        while (!global_heap.empty()) {
            final_results.push_back(global_heap.top());
            global_heap.pop();
        }
        // 翻转为：距离最小在前
        std::reverse(final_results.begin(), final_results.end());
        return final_results;
    }

private:
    size_t dim_;
    int num_shards_;
    std::vector<std::vector<VectorEntry>> shards_; // 模拟分片存储

    // 在单个分片内搜索 Top-K
    std::vector<SearchResult> SearchShard(int shard_idx, const std::vector<float>& query, int k) {
        std::priority_queue<SearchResult> heap; // 大顶堆
        const auto& shard_data = shards_[shard_idx];

        for (const auto& entry : shard_data) {
            // 使用 SIMD 加速计算距离
            float dist = VectorOps::L2DistanceSquare_AVX2(query.data(), entry.data.data(), dim_);

            if (heap.size() < k) {
                heap.push({entry.key, dist});
            } else if (dist < heap.top().distance) {
                heap.pop();
                heap.push({entry.key, dist});
            }
        }

        // 导出结果
        std::vector<SearchResult> results;
        while (!heap.empty()) {
            results.push_back(heap.top());
            heap.pop();
        }
        return results;
    }
};

// ==========================================
// 演示主程序
// ==========================================
int main() {
    const size_t DIM = 128;
    const int NUM_VECTORS = 100000; // 10万数据
    const int NUM_SHARDS = 8;       // 8个分片 (模拟8核)
    const int TOP_K = 10;

    std::cout << "=== VectorDB Lite Demo (Map-Reduce) ===" << std::endl;
    std::cout << "Initializing DB with " << NUM_VECTORS << " vectors, " << NUM_SHARDS << " shards..." << std::endl;

    VectorDBLite db(DIM, NUM_SHARDS);

    // 1. 灌数据
    // 故意埋藏一个“最近”的向量，看能不能搜出来
    std::vector<float> target_vec(DIM, 0.5f); // 目标向量全为 0.5
    
    // 生成随机数据
    std::mt19937 gen(42);
    std::uniform_real_distribution<float> dis(0.0f, 1.0f);
    
    for (int i = 0; i < NUM_VECTORS; ++i) {
        std::vector<float> vec(DIM);
        for (int j = 0; j < DIM; ++j) vec[j] = dis(gen);
        db.Put("user_" + std::to_string(i), vec);
    }

    // 插入几个特别相似的 Case
    // Case 1: 完全一样 -> 距离 0
    db.Put("TARGET_EXACT", target_vec); 
    // Case 2: 微小扰动 -> 距离很小
    std::vector<float> near_vec = target_vec;
    near_vec[0] += 0.01f;
    db.Put("TARGET_NEAR", near_vec);

    std::cout << "Data loaded. Starting search..." << std::endl;

    // 2. 执行并发搜索
    auto start = std::chrono::high_resolution_clock::now();
    
    auto results = db.Search(target_vec, TOP_K);
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    // 3. 输出结果
    std::cout << "\nSearch completed in " << duration << " ms" << std::endl;
    std::cout << "Top " << TOP_K << " Results:" << std::endl;
    std::cout << std::left << std::setw(20) << "KEY" << "DISTANCE (Squared)" << std::endl;
    std::cout << "----------------------------------------" << std::endl;
    
    for (const auto& res : results) {
        std::cout << std::left << std::setw(20) << res.key << res.distance << std::endl;
    }

    return 0;
}
