/**
 * @file search_performance_benchmark.cpp
 * @brief Top-K向量搜索性能基准测试
 * 
 * 测试目标：
 * 1. 测试不同向量数量的搜索延迟 (100/1K/10K/100K)
 * 2. 测试不同K值的性能影响 (1/5/10/20/50)
 * 3. 测试不同维度的性能影响 (128/256/512/768/1536)
 * 4. 记录P50/P95/P99延迟和QPS数据
 * 
 * 验收标准：
 * - 搜索延迟 < 10ms (1K向量)
 * - 搜索延迟 < 100ms (10K向量)
 * 
 * 编译命令：
 *   g++ -std=c++17 -O3 -mavx2 -mfma search_performance_benchmark.cpp \
 *       ../vector/vector_value.cpp ../vector/vector_index.cpp \
 *       ../vector/vector_search.cpp ../vector/vector_ops.cpp \
 *       -I.. -o search_benchmark
 */

#include "../vector/vector_index.h"
#include "../vector/vector_search.h"
#include "../vector/vector_ops.h"
#include <iostream>
#include <iomanip>
#include <chrono>
#include <vector>
#include <random>
#include <algorithm>
#include <numeric>
#include <fstream>
#include <map>

using namespace std;
using namespace std::chrono;
using namespace minkv::vector;

// ==========================================
// 性能测试工具类
// ==========================================

class SearchBenchmark {
public:
    struct LatencyStats {
        double p50_ms;
        double p95_ms;
        double p99_ms;
        double avg_ms;
        double min_ms;
        double max_ms;
        double qps;
    };

    struct BenchmarkConfig {
        size_t vector_count;    // 向量数量
        size_t dimension;       // 向量维度
        size_t k;               // Top-K
        DistanceMetric metric;  // 距离度量
        size_t num_queries;     // 查询次数
    };

    struct BenchmarkResult {
        BenchmarkConfig config;
        LatencyStats stats;
        bool meets_requirement;  // 是否满足性能要求
    };

    /**
     * 生成随机向量
     */
    static vector<float> generate_random_vector(size_t dim) {
        static random_device rd;
        static mt19937 gen(rd());
        static uniform_real_distribution<float> dis(-1.0f, 1.0f);
        
        vector<float> vec(dim);
        for (size_t i = 0; i < dim; ++i) {
            vec[i] = dis(gen);
        }
        return vec;
    }

    /**
     * 初始化向量索引
     */
    static void populate_index(VectorIndex& index, size_t count, size_t dim) {
        cout << "  初始化 " << count << " 个 " << dim << " 维向量..." << flush;
        
        for (size_t i = 0; i < count; ++i) {
            string key = "vec_" + to_string(i);
            VectorValue value;
            value.embedding = generate_random_vector(dim);
            value.metadata = "{\"id\":" + to_string(i) + "}";
            value.timestamp = i;
            
            index.insert(key, value);
            
            // 进度显示
            if ((i + 1) % (count / 10) == 0 || i == count - 1) {
                cout << "." << flush;
            }
        }
        
        cout << " 完成" << endl;
    }

    /**
     * 计算延迟统计
     */
    static LatencyStats calculate_stats(vector<double>& latencies_ms, double total_time_s) {
        sort(latencies_ms.begin(), latencies_ms.end());
        
        size_t n = latencies_ms.size();
        LatencyStats stats;
        
        stats.p50_ms = latencies_ms[n * 50 / 100];
        stats.p95_ms = latencies_ms[n * 95 / 100];
        stats.p99_ms = latencies_ms[n * 99 / 100];
        stats.avg_ms = accumulate(latencies_ms.begin(), latencies_ms.end(), 0.0) / n;
        stats.min_ms = latencies_ms.front();
        stats.max_ms = latencies_ms.back();
        stats.qps = n / total_time_s;
        
        return stats;
    }

    /**
     * 运行搜索性能测试
     */
    static BenchmarkResult run_benchmark(const BenchmarkConfig& config) {
        cout << "\n测试配置:" << endl;
        cout << "  向量数量: " << config.vector_count << endl;
        cout << "  向量维度: " << config.dimension << endl;
        cout << "  Top-K: " << config.k << endl;
        cout << "  查询次数: " << config.num_queries << endl;

        // 1. 初始化索引
        VectorIndex index;
        populate_index(index, config.vector_count, config.dimension);

        // 2. 创建搜索引擎
        VectorSearch search(&index);

        // 3. 生成查询向量
        cout << "  生成查询向量..." << flush;
        vector<vector<float>> queries;
        for (size_t i = 0; i < config.num_queries; ++i) {
            queries.push_back(generate_random_vector(config.dimension));
        }
        cout << " 完成" << endl;

        // 4. 预热
        cout << "  预热..." << flush;
        for (size_t i = 0; i < min(size_t(10), config.num_queries); ++i) {
            search.search(queries[i], config.k, config.metric);
        }
        cout << " 完成" << endl;

        // 5. 正式测试
        cout << "  执行搜索测试..." << flush;
        vector<double> latencies_ms;
        latencies_ms.reserve(config.num_queries);

        auto total_start = high_resolution_clock::now();
        
        for (size_t i = 0; i < config.num_queries; ++i) {
            auto start = high_resolution_clock::now();
            auto results = search.search(queries[i], config.k, config.metric);
            auto end = high_resolution_clock::now();
            
            double latency_ms = duration_cast<microseconds>(end - start).count() / 1000.0;
            latencies_ms.push_back(latency_ms);
            
            // 进度显示
            if ((i + 1) % (config.num_queries / 10) == 0 || i == config.num_queries - 1) {
                cout << "." << flush;
            }
        }
        
        auto total_end = high_resolution_clock::now();
        double total_time_s = duration_cast<milliseconds>(total_end - total_start).count() / 1000.0;
        
        cout << " 完成" << endl;

        // 6. 计算统计数据
        LatencyStats stats = calculate_stats(latencies_ms, total_time_s);

        // 7. 检查是否满足性能要求
        bool meets_requirement = true;
        if (config.vector_count == 1000 && stats.p99_ms >= 10.0) {
            meets_requirement = false;
        }
        if (config.vector_count == 10000 && stats.p99_ms >= 100.0) {
            meets_requirement = false;
        }

        return BenchmarkResult{
            .config = config,
            .stats = stats,
            .meets_requirement = meets_requirement
        };
    }

    /**
     * 打印单个测试结果
     */
    static void print_result(const BenchmarkResult& result) {
        const auto& cfg = result.config;
        const auto& stats = result.stats;

        cout << "\n结果:" << endl;
        cout << "  P50延迟: " << fixed << setprecision(3) << stats.p50_ms << " ms" << endl;
        cout << "  P95延迟: " << fixed << setprecision(3) << stats.p95_ms << " ms" << endl;
        cout << "  P99延迟: " << fixed << setprecision(3) << stats.p99_ms << " ms" << endl;
        cout << "  平均延迟: " << fixed << setprecision(3) << stats.avg_ms << " ms" << endl;
        cout << "  最小延迟: " << fixed << setprecision(3) << stats.min_ms << " ms" << endl;
        cout << "  最大延迟: " << fixed << setprecision(3) << stats.max_ms << " ms" << endl;
        cout << "  QPS: " << fixed << setprecision(0) << stats.qps << endl;
        
        // 性能要求检查
        if (cfg.vector_count == 1000) {
            cout << "  性能要求 (P99 < 10ms): " 
                 << (result.meets_requirement ? "✓ 通过" : "✗ 未通过") << endl;
        } else if (cfg.vector_count == 10000) {
            cout << "  性能要求 (P99 < 100ms): " 
                 << (result.meets_requirement ? "✓ 通过" : "✗ 未通过") << endl;
        }
    }

    /**
     * 打印汇总报告
     */
    static void print_summary(const vector<BenchmarkResult>& results) {
        cout << "\n========================================" << endl;
        cout << "Top-K搜索性能基准测试报告" << endl;
        cout << "========================================\n" << endl;

        cout << left << setw(12) << "向量数量"
             << setw(8) << "维度"
             << setw(6) << "K值"
             << setw(12) << "P50(ms)"
             << setw(12) << "P95(ms)"
             << setw(12) << "P99(ms)"
             << setw(12) << "QPS"
             << setw(8) << "状态" << endl;
        cout << string(90, '-') << endl;

        for (const auto& result : results) {
            const auto& cfg = result.config;
            const auto& stats = result.stats;

            cout << left << setw(12) << cfg.vector_count
                 << setw(8) << cfg.dimension
                 << setw(6) << cfg.k
                 << setw(12) << fixed << setprecision(3) << stats.p50_ms
                 << setw(12) << fixed << setprecision(3) << stats.p95_ms
                 << setw(12) << fixed << setprecision(3) << stats.p99_ms
                 << setw(12) << fixed << setprecision(0) << stats.qps
                 << setw(8) << (result.meets_requirement ? "✓" : "✗") << endl;
        }

        // 统计通过率
        size_t passed = count_if(results.begin(), results.end(),
                                 [](const BenchmarkResult& r) { return r.meets_requirement; });
        cout << "\n通过率: " << passed << "/" << results.size() 
             << " (" << fixed << setprecision(1) << (100.0 * passed / results.size()) << "%)" << endl;
    }

    /**
     * 导出CSV报告
     */
    static void export_csv(const vector<BenchmarkResult>& results, const string& filename) {
        ofstream file(filename);
        if (!file.is_open()) {
            cerr << "无法创建文件: " << filename << endl;
            return;
        }

        // CSV头部
        file << "VectorCount,Dimension,K,P50_ms,P95_ms,P99_ms,Avg_ms,Min_ms,Max_ms,QPS,MeetsRequirement\n";

        // 数据行
        for (const auto& result : results) {
            const auto& cfg = result.config;
            const auto& stats = result.stats;

            file << cfg.vector_count << ","
                 << cfg.dimension << ","
                 << cfg.k << ","
                 << fixed << setprecision(3) << stats.p50_ms << ","
                 << fixed << setprecision(3) << stats.p95_ms << ","
                 << fixed << setprecision(3) << stats.p99_ms << ","
                 << fixed << setprecision(3) << stats.avg_ms << ","
                 << fixed << setprecision(3) << stats.min_ms << ","
                 << fixed << setprecision(3) << stats.max_ms << ","
                 << fixed << setprecision(0) << stats.qps << ","
                 << (result.meets_requirement ? "Yes" : "No") << "\n";
        }

        file.close();
        cout << "\n性能报告已导出到: " << filename << endl;
    }
};

// ==========================================
// 主测试函数
// ==========================================

int main(int argc, char* argv[]) {
    cout << "MinKV Top-K搜索性能基准测试" << endl;
    cout << "========================================\n" << endl;

    // 检测CPU特性
    bool has_avx2 = minkv::vector::VectorOps::has_avx2();
    cout << "CPU特性检测:" << endl;
    cout << "  AVX2支持: " << (has_avx2 ? "是" : "否") << endl;
    cout << endl;

    vector<SearchBenchmark::BenchmarkResult> all_results;

    // ==========================================
    // 测试1: 不同向量数量的性能影响
    // ==========================================
    cout << "========================================" << endl;
    cout << "测试1: 不同向量数量的性能影响" << endl;
    cout << "========================================" << endl;

    vector<size_t> vector_counts = {100, 1000, 10000};
    // 如果有足够时间，可以测试100K
    if (argc > 1 && string(argv[1]) == "--full") {
        vector_counts.push_back(100000);
    }

    for (size_t count : vector_counts) {
        SearchBenchmark::BenchmarkConfig config{
            .vector_count = count,
            .dimension = 768,  // 标准维度
            .k = 10,           // 标准K值
            .metric = DistanceMetric::COSINE,
            .num_queries = min(size_t(100), count / 10)  // 查询次数
        };

        auto result = SearchBenchmark::run_benchmark(config);
        SearchBenchmark::print_result(result);
        all_results.push_back(result);
    }

    // ==========================================
    // 测试2: 不同K值的性能影响
    // ==========================================
    cout << "\n========================================" << endl;
    cout << "测试2: 不同K值的性能影响" << endl;
    cout << "========================================" << endl;

    vector<size_t> k_values = {1, 5, 10, 20, 50};
    
    for (size_t k : k_values) {
        SearchBenchmark::BenchmarkConfig config{
            .vector_count = 1000,
            .dimension = 768,
            .k = k,
            .metric = DistanceMetric::COSINE,
            .num_queries = 100
        };

        auto result = SearchBenchmark::run_benchmark(config);
        SearchBenchmark::print_result(result);
        all_results.push_back(result);
    }

    // ==========================================
    // 测试3: 不同维度的性能影响
    // ==========================================
    cout << "\n========================================" << endl;
    cout << "测试3: 不同维度的性能影响" << endl;
    cout << "========================================" << endl;

    vector<size_t> dimensions = {128, 256, 512, 768, 1536};
    
    for (size_t dim : dimensions) {
        SearchBenchmark::BenchmarkConfig config{
            .vector_count = 1000,
            .dimension = dim,
            .k = 10,
            .metric = DistanceMetric::COSINE,
            .num_queries = 100
        };

        auto result = SearchBenchmark::run_benchmark(config);
        SearchBenchmark::print_result(result);
        all_results.push_back(result);
    }

    // ==========================================
    // 打印汇总报告
    // ==========================================
    SearchBenchmark::print_summary(all_results);

    // ==========================================
    // 导出CSV报告
    // ==========================================
    SearchBenchmark::export_csv(all_results, "search_performance_report.csv");

    // ==========================================
    // 性能要求验证
    // ==========================================
    cout << "\n========================================" << endl;
    cout << "性能要求验证" << endl;
    cout << "========================================" << endl;

    bool all_passed = true;
    
    // 检查1K向量的P99延迟
    auto it_1k = find_if(all_results.begin(), all_results.end(),
                         [](const SearchBenchmark::BenchmarkResult& r) {
                             return r.config.vector_count == 1000 && 
                                    r.config.dimension == 768 && 
                                    r.config.k == 10;
                         });
    
    if (it_1k != all_results.end()) {
        cout << "1K向量搜索 (768维, K=10):" << endl;
        cout << "  P99延迟: " << fixed << setprecision(3) << it_1k->stats.p99_ms << " ms" << endl;
        cout << "  要求: < 10 ms" << endl;
        cout << "  结果: " << (it_1k->meets_requirement ? "✓ 通过" : "✗ 未通过") << endl;
        all_passed &= it_1k->meets_requirement;
    }

    // 检查10K向量的P99延迟
    auto it_10k = find_if(all_results.begin(), all_results.end(),
                          [](const SearchBenchmark::BenchmarkResult& r) {
                              return r.config.vector_count == 10000 && 
                                     r.config.dimension == 768 && 
                                     r.config.k == 10;
                          });
    
    if (it_10k != all_results.end()) {
        cout << "\n10K向量搜索 (768维, K=10):" << endl;
        cout << "  P99延迟: " << fixed << setprecision(3) << it_10k->stats.p99_ms << " ms" << endl;
        cout << "  要求: < 100 ms" << endl;
        cout << "  结果: " << (it_10k->meets_requirement ? "✓ 通过" : "✗ 未通过") << endl;
        all_passed &= it_10k->meets_requirement;
    }

    cout << "\n总体结果: " << (all_passed ? "✓ 所有性能要求已满足" : "✗ 部分性能要求未满足") << endl;

    cout << "\n测试完成!" << endl;
    return all_passed ? 0 : 1;
}
