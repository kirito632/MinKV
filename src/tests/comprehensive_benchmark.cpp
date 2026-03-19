#include "../core/sharded_cache.h"
#include <iostream>
#include <fstream>
#include <vector>
#include <thread>
#include <chrono>
#include <random>
#include <atomic>
#include <iomanip>
#include <sstream>
#include <algorithm>
#include <ctime>

using namespace minkv::db;
using Cache = ShardedCache<std::string, std::string>;

// 获取当前时间字符串
std::string get_current_time() {
    auto now = std::chrono::system_clock::now();
    auto time_t_now = std::chrono::system_clock::to_time_t(now);
    std::tm tm_now;
    localtime_r(&time_t_now, &tm_now);
    
    char buffer[100];
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &tm_now);
    return std::string(buffer);
}

// 格式化持续时间
std::string format_duration(double seconds) {
    int hours = static_cast<int>(seconds / 3600);
    int minutes = static_cast<int>((seconds - hours * 3600) / 60);
    int secs = static_cast<int>(seconds - hours * 3600 - minutes * 60);
    
    std::ostringstream oss;
    if (hours > 0) {
        oss << hours << "h " << minutes << "m " << secs << "s";
    } else if (minutes > 0) {
        oss << minutes << "m " << secs << "s";
    } else {
        oss << secs << "s";
    }
    return oss.str();
}

// 测试结果结构
struct BenchmarkResult {
    std::string test_name;
    int thread_count;
    int64_t total_ops;
    double duration_ms;
    double qps;
    double avg_latency_us;
    double p50_latency_us;
    double p95_latency_us;
    double p99_latency_us;
    size_t cache_hit_rate;
};

// 🔥 优化：无锁延迟统计（Thread-Local）
class LatencyStats {
private:
    std::vector<std::vector<double>> thread_local_latencies_;
    
public:
    explicit LatencyStats(int thread_count) : thread_local_latencies_(thread_count) {
        for (auto& vec : thread_local_latencies_) {
            vec.reserve(10000); // 预分配，避免动态扩容
        }
    }
    
    // 线程局部记录，无锁
    void record(int thread_id, double latency_us) {
        thread_local_latencies_[thread_id].push_back(latency_us);
    }
    
    void get_percentiles(double& p50, double& p95, double& p99) {
        // 合并所有线程的数据
        std::vector<double> merged;
        size_t total_size = 0;
        for (const auto& vec : thread_local_latencies_) {
            total_size += vec.size();
        }
        merged.reserve(total_size);
        
        for (const auto& vec : thread_local_latencies_) {
            merged.insert(merged.end(), vec.begin(), vec.end());
        }
        
        if (merged.empty()) {
            p50 = p95 = p99 = 0;
            return;
        }
        
        std::sort(merged.begin(), merged.end());
        size_t n = merged.size();
        p50 = merged[n * 50 / 100];
        p95 = merged[n * 95 / 100];
        p99 = merged[n * 99 / 100];
    }
};

// 生成随机字符串
std::string random_string(size_t length) {
    static const char charset[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<> dis(0, sizeof(charset) - 2);
    
    std::string result;
    result.reserve(length);
    for (size_t i = 0; i < length; ++i) {
        result += charset[dis(gen)];
    }
    return result;
}

// 🔥 优化后的压力测试：并发读写
BenchmarkResult benchmark_concurrent_rw(int thread_count, int ops_per_thread, int read_ratio) {
    Cache cache(10000, 32);  // 10000 capacity per shard, 32 shards
    
    // 🔥 优化1：预填充更多数据（10万），减少热点竞争
    std::cout << "  预填充数据..." << std::flush;
    for (int i = 0; i < 100000; ++i) {
        cache.put("key_" + std::to_string(i), "val");
    }
    std::cout << " 完成！" << std::endl;
    
    std::atomic<int64_t> total_ops{0};
    LatencyStats latency_stats(thread_count);  // 传入线程数
    
    auto worker = [&](int thread_id) {
        std::mt19937 gen(thread_id);
        // 🔥 优化2：Key范围扩大到100万，避免热点碰撞
        std::uniform_int_distribution<> key_dis(0, 999999);
        std::uniform_int_distribution<> op_dis(0, 99);
        
        for (int i = 0; i < ops_per_thread; ++i) {
            std::string key = "key_" + std::to_string(key_dis(gen));
            
            // 🔥 优化3：使用 steady_clock 更精确
            auto start = std::chrono::steady_clock::now();
            
            if (op_dis(gen) < read_ratio) {
                // 读操作
                cache.get(key);
            } else {
                // 写操作
                cache.put(key, "val");
            }
            
            // 🔥 优化4：采样记录延迟，无锁写入线程局部存储
            if (i % 100 == 0) {
                auto end = std::chrono::steady_clock::now();
                double latency_us = std::chrono::duration<double, std::micro>(end - start).count();
                latency_stats.record(thread_id, latency_us);
            }
            
            total_ops++;  // atomic自增，唯一的竞争点
        }
    };
    
    // 启动压力测试
    auto start_time = std::chrono::high_resolution_clock::now();
    
    std::vector<std::thread> threads;
    for (int i = 0; i < thread_count; ++i) {
        threads.emplace_back(worker, i);
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    double duration_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
    
    // 计算统计数据
    BenchmarkResult result;
    result.test_name = "Concurrent_R" + std::to_string(read_ratio) + "W" + std::to_string(100 - read_ratio);
    result.thread_count = thread_count;
    result.total_ops = total_ops;
    result.duration_ms = duration_ms;
    result.qps = (total_ops * 1000.0) / duration_ms;
    result.avg_latency_us = duration_ms * 1000.0 / total_ops;
    
    latency_stats.get_percentiles(result.p50_latency_us, result.p95_latency_us, result.p99_latency_us);
    
    auto stats = cache.getStats();
    result.cache_hit_rate = (stats.hits * 100) / (stats.hits + stats.misses + 1);
    
    return result;
}

// 🔥 优化后的压力测试：向量检索
BenchmarkResult benchmark_vector_search(int thread_count, int searches_per_thread) {
    Cache cache(10000, 32);
    
    // 🔥 优化5：增加向量数量到10万，测出SIMD真实性能
    std::cout << "  预填充向量数据（10万条）..." << std::flush;
    for (int i = 0; i < 100000; ++i) {
        std::vector<float> vec(128);
        for (int j = 0; j < 128; ++j) {
            vec[j] = static_cast<float>(rand()) / RAND_MAX;
        }
        cache.vectorPut("vec_" + std::to_string(i), vec);
        
        if (i % 10000 == 0 && i > 0) {
            std::cout << i << "..." << std::flush;
        }
    }
    std::cout << " 完成！" << std::endl;
    
    std::atomic<int64_t> total_ops{0};
    LatencyStats latency_stats(thread_count);
    
    auto worker = [&](int thread_id) {
        std::mt19937 gen(thread_id);
        std::uniform_real_distribution<float> dis(0.0f, 1.0f);
        
        for (int i = 0; i < searches_per_thread; ++i) {
            std::vector<float> query(128);
            for (int j = 0; j < 128; ++j) {
                query[j] = dis(gen);
            }
            
            auto start = std::chrono::steady_clock::now();
            cache.vectorSearch(query, 10);  // Top-10
            auto end = std::chrono::steady_clock::now();
            
            double latency_us = std::chrono::duration<double, std::micro>(end - start).count();
            latency_stats.record(thread_id, latency_us);
            
            total_ops++;
        }
    };
    
    auto start_time = std::chrono::high_resolution_clock::now();
    
    std::vector<std::thread> threads;
    for (int i = 0; i < thread_count; ++i) {
        threads.emplace_back(worker, i);
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    double duration_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
    
    BenchmarkResult result;
    result.test_name = "Vector_Search";
    result.thread_count = thread_count;
    result.total_ops = total_ops;
    result.duration_ms = duration_ms;
    result.qps = (total_ops * 1000.0) / duration_ms;
    result.avg_latency_us = duration_ms * 1000.0 / total_ops;
    
    latency_stats.get_percentiles(result.p50_latency_us, result.p95_latency_us, result.p99_latency_us);
    result.cache_hit_rate = 0;  // N/A for vector search
    
    return result;
}

// 保存结果到CSV（带时间戳）
void save_to_csv(const std::vector<BenchmarkResult>& results, const std::string& filename,
                 const std::string& start_time, const std::string& end_time, double total_duration) {
    std::ofstream file(filename);
    
    // 添加测试元数据
    file << "# MinKV Comprehensive Benchmark Report\n";
    file << "# Test Start Time: " << start_time << "\n";
    file << "# Test End Time: " << end_time << "\n";
    file << "# Total Duration: " << format_duration(total_duration) << " (" 
         << std::fixed << std::setprecision(2) << total_duration << "s)\n";
    file << "# Optimization Version: v2.0 (100w keys, thread-local stats, 10w vectors)\n";
    file << "#\n";
    
    // CSV头部
    file << "Test,Threads,TotalOps,Duration(ms),QPS,AvgLatency(us),P50(us),P95(us),P99(us),HitRate(%)\n";
    
    // 数据行
    for (const auto& r : results) {
        file << r.test_name << ","
             << r.thread_count << ","
             << r.total_ops << ","
             << std::fixed << std::setprecision(2) << r.duration_ms << ","
             << std::fixed << std::setprecision(0) << r.qps << ","
             << std::fixed << std::setprecision(2) << r.avg_latency_us << ","
             << std::fixed << std::setprecision(2) << r.p50_latency_us << ","
             << std::fixed << std::setprecision(2) << r.p95_latency_us << ","
             << std::fixed << std::setprecision(2) << r.p99_latency_us << ","
             << r.cache_hit_rate << "\n";
    }
    
    file.close();
    std::cout << "结果已保存到: " << filename << std::endl;
}

// 打印结果表格
void print_results(const std::vector<BenchmarkResult>& results) {
    std::cout << "\n╔════════════════════════════════════════════════════════════════════════════╗\n";
    std::cout << "║                    MinKV 综合压力测试报告                                  ║\n";
    std::cout << "╚════════════════════════════════════════════════════════════════════════════╝\n\n";
    
    std::cout << std::left << std::setw(20) << "测试场景"
              << std::right << std::setw(8) << "线程数"
              << std::setw(12) << "QPS"
              << std::setw(12) << "P50(us)"
              << std::setw(12) << "P95(us)"
              << std::setw(12) << "P99(us)"
              << std::setw(10) << "命中率" << "\n";
    std::cout << std::string(86, '-') << "\n";
    
    for (const auto& r : results) {
        std::cout << std::left << std::setw(20) << r.test_name
                  << std::right << std::setw(8) << r.thread_count
                  << std::setw(12) << std::fixed << std::setprecision(0) << r.qps
                  << std::setw(12) << std::fixed << std::setprecision(2) << r.p50_latency_us
                  << std::setw(12) << std::fixed << std::setprecision(2) << r.p95_latency_us
                  << std::setw(12) << std::fixed << std::setprecision(2) << r.p99_latency_us
                  << std::setw(9) << r.cache_hit_rate << "%\n";
    }
    std::cout << "\n";
}

int main() {
    // 记录测试开始时间
    auto test_start_time = std::chrono::system_clock::now();
    std::string start_time_str = get_current_time();
    
    std::cout << "╔════════════════════════════════════════════════════════════════════════════╗\n";
    std::cout << "║                    MinKV 综合压力测试套件 v2.0                            ║\n";
    std::cout << "║              Comprehensive Benchmark Suite (Optimized)                    ║\n";
    std::cout << "║                                                                            ║\n";
    std::cout << "║  优化点：                                                                  ║\n";
    std::cout << "║  1. Key范围扩大到100万，避免热点竞争                                      ║\n";
    std::cout << "║  2. 无锁延迟统计，消除观测者效应                                          ║\n";
    std::cout << "║  3. 向量数据增加到10万，测出SIMD真实性能                                  ║\n";
    std::cout << "╚════════════════════════════════════════════════════════════════════════════╝\n\n";
    
    std::cout << "⏰ 测试开始时间: " << start_time_str << "\n\n";
    
    std::vector<BenchmarkResult> results;
    
    // 测试1: 不同线程数的读写性能
    std::cout << "[1/5] 测试不同线程数的并发性能（90%读）...\n";
    for (int threads : {1, 2, 4, 8, 16}) {
        std::cout << "  - " << threads << " 线程:\n";
        auto result = benchmark_concurrent_rw(threads, 100000, 90);  // 90% 读
        results.push_back(result);
        std::cout << "    QPS: " << std::fixed << std::setprecision(0) << result.qps 
                  << ", P99: " << std::fixed << std::setprecision(2) << result.p99_latency_us << "μs\n";
    }
    
    // 测试2: 不同读写比例
    std::cout << "\n[2/5] 测试不同读写比例（8线程）...\n";
    for (int read_ratio : {50, 70, 90, 95, 99}) {
        std::cout << "  - R" << read_ratio << "/W" << (100-read_ratio) << ":\n";
        auto result = benchmark_concurrent_rw(8, 100000, read_ratio);
        results.push_back(result);
        std::cout << "    QPS: " << std::fixed << std::setprecision(0) << result.qps 
                  << ", P99: " << std::fixed << std::setprecision(2) << result.p99_latency_us << "μs\n";
    }
    
    // 测试3: 向量检索性能
    std::cout << "\n[3/5] 测试向量检索性能（10万向量，SIMD优化）...\n";
    for (int threads : {1, 2, 4, 8}) {
        std::cout << "  - " << threads << " 线程:\n";
        auto result = benchmark_vector_search(threads, 500);  // 减少搜索次数，因为数据量大了
        results.push_back(result);
        std::cout << "    QPS: " << std::fixed << std::setprecision(0) << result.qps 
                  << ", P99: " << std::fixed << std::setprecision(2) << result.p99_latency_us << "μs\n";
    }
    
    // 记录测试结束时间
    auto test_end_time = std::chrono::system_clock::now();
    std::string end_time_str = get_current_time();
    double total_duration = std::chrono::duration<double>(test_end_time - test_start_time).count();
    
    // 打印结果
    std::cout << "\n[4/5] 生成测试报告...\n";
    print_results(results);
    
    // 保存到CSV
    std::cout << "[5/5] 保存数据文件...\n";
    save_to_csv(results, "benchmark_results.csv", start_time_str, end_time_str, total_duration);
    
    std::cout << "\n╔════════════════════════════════════════════════════════════════════════════╗\n";
    std::cout << "║                          ✓ 压力测试完成！                                  ║\n";
    std::cout << "╚════════════════════════════════════════════════════════════════════════════╝\n\n";
    
    std::cout << "⏰ 测试时间统计:\n";
    std::cout << "  开始时间: " << start_time_str << "\n";
    std::cout << "  结束时间: " << end_time_str << "\n";
    std::cout << "  总耗时:   " << format_duration(total_duration) 
              << " (" << std::fixed << std::setprecision(2) << total_duration << "s)\n\n";
    
    std::cout << "📊 数据文件:\n";
    std::cout << "  - 查看详细数据: benchmark_results.csv\n";
    std::cout << "  - 生成可视化图表: python3 visualize_benchmark.py\n\n";
    
    return 0;
}
