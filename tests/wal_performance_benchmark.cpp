#include <iostream>
#include <chrono>
#include <thread>
#include <vector>
#include <random>
#include <atomic>
#include <fstream>
#include <filesystem>
#include "core/sharded_cache.h"

using namespace minkv::db;
using Cache = ShardedCache<std::string, std::string>;

struct WALBenchmarkResult {
    std::string scenario;
    bool wal_enabled;
    int thread_count;
    double qps;
    double avg_latency_us;
    double p99_latency_us;
    double hit_rate;
    size_t total_ops;
    double duration_sec;
};

class WALPerformanceBenchmark {
private:
    std::atomic<bool> stop_flag_{false};
    std::atomic<size_t> total_ops_{0};
    std::atomic<size_t> total_hits_{0};
    std::vector<double> latencies_;
    std::mutex latencies_mutex_;
    
    // 测试参数
    static constexpr int PREFILL_RANGE = 5000;
    static constexpr int ACCESS_RANGE = 10000;
    static constexpr int TEST_DURATION_SEC = 10;
    
public:
    std::vector<WALBenchmarkResult> run_all_tests() {
        std::vector<WALBenchmarkResult> results;
        
        std::cout << "=== WAL Performance Impact Benchmark ===" << std::endl;
        std::cout << "Testing WAL ON vs WAL OFF performance..." << std::endl;
        std::cout << "Duration: " << TEST_DURATION_SEC << " seconds per test" << std::endl;
        std::cout << std::endl;
        
        // 测试场景：不同线程数 × WAL开关
        std::vector<int> thread_counts = {1, 2, 4, 8};
        std::vector<bool> wal_settings = {false, true}; // 先测试WAL关闭，再测试WAL开启
        
        for (int threads : thread_counts) {
            for (bool wal_enabled : wal_settings) {
                std::cout << "Testing: " << threads << " threads, WAL " 
                         << (wal_enabled ? "ENABLED" : "DISABLED") << std::endl;
                
                auto result = run_single_test(threads, wal_enabled);
                results.push_back(result);
                
                // 清理测试数据
                cleanup_test_data();
                
                // 短暂休息，让系统稳定
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
        }
        
        return results;
    }
    
private:
    WALBenchmarkResult run_single_test(int thread_count, bool wal_enabled) {
        // 重置统计
        stop_flag_ = false;
        total_ops_ = 0;
        total_hits_ = 0;
        latencies_.clear();
        
        // 创建缓存
        Cache cache(10000, 32); // 每分片10000容量，32分片
        
        // 配置WAL
        if (wal_enabled) {
            cache.enable_persistence("./test_wal_perf_data", 100); // 100ms fsync间隔
        }
        
        // 预填充数据
        prefill_cache(cache);
        
        // 启动工作线程
        std::vector<std::thread> workers;
        auto start_time = std::chrono::high_resolution_clock::now();
        
        for (int i = 0; i < thread_count; ++i) {
            workers.emplace_back([this, &cache]() {
                this->worker_thread(cache);
            });
        }
        
        // 运行指定时间
        std::this_thread::sleep_for(std::chrono::seconds(TEST_DURATION_SEC));
        stop_flag_ = true;
        
        // 等待所有线程结束
        for (auto& worker : workers) {
            worker.join();
        }
        
        auto end_time = std::chrono::high_resolution_clock::now();
        double duration = std::chrono::duration<double>(end_time - start_time).count();
        
        // 计算统计数据
        WALBenchmarkResult result;
        result.scenario = "R99W1"; // 99%读，1%写
        result.wal_enabled = wal_enabled;
        result.thread_count = thread_count;
        result.total_ops = total_ops_.load();
        result.duration_sec = duration;
        result.qps = result.total_ops / duration;
        result.hit_rate = total_hits_.load() * 100.0 / result.total_ops;
        
        // 计算延迟统计
        calculate_latency_stats(result);
        
        // 输出结果
        print_result(result);
        
        return result;
    }
    
    void prefill_cache(Cache& cache) {
        // 预填充5000个key，访问范围是10000，所以命中率约50%
        for (int i = 0; i < PREFILL_RANGE; ++i) {
            std::string key = "key_" + std::to_string(i);
            std::string value = "value_" + std::to_string(i) + "_data";
            cache.put(key, value);
        }
    }
    
    void worker_thread(Cache& cache) {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> key_dist(0, ACCESS_RANGE - 1);
        std::uniform_int_distribution<> op_dist(1, 100);
        
        while (!stop_flag_.load()) {
            auto start = std::chrono::high_resolution_clock::now();
            
            int key_id = key_dist(gen);
            std::string key = "key_" + std::to_string(key_id);
            
            bool hit = false;
            
            if (op_dist(gen) <= 99) {
                // 99% 读操作
                auto result = cache.get(key);
                hit = result.has_value();
            } else {
                // 1% 写操作
                std::string value = "value_" + std::to_string(key_id) + "_updated";
                cache.put(key, value);
                hit = true; // 写操作算作命中
            }
            
            auto end = std::chrono::high_resolution_clock::now();
            double latency_us = std::chrono::duration<double, std::micro>(end - start).count();
            
            // 记录统计
            total_ops_.fetch_add(1);
            if (hit) {
                total_hits_.fetch_add(1);
            }
            
            // 采样延迟数据（避免内存爆炸）
            if (total_ops_.load() % 1000 == 0) {
                std::lock_guard<std::mutex> lock(latencies_mutex_);
                latencies_.push_back(latency_us);
            }
        }
    }
    
    void calculate_latency_stats(WALBenchmarkResult& result) {
        if (latencies_.empty()) {
            result.avg_latency_us = 0;
            result.p99_latency_us = 0;
            return;
        }
        
        // 排序计算百分位
        std::sort(latencies_.begin(), latencies_.end());
        
        double sum = 0;
        for (double lat : latencies_) {
            sum += lat;
        }
        result.avg_latency_us = sum / latencies_.size();
        
        // P99延迟
        size_t p99_idx = static_cast<size_t>(latencies_.size() * 0.99);
        if (p99_idx >= latencies_.size()) p99_idx = latencies_.size() - 1;
        result.p99_latency_us = latencies_[p99_idx];
    }
    
    void print_result(const WALBenchmarkResult& result) {
        std::cout << "  QPS: " << static_cast<int>(result.qps) 
                  << ", P99: " << result.p99_latency_us << "μs"
                  << ", Hit Rate: " << result.hit_rate << "%"
                  << ", WAL: " << (result.wal_enabled ? "ON" : "OFF") << std::endl;
    }
    
    void cleanup_test_data() {
        // 清理测试产生的WAL文件
        try {
            std::filesystem::remove_all("./test_wal_perf_data");
        } catch (...) {
            // 忽略清理错误
        }
    }
};

void save_results_to_csv(const std::vector<WALBenchmarkResult>& results) {
    std::ofstream csv("wal_performance_results.csv");
    
    // CSV头部
    csv << "Scenario,WAL_Enabled,Thread_Count,QPS,Avg_Latency_us,P99_Latency_us,Hit_Rate,Total_Ops,Duration_sec\n";
    
    // 数据行
    for (const auto& result : results) {
        csv << result.scenario << ","
            << (result.wal_enabled ? "true" : "false") << ","
            << result.thread_count << ","
            << result.qps << ","
            << result.avg_latency_us << ","
            << result.p99_latency_us << ","
            << result.hit_rate << ","
            << result.total_ops << ","
            << result.duration_sec << "\n";
    }
    
    csv.close();
    std::cout << "\nResults saved to: wal_performance_results.csv" << std::endl;
}

void print_summary_analysis(const std::vector<WALBenchmarkResult>& results) {
    std::cout << "\n=== WAL Performance Impact Analysis ===" << std::endl;
    
    // 按线程数分组分析
    std::map<int, std::pair<WALBenchmarkResult, WALBenchmarkResult>> grouped;
    
    for (const auto& result : results) {
        if (result.wal_enabled) {
            grouped[result.thread_count].second = result;
        } else {
            grouped[result.thread_count].first = result;
        }
    }
    
    std::cout << "\nPerformance Impact Summary:\n";
    std::cout << "Threads | WAL OFF QPS | WAL ON QPS  | Performance Drop | Latency Impact\n";
    std::cout << "--------|-------------|-------------|------------------|----------------\n";
    
    for (const auto& [threads, pair] : grouped) {
        const auto& wal_off = pair.first;
        const auto& wal_on = pair.second;
        
        double qps_drop = (wal_off.qps - wal_on.qps) / wal_off.qps * 100;
        double latency_increase = (wal_on.p99_latency_us - wal_off.p99_latency_us) / wal_off.p99_latency_us * 100;
        
        printf("%7d | %11.0f | %11.0f | %15.1f%% | %13.1f%%\n",
               threads, wal_off.qps, wal_on.qps, qps_drop, latency_increase);
    }
    
    std::cout << "\n🎯 Key Findings:" << std::endl;
    std::cout << "• WAL provides ACID guarantees at the cost of performance" << std::endl;
    std::cout << "• Performance drop is due to disk I/O and fsync() overhead" << std::endl;
    std::cout << "• In production, can be optimized with Group Commit batching" << std::endl;
    std::cout << "• Trade-off: Consistency vs Performance" << std::endl;
}

int main() {
    try {
        WALPerformanceBenchmark benchmark;
        auto results = benchmark.run_all_tests();
        
        save_results_to_csv(results);
        print_summary_analysis(results);
        
        std::cout << "\n✅ WAL Performance Benchmark Complete!" << std::endl;
        std::cout << "📊 Data saved for interview preparation" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Benchmark failed: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}