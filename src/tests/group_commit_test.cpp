#include "../base/group_commit.h"
#include "../base/append_file.h"
#include <iostream>
#include <thread>
#include <vector>
#include <chrono>
#include <random>
#include <atomic>

using namespace minkv::base;

void test_basic_group_commit() {
    std::cout << "=== Group Commit 基础测试 ===" << std::endl;
    
    GroupCommitManager manager("test_group_commit.log");
    manager.start();
    
    // 测试同步提交
    bool result1 = manager.commitSync("Test data 1\n");
    bool result2 = manager.commitSync("Test data 2\n");
    bool result3 = manager.commitSync("Test data 3\n");
    
    std::cout << "同步提交结果: " << result1 << ", " << result2 << ", " << result3 << std::endl;
    
    // 测试异步提交
    std::atomic<int> asyncResults{0};
    
    for (int i = 0; i < 10; ++i) {
        std::string data = "Async data " + std::to_string(i) + "\n";
        manager.commitAsync(data, [&asyncResults](bool success) {
            if (success) {
                asyncResults++;
            }
        });
    }
    
    // 等待异步操作完成
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    manager.stop();
    
    std::cout << "异步提交成功数: " << asyncResults.load() << "/10" << std::endl;
    
    auto stats = manager.getStats();
    std::cout << "统计信息:" << std::endl;
    std::cout << "  总提交数: " << stats.totalCommits << std::endl;
    std::cout << "  总批次数: " << stats.totalBatches << std::endl;
    std::cout << "  总字节数: " << stats.totalBytes << std::endl;
    std::cout << "  平均批次大小: " << stats.avgBatchSize << std::endl;
}

void test_performance_comparison() {
    std::cout << "=== 性能对比测试 ===" << std::endl;
    
    const int test_count = 1000;
    const std::string test_data = "Performance test data line with some content\n";
    
    // 测试直接写入性能
    auto start_time = std::chrono::high_resolution_clock::now();
    
    {
        AppendFile direct_file("test_direct.log");
        for (int i = 0; i < test_count; ++i) {
            direct_file.append(test_data.c_str(), test_data.size());
            direct_file.sync();  // 每次都同步
        }
    }
    
    auto direct_time = std::chrono::high_resolution_clock::now();
    
    // 测试Group Commit性能
    {
        GroupCommitManager manager("test_group.log");
        manager.start();
        
        for (int i = 0; i < test_count; ++i) {
            manager.commitSync(test_data);
        }
        
        manager.stop();
    }
    
    auto group_time = std::chrono::high_resolution_clock::now();
    
    auto direct_duration = std::chrono::duration_cast<std::chrono::milliseconds>(direct_time - start_time);
    auto group_duration = std::chrono::duration_cast<std::chrono::milliseconds>(group_time - direct_time);
    
    std::cout << "性能对比结果 (" << test_count << " 次写入):" << std::endl;
    std::cout << "  直接写入耗时: " << direct_duration.count() << "ms" << std::endl;
    std::cout << "  Group Commit耗时: " << group_duration.count() << "ms" << std::endl;
    
    if (group_duration.count() > 0) {
        double speedup = static_cast<double>(direct_duration.count()) / group_duration.count();
        std::cout << "  性能提升: " << speedup << "x" << std::endl;
    }
}

void test_concurrent_group_commit() {
    std::cout << "=== 并发Group Commit测试 ===" << std::endl;
    
    const int thread_count = 4;
    const int commits_per_thread = 500;
    
    GroupCommitManager manager("test_concurrent.log");
    manager.start();
    
    std::vector<std::thread> threads;
    std::atomic<int> total_success{0};
    
    auto start_time = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < thread_count; ++i) {
        threads.emplace_back([i, commits_per_thread, &manager, &total_success]() {
            int local_success = 0;
            
            for (int j = 0; j < commits_per_thread; ++j) {
                std::string data = "Thread-" + std::to_string(i) + 
                                 " Commit-" + std::to_string(j) + "\n";
                
                manager.commitAsync(data, [&local_success](bool success) {
                    if (success) {
                        local_success++;
                    }
                });
            }
            
            // 等待所有异步操作完成
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            total_success += local_success;
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    // 等待所有提交完成
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    manager.stop();
    
    auto stats = manager.getStats();
    
    int total_commits = thread_count * commits_per_thread;
    double qps = static_cast<double>(total_commits) / duration.count() * 1000;
    
    std::cout << "并发测试结果:" << std::endl;
    std::cout << "  线程数: " << thread_count << std::endl;
    std::cout << "  总提交数: " << total_commits << std::endl;
    std::cout << "  成功提交数: " << total_success.load() << std::endl;
    std::cout << "  耗时: " << duration.count() << "ms" << std::endl;
    std::cout << "  QPS: " << static_cast<int>(qps) << std::endl;
    std::cout << "  实际批次数: " << stats.totalBatches << std::endl;
    std::cout << "  平均批次大小: " << stats.avgBatchSize << std::endl;
}

void test_batch_optimization() {
    std::cout << "=== 批次优化测试 ===" << std::endl;
    
    // 测试不同批次大小的性能
    std::vector<size_t> batch_sizes = {1024, 4096, 8192, 16384};
    const int test_commits = 1000;
    const std::string test_data = "Batch optimization test data\n";
    
    for (size_t batch_size : batch_sizes) {
        GroupCommitManager manager("test_batch_" + std::to_string(batch_size) + ".log", 
                                 batch_size, std::chrono::milliseconds(50));
        manager.start();
        
        auto start_time = std::chrono::high_resolution_clock::now();
        
        for (int i = 0; i < test_commits; ++i) {
            manager.commitSync(test_data);
        }
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        
        manager.stop();
        
        auto stats = manager.getStats();
        double qps = static_cast<double>(test_commits) / duration.count() * 1000;
        
        std::cout << "批次大小 " << batch_size << " 字节:" << std::endl;
        std::cout << "  耗时: " << duration.count() << "ms" << std::endl;
        std::cout << "  QPS: " << static_cast<int>(qps) << std::endl;
        std::cout << "  批次数: " << stats.totalBatches << std::endl;
        std::cout << "  平均批次大小: " << stats.avgBatchSize << std::endl;
        std::cout << std::endl;
    }
}

int main() {
    std::cout << "MinKV Group Commit 系统测试开始" << std::endl;
    std::cout << std::endl;
    
    test_basic_group_commit();
    std::cout << std::endl;
    
    test_performance_comparison();
    std::cout << std::endl;
    
    test_concurrent_group_commit();
    std::cout << std::endl;
    
    test_batch_optimization();
    
    std::cout << "所有测试完成！" << std::endl;
    
    return 0;
}