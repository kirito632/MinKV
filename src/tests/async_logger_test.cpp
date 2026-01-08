#include "../base/async_logger.h"
#include <iostream>
#include <thread>
#include <vector>
#include <chrono>

using namespace minkv::base;

void test_basic_logging() {
    std::cout << "=== 基础日志测试 ===" << std::endl;
    
    LOG_INFO << "MinKV 异步日志系统启动";
    LOG_DEBUG << "这是调试信息，默认不会显示";
    LOG_WARN << "这是警告信息";
    LOG_ERROR << "这是错误信息";
    
    // 测试不同数据类型
    int count = 42;
    double pi = 3.14159;
    std::string name = "MinKV";
    
    LOG_INFO << "测试数据类型: count=" << count << ", pi=" << pi << ", name=" << name;
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::cout << "基础日志测试完成" << std::endl;
}

void test_concurrent_logging() {
    std::cout << "=== 并发日志测试 ===" << std::endl;
    
    const int thread_count = 4;
    const int logs_per_thread = 1000;
    
    std::vector<std::thread> threads;
    
    auto start_time = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < thread_count; ++i) {
        threads.emplace_back([i, logs_per_thread]() {
            for (int j = 0; j < logs_per_thread; ++j) {
                LOG_INFO << "Thread-" << i << " Log-" << j << " 测试并发写入性能";
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    int total_logs = thread_count * logs_per_thread;
    double qps = (double)total_logs / duration.count() * 1000;
    
    std::cout << "并发测试完成:" << std::endl;
    std::cout << "  线程数: " << thread_count << std::endl;
    std::cout << "  总日志数: " << total_logs << std::endl;
    std::cout << "  耗时: " << duration.count() << "ms" << std::endl;
    std::cout << "  QPS: " << static_cast<int>(qps) << std::endl;
}

void test_log_levels() {
    std::cout << "=== 日志级别测试 ===" << std::endl;
    
    // 设置为DEBUG级别
    AsyncLogger::setLogLevel(LogLevel::DEBUG);
    LOG_DEBUG << "现在可以看到DEBUG信息了";
    LOG_INFO << "INFO级别信息";
    
    // 设置为ERROR级别
    AsyncLogger::setLogLevel(LogLevel::ERROR);
    LOG_INFO << "这条INFO不会显示";
    LOG_WARN << "这条WARN不会显示";
    LOG_ERROR << "只有ERROR及以上才显示";
    
    // 恢复INFO级别
    AsyncLogger::setLogLevel(LogLevel::INFO);
    LOG_INFO << "恢复INFO级别";
    
    std::cout << "日志级别测试完成" << std::endl;
}

void test_performance_comparison() {
    std::cout << "=== 性能对比测试 ===" << std::endl;
    
    const int test_count = 10000;
    
    // 测试cout性能
    auto start_time = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < test_count; ++i) {
        // std::cout << "cout test " << i << std::endl;  // 注释掉避免输出太多
    }
    auto cout_time = std::chrono::high_resolution_clock::now();
    
    // 测试异步日志性能
    for (int i = 0; i < test_count; ++i) {
        LOG_INFO << "async log test " << i;
    }
    auto async_time = std::chrono::high_resolution_clock::now();
    
    auto cout_duration = std::chrono::duration_cast<std::chrono::microseconds>(cout_time - start_time);
    auto async_duration = std::chrono::duration_cast<std::chrono::microseconds>(async_time - cout_time);
    
    std::cout << "性能对比结果:" << std::endl;
    std::cout << "  cout耗时: " << cout_duration.count() << "μs" << std::endl;
    std::cout << "  异步日志耗时: " << async_duration.count() << "μs" << std::endl;
    std::cout << "  性能提升: " << (double)cout_duration.count() / async_duration.count() << "x" << std::endl;
}

int main() {
    std::cout << "MinKV 异步日志系统测试开始" << std::endl;
    std::cout << "日志文件: minkv.log" << std::endl;
    std::cout << std::endl;
    
    test_basic_logging();
    std::cout << std::endl;
    
    test_log_levels();
    std::cout << std::endl;
    
    test_concurrent_logging();
    std::cout << std::endl;
    
    test_performance_comparison();
    std::cout << std::endl;
    
    // 等待日志写入完成
    std::this_thread::sleep_for(std::chrono::seconds(1));
    
    std::cout << "所有测试完成！请查看 minkv.log 文件" << std::endl;
    
    return 0;
}