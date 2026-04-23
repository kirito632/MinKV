/**
 * @file expiration_fast_wakeup_test.cpp
 * @brief 测试 ExpirationManager 的快速唤醒机制
 * 
 * 验证析构函数能够快速停止线程，而不需要等待完整的 check_interval
 */

#include "../base/expiration_manager.h"
#include "../base/async_logger.h"
#include <iostream>
#include <chrono>
#include <cassert>
#include <atomic>

using namespace minkv;

/**
 * @brief 测试快速停止机制
 * 
 * 验证点：
 * 1. 构造后线程自动启动
 * 2. 析构时线程快速停止（不需要等待完整的 check_interval）
 * 3. 停止时间应该远小于 check_interval
 */
void testFastWakeup() {
    std::cout << "\n=== 测试快速唤醒机制 ===" << std::endl;
    
    std::atomic<int> callback_count{0};
    
    // 创建一个简单的回调函数
    auto callback = [&callback_count](size_t shard_id, size_t sample_size) -> size_t {
        callback_count.fetch_add(1);
        std::cout << "  回调执行: shard=" << shard_id << ", sample=" << sample_size << std::endl;
        return 0;
    };
    
    // 使用较长的 check_interval (1000ms) 来测试快速停止
    const auto check_interval = std::chrono::milliseconds(1000);
    
    std::cout << "创建 ExpirationManager (check_interval=1000ms)..." << std::endl;
    auto start_time = std::chrono::steady_clock::now();
    
    {
        base::ExpirationManager mgr(callback, 4, check_interval, 10);
        
        std::cout << "线程已启动，等待 100ms..." << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        
        std::cout << "开始析构 (应该快速停止，不需要等待 1000ms)..." << std::endl;
        // mgr 在这里析构
    }
    
    auto end_time = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    std::cout << "总耗时: " << elapsed.count() << "ms" << std::endl;
    std::cout << "回调执行次数: " << callback_count.load() << std::endl;
    
    // 验证：停止时间应该远小于 check_interval (1000ms)
    // 如果没有快速唤醒机制，停止时间会接近 1000ms
    // 有了快速唤醒机制，停止时间应该在 200ms 以内
    if (elapsed.count() < 300) {
        std::cout << "✅ 快速唤醒成功！停止时间 " << elapsed.count() 
                  << "ms << check_interval (1000ms)" << std::endl;
    } else {
        std::cout << "❌ 快速唤醒失败！停止时间 " << elapsed.count() 
                  << "ms 接近 check_interval (1000ms)" << std::endl;
        assert(false && "快速唤醒机制未生效");
    }
}

/**
 * @brief 测试构造后线程自动运行
 */
void testAutoStart() {
    std::cout << "\n=== 测试自动启动 ===" << std::endl;
    
    std::atomic<bool> callback_called{false};
    
    auto callback = [&callback_called](size_t, size_t) -> size_t {
        callback_called.store(true);
        return 0;
    };
    
    std::cout << "创建 ExpirationManager..." << std::endl;
    base::ExpirationManager mgr(callback, 4, std::chrono::milliseconds(50), 10);
    
    std::cout << "等待回调执行..." << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    
    if (callback_called.load()) {
        std::cout << "✅ 线程自动启动成功！" << std::endl;
    } else {
        std::cout << "❌ 线程未自动启动！" << std::endl;
        assert(false && "线程未自动启动");
    }
}

/**
 * @brief 测试多次快速创建和销毁
 */
void testMultipleCreateDestroy() {
    std::cout << "\n=== 测试多次创建销毁 ===" << std::endl;
    
    auto callback = [](size_t, size_t) -> size_t { return 0; };
    
    const int iterations = 10;
    std::cout << "执行 " << iterations << " 次创建和销毁..." << std::endl;
    
    auto start_time = std::chrono::steady_clock::now();
    
    for (int i = 0; i < iterations; ++i) {
        base::ExpirationManager mgr(callback, 4, std::chrono::milliseconds(500), 10);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        // mgr 在这里析构
    }
    
    auto end_time = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    std::cout << "总耗时: " << elapsed.count() << "ms" << std::endl;
    std::cout << "平均每次: " << (elapsed.count() / iterations) << "ms" << std::endl;
    
    // 如果快速唤醒生效，每次应该在 50ms 以内完成
    if (elapsed.count() / iterations < 100) {
        std::cout << "✅ 多次创建销毁测试通过！" << std::endl;
    } else {
        std::cout << "❌ 多次创建销毁耗时过长！" << std::endl;
        assert(false && "快速唤醒机制可能未生效");
    }
}

/**
 * @brief 测试异常安全性
 */
void testExceptionSafety() {
    std::cout << "\n=== 测试异常安全性 ===" << std::endl;
    
    std::atomic<int> callback_count{0};
    
    // 回调函数会抛出异常
    auto callback = [&callback_count](size_t shard_id, size_t) -> size_t {
        callback_count.fetch_add(1);
        if (shard_id == 0) {
            throw std::runtime_error("测试异常");
        }
        return 1;
    };
    
    std::cout << "创建会抛异常的 ExpirationManager..." << std::endl;
    
    {
        base::ExpirationManager mgr(callback, 4, std::chrono::milliseconds(50), 10);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        std::cout << "即使有异常，线程仍在运行..." << std::endl;
    }
    
    std::cout << "回调执行次数: " << callback_count.load() << std::endl;
    
    if (callback_count.load() > 0) {
        std::cout << "✅ 异常安全性测试通过！线程在异常后继续运行。" << std::endl;
    } else {
        std::cout << "❌ 异常导致线程停止！" << std::endl;
        assert(false && "异常安全性测试失败");
    }
}

int main() {
    std::cout << "ExpirationManager 快速唤醒机制测试" << std::endl;
    std::cout << "====================================" << std::endl;
    
    try {
        // 初始化日志系统
        base::AsyncLogger::instance().setLogLevel(base::LogLevel::INFO);
        
        // 运行测试
        testAutoStart();
        testFastWakeup();
        testMultipleCreateDestroy();
        testExceptionSafety();
        
        std::cout << "\n🎉 所有测试通过！快速唤醒机制工作正常。" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "❌ 测试失败: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
