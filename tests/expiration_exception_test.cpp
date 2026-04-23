/**
 * @file expiration_exception_test.cpp
 * @brief 测试 ExpirationManager 的异常处理能力
 * 
 * 验证点：
 * 1. 构造函数的强异常保证
 * 2. 回调函数抛出异常时线程继续运行
 * 3. 多次异常后线程仍然正常工作
 */

#include "base/expiration_manager.h"
#include <atomic>
#include <stdexcept>
#include <thread>
#include <chrono>
#include <iostream>
#include <cassert>

using namespace minkv::base;
using namespace std::chrono_literals;

// 简单的测试框架
#define TEST_ASSERT(condition, message) \
    do { \
        if (!(condition)) { \
            std::cerr << "❌ FAILED: " << message << std::endl; \
            return false; \
        } \
    } while(0)

#define TEST_PASS(message) \
    std::cout << "✅ PASSED: " << message << std::endl

/**
 * @brief 测试构造函数参数验证
 */
bool test_constructor_validation() {
    std::cout << "\n=== Test: Constructor Validation ===" << std::endl;
    
    // 测试空回调
    try {
        ExpirationManager mgr(nullptr, 16, 100ms, 20);
        TEST_ASSERT(false, "Should throw for null callback");
    } catch (const std::invalid_argument& e) {
        TEST_PASS("Null callback throws invalid_argument");
    }
    
    // 测试无效分片数
    auto callback = [](size_t, size_t) { return 0; };
    try {
        ExpirationManager mgr(callback, 0, 100ms, 20);
        TEST_ASSERT(false, "Should throw for zero shard_count");
    } catch (const std::invalid_argument& e) {
        TEST_PASS("Zero shard_count throws invalid_argument");
    }
    
    // 测试无效检查间隔
    try {
        ExpirationManager mgr(callback, 16, 0ms, 20);
        TEST_ASSERT(false, "Should throw for zero check_interval");
    } catch (const std::invalid_argument& e) {
        TEST_PASS("Zero check_interval throws invalid_argument");
    }
    
    // 测试无效采样大小
    try {
        ExpirationManager mgr(callback, 16, 100ms, 0);
        TEST_ASSERT(false, "Should throw for zero sample_size");
    } catch (const std::invalid_argument& e) {
        TEST_PASS("Zero sample_size throws invalid_argument");
    }
    
    return true;
}

/**
 * @brief 测试回调函数抛出 std::exception 时的异常安全
 */
bool test_callback_std_exception() {
    std::cout << "\n=== Test: Callback std::exception Safety ===" << std::endl;
    
    std::atomic<int> call_count{0};
    std::atomic<int> exception_count{0};
    
    auto callback = [&](size_t shard_id, size_t sample_size) -> size_t {
        call_count++;
        
        // 偶数分片抛出异常
        if (shard_id % 2 == 0) {
            exception_count++;
            throw std::runtime_error("Test exception from shard " + std::to_string(shard_id));
        }
        
        // 奇数分片正常返回
        return 1;
    };
    
    // 创建管理器，检查间隔50ms
    ExpirationManager mgr(callback, 16, 50ms, 20);
    
    // 等待至少2轮检查（150ms）
    std::this_thread::sleep_for(150ms);
    
    // 验证：线程仍在运行
    TEST_ASSERT(call_count.load() > 0, "Callback should have been called");
    TEST_ASSERT(exception_count.load() > 0, "Exceptions should have been thrown");
    
    // 验证：异常没有导致线程崩溃
    int count_before = call_count.load();
    std::this_thread::sleep_for(100ms);
    int count_after = call_count.load();
    
    TEST_ASSERT(count_after > count_before, "Thread should continue running after exceptions");
    
    TEST_PASS("Thread continues running despite std::exception");
    return true;
}

/**
 * @brief 测试回调函数抛出未知异常时的异常安全
 */
bool test_callback_unknown_exception() {
    std::cout << "\n=== Test: Callback Unknown Exception Safety ===" << std::endl;
    
    std::atomic<int> call_count{0};
    std::atomic<int> exception_count{0};
    
    auto callback = [&](size_t shard_id, size_t sample_size) -> size_t {
        call_count++;
        
        // 某些分片抛出未知异常
        if (shard_id == 5 || shard_id == 10) {
            exception_count++;
            throw 42;  // 抛出非 std::exception 类型
        }
        
        return 1;
    };
    
    // 创建管理器
    ExpirationManager mgr(callback, 16, 50ms, 20);
    
    // 等待至少2轮检查
    std::this_thread::sleep_for(150ms);
    
    // 验证：线程仍在运行
    TEST_ASSERT(call_count.load() > 0, "Callback should have been called");
    TEST_ASSERT(exception_count.load() > 0, "Unknown exceptions should have been thrown");
    
    // 验证：未知异常没有导致线程崩溃
    int count_before = call_count.load();
    std::this_thread::sleep_for(100ms);
    int count_after = call_count.load();
    
    TEST_ASSERT(count_after > count_before, "Thread should continue running after unknown exceptions");
    
    TEST_PASS("Thread continues running despite unknown exceptions");
    return true;
}

/**
 * @brief 测试混合异常场景
 */
bool test_mixed_exceptions() {
    std::cout << "\n=== Test: Mixed Exceptions ===" << std::endl;
    
    std::atomic<int> call_count{0};
    std::atomic<int> std_exception_count{0};
    std::atomic<int> unknown_exception_count{0};
    std::atomic<int> success_count{0};
    
    auto callback = [&](size_t shard_id, size_t sample_size) -> size_t {
        call_count++;
        
        // 根据分片ID决定行为
        switch (shard_id % 4) {
            case 0:
                // 正常返回
                success_count++;
                return 1;
            case 1:
                // 抛出 std::exception
                std_exception_count++;
                throw std::runtime_error("std::exception");
            case 2:
                // 抛出未知异常
                unknown_exception_count++;
                throw 42;
            case 3:
                // 正常返回
                success_count++;
                return 2;
        }
        return 0;
    };
    
    // 创建管理器
    ExpirationManager mgr(callback, 16, 50ms, 20);
    
    // 等待多轮检查
    std::this_thread::sleep_for(200ms);
    
    // 验证：所有类型的调用都发生了
    TEST_ASSERT(call_count.load() > 0, "Callback should have been called");
    TEST_ASSERT(std_exception_count.load() > 0, "std::exception should have been thrown");
    TEST_ASSERT(unknown_exception_count.load() > 0, "Unknown exception should have been thrown");
    TEST_ASSERT(success_count.load() > 0, "Some calls should succeed");
    
    // 验证：线程持续运行
    int count_before = call_count.load();
    std::this_thread::sleep_for(100ms);
    int count_after = call_count.load();
    
    TEST_ASSERT(count_after > count_before, "Thread should continue running with mixed exceptions");
    
    TEST_PASS("Thread handles mixed exceptions correctly");
    return true;
}

/**
 * @brief 测试所有分片都抛出异常的极端情况
 */
bool test_all_shards_throw() {
    std::cout << "\n=== Test: All Shards Throw Exception ===" << std::endl;
    
    std::atomic<int> call_count{0};
    
    auto callback = [&](size_t shard_id, size_t sample_size) -> size_t {
        call_count++;
        throw std::runtime_error("All shards fail");
    };
    
    // 创建管理器
    ExpirationManager mgr(callback, 16, 50ms, 20);
    
    // 等待多轮检查
    std::this_thread::sleep_for(200ms);
    
    // 验证：回调被持续调用（线程没有崩溃）
    TEST_ASSERT(call_count.load() > 16, "Thread should continue calling callback despite all failures");
    
    // 验证：线程仍在运行
    int count_before = call_count.load();
    std::this_thread::sleep_for(100ms);
    int count_after = call_count.load();
    
    TEST_ASSERT(count_after > count_before, "Thread should continue running even when all callbacks fail");
    
    TEST_PASS("Thread survives when all callbacks throw");
    return true;
}

/**
 * @brief 测试统计信息在异常情况下的正确性
 */
bool test_stats_with_exceptions() {
    std::cout << "\n=== Test: Stats With Exceptions ===" << std::endl;
    
    std::atomic<int> exception_count{0};
    
    auto callback = [&](size_t shard_id, size_t sample_size) -> size_t {
        // 一半分片抛出异常
        if (shard_id < 8) {
            exception_count++;
            throw std::runtime_error("Test exception");
        }
        // 另一半正常返回
        return 5;
    };
    
    // 创建管理器
    ExpirationManager mgr(callback, 16, 50ms, 20);
    
    // 等待至少2轮检查
    std::this_thread::sleep_for(150ms);
    
    // 获取统计信息
    auto stats = mgr.getStats();
    
    // 验证：统计信息正常更新
    TEST_ASSERT(stats.total_checks > 0, "Should have completed some checks");
    TEST_ASSERT(stats.total_skipped > 0, "Exception shards should be counted as skipped");
    TEST_ASSERT(stats.total_expired > 0, "Successful shards should contribute to expired count");
    
    // 验证：异常确实发生了
    TEST_ASSERT(exception_count.load() > 0, "Exceptions should have been thrown");
    
    TEST_PASS("Statistics updated correctly despite exceptions");
    return true;
}

int main() {
    std::cout << "========================================" << std::endl;
    std::cout << "ExpirationManager Exception Safety Tests" << std::endl;
    std::cout << "========================================" << std::endl;
    
    int passed = 0;
    int failed = 0;
    
    // 运行所有测试
    if (test_constructor_validation()) passed++; else failed++;
    if (test_callback_std_exception()) passed++; else failed++;
    if (test_callback_unknown_exception()) passed++; else failed++;
    if (test_mixed_exceptions()) passed++; else failed++;
    if (test_all_shards_throw()) passed++; else failed++;
    if (test_stats_with_exceptions()) passed++; else failed++;
    
    // 输出总结
    std::cout << "\n========================================" << std::endl;
    std::cout << "Test Summary:" << std::endl;
    std::cout << "  Passed: " << passed << std::endl;
    std::cout << "  Failed: " << failed << std::endl;
    std::cout << "========================================" << std::endl;
    
    return (failed == 0) ? 0 : 1;
}

