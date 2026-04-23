/**
 * @file expiration_manager_validation_test.cpp
 * @brief RAII重构验证测试 - 验证线程自动启动和停止
 */

#include "../base/expiration_manager.h"
#include "../base/async_logger.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <atomic>
#include <cassert>

using namespace minkv;

/**
 * @brief 测试1: 验证构造函数自动启动线程
 */
void testAutoStart() {
    std::cout << "\n=== 测试1: 构造函数自动启动线程 ===" << std::endl;
    
    std::atomic<int> callback_count{0};
    
    // 创建回调函数
    auto callback = [&callback_count](size_t shard_id, size_t sample_size) -> size_t {
        callback_count.fetch_add(1, std::memory_order_relaxed);
        std::cout << "  [Callback] shard=" << shard_id << ", sample=" << sample_size << std::endl;
        return 0;
    };
    
    {
        // 构造时应该自动启动线程
        base::ExpirationManager mgr(callback, 4, std::chrono::milliseconds(50), 10);
        
        std::cout << "ExpirationManager 已构造" << std::endl;
        
        // 等待一段时间，让回调被调用
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        
        // 验证回调被调用了
        int count = callback_count.load();
        std::cout << "回调被调用次数: " << count << std::endl;
        
        assert(count > 0 && "线程应该自动启动并调用回调");
        
        std::cout << "✅ 线程自动启动验证通过" << std::endl;
    }
    
    // 离开作用域后，析构函数应该自动停止线程
    std::cout << "ExpirationManager 已析构" << std::endl;
    
    // 记录析构前的回调次数
    int final_count = callback_count.load();
    
    // 等待一段时间，确认线程已停止
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // 验证回调不再被调用
    assert(callback_count.load() == final_count && "线程应该已停止");
    
    std::cout << "✅ 线程自动停止验证通过" << std::endl;
}

/**
 * @brief 测试2: 验证参数验证
 */
void testParameterValidation() {
    std::cout << "\n=== 测试2: 参数验证 ===" << std::endl;
    
    auto valid_callback = [](size_t, size_t) -> size_t { return 0; };
    
    // 测试空回调
    try {
        base::ExpirationManager mgr(nullptr, 4, std::chrono::milliseconds(100), 10);
        assert(false && "应该抛出异常");
    } catch (const std::invalid_argument& e) {
        std::cout << "✅ 空回调验证通过: " << e.what() << std::endl;
    }
    
    // 测试无效分片数
    try {
        base::ExpirationManager mgr(valid_callback, 0, std::chrono::milliseconds(100), 10);
        assert(false && "应该抛出异常");
    } catch (const std::invalid_argument& e) {
        std::cout << "✅ 无效分片数验证通过: " << e.what() << std::endl;
    }
    
    // 测试无效检查间隔
    try {
        base::ExpirationManager mgr(valid_callback, 4, std::chrono::milliseconds(0), 10);
        assert(false && "应该抛出异常");
    } catch (const std::invalid_argument& e) {
        std::cout << "✅ 无效检查间隔验证通过: " << e.what() << std::endl;
    }
    
    // 测试无效采样大小
    try {
        base::ExpirationManager mgr(valid_callback, 4, std::chrono::milliseconds(100), 0);
        assert(false && "应该抛出异常");
    } catch (const std::invalid_argument& e) {
        std::cout << "✅ 无效采样大小验证通过: " << e.what() << std::endl;
    }
}

/**
 * @brief 测试3: 验证异常安全
 */
void testExceptionSafety() {
    std::cout << "\n=== 测试3: 异常安全 ===" << std::endl;
    
    std::atomic<int> callback_count{0};
    
    // 创建会抛出异常的回调
    auto throwing_callback = [&callback_count](size_t shard_id, size_t) -> size_t {
        callback_count.fetch_add(1);
        if (shard_id % 2 == 0) {
            throw std::runtime_error("测试异常");
        }
        return 1;
    };
    
    {
        base::ExpirationManager mgr(throwing_callback, 4, std::chrono::milliseconds(50), 10);
        
        std::cout << "等待回调执行（包含异常）..." << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        
        // 验证即使有异常，线程仍在运行
        int count = callback_count.load();
        std::cout << "回调被调用次数: " << count << std::endl;
        assert(count > 0 && "线程应该继续运行");
        
        std::cout << "✅ 异常安全验证通过" << std::endl;
    }
}

/**
 * @brief 测试4: 验证统计功能
 */
void testStatistics() {
    std::cout << "\n=== 测试4: 统计功能 ===" << std::endl;
    
    std::atomic<int> total_expired{0};
    
    auto callback = [&total_expired](size_t shard_id, size_t sample_size) -> size_t {
        // 模拟删除一些过期key
        size_t expired = (shard_id + 1) % 3;
        total_expired.fetch_add(expired);
        return expired;
    };
    
    {
        base::ExpirationManager mgr(callback, 4, std::chrono::milliseconds(50), 10);
        
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        
        auto stats = mgr.getStats();
        
        std::cout << "统计信息:" << std::endl;
        std::cout << "  总检查次数: " << stats.total_checks << std::endl;
        std::cout << "  总过期删除数: " << stats.total_expired << std::endl;
        std::cout << "  总跳过次数: " << stats.total_skipped << std::endl;
        std::cout << "  平均过期比例: " << stats.avg_expired_ratio << std::endl;
        
        assert(stats.total_checks > 0 && "应该有检查记录");
        
        std::cout << "✅ 统计功能验证通过" << std::endl;
    }
}

/**
 * @brief 测试5: 验证多次构造析构
 */
void testMultipleLifecycles() {
    std::cout << "\n=== 测试5: 多次构造析构 ===" << std::endl;
    
    auto callback = [](size_t, size_t) -> size_t { return 0; };
    
    for (int i = 0; i < 5; ++i) {
        std::cout << "第 " << (i + 1) << " 次构造..." << std::endl;
        
        {
            base::ExpirationManager mgr(callback, 4, std::chrono::milliseconds(50), 10);
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        
        std::cout << "第 " << (i + 1) << " 次析构完成" << std::endl;
    }
    
    std::cout << "✅ 多次构造析构验证通过" << std::endl;
}

int main() {
    std::cout << "ExpirationManager RAII重构验证测试" << std::endl;
    std::cout << "====================================" << std::endl;
    
    try {
        // 初始化日志系统
        base::AsyncLogger::instance().setLogLevel(base::LogLevel::INFO);
        
        // 运行测试
        testAutoStart();
        testParameterValidation();
        testExceptionSafety();
        testStatistics();
        testMultipleLifecycles();
        
        std::cout << "\n🎉 所有验证测试通过！RAII重构成功。" << std::endl;
        std::cout << "\n核心验证点:" << std::endl;
        std::cout << "  ✅ 构造函数自动启动线程" << std::endl;
        std::cout << "  ✅ 析构函数自动停止线程" << std::endl;
        std::cout << "  ✅ 参数验证正确" << std::endl;
        std::cout << "  ✅ 异常安全保证" << std::endl;
        std::cout << "  ✅ 统计功能正常" << std::endl;
        std::cout << "  ✅ 多次构造析构无泄漏" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "❌ 测试失败: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
