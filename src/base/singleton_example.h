#pragma once

#include "singleton.h"
#include <string>
#include <iostream>

namespace minkv {
namespace base {

/**
 * @brief 配置管理器示例
 * 
 * 展示如何使用Singleton模板创建单例类
 */
class ConfigManager : public Singleton<ConfigManager> {
    // 声明Singleton为友元，允许其访问私有构造函数
    friend class Singleton<ConfigManager>;
    
public:
    void SetConfig(const std::string& key, const std::string& value) {
        configs_[key] = value;
    }
    
    std::string GetConfig(const std::string& key) const {
        auto it = configs_.find(key);
        return it != configs_.end() ? it->second : "";
    }
    
    void PrintConfigs() const {
        std::cout << "=== Configuration ===" << std::endl;
        for (const auto& [key, value] : configs_) {
            std::cout << key << " = " << value << std::endl;
        }
    }

private:
    // 私有构造函数，只能通过Singleton::GetInstance()创建
    ConfigManager() {
        std::cout << "ConfigManager initialized" << std::endl;
    }
    
    std::unordered_map<std::string, std::string> configs_;
};

/**
 * @brief 性能监控器示例
 * 
 * 展示如何使用LazySingleton进行复杂初始化
 */
class PerformanceMonitor : public LazySingleton<PerformanceMonitor> {
    friend class LazySingleton<PerformanceMonitor>;
    
public:
    void RecordOperation(const std::string& operation, double latency_ms) {
        operation_count_++;
        total_latency_ms_ += latency_ms;
        std::cout << "Recorded: " << operation << " took " << latency_ms << "ms" << std::endl;
    }
    
    double GetAverageLatency() const {
        return operation_count_ > 0 ? total_latency_ms_ / operation_count_ : 0.0;
    }
    
    void PrintStats() const {
        std::cout << "=== Performance Stats ===" << std::endl;
        std::cout << "Operations: " << operation_count_ << std::endl;
        std::cout << "Average Latency: " << GetAverageLatency() << "ms" << std::endl;
    }

private:
    PerformanceMonitor() = default;
    
    uint64_t operation_count_ = 0;
    double total_latency_ms_ = 0.0;
};

} // namespace base
} // namespace minkv

// 使用示例：
/*
int main() {
    // 使用简单单例
    auto& config = minkv::base::ConfigManager::GetInstance();
    config.SetConfig("log_level", "INFO");
    config.SetConfig("max_connections", "1000");
    config.PrintConfigs();
    
    // 使用延迟初始化单例
    auto& monitor = minkv::base::PerformanceMonitor::GetInstance([](auto& instance) {
        std::cout << "PerformanceMonitor lazy initialized" << std::endl;
        // 可以在这里进行复杂的初始化逻辑
    });
    
    monitor.RecordOperation("cache_get", 1.5);
    monitor.RecordOperation("cache_set", 2.3);
    monitor.PrintStats();
    
    return 0;
}
*/