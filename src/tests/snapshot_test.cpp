#include "../db/snapshot.h"
#include "../base/async_logger.h"
#include <iostream>
#include <unordered_map>
#include <chrono>
#include <thread>
#include <random>

using namespace minkv::db;
using namespace minkv::base;

// 模拟的内存数据库
class MockDatabase {
public:
    struct Record {
        std::string value;
        uint64_t expiration;
        
        Record() : expiration(0) {}
        Record(const std::string& v, uint64_t exp) : value(v), expiration(exp) {}
    };
    
    void set(const std::string& key, const std::string& value, uint64_t ttl = 0) {
        uint64_t expiration = 0;
        if (ttl > 0) {
            expiration = getCurrentTimestamp() + ttl;
        }
        data_[key] = Record(value, expiration);
    }
    
    bool get(const std::string& key, std::string& value) {
        auto it = data_.find(key);
        if (it == data_.end()) {
            return false;
        }
        
        // 检查是否过期
        if (it->second.expiration > 0 && getCurrentTimestamp() > it->second.expiration) {
            data_.erase(it);
            return false;
        }
        
        value = it->second.value;
        return true;
    }
    
    void clear() {
        data_.clear();
    }
    
    size_t size() const {
        return data_.size();
    }
    
    // 遍历所有数据（用于快照）
    void forEach(std::function<void(const std::string&, const std::string&, uint64_t)> callback) const {
        for (const auto& pair : data_) {
            callback(pair.first, pair.second.value, pair.second.expiration);
        }
    }
    
private:
    std::unordered_map<std::string, Record> data_;
    
    uint64_t getCurrentTimestamp() const {
        return std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }
};

void test_basic_snapshot() {
    std::cout << "=== 基础快照测试 ===" << std::endl;
    
    // 创建模拟数据
    MockDatabase db;
    db.set("key1", "value1");
    db.set("key2", "value2");
    db.set("key3", "value3", 3600); // 1小时后过期
    
    std::cout << "原始数据库记录数: " << db.size() << std::endl;
    
    // 创建快照管理器
    SnapshotManager snapMgr("./test_snapshots");
    
    // 创建快照
    bool success = snapMgr.createSnapshotSync("test_basic.snap", 
        [&db](SnapshotManager::SnapshotCallback callback) {
            db.forEach(callback);
        });
    
    std::cout << "快照创建结果: " << (success ? "成功" : "失败") << std::endl;
    
    // 清空数据库
    db.clear();
    std::cout << "清空后数据库记录数: " << db.size() << std::endl;
    
    // 从快照恢复
    bool loaded = snapMgr.loadSnapshot("test_basic.snap", 
        [&db](const std::string& key, const std::string& value, uint64_t expiration) {
            uint64_t ttl = 0;
            if (expiration > 0) {
                uint64_t now = std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
                if (expiration > now) {
                    ttl = expiration - now;
                }
            }
            db.set(key, value, ttl);
        });
    
    std::cout << "快照加载结果: " << (loaded ? "成功" : "失败") << std::endl;
    std::cout << "恢复后数据库记录数: " << db.size() << std::endl;
    
    // 验证数据
    std::string value;
    std::cout << "数据验证:" << std::endl;
    std::cout << "  key1: " << (db.get("key1", value) ? value : "not found") << std::endl;
    std::cout << "  key2: " << (db.get("key2", value) ? value : "not found") << std::endl;
    std::cout << "  key3: " << (db.get("key3", value) ? value : "not found") << std::endl;
}

void test_large_snapshot() {
    std::cout << "=== 大数据快照测试 ===" << std::endl;
    
    MockDatabase db;
    const int record_count = 10000;
    
    // 生成大量测试数据
    std::cout << "生成 " << record_count << " 条测试数据..." << std::endl;
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1000, 9999);
    
    auto start_time = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < record_count; ++i) {
        std::string key = "key_" + std::to_string(i);
        std::string value = "value_" + std::to_string(i) + "_" + std::to_string(dis(gen));
        db.set(key, value);
    }
    
    auto data_gen_time = std::chrono::high_resolution_clock::now();
    auto gen_duration = std::chrono::duration_cast<std::chrono::milliseconds>(data_gen_time - start_time);
    
    std::cout << "数据生成耗时: " << gen_duration.count() << "ms" << std::endl;
    
    // 创建快照
    SnapshotManager snapMgr("./test_snapshots");
    
    std::cout << "开始创建快照..." << std::endl;
    
    bool success = snapMgr.createSnapshotSync("test_large.snap",
        [&db](SnapshotManager::SnapshotCallback callback) {
            db.forEach(callback);
        });
    
    auto snapshot_time = std::chrono::high_resolution_clock::now();
    auto snap_duration = std::chrono::duration_cast<std::chrono::milliseconds>(snapshot_time - data_gen_time);
    
    std::cout << "快照创建结果: " << (success ? "成功" : "失败") << std::endl;
    std::cout << "快照创建耗时: " << snap_duration.count() << "ms" << std::endl;
    
    // 获取快照信息
    auto info = snapMgr.getSnapshotInfo("test_large.snap");
    if (info.isValid) {
        std::cout << "快照信息:" << std::endl;
        std::cout << "  文件大小: " << info.fileSize << " 字节" << std::endl;
        std::cout << "  记录数: " << info.recordCount << std::endl;
        std::cout << "  平均记录大小: " << (info.fileSize / info.recordCount) << " 字节" << std::endl;
    }
    
    // 测试加载性能
    db.clear();
    
    std::cout << "开始加载快照..." << std::endl;
    auto load_start = std::chrono::high_resolution_clock::now();
    
    bool loaded = snapMgr.loadSnapshot("test_large.snap",
        [&db](const std::string& key, const std::string& value, uint64_t expiration) {
            db.set(key, value, expiration > 0 ? expiration : 0);
        });
    
    auto load_end = std::chrono::high_resolution_clock::now();
    auto load_duration = std::chrono::duration_cast<std::chrono::milliseconds>(load_end - load_start);
    
    std::cout << "快照加载结果: " << (loaded ? "成功" : "失败") << std::endl;
    std::cout << "快照加载耗时: " << load_duration.count() << "ms" << std::endl;
    std::cout << "恢复后记录数: " << db.size() << std::endl;
    
    // 验证数据完整性
    int verified = 0;
    for (int i = 0; i < std::min(100, record_count); ++i) {
        std::string key = "key_" + std::to_string(i);
        std::string value;
        if (db.get(key, value) && value.find("value_" + std::to_string(i)) == 0) {
            verified++;
        }
    }
    
    std::cout << "数据完整性验证: " << verified << "/100 正确" << std::endl;
}

void test_concurrent_snapshot() {
    std::cout << "=== 并发快照测试 ===" << std::endl;
    
    MockDatabase db;
    
    // 创建初始数据
    for (int i = 0; i < 1000; ++i) {
        db.set("key_" + std::to_string(i), "initial_value_" + std::to_string(i));
    }
    
    SnapshotManager snapMgr("./test_snapshots");
    
    // 启动快照创建
    std::cout << "启动异步快照创建..." << std::endl;
    
    std::atomic<bool> snapshotCompleted{false};
    std::string snapshotError;
    
    bool started = snapMgr.createSnapshot("test_concurrent.snap",
        [&db](SnapshotManager::SnapshotCallback callback) {
            // 模拟快照过程中的延迟
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            db.forEach(callback);
        },
        [&snapshotCompleted, &snapshotError](bool success, const std::string& error) {
            snapshotCompleted = true;
            if (!success) {
                snapshotError = error;
            }
            std::cout << "快照异步完成: " << (success ? "成功" : "失败") << std::endl;
            if (!success) {
                std::cout << "错误: " << error << std::endl;
            }
        });
    
    if (!started) {
        std::cout << "快照启动失败" << std::endl;
        return;
    }
    
    // 在快照进行过程中修改数据（测试COW）
    std::cout << "在快照过程中修改数据..." << std::endl;
    
    for (int i = 0; i < 100; ++i) {
        db.set("new_key_" + std::to_string(i), "new_value_" + std::to_string(i));
        db.set("key_" + std::to_string(i), "modified_value_" + std::to_string(i));
    }
    
    // 等待快照完成
    while (!snapshotCompleted) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    
    std::cout << "当前数据库记录数: " << db.size() << std::endl;
    
    // 验证快照内容（应该是修改前的数据）
    MockDatabase snapDb;
    bool loaded = snapMgr.loadSnapshot("test_concurrent.snap",
        [&snapDb](const std::string& key, const std::string& value, uint64_t expiration) {
            snapDb.set(key, value, expiration > 0 ? expiration : 0);
        });
    
    if (loaded) {
        std::cout << "快照中的记录数: " << snapDb.size() << std::endl;
        
        // 验证快照中没有新增的数据
        std::string value;
        bool hasNewKey = snapDb.get("new_key_0", value);
        bool hasOriginalKey = snapDb.get("key_0", value);
        
        std::cout << "快照验证:" << std::endl;
        std::cout << "  包含新增key: " << (hasNewKey ? "是" : "否") << std::endl;
        std::cout << "  包含原始key: " << (hasOriginalKey ? "是" : "否") << std::endl;
        
        if (hasOriginalKey) {
            std::cout << "  key_0的值: " << value << std::endl;
            std::cout << "  是否为原始值: " << (value.find("initial_value") == 0 ? "是" : "否") << std::endl;
        }
    }
}

int main() {
    // 设置日志级别
    AsyncLogger::setLogLevel(LogLevel::INFO);
    
    std::cout << "MinKV 快照系统测试开始" << std::endl;
    std::cout << "使用 fork() + COW 机制" << std::endl;
    std::cout << std::endl;
    
    test_basic_snapshot();
    std::cout << std::endl;
    
    test_large_snapshot();
    std::cout << std::endl;
    
    test_concurrent_snapshot();
    std::cout << std::endl;
    
    std::cout << "所有测试完成！" << std::endl;
    
    return 0;
}