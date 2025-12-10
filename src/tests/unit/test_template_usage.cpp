#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include "../../db/lru_cache.h"
#include "../../db/sharded_cache.h"

using namespace minkv::db;

// 演示结构体
struct UserInfo {
    int uid;
    std::string name;
    std::string email;
    
    UserInfo() = default;
    UserInfo(int u, const std::string& n, const std::string& e) 
        : uid(u), name(n), email(e) {}
};

int main() {
    std::cout << "╔════════════════════════════════════════════════════════════════╗\n";
    std::cout << "║          FlashCache 模板化使用演示                            ║\n";
    std::cout << "╚════════════════════════════════════════════════════════════════╝\n\n";

    // ========== 示例 1: 字符串 Key-Value 缓存 ==========
    std::cout << "[示例 1] 字符串 Key-Value 缓存\n";
    std::cout << "─────────────────────────────────\n";
    {
        LruCache<std::string, std::string> cache(100);
        
        cache.put("user:1001", "Alice");
        cache.put("user:1002", "Bob");
        cache.put("user:1003", "Charlie");
        
        auto val = cache.get("user:1002");
        if (val.has_value()) {
            std::cout << "✓ 获取 user:1002 = " << val.value() << std::endl;
        }
        
        std::cout << "✓ 缓存大小: " << cache.size() << std::endl << std::endl;
    }

    // ========== 示例 2: 整数 Key-Value 缓存 ==========
    std::cout << "[示例 2] 整数 Key-Value 缓存\n";
    std::cout << "─────────────────────────────────\n";
    {
        LruCache<int, int> cache(100);
        
        cache.put(1, 100);
        cache.put(2, 200);
        cache.put(3, 300);
        
        auto val = cache.get(2);
        if (val.has_value()) {
            std::cout << "✓ 获取 key=2 => value=" << val.value() << std::endl;
        }
        
        std::cout << "✓ 缓存大小: " << cache.size() << std::endl << std::endl;
    }

    // ========== 示例 3: 字符串 Key + 自定义结构体 Value ==========
    std::cout << "[示例 3] 字符串 Key + 自定义结构体 Value\n";
    std::cout << "─────────────────────────────────────────\n";
    {
        LruCache<std::string, UserInfo> cache(100);
        
        cache.put("uid:1001", UserInfo(1001, "Alice", "alice@example.com"));
        cache.put("uid:1002", UserInfo(1002, "Bob", "bob@example.com"));
        
        auto user = cache.get("uid:1001");
        if (user.has_value()) {
            std::cout << "✓ 获取用户信息:\n";
            std::cout << "  - UID: " << user.value().uid << "\n";
            std::cout << "  - Name: " << user.value().name << "\n";
            std::cout << "  - Email: " << user.value().email << std::endl;
        }
        
        std::cout << "✓ 缓存大小: " << cache.size() << std::endl << std::endl;
    }

    // ========== 示例 4: 分片缓存 (字符串 Key-Value) ==========
    std::cout << "[示例 4] 分片缓存 (字符串 Key-Value)\n";
    std::cout << "──────────────────────────────────────\n";
    {
        ShardedCache<std::string, std::string> cache(50, 16); // 总容量 800，16 个分片
        
        for (int i = 0; i < 100; ++i) {
            cache.put("key_" + std::to_string(i), "value_" + std::to_string(i));
        }
        
        auto val = cache.get("key_50");
        if (val.has_value()) {
            std::cout << "✓ 获取 key_50 = " << val.value() << std::endl;
        }
        
        std::cout << "✓ 缓存大小: " << cache.size() << "\n";
        std::cout << "✓ 分片数: " << cache.shard_count() << std::endl << std::endl;
    }

    // ========== 示例 5: 分片缓存 (整数 Key + 自定义 Value) ==========
    std::cout << "[示例 5] 分片缓存 (整数 Key + 自定义 Value)\n";
    std::cout << "──────────────────────────────────────────\n";
    {
        ShardedCache<int, UserInfo> cache(50, 32); // 总容量 1600，32 个分片
        
        for (int i = 1001; i <= 1010; ++i) {
            cache.put(i, UserInfo(i, "User_" + std::to_string(i), 
                                 "user" + std::to_string(i) + "@example.com"));
        }
        
        auto user = cache.get(1005);
        if (user.has_value()) {
            std::cout << "✓ 获取用户 1005:\n";
            std::cout << "  - Name: " << user.value().name << "\n";
            std::cout << "  - Email: " << user.value().email << std::endl;
        }
        
        std::cout << "✓ 缓存大小: " << cache.size() << "\n";
        std::cout << "✓ 分片数: " << cache.shard_count() << std::endl << std::endl;
    }

    // ========== 示例 6: TTL 支持 ==========
    std::cout << "[示例 6] TTL 支持 (过期时间)\n";
    std::cout << "────────────────────────────\n";
    {
        LruCache<std::string, std::string> cache(100);
        
        // 设置 500ms 过期时间
        cache.put("temp_key", "temp_value", 500);
        
        auto val1 = cache.get("temp_key");
        std::cout << "✓ 立即获取: " << (val1.has_value() ? "命中" : "未命中") << std::endl;
        
        // 等待 600ms，让 TTL 过期
        std::this_thread::sleep_for(std::chrono::milliseconds(600));
        
        auto val2 = cache.get("temp_key");
        std::cout << "✓ 600ms 后获取: " << (val2.has_value() ? "命中" : "未命中 (已过期)") << std::endl << std::endl;
    }

    std::cout << "╔════════════════════════════════════════════════════════════════╗\n";
    std::cout << "║                    所有示例执行完成！                         ║\n";
    std::cout << "╚════════════════════════════════════════════════════════════════╝\n";

    return 0;
}
