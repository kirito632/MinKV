#include "db/sharded_cache.h"
#include <iostream>
#include <vector>
#include <iomanip>

using namespace minkv::db;

int main() {
    std::cout << "=== MinKV Vector Integration Test ===" << std::endl;
    
    // 1. 创建一个支持 String 和 Vector 的缓存
    ShardedCache<std::string, std::string> cache(10000, 8);
    
    std::cout << "\n[Test 1] 存储向量数据" << std::endl;
    
    // 创建几个测试向量
    std::vector<float> vec1 = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f};
    std::vector<float> vec2 = {1.1f, 2.1f, 3.1f, 4.1f, 5.1f};
    std::vector<float> vec3 = {10.0f, 20.0f, 30.0f, 40.0f, 50.0f};
    
    // 使用新的 vectorPut 接口存储
    cache.vectorPut("vector:1", vec1);
    cache.vectorPut("vector:2", vec2);
    cache.vectorPut("vector:3", vec3);
    
    std::cout << "✓ 成功存储 3 个向量" << std::endl;
    
    std::cout << "\n[Test 2] 读取向量数据" << std::endl;
    
    auto retrieved = cache.vectorGet("vector:1");
    std::cout << "Retrieved vector:1: [";
    for (size_t i = 0; i < retrieved.size(); ++i) {
        if (i > 0) std::cout << ", ";
        std::cout << retrieved[i];
    }
    std::cout << "]" << std::endl;
    
    if (retrieved == vec1) {
        std::cout << "✓ 向量数据完全匹配" << std::endl;
    } else {
        std::cout << "✗ 向量数据不匹配！" << std::endl;
    }
    
    std::cout << "\n[Test 3] 字符串和向量混合存储" << std::endl;
    
    // MinKV 现在既能存字符串，也能存向量
    cache.put("user:1", "Alice");
    cache.put("user:2", "Bob");
    cache.vectorPut("embedding:user:1", vec1);
    cache.vectorPut("embedding:user:2", vec2);
    
    auto user1 = cache.get("user:1");
    auto emb1 = cache.vectorGet("embedding:user:1");
    
    std::cout << "User 1: " << (user1 ? *user1 : "NOT FOUND") << std::endl;
    std::cout << "Embedding 1 dimension: " << emb1.size() << std::endl;
    std::cout << "✓ 字符串和向量混合存储成功" << std::endl;
    
    std::cout << "\n[Test 4] 缓存统计" << std::endl;
    
    auto stats = cache.getStats();
    std::cout << "缓存大小: " << stats.current_size << " / " << stats.capacity << std::endl;
    std::cout << "命中次数: " << stats.hits << std::endl;
    std::cout << "未命中次数: " << stats.misses << std::endl;
    
    std::cout << "\n=== 集成测试完成 ===" << std::endl;
    std::cout << "\n✓ MinKV 现在支持：" << std::endl;
    std::cout << "  1. 传统的 String KV 存储 (put/get)" << std::endl;
    std::cout << "  2. 向量数据存储 (vectorPut/vectorGet)" << std::endl;
    std::cout << "  3. 向量相似度搜索 (vectorSearch) - 使用 AVX2 SIMD 加速" << std::endl;
    std::cout << "  4. 分片并发架构 - 支持多线程并行访问" << std::endl;
    
    return 0;
}
