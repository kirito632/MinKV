#include "../db/vector_ops.h"
#include <iostream>
#include <vector>
#include <random>
#include <chrono>
#include <iomanip>

// 生成随机向量
std::vector<float> generate_random_vector(size_t dim) {
    std::vector<float> vec(dim);
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_real_distribution<float> dis(-1.0f, 1.0f);
    for (size_t i = 0; i < dim; ++i) {
        vec[i] = dis(gen);
    }
    return vec;
}

int main() {
    // 参数设置
    const size_t DIM = 512;          // 向量维度 (常见大模型维度 512/768/1024)
    const size_t NUM_VECTORS = 10000; // 向量数量
    const size_t NUM_QUERIES = 100;   // 查询次数
    
    std::cout << "=== MinKV Vector Engine Benchmark ===" << std::endl;
    std::cout << "Dimension: " << DIM << std::endl;
    std::cout << "Dataset Size: " << NUM_VECTORS << std::endl;
    std::cout << "Total Computations: " << NUM_VECTORS * NUM_QUERIES << std::endl;
    std::cout << "-------------------------------------" << std::endl;

    // 1. 准备数据
    std::vector<std::vector<float>> database;
    database.reserve(NUM_VECTORS);
    for (size_t i = 0; i < NUM_VECTORS; ++i) {
        database.push_back(generate_random_vector(DIM));
    }
    
    std::vector<float> query = generate_random_vector(DIM);

    // 2. 测试 Baseline (普通 C++ 循环)
    auto start_ref = std::chrono::high_resolution_clock::now();
    float sum_ref = 0;
    for (int q = 0; q < NUM_QUERIES; ++q) {
        for (const auto& vec : database) {
            // 使用我们写的 Reference 实现
            sum_ref += VectorOps::L2DistanceSquare_Ref(query.data(), vec.data(), DIM);
        }
    }
    auto end_ref = std::chrono::high_resolution_clock::now();
    auto duration_ref = std::chrono::duration_cast<std::chrono::milliseconds>(end_ref - start_ref).count();

    // 3. 测试 AVX2 (SIMD 加速)
    auto start_avx = std::chrono::high_resolution_clock::now();
    float sum_avx = 0;
    for (int q = 0; q < NUM_QUERIES; ++q) {
        for (const auto& vec : database) {
            // 使用我们写的 AVX2 实现
            sum_avx += VectorOps::L2DistanceSquare_AVX2(query.data(), vec.data(), DIM);
        }
    }
    auto end_avx = std::chrono::high_resolution_clock::now();
    auto duration_avx = std::chrono::duration_cast<std::chrono::milliseconds>(end_avx - start_avx).count();

    // 4. 结果输出
    std::cout << "Baseline Time: " << duration_ref << " ms" << std::endl;
    std::cout << "AVX2 SIMD Time: " << duration_avx << " ms" << std::endl;
    
    double speedup = (double)duration_ref / duration_avx;
    std::cout << "Speedup: " << std::fixed << std::setprecision(2) << speedup << "x" << std::endl;
    
    // 简单校验结果一致性 (防止优化错了)
    // 浮点数累加可能有微小误差，这里只打印出来看看
    std::cout << "Check Sum Ref: " << sum_ref << std::endl;
    std::cout << "Check Sum AVX: " << sum_avx << std::endl;

    return 0;
}
