#pragma once

#include <string>
#include <vector>
#include <immintrin.h> // AVX/AVX2
#include <cmath>
#include <stdexcept>
#include <iostream>

/**
 * VectorOps: 向量数据库的核心计算引擎
 * 负责向量的序列化、反序列化以及 SIMD 加速的距离计算
 */
class VectorOps {
public:
    // ==========================================
    // Phase 1: 存储层 (Zero-Copy 思想)
    // ==========================================

    /**
     * 将 float 向量序列化为 string (Raw Bytes)
     * 对应 "putVector" 逻辑
     */
    static std::string Serialize(const std::vector<float>& vec) {
        // 直接使用 string 的构造函数拷贝内存
        // 这种方式最稳健，且复用了 string 的内存管理
        return std::string(
            reinterpret_cast<const char*>(vec.data()), 
            vec.size() * sizeof(float)
        );
    }

    /**
     * 从 string 解析出 float 数组视图
     * 注意：这里返回的是指针，没有内存拷贝 (Zero-Copy)
     * 安全性：调用者必须确保 rawData 的生命周期
     */
    static const float* DeserializeView(const std::string& rawData, size_t& outDim) {
        if (rawData.size() % sizeof(float) != 0) {
            return nullptr; // 大小不对，不是合法的向量数据
        }
        outDim = rawData.size() / sizeof(float);
        return reinterpret_cast<const float*>(rawData.data());
    }

    /**
     * 安全版本：从 string 反序列化为 vector<float> 拷贝
     * 用于需要长期持有数据的场景，避免指针悬空问题
     */
    static std::vector<float> DeserializeCopy(const std::string& rawData) {
        if (rawData.size() % sizeof(float) != 0) {
            return {}; // 返回空向量表示错误
        }
        
        size_t dim = rawData.size() / sizeof(float);
        const float* ptr = reinterpret_cast<const float*>(rawData.data());
        return std::vector<float>(ptr, ptr + dim);
    }

    // ==========================================
    // Phase 2 & 3: 计算层 (SIMD 加速)
    // ==========================================

    /**
     * 参考实现：普通 C++ 循环计算欧式距离平方
     * 用来做 Benchmark 的基准 (Baseline)
     */
    static float L2DistanceSquare_Ref(const float* a, const float* b, size_t dim) {
        float sum = 0.0f;
        for (size_t i = 0; i < dim; ++i) {
            float diff = a[i] - b[i];
            sum += diff * diff;
        }
        return sum;
    }

    /**
     * SIMD 实现：AVX2 加速欧式距离平方
     * 优化点：
     * 1. _mm256_loadu_ps: 处理非对齐内存 (std::string data)
     * 2. _mm256_fmadd_ps: FMA 指令 (a*b + c) 减少指令数
     * 3. Loop Unrolling: 一次循环处理 8 个 float
     * 4. Tail Handling: 处理剩余的维度
     */
    static float L2DistanceSquare_AVX2(const float* a, const float* b, size_t dim) {
        // 参数验证：防止空指针和无效维度
        if (!a || !b || dim == 0) {
            return 0.0f;
        }

        float sum = 0.0f;
        size_t i = 0;

        // 累加器，初始化为 0
        __m256 sum_vec = _mm256_setzero_ps();

        // 每次处理 8 个 float (256 bits)
        // 32 byte alignment is NOT guaranteed for std::string, so use loadu
        for (; i + 8 <= dim; i += 8) {
            __m256 va = _mm256_loadu_ps(a + i); // Load unaligned
            __m256 vb = _mm256_loadu_ps(b + i);
            
            __m256 diff = _mm256_sub_ps(va, vb); // diff = a - b
            
            // sum_vec += diff * diff
            // 使用 FMA: result = a * b + c
            sum_vec = _mm256_fmadd_ps(diff, diff, sum_vec);
        }

        // Horizontal sum: 将 8 个 float 加到一个 float 上
        // 这是一个比较慢的操作，但整个向量只需要做一次
        float buffer[8];
        _mm256_storeu_ps(buffer, sum_vec);
        for (int k = 0; k < 8; ++k) {
            sum += buffer[k];
        }

        // Tail Handling: 处理剩下的不足 8 个的元素
        for (; i < dim; ++i) {
            float diff = a[i] - b[i];
            sum += diff * diff;
        }

        return sum;
    }
    
    // 实际距离需开根号 (为了排序通常只比平方即可，能省一次 sqrt)
    static float L2Distance(const float* a, const float* b, size_t dim) {
        return std::sqrt(L2DistanceSquare_AVX2(a, b, dim));
    }
};
