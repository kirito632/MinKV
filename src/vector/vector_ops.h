#pragma once

#include <string>
#include <vector>
#include <immintrin.h> // AVX/AVX2
#include <cmath>
#include <stdexcept>
#include <iostream>

/**
 * VectorOps: AI/RAG场景下的高性能向量计算引擎
 * 
 * 专为大语言模型(LLM)的检索增强生成(RAG)场景设计，支持：
 * - 高维向量存储与检索 (768D/1024D/1536D embeddings)
 * - SIMD加速的相似度计算 (AVX2指令集)
 * - 零拷贝序列化，适配AI推理的低延迟需求
 * 
 * 典型应用场景：
 * - 知识库向量化存储 (PDF/文档 -> embeddings)
 * - 实时语义检索 (用户query -> 最相似的K个文档)
 * - AI Agent的记忆系统 (对话历史向量化)
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
    
    // ==========================================
    // SIMD 开关：用于 A/B 测试
    // ==========================================
    
    /**
     * 统一接口：L2距离平方计算
     * 通过 USE_SIMD_OPTIMIZATION 宏控制是否使用SIMD优化
     * 
     * 编译选项：
     * - 开启SIMD: g++ -DUSE_SIMD_OPTIMIZATION=1 ...
     * - 关闭SIMD: g++ -DUSE_SIMD_OPTIMIZATION=0 ...
     */
    static float L2DistanceSquare(const float* a, const float* b, size_t dim) {
#if defined(USE_SIMD_OPTIMIZATION) && USE_SIMD_OPTIMIZATION == 0
        // 强制使用标量版本（用于A/B测试的Baseline）
        return L2DistanceSquare_Ref(a, b, dim);
#else
        // 默认使用SIMD优化版本
        return L2DistanceSquare_AVX2(a, b, dim);
#endif
    }
    
    // 实际距离需开根号 (为了排序通常只比平方即可，能省一次 sqrt)
    static float L2Distance(const float* a, const float* b, size_t dim) {
        return std::sqrt(L2DistanceSquare(a, b, dim));
    }

    // ==========================================
    // Phase 4: AI/RAG 专用计算 (Cosine Similarity)
    // ==========================================

    /**
     * 余弦相似度计算 - AI/RAG场景的核心指标
     * 
     * 在语义检索中，余弦相似度比欧式距离更适合：
     * - 不受向量长度影响，只关注方向
     * - 适合处理归一化的embedding向量
     * - 返回值范围 [-1, 1]，1表示完全相似
     * 
     * 应用场景：
     * - 文档相似度检索
     * - 问答系统的语义匹配
     * - 推荐系统的用户/物品相似度
     */
    static float CosineSimilarity_AVX2(const float* a, const float* b, size_t dim) {
        if (!a || !b || dim == 0) {
            return 0.0f;
        }

        __m256 dot_product = _mm256_setzero_ps();
        __m256 norm_a = _mm256_setzero_ps();
        __m256 norm_b = _mm256_setzero_ps();

        size_t i = 0;
        // SIMD计算：同时计算点积和模长
        for (; i + 8 <= dim; i += 8) {
            __m256 va = _mm256_loadu_ps(a + i);
            __m256 vb = _mm256_loadu_ps(b + i);
            
            // 点积累加
            dot_product = _mm256_fmadd_ps(va, vb, dot_product);
            
            // 模长平方累加
            norm_a = _mm256_fmadd_ps(va, va, norm_a);
            norm_b = _mm256_fmadd_ps(vb, vb, norm_b);
        }

        // 水平求和
        float dot_sum = 0.0f, norm_a_sum = 0.0f, norm_b_sum = 0.0f;
        float buffer[8];
        
        _mm256_storeu_ps(buffer, dot_product);
        for (int k = 0; k < 8; ++k) dot_sum += buffer[k];
        
        _mm256_storeu_ps(buffer, norm_a);
        for (int k = 0; k < 8; ++k) norm_a_sum += buffer[k];
        
        _mm256_storeu_ps(buffer, norm_b);
        for (int k = 0; k < 8; ++k) norm_b_sum += buffer[k];

        // 处理剩余元素
        for (; i < dim; ++i) {
            dot_sum += a[i] * b[i];
            norm_a_sum += a[i] * a[i];
            norm_b_sum += b[i] * b[i];
        }

        // 计算余弦相似度
        float norm_product = std::sqrt(norm_a_sum * norm_b_sum);
        if (norm_product < 1e-8f) {
            return 0.0f; // 避免除零
        }
        
        return dot_sum / norm_product;
    }

    /**
     * 批量相似度计算 - 适配RAG的Top-K检索场景
     * 
     * 给定查询向量，计算与候选向量集合的相似度
     * 典型用法：用户提问 -> 从知识库中找最相似的K个文档
     * 
     * @param query 查询向量 (用户问题的embedding)
     * @param candidates 候选向量集合 (知识库文档的embeddings)
     * @param dim 向量维度
     * @return 相似度分数数组
     */
    static std::vector<float> BatchCosineSimilarity(
        const float* query, 
        const std::vector<const float*>& candidates, 
        size_t dim) {
        
        std::vector<float> similarities;
        similarities.reserve(candidates.size());
        
        for (const float* candidate : candidates) {
            similarities.push_back(CosineSimilarity_AVX2(query, candidate, dim));
        }
        
        return similarities;
    }
};
