#include <chrono>
#include <immintrin.h>
#include <iomanip>
#include <iostream>
#include <random>
#include <vector>

// 标量版本（禁用SIMD）
float L2Distance_Scalar(const float *a, const float *b, size_t dim) {
  float sum = 0.0f;
  for (size_t i = 0; i < dim; ++i) {
    float diff = a[i] - b[i];
    sum += diff * diff;
  }
  return sum;
}

// SIMD版本（AVX2优化）
float L2Distance_SIMD(const float *a, const float *b, size_t dim) {
  __m256 sum_vec = _mm256_setzero_ps();
  size_t i = 0;

  // 并行处理8个float
  for (; i + 8 <= dim; i += 8) {
    __m256 va = _mm256_loadu_ps(a + i);
    __m256 vb = _mm256_loadu_ps(b + i);
    __m256 diff = _mm256_sub_ps(va, vb);
    sum_vec = _mm256_fmadd_ps(diff, diff, sum_vec);
  }

  // 水平求和
  float buffer[8];
  _mm256_storeu_ps(buffer, sum_vec);
  float sum = 0.0f;
  for (int k = 0; k < 8; ++k) {
    sum += buffer[k];
  }

  // 处理剩余元素
  for (; i < dim; ++i) {
    float diff = a[i] - b[i];
    sum += diff * diff;
  }

  return sum;
}

int main() {
  std::cout << "========================================" << std::endl;
  std::cout << "MinKV SIMD 优化效果对比测试" << std::endl;
  std::cout << "========================================" << std::endl;
  std::cout << std::endl;

  // 测试配置
  const size_t DIM = 512;
  const size_t NUM_VECTORS = 10000;
  const size_t NUM_QUERIES = 100000;

  std::cout << "测试配置:" << std::endl;
  std::cout << "  向量维度: " << DIM << std::endl;
  std::cout << "  数据集大小: " << NUM_VECTORS << " 个向量" << std::endl;
  std::cout << "  查询次数: " << NUM_QUERIES << " 次" << std::endl;
  std::cout << std::endl;

  // 生成随机数据
  std::cout << "生成测试数据..." << std::endl;
  std::vector<std::vector<float>> dataset(NUM_VECTORS);
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<float> dis(0.0f, 1.0f);

  for (size_t i = 0; i < NUM_VECTORS; ++i) {
    dataset[i].resize(DIM);
    for (size_t j = 0; j < DIM; ++j) {
      dataset[i][j] = dis(gen);
    }
  }

  std::vector<float> query(DIM);
  for (size_t j = 0; j < DIM; ++j) {
    query[j] = dis(gen);
  }

  std::cout << "✅ 数据生成完成" << std::endl;
  std::cout << std::endl;

  // ========================================
  // 测试1: 标量版本（Baseline）
  // ========================================
  std::cout << "========================================" << std::endl;
  std::cout << "测试1: 标量版本（Baseline）" << std::endl;
  std::cout << "========================================" << std::endl;

  auto start_scalar = std::chrono::high_resolution_clock::now();

  volatile float result_scalar = 0.0f; // volatile防止编译器优化掉
  for (size_t q = 0; q < NUM_QUERIES; ++q) {
    size_t idx = q % NUM_VECTORS;
    result_scalar = L2Distance_Scalar(query.data(), dataset[idx].data(), DIM);
  }

  auto end_scalar = std::chrono::high_resolution_clock::now();
  auto duration_scalar = std::chrono::duration_cast<std::chrono::microseconds>(
                             end_scalar - start_scalar)
                             .count();

  double qps_scalar = NUM_QUERIES * 1000000.0 / duration_scalar;
  double latency_scalar = duration_scalar * 1.0 / NUM_QUERIES;

  std::cout << "耗时: " << duration_scalar / 1000 << " ms" << std::endl;
  std::cout << "QPS: " << std::fixed << std::setprecision(0) << qps_scalar
            << std::endl;
  std::cout << "平均延迟: " << std::fixed << std::setprecision(2)
            << latency_scalar << " μs" << std::endl;
  std::cout << std::endl;

  // ========================================
  // 测试2: SIMD版本（AVX2优化）
  // ========================================
  std::cout << "========================================" << std::endl;
  std::cout << "测试2: SIMD版本（AVX2优化）" << std::endl;
  std::cout << "========================================" << std::endl;

  auto start_simd = std::chrono::high_resolution_clock::now();

  volatile float result_simd = 0.0f;
  for (size_t q = 0; q < NUM_QUERIES; ++q) {
    size_t idx = q % NUM_VECTORS;
    result_simd = L2Distance_SIMD(query.data(), dataset[idx].data(), DIM);
  }

  auto end_simd = std::chrono::high_resolution_clock::now();
  auto duration_simd = std::chrono::duration_cast<std::chrono::microseconds>(
                           end_simd - start_simd)
                           .count();

  double qps_simd = NUM_QUERIES * 1000000.0 / duration_simd;
  double latency_simd = duration_simd * 1.0 / NUM_QUERIES;

  std::cout << "耗时: " << duration_simd / 1000 << " ms" << std::endl;
  std::cout << "QPS: " << std::fixed << std::setprecision(0) << qps_simd
            << std::endl;
  std::cout << "平均延迟: " << std::fixed << std::setprecision(2)
            << latency_simd << " μs" << std::endl;
  std::cout << std::endl;

  // ========================================
  // 性能对比
  // ========================================
  std::cout << "========================================" << std::endl;
  std::cout << "📊 性能对比结果" << std::endl;
  std::cout << "========================================" << std::endl;
  std::cout << std::endl;

  double speedup = static_cast<double>(duration_scalar) / duration_simd;
  double improvement = (qps_simd - qps_scalar) / qps_scalar * 100;

  std::cout << "| 指标 | 标量版本 | SIMD版本 | 提升 |" << std::endl;
  std::cout << "|------|----------|----------|------|" << std::endl;
  std::cout << "| 耗时 | " << duration_scalar / 1000 << " ms | "
            << duration_simd / 1000 << " ms | " << std::fixed
            << std::setprecision(1)
            << (1 - static_cast<double>(duration_simd) / duration_scalar) * 100
            << "% |" << std::endl;
  std::cout << "| QPS | " << std::fixed << std::setprecision(0) << qps_scalar
            << " | " << qps_simd << " | " << std::setprecision(1) << improvement
            << "% |" << std::endl;
  std::cout << "| 延迟 | " << std::fixed << std::setprecision(2)
            << latency_scalar << " μs | " << latency_simd << " μs | "
            << std::setprecision(1) << (1 - latency_simd / latency_scalar) * 100
            << "% |" << std::endl;
  std::cout << std::endl;

  std::cout << "🚀 加速比: " << std::fixed << std::setprecision(2) << speedup
            << "x" << std::endl;
  std::cout << "📈 性能提升: " << std::setprecision(1) << improvement << "%"
            << std::endl;
  std::cout << std::endl;

  // 理论分析
  std::cout << "========================================" << std::endl;
  std::cout << "💡 性能分析" << std::endl;
  std::cout << "========================================" << std::endl;
  std::cout << std::endl;
  std::cout << "理论最大加速比: 8x (AVX2并行处理8个float)" << std::endl;
  std::cout << "实际加速比: " << std::fixed << std::setprecision(2) << speedup
            << "x" << std::endl;
  std::cout << "效率: " << std::setprecision(1) << (speedup / 8.0 * 100) << "%"
            << std::endl;
  std::cout << std::endl;

  if (speedup >= 2.5) {
    std::cout << "✅ SIMD优化效果优秀！" << std::endl;
  } else if (speedup >= 2.0) {
    std::cout << "✅ SIMD优化效果良好！" << std::endl;
  } else if (speedup >= 1.5) {
    std::cout << "⚠️ SIMD优化效果一般" << std::endl;
  } else {
    std::cout << "❌ SIMD优化效果不佳" << std::endl;
  }
  std::cout << std::endl;

  std::cout << "========================================" << std::endl;
  std::cout << "✅ 测试完成！" << std::endl;
  std::cout << "========================================" << std::endl;

  return 0;
}
