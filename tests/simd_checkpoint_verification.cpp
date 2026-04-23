/**
 * SIMD Optimization Checkpoint Verification (Task 2.4)
 * 
 * This test verifies:
 * 1. AVX2 version correctness (results match scalar version)
 * 2. Performance improvement > 3x
 * 3. Fallback mechanism on non-AVX2 CPUs
 * 
 * Requirements: FR-1.3
 */

#include "../vector/vector_ops.h"
#include <iostream>
#include <vector>
#include <random>
#include <chrono>
#include <iomanip>
#include <cmath>
#include <algorithm>

// Test configuration
const size_t TEST_DIMENSIONS[] = {128, 256, 512, 768, 1536};
const size_t NUM_TEST_DIMS = 5;
const size_t NUM_CORRECTNESS_TESTS = 1000;
const size_t NUM_PERFORMANCE_TESTS = 100000;

// Tolerance for floating point comparison
const float EPSILON = 1e-4f;

// Test result tracking
struct TestResult {
    std::string test_name;
    bool passed;
    std::string message;
};

std::vector<TestResult> test_results;

void add_test_result(const std::string& name, bool passed, const std::string& message = "") {
    test_results.push_back({name, passed, message});
    if (passed) {
        std::cout << "  ✅ " << name << std::endl;
    } else {
        std::cout << "  ❌ " << name << ": " << message << std::endl;
    }
}

// Generate random vector
std::vector<float> generate_random_vector(size_t dim, std::mt19937& gen) {
    std::uniform_real_distribution<float> dis(-1.0f, 1.0f);
    std::vector<float> vec(dim);
    for (size_t i = 0; i < dim; ++i) {
        vec[i] = dis(gen);
    }
    return vec;
}

// Check if two floats are approximately equal
bool approx_equal(float a, float b, float epsilon = EPSILON) {
    if (std::isnan(a) || std::isnan(b)) return false;
    if (std::isinf(a) || std::isinf(b)) return a == b;
    
    float abs_diff = std::abs(a - b);
    float abs_max = std::max(std::abs(a), std::abs(b));
    
    // Use relative error for large values, absolute error for small values
    return abs_diff <= epsilon || abs_diff <= epsilon * abs_max;
}

// ============================================================================
// Test 1: AVX2 Correctness Verification
// ============================================================================

bool test_avx2_correctness() {
    std::cout << "\n========================================" << std::endl;
    std::cout << "Test 1: AVX2 Correctness Verification" << std::endl;
    std::cout << "========================================" << std::endl;
    
    std::random_device rd;
    std::mt19937 gen(rd());
    
    bool all_passed = true;
    
    for (size_t dim_idx = 0; dim_idx < NUM_TEST_DIMS; ++dim_idx) {
        size_t dim = TEST_DIMENSIONS[dim_idx];
        std::cout << "\nTesting dimension " << dim << "..." << std::endl;
        
        size_t failures = 0;
        float max_error = 0.0f;
        
        for (size_t test = 0; test < NUM_CORRECTNESS_TESTS; ++test) {
            auto vec_a = generate_random_vector(dim, gen);
            auto vec_b = generate_random_vector(dim, gen);
            
            const float* a = vec_a.data();
            const float* b = vec_b.data();
            
            // Test dot product
            float dot_scalar = VectorOps::dot_product_scalar(a, b, dim);
            float dot_avx2 = VectorOps::dot_product_avx2(a, b, dim);
            
            if (!approx_equal(dot_scalar, dot_avx2)) {
                failures++;
                max_error = std::max(max_error, std::abs(dot_scalar - dot_avx2));
            }
            
            // Test cosine similarity
            float cos_scalar = VectorOps::cosine_similarity_scalar(a, b, dim);
            float cos_avx2 = VectorOps::cosine_similarity_avx2(a, b, dim);
            
            if (!approx_equal(cos_scalar, cos_avx2)) {
                failures++;
                max_error = std::max(max_error, std::abs(cos_scalar - cos_avx2));
            }
            
            // Test euclidean distance
            float euc_scalar = VectorOps::euclidean_distance_scalar(a, b, dim);
            float euc_avx2 = VectorOps::euclidean_distance_avx2(a, b, dim);
            
            if (!approx_equal(euc_scalar, euc_avx2)) {
                failures++;
                max_error = std::max(max_error, std::abs(euc_scalar - euc_avx2));
            }
        }
        
        float accuracy = 100.0f * (NUM_CORRECTNESS_TESTS * 3 - failures) / (NUM_CORRECTNESS_TESTS * 3);
        
        std::stringstream ss;
        ss << "Dim " << dim << ": " << std::fixed << std::setprecision(2) 
           << accuracy << "% accuracy, max error: " << std::scientific << max_error;
        
        bool passed = (failures == 0);
        add_test_result(ss.str(), passed);
        
        if (!passed) {
            all_passed = false;
        }
    }
    
    return all_passed;
}

// ============================================================================
// Test 2: Performance Improvement Verification (> 3x)
// ============================================================================

bool test_performance_improvement() {
    std::cout << "\n========================================" << std::endl;
    std::cout << "Test 2: Performance Improvement (> 3x)" << std::endl;
    std::cout << "========================================" << std::endl;
    
    std::random_device rd;
    std::mt19937 gen(rd());
    
    bool all_passed = true;
    
    std::cout << "\n| Dimension | Scalar (μs) | AVX2 (μs) | Speedup | Status |" << std::endl;
    std::cout << "|-----------|-------------|-----------|---------|--------|" << std::endl;
    
    for (size_t dim_idx = 0; dim_idx < NUM_TEST_DIMS; ++dim_idx) {
        size_t dim = TEST_DIMENSIONS[dim_idx];
        
        // Generate test data
        auto vec_a = generate_random_vector(dim, gen);
        auto vec_b = generate_random_vector(dim, gen);
        const float* a = vec_a.data();
        const float* b = vec_b.data();
        
        // Benchmark scalar version
        auto start_scalar = std::chrono::high_resolution_clock::now();
        volatile float result_scalar = 0.0f;
        for (size_t i = 0; i < NUM_PERFORMANCE_TESTS; ++i) {
            result_scalar = VectorOps::cosine_similarity_scalar(a, b, dim);
        }
        auto end_scalar = std::chrono::high_resolution_clock::now();
        auto duration_scalar = std::chrono::duration_cast<std::chrono::microseconds>(
            end_scalar - start_scalar).count();
        
        // Benchmark AVX2 version
        auto start_avx2 = std::chrono::high_resolution_clock::now();
        volatile float result_avx2 = 0.0f;
        for (size_t i = 0; i < NUM_PERFORMANCE_TESTS; ++i) {
            result_avx2 = VectorOps::cosine_similarity_avx2(a, b, dim);
        }
        auto end_avx2 = std::chrono::high_resolution_clock::now();
        auto duration_avx2 = std::chrono::duration_cast<std::chrono::microseconds>(
            end_avx2 - start_avx2).count();
        
        // Calculate speedup
        double avg_scalar = static_cast<double>(duration_scalar) / NUM_PERFORMANCE_TESTS;
        double avg_avx2 = static_cast<double>(duration_avx2) / NUM_PERFORMANCE_TESTS;
        double speedup = avg_scalar / avg_avx2;
        
        bool passed = (speedup >= 3.0);
        std::string status = passed ? "✅ PASS" : "❌ FAIL";
        
        std::cout << "| " << std::setw(9) << dim 
                  << " | " << std::setw(11) << std::fixed << std::setprecision(3) << avg_scalar
                  << " | " << std::setw(9) << avg_avx2
                  << " | " << std::setw(7) << std::setprecision(2) << speedup << "x"
                  << " | " << status << " |" << std::endl;
        
        std::stringstream ss;
        ss << "Dim " << dim << " speedup: " << std::fixed << std::setprecision(2) << speedup << "x";
        add_test_result(ss.str(), passed, passed ? "" : "Speedup < 3x");
        
        if (!passed) {
            all_passed = false;
        }
    }
    
    return all_passed;
}

// ============================================================================
// Test 3: Fallback Mechanism Verification
// ============================================================================

bool test_fallback_mechanism() {
    std::cout << "\n========================================" << std::endl;
    std::cout << "Test 3: Fallback Mechanism" << std::endl;
    std::cout << "========================================" << std::endl;
    
    // Check CPU AVX2 support
    bool has_avx2 = VectorOps::has_avx2();
    std::cout << "\nCPU AVX2 Support: " << (has_avx2 ? "YES ✅" : "NO ❌") << std::endl;
    
    if (!has_avx2) {
        std::cout << "\n⚠️  AVX2 not supported on this CPU" << std::endl;
        std::cout << "Testing fallback to scalar version..." << std::endl;
    }
    
    // Test compute_distance auto-selection
    std::random_device rd;
    std::mt19937 gen(rd());
    
    size_t dim = 512;
    auto vec_a = generate_random_vector(dim, gen);
    auto vec_b = generate_random_vector(dim, gen);
    const float* a = vec_a.data();
    const float* b = vec_b.data();
    
    // Test all three metrics
    float cos_result = minkv::vector::VectorOps::compute_distance(
        a, b, dim, minkv::vector::DistanceMetric::COSINE);
    float euc_result = minkv::vector::VectorOps::compute_distance(
        a, b, dim, minkv::vector::DistanceMetric::EUCLIDEAN);
    float dot_result = minkv::vector::VectorOps::compute_distance(
        a, b, dim, minkv::vector::DistanceMetric::DOT_PRODUCT);
    
    // Verify results are valid (not NaN or Inf)
    bool cos_valid = !std::isnan(cos_result) && !std::isinf(cos_result);
    bool euc_valid = !std::isnan(euc_result) && !std::isinf(euc_result);
    bool dot_valid = !std::isnan(dot_result) && !std::isinf(dot_result);
    
    add_test_result("Cosine similarity auto-selection", cos_valid);
    add_test_result("Euclidean distance auto-selection", euc_valid);
    add_test_result("Dot product auto-selection", dot_valid);
    
    // Verify results match expected implementation
    float cos_expected = has_avx2 ? 
        VectorOps::cosine_similarity_avx2(a, b, dim) :
        VectorOps::cosine_similarity_scalar(a, b, dim);
    
    bool cos_matches = approx_equal(cos_result, cos_expected);
    add_test_result("Auto-selection matches expected implementation", cos_matches);
    
    if (has_avx2) {
        std::cout << "\n✅ AVX2 supported: Using optimized implementation" << std::endl;
    } else {
        std::cout << "\n✅ AVX2 not supported: Fallback to scalar implementation" << std::endl;
    }
    
    return cos_valid && euc_valid && dot_valid && cos_matches;
}

// ============================================================================
// Main Test Runner
// ============================================================================

int main() {
    std::cout << "========================================" << std::endl;
    std::cout << "SIMD Optimization Checkpoint (Task 2.4)" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "\nRequirements: FR-1.3" << std::endl;
    std::cout << "Date: " << __DATE__ << std::endl;
    
    // Run all tests
    bool test1_passed = test_avx2_correctness();
    bool test2_passed = test_performance_improvement();
    bool test3_passed = test_fallback_mechanism();
    
    // Print summary
    std::cout << "\n========================================" << std::endl;
    std::cout << "Test Summary" << std::endl;
    std::cout << "========================================" << std::endl;
    
    size_t total_tests = test_results.size();
    size_t passed_tests = 0;
    for (const auto& result : test_results) {
        if (result.passed) passed_tests++;
    }
    
    std::cout << "\nTotal Tests: " << total_tests << std::endl;
    std::cout << "Passed: " << passed_tests << " ✅" << std::endl;
    std::cout << "Failed: " << (total_tests - passed_tests) << " ❌" << std::endl;
    std::cout << "Success Rate: " << std::fixed << std::setprecision(1) 
              << (100.0 * passed_tests / total_tests) << "%" << std::endl;
    
    std::cout << "\n========================================" << std::endl;
    std::cout << "Checkpoint Status" << std::endl;
    std::cout << "========================================" << std::endl;
    
    std::cout << "\n1. AVX2 Correctness: " << (test1_passed ? "✅ PASS" : "❌ FAIL") << std::endl;
    std::cout << "2. Performance > 3x: " << (test2_passed ? "✅ PASS" : "❌ FAIL") << std::endl;
    std::cout << "3. Fallback Mechanism: " << (test3_passed ? "✅ PASS" : "❌ FAIL") << std::endl;
    
    bool all_passed = test1_passed && test2_passed && test3_passed;
    
    if (all_passed) {
        std::cout << "\n🎉 All checkpoint requirements met!" << std::endl;
        std::cout << "✅ Ready to proceed to Phase 3: Top-K Search" << std::endl;
    } else {
        std::cout << "\n⚠️  Some checkpoint requirements not met" << std::endl;
        std::cout << "Please review failed tests before proceeding" << std::endl;
    }
    
    std::cout << "\n========================================" << std::endl;
    
    return all_passed ? 0 : 1;
}
