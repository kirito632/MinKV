#include "../vector/vector_value.h"
#include "../vector/vector_index.h"
#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <cmath>

using namespace minkv::vector;

// Test VectorValue basic functionality
TEST(VectorValueTest, BasicConstruction) {
    std::vector<float> embedding = {1.0f, 2.0f, 3.0f, 4.0f};
    std::string metadata = R"({"id": "test1", "source": "unit_test"})";
    uint64_t timestamp = 1234567890;
    
    VectorValue value(embedding, metadata, timestamp);
    
    EXPECT_EQ(value.dimension(), 4);
    EXPECT_EQ(value.embedding, embedding);
    EXPECT_EQ(value.metadata, metadata);
    EXPECT_EQ(value.timestamp, timestamp);
    EXPECT_TRUE(value.is_valid());
}

// Test VectorValue validation
TEST(VectorValueTest, Validation) {
    // Valid vector
    VectorValue valid({1.0f, 2.0f, 3.0f}, "{}", 0);
    EXPECT_TRUE(valid.is_valid());
    
    // Empty vector (invalid)
    VectorValue empty({}, "{}", 0);
    EXPECT_FALSE(empty.is_valid());
    
    // Vector with NaN (invalid)
    VectorValue with_nan({1.0f, NAN, 3.0f}, "{}", 0);
    EXPECT_FALSE(with_nan.is_valid());
    
    // Vector with Inf (invalid)
    VectorValue with_inf({1.0f, INFINITY, 3.0f}, "{}", 0);
    EXPECT_FALSE(with_inf.is_valid());
}

// Test VectorValue serialization
TEST(VectorValueTest, Serialization) {
    std::vector<float> embedding = {1.5f, 2.5f, 3.5f, 4.5f};
    std::string metadata = R"({"test": "data"})";
    uint64_t timestamp = 9876543210;
    
    VectorValue original(embedding, metadata, timestamp);
    
    // Serialize
    std::string serialized = original.serialize();
    EXPECT_GT(serialized.size(), 0);
    
    // Deserialize
    VectorValue deserialized = VectorValue::deserialize(serialized);
    
    // Verify
    EXPECT_EQ(deserialized.dimension(), original.dimension());
    EXPECT_EQ(deserialized.embedding, original.embedding);
    EXPECT_EQ(deserialized.metadata, original.metadata);
    EXPECT_EQ(deserialized.timestamp, original.timestamp);
}

// Test serialization with different dimensions
TEST(VectorValueTest, SerializationDifferentDimensions) {
    std::vector<size_t> dimensions = {1, 10, 128, 256, 512, 768, 1536};
    
    for (size_t dim : dimensions) {
        std::vector<float> embedding(dim);
        for (size_t i = 0; i < dim; ++i) {
            embedding[i] = static_cast<float>(i) * 0.1f;
        }
        
        VectorValue original(embedding, "{}", 12345);
        std::string serialized = original.serialize();
        VectorValue deserialized = VectorValue::deserialize(serialized);
        
        EXPECT_EQ(deserialized.dimension(), dim);
        EXPECT_EQ(deserialized.embedding, embedding);
    }
}

// Test VectorIndex basic operations
TEST(VectorIndexTest, BasicOperations) {
    VectorIndex index;
    
    EXPECT_EQ(index.size(), 0);
    EXPECT_EQ(index.dimension(), 0);
    
    // Insert first vector
    VectorValue v1({1.0f, 2.0f, 3.0f}, R"({"id": 1})", 100);
    EXPECT_TRUE(index.insert("vec1", v1));
    EXPECT_EQ(index.size(), 1);
    EXPECT_EQ(index.dimension(), 3);
    
    // Insert second vector with same dimension
    VectorValue v2({4.0f, 5.0f, 6.0f}, R"({"id": 2})", 200);
    EXPECT_TRUE(index.insert("vec2", v2));
    EXPECT_EQ(index.size(), 2);
    
    // Try to insert vector with different dimension (should fail)
    VectorValue v3({7.0f, 8.0f}, R"({"id": 3})", 300);
    EXPECT_FALSE(index.insert("vec3", v3));
    EXPECT_EQ(index.size(), 2);
}

// Test VectorIndex get operation
TEST(VectorIndexTest, GetOperation) {
    VectorIndex index;
    
    VectorValue v1({1.0f, 2.0f}, "{}", 100);
    index.insert("key1", v1);
    
    // Get existing key
    auto result = index.get("key1");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->embedding, v1.embedding);
    
    // Get non-existing key
    auto missing = index.get("key2");
    EXPECT_FALSE(missing.has_value());
}

// Test VectorIndex remove operation
TEST(VectorIndexTest, RemoveOperation) {
    VectorIndex index;
    
    VectorValue v1({1.0f, 2.0f}, "{}", 100);
    index.insert("key1", v1);
    EXPECT_EQ(index.size(), 1);
    
    // Remove existing key
    EXPECT_TRUE(index.remove("key1"));
    EXPECT_EQ(index.size(), 0);
    EXPECT_EQ(index.dimension(), 0);  // Dimension resets when empty
    
    // Remove non-existing key
    EXPECT_FALSE(index.remove("key2"));
}

// Test VectorIndex list_keys with prefix
TEST(VectorIndexTest, ListKeysWithPrefix) {
    VectorIndex index;
    
    VectorValue v({1.0f, 2.0f}, "{}", 100);
    index.insert("user:123", v);
    index.insert("user:456", v);
    index.insert("doc:789", v);
    
    // List all keys
    auto all_keys = index.list_keys();
    EXPECT_EQ(all_keys.size(), 3);
    
    // List with prefix "user:"
    auto user_keys = index.list_keys("user:");
    EXPECT_EQ(user_keys.size(), 2);
    
    // List with prefix "doc:"
    auto doc_keys = index.list_keys("doc:");
    EXPECT_EQ(doc_keys.size(), 1);
    
    // List with non-matching prefix
    auto empty_keys = index.list_keys("admin:");
    EXPECT_EQ(empty_keys.size(), 0);
}

// Test VectorIndex thread safety
TEST(VectorIndexTest, ThreadSafety) {
    VectorIndex index;
    const int num_threads = 10;
    const int ops_per_thread = 100;
    
    std::vector<std::thread> threads;
    
    // Concurrent inserts
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&index, t, ops_per_thread]() {
            for (int i = 0; i < ops_per_thread; ++i) {
                std::string key = "thread" + std::to_string(t) + "_key" + std::to_string(i);
                VectorValue v({1.0f, 2.0f, 3.0f}, "{}", i);
                index.insert(key, v);
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    EXPECT_EQ(index.size(), num_threads * ops_per_thread);
    EXPECT_EQ(index.dimension(), 3);
}

// Test VectorIndex concurrent read/write
TEST(VectorIndexTest, ConcurrentReadWrite) {
    VectorIndex index;
    
    // Pre-populate
    for (int i = 0; i < 100; ++i) {
        VectorValue v({1.0f, 2.0f}, "{}", i);
        index.insert("key" + std::to_string(i), v);
    }
    
    std::vector<std::thread> threads;
    std::atomic<int> read_count{0};
    
    // Readers
    for (int t = 0; t < 5; ++t) {
        threads.emplace_back([&index, &read_count]() {
            for (int i = 0; i < 1000; ++i) {
                auto result = index.get("key" + std::to_string(i % 100));
                if (result.has_value()) {
                    read_count++;
                }
            }
        });
    }
    
    // Writers
    for (int t = 0; t < 2; ++t) {
        threads.emplace_back([&index, t]() {
            for (int i = 0; i < 100; ++i) {
                VectorValue v({3.0f, 4.0f}, "{}", i);
                index.insert("writer" + std::to_string(t) + "_" + std::to_string(i), v);
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    EXPECT_GT(read_count.load(), 0);
    EXPECT_EQ(index.size(), 300);  // 100 original + 200 from writers
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
