#pragma once

#include <string>
#include <type_traits>
#include <stdexcept>

namespace minkv {

/**
 * @brief 通用序列化器模板
 * 
 * 这个设计解决了模板通用性与WAL序列化限制的矛盾：
 * 1. 编译时类型检查：不支持的类型会在编译时报错
 * 2. 可扩展性：通过模板特化支持新类型
 * 3. 类型安全：避免运行时的静默失败
 * 
 * 面试要点：
 * - 展示了接口契约的一致性设计
 * - 使用了模板特化和SFINAE技术
 * - 体现了开闭原则（对扩展开放，对修改关闭）
 */
template<typename T>
struct Serializer {
    // 编译时友好的错误信息
    static_assert(sizeof(T) == 0, 
        "Unsupported type for WAL serialization! "
        "Supported types: int, long, float, double, string. "
        "To add support for custom types, specialize Serializer<YourType>.");
    
    static std::string serialize(const T& obj);
    static T deserialize(const std::string& data);
};

// ============================================================================
// 基础数据类型特化
// ============================================================================

/**
 * @brief int类型序列化特化
 */
template<>
struct Serializer<int> {
    static std::string serialize(const int& obj) {
        return std::to_string(obj);
    }
    
    static int deserialize(const std::string& data) {
        try {
            return std::stoi(data);
        } catch (const std::exception& e) {
            throw std::runtime_error("Failed to deserialize int from: " + data);
        }
    }
};

/**
 * @brief long类型序列化特化
 */
template<>
struct Serializer<long> {
    static std::string serialize(const long& obj) {
        return std::to_string(obj);
    }
    
    static long deserialize(const std::string& data) {
        try {
            return std::stol(data);
        } catch (const std::exception& e) {
            throw std::runtime_error("Failed to deserialize long from: " + data);
        }
    }
};

/**
 * @brief float类型序列化特化
 */
template<>
struct Serializer<float> {
    static std::string serialize(const float& obj) {
        return std::to_string(obj);
    }
    
    static float deserialize(const std::string& data) {
        try {
            return std::stof(data);
        } catch (const std::exception& e) {
            throw std::runtime_error("Failed to deserialize float from: " + data);
        }
    }
};

/**
 * @brief double类型序列化特化
 */
template<>
struct Serializer<double> {
    static std::string serialize(const double& obj) {
        return std::to_string(obj);
    }
    
    static double deserialize(const std::string& data) {
        try {
            return std::stod(data);
        } catch (const std::exception& e) {
            throw std::runtime_error("Failed to deserialize double from: " + data);
        }
    }
};

/**
 * @brief string类型序列化特化
 */
template<>
struct Serializer<std::string> {
    static std::string serialize(const std::string& obj) {
        return obj;
    }
    
    static std::string deserialize(const std::string& data) {
        return data;
    }
};

// ============================================================================
// 辅助函数和类型检查
// ============================================================================

/**
 * @brief 编译时检查类型是否支持序列化
 */
template<typename T>
constexpr bool is_serializable_v = 
    std::is_same_v<T, int> ||
    std::is_same_v<T, long> ||
    std::is_same_v<T, float> ||
    std::is_same_v<T, double> ||
    std::is_same_v<T, std::string>;

/**
 * @brief 便利函数：序列化任意支持的类型
 */
template<typename T>
std::string serialize(const T& obj) {
    static_assert(is_serializable_v<T>, 
        "Type is not serializable. Check supported types in Serializer documentation.");
    return Serializer<T>::serialize(obj);
}

/**
 * @brief 便利函数：反序列化任意支持的类型
 */
template<typename T>
T deserialize(const std::string& data) {
    static_assert(is_serializable_v<T>, 
        "Type is not serializable. Check supported types in Serializer documentation.");
    return Serializer<T>::deserialize(data);
}

} // namespace minkv