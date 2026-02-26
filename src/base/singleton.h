#pragma once

#include <memory>
#include <mutex>

namespace minkv {
namespace base {

/**
 * @brief 高性能单例模板基类
 * 
 * [设计模式] 使用CRTP (Curiously Recurring Template Pattern) 实现单例模式
 * 相比传统的shared_ptr实现，这个版本：
 * - 返回引用而不是智能指针，避免引用计数开销
 * - 使用Meyer's Singleton保证线程安全
 * - 支持自定义初始化参数
 * 
 * @tparam T 派生类类型
 * 
 * @note 适用于高频访问的单例对象，如日志器、缓存管理器等
 */
template<typename T>
class Singleton {
public:
    /**
     * @brief 获取单例实例
     * @return 单例对象的引用
     * 
     * [核心优化] 使用Meyer's Singleton，C++11保证线程安全的延迟初始化
     * 返回引用避免智能指针的引用计数开销，适合高频调用场景
     */
    static T& GetInstance() {
        static T instance;
        return instance;
    }
    
    /**
     * @brief 获取单例实例（支持构造参数）
     * @tparam Args 构造函数参数类型
     * @param args 构造函数参数
     * @return 单例对象的引用
     * 
     * [灵活性] 支持带参数的单例初始化，只在第一次调用时使用参数
     */
    template<typename... Args>
    static T& GetInstance(Args&&... args) {
        static T instance(std::forward<Args>(args)...);
        return instance;
    }

protected:
    /**
     * @brief 受保护的构造函数
     * 
     * 防止外部直接实例化，只能通过GetInstance()获取实例
     */
    Singleton() = default;
    
    /**
     * @brief 受保护的析构函数
     * 
     * [RAII] 确保单例对象在程序结束时正确析构
     */
    virtual ~Singleton() = default;

    // 禁止拷贝和赋值
    Singleton(const Singleton&) = delete;
    Singleton& operator=(const Singleton&) = delete;
    
    // 禁止移动
    Singleton(Singleton&&) = delete;
    Singleton& operator=(Singleton&&) = delete;
};

/**
 * @brief 线程安全的延迟初始化单例模板
 * 
 * [高级特性] 支持复杂的初始化逻辑，使用std::call_once保证初始化只执行一次
 * 适用于需要异步初始化或者初始化可能失败的场景
 * 
 * @tparam T 派生类类型
 */
template<typename T>
class LazySingleton {
public:
    /**
     * @brief 获取单例实例
     * @param init_func 初始化函数，只在第一次调用时执行
     * @return 单例对象的引用
     * 
     * [线程安全] 使用std::call_once确保初始化函数只被调用一次
     */
    template<typename InitFunc>
    static T& GetInstance(InitFunc&& init_func) {
        std::call_once(init_flag_, [&]() {
            instance_ = std::make_unique<T>();
            init_func(*instance_);
        });
        return *instance_;
    }
    
    /**
     * @brief 检查单例是否已初始化
     * @return true表示已初始化，false表示未初始化
     */
    static bool IsInitialized() {
        return instance_ != nullptr;
    }

protected:
    LazySingleton() = default;
    virtual ~LazySingleton() = default;
    
    LazySingleton(const LazySingleton&) = delete;
    LazySingleton& operator=(const LazySingleton&) = delete;
    LazySingleton(LazySingleton&&) = delete;
    LazySingleton& operator=(LazySingleton&&) = delete;

private:
    static std::unique_ptr<T> instance_;
    static std::once_flag init_flag_;
};

// 静态成员定义
template<typename T>
std::unique_ptr<T> LazySingleton<T>::instance_ = nullptr;

template<typename T>
std::once_flag LazySingleton<T>::init_flag_;

} // namespace base
} // namespace minkv