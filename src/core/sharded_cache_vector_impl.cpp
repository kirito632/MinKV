/**
 * @file sharded_cache_vector_impl.cpp
 * @brief ShardedCache模板显式实例化定义文件
 *
 * [核心作用] 集中实例化ShardedCache<string, string>模板，避免重复编译和链接冲突
 *
 * 设计原理：
 * - 隐式实例化问题：每个使用ShardedCache的.cpp文件都会生成相同代码 → 重复编译
 * - 显式实例化优势：在此文件统一生成一次，其他文件仅引用声明 → 编译加速，链接无冲突
 *
 * [模板实例化说明]
 * - 类型参数：<std::string, std::string>
 * - 应用场景：HTTP服务器的KV存储、Agent工作记忆缓存
 * - 链接属性：强符号定义，确保链接时唯一性（遵守ODR原则）
 *
 * 若需添加其他类型组合（如<int, string>），在此文件追加实例化声明：
 *   template class ShardedCache<int, std::string>;
 */
#include "sharded_cache.h"

namespace minkv {
namespace db {

// ─── ShardedCache模板显式实例化 ─────────────────────────────────────────────
// 实例化类型：ShardedCache<std::string, std::string>
// 用途：HTTP服务器KV接口的底层存储实现
template class ShardedCache<std::string, std::string>;

} // namespace db
} // namespace minkv
