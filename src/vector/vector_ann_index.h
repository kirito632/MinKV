#pragma once

#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "hnsw_index.h"

namespace minkv {
namespace vector {

/**
 * @brief 索引运行统计
 */
struct AnnStats {
  size_t live = 0;          ///< 当前可被检索到的 key 数
  size_t tombstones = 0;    ///< 已删除但仍占槽位的节点数
  size_t memory_bytes = 0;  ///< 索引自身内存（不含 KV 里的向量副本）
  int max_level = -1;

  /// Tombstone 占（存活 + Tombstone）的比例
  double tombstone_ratio() const {
    const size_t total = live + tombstones;
    if (total == 0) {
      return 0.0;
    }
    return static_cast<double>(tombstones) / static_cast<double>(total);
  }
};

/**
 * @brief 一条近邻结果
 *
 * `score` 是距离，越小越近（L2 为平方距离，COSINE 为 1 - 相似度）。
 */
struct AnnMatch {
  std::string key;
  float score = 0.0f;
};

/**
 * @brief 把 HnswIndex 接到字符串 key 上
 *
 * 职责：
 * - key ↔ 内部 id 的映射；
 * - 读写锁：搜索持共享锁，写操作持独占锁；
 * - 更新语义：同一个 key 再次 upsert 会「插入新版本 + 给旧版本打 Tombstone」，
 *   而不是原地改向量（HNSW 的邻接关系是按位置建立的，改向量会让邻居失效）；
 * - Tombstone 达到阈值后在 erase 路径上自动 compact。
 *
 * 维度在构造时固定，不匹配的向量一律拒绝。混合维度的场景由上层按维度
 * 持有多个实例（`map<size_t, VectorAnnIndex>`），而不是让一个实例兼容多维度。
 *
 * ## 并发约定
 *
 * 锁序固定为 `mu_ → HnswIndex 内部结构锁`，不存在反向路径。
 * `upsert` 把最贵的建图放在 `mu_` 之外（HnswIndex 自身保证并发安全），
 * 插入完成后才短暂独占以发布 key，因此写操作不会把检索阻塞一次建图的时长。
 *
 * 代价是 `compact()` 可能在建图期间换掉整代索引，所以 `index_` 用 `shared_ptr`
 * 持有：在用的旧索引对象不会随换代被析构，而发布阶段会校验代号，
 * 若期间发生过重建，就把这个向量写进当前代（最多重做一次）。
 * 极端并发下还可能短暂出现「节点已可检索但 key 尚未发布」，
 * 这类结果会被搜索按空槽位过滤掉，属于有意的最终一致。
 */
class VectorAnnIndex {
public:
  /**
   * @param config        维度与度量在构造时固定
   * @param compact_ratio Tombstone 比例超过该值时在 erase 后自动重建，默认 0.3
   * @throws std::invalid_argument 维度为 0
   */
  explicit VectorAnnIndex(const HnswConfig &config, double compact_ratio = 0.3);

  VectorAnnIndex(const VectorAnnIndex &) = delete;
  VectorAnnIndex &operator=(const VectorAnnIndex &) = delete;

  /**
   * @brief 写入或更新一个向量
   * @return key 为空、向量维度不符或插入失败时返回 false
   */
  bool upsert(const std::string &key, const std::vector<float> &vec);

  /**
   * @brief 删除一个 key
   * @return key 不存在时返回 false
   */
  bool erase(const std::string &key);

  /**
   * @brief Erase the key and tombstone the node without ever compacting.
   *
   * Callers that already hold a lock must not trigger a full graph rebuild;
   * ShardedCache's eviction hook is the main user, since it fires inside the
   * shard lock.
   */
  bool erase_without_compaction(const std::string &key);

  /**
   * @brief Whether the key is currently mapped (cheap, shared lock).
   *
   * Lets a caller avoid the exclusive lock just to find out that nothing is
   * there, which is the common case for non-vector writes.
   */
  bool contains(const std::string &key) const;

  /**
   * @brief 近邻查询
   * @param query 维度必须等于构造时的 dim
   * @param k     返回条数上限
   * @param ef    搜索队列宽度，实际使用 max(ef, k)
   */
  std::vector<AnnMatch> search(const std::vector<float> &query, size_t k, size_t ef) const;

  /**
   * @brief 批量构建（仅允许在空索引上调用）
   *
   * @param keys   与 flat 一一对应的 key，必须非空且不得重复
   * @param flat   连续存放的 count 个向量（count * dim 个 float）
   * @param count  向量个数
   * @param threads 建图线程数，0/1 表示单线程
   * @return 成功收录的 key 数；参数非法或索引非空时返回 0
   */
  size_t build(const std::vector<std::string> &keys, const float *flat, size_t count,
               size_t threads);

  /// 清空全部内容
  void clear();

  /// 预留节点容量，避免插入期扩容
  void reserve(size_t expected);

  /// 在存活 key 上重建整个索引（清掉 Tombstone 并回收 id 空间）
  void compact();

  AnnStats stats() const;
  size_t size() const;
  const HnswConfig &config() const { return config_; }

private:
  HnswConfig config_;
  mutable std::shared_mutex mu_;
  std::shared_ptr<HnswIndex> index_; ///< shared_ptr：换代的旧索引由在用者延迟释放
  std::unordered_map<std::string, uint32_t> ids_; ///< 只含存活 key
  std::vector<std::string> keys_;                 ///< id → key；空串表示尚未发布
  double compact_ratio_;
  /// Counter-only tombstone check: never walks the graph for memory accounting.
  bool should_compact() const;
};

} // namespace vector
} // namespace minkv
