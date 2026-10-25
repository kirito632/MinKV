#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace minkv {
namespace vector {

/**
 * @brief HNSW 的距离度量
 *
 * 一张 HNSW 图是按某一种度量构建的：分层的贪心下降依赖「距离沿边单调下降」，
 * 换一种度量查询会让这个假设失效，recall 会明显掉。因此一个索引实例只认一种度量，
 * 需要多种度量时由上层建立多个实例（共享本实现）。
 */
enum class AnnMetric { L2, COSINE };

/**
 * @brief HNSW 构建参数
 */
struct HnswConfig {
  size_t dim = 0;                ///< 向量维度，0 视为非法配置
  size_t M = 16;                 ///< 非零层的邻居上限
  size_t M0 = 32;                ///< 第 0 层的邻居上限，惯例为 2*M
  size_t ef_construction = 200;  ///< 建图时候选队列宽度
  AnnMetric metric = AnnMetric::L2;
  uint64_t seed = 0x9e3779b97f4a7c15ull; ///< 层级抽样种子，固定后可复现
};

/**
 * @brief 一个近邻结果
 *
 * `score` 统一表示「距离」，越小越近：L2 返回平方距离（省一次 sqrt 且保序），
 * COSINE 返回 1 - 余弦相似度。调用方若需要相似度，用 1 - score 还原。
 */
struct AnnHit {
  uint32_t id = 0;
  float score = 0.0f;
};

/**
 * @brief 纯内存 HNSW 图
 *
 * 这一层只负责算法：不持锁、不了解 KV、不认识字符串 key。
 * 并发控制、key 映射、Tombstone 压缩策略都在 VectorAnnIndex 里，
 * 因为它们的取舍（锁粒度、何时 compact）随上层用法而变。
 *
 * 生命周期的约定：
 * - insert 只追加，内部 id 单调递增且永不复用；
 * - erase 只打 Tombstone，图结构不动（避免 O(M) 邻接修复）；
 * - 带 Tombstone 的节点仍参与导航，但不会出现在结果里；
 * - compact() 在原 id 空间上重建图，被删节点的数据槽位保留（内存回收由上层整体重建负责）。
 *
 * 线程安全：本类所有方法都不是线程安全的，调用方必须自行串行化写入。
 */
class HnswIndex {
public:
  static constexpr uint32_t kInvalidId = 0xFFFFFFFFu;

  /**
   * @param config 维度必须 > 0；M/M0/ef_construction 为 0 时回落到默认值
   * @throws std::invalid_argument 维度为 0
   */
  explicit HnswIndex(const HnswConfig &config);

  /**
   * @brief 插入一个向量
   * @param vec 长度必须等于 config().dim
   * @return 内部 id；参数非法时返回 kInvalidId
   */
  uint32_t insert(const float *vec);

  /**
   * @brief 删除一个向量（只打 Tombstone）
   * @return 该 id 有效且此前未被删除时返回 true
   */
  bool erase(uint32_t id);

  /**
   * @brief 近邻查询
   * @param query 长度必须等于 config().dim
   * @param k     返回条数上限（不含已删除节点）
   * @param ef    搜索队列宽度；实际使用 max(ef, k)。越大越准越慢
   * @return 按距离升序排列的结果
   */
  std::vector<AnnHit> knn(const float *query, size_t k, size_t ef) const;

  /** @brief 取某个 id 的向量（调用方保证 id < capacity()） */
  const float *vector_of(uint32_t id) const;

  const HnswConfig &config() const { return config_; }
  size_t dim() const { return config_.dim; }
  size_t size() const { return live_; }                 ///< 存活节点数
  size_t capacity() const { return levels_.size(); }    ///< 已分配槽位数
  size_t tombstone_count() const { return levels_.size() - live_; }
  int max_level() const { return max_level_; }
  uint32_t entry_point() const { return entry_point_; }
  size_t memory_bytes() const;

  /**
   * @brief 第 0 层从入口点出发可达的节点数
   *
   * 图健康的体检指标：可达数远小于 size() 说明图被割裂成了若干连通分量，
   * 此时搜索会卡在错误的分量里，表现为「ef 调多大 recall 都不涨」。
   * 只统计第 0 层的出边，不做双向补全，因此是一个下界。
   */
  size_t reachable_count() const;

  /**
   * @brief 在存活节点上重建整张图，清掉 Tombstone 对导航的污染
   *
   * 只在没有并发写入时调用。id 保持不变，因此上层的 key→id 映射无需重建。
   */
  void compact();

private:
  using NodeLinks = std::vector<std::vector<uint32_t>>; ///< [level] -> 邻居 id

  size_t max_degree(int level) const { return level == 0 ? config_.M0 : config_.M; }
  float distance(const float *a, const float *b) const;
  float distance_to(uint32_t id, const float *query) const;
  bool alive(uint32_t id) const { return deleted_[id] == 0; }

  int random_level();
  uint32_t greedy_descend(uint32_t entry, const float *query, int level) const;
  std::vector<AnnHit> search_layer(const float *query, uint32_t entry, size_t ef,
                                   int level) const;
  std::vector<uint32_t> select_neighbors(const std::vector<AnnHit> &candidates,
                                         size_t m) const;
  void link_node(uint32_t id, const float *vec);
  void prune(uint32_t owner, int level);
  void reset_graph();

  HnswConfig config_;
  double level_multiplier_ = 1.0; ///< 1 / ln(M)
  std::vector<float> data_;       ///< id * dim 连续存放的向量副本
  std::vector<int> levels_;       ///< 每个节点的最高层
  std::vector<NodeLinks> links_;  ///< [node][level] -> 邻居
  std::vector<uint8_t> deleted_;  ///< Tombstone 位图
  uint32_t entry_point_ = kInvalidId;
  int max_level_ = -1;
  size_t live_ = 0;
  uint64_t rng_state_;
};

} // namespace vector
} // namespace minkv
