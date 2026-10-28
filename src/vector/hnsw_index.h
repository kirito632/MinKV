#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
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
  size_t dim = 0;               ///< 向量维度，0 视为非法配置
  size_t M = 16;                ///< 非零层的邻居上限
  size_t M0 = 32;               ///< 第 0 层的邻居上限，惯例为 2*M
  size_t ef_construction = 200; ///< 建图时候选队列宽度
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
 * 这一层只负责算法：不认识字符串 key，也不实现统计口径。
 * key 映射与 Tombstone 压缩策略属于 VectorAnnIndex。
 *
 * ## 内存布局
 *
 * 第 0 层占了全部访问量的 99%，因此单独用「定长平铺数组 + 原子计数」：
 * 每个节点固定 `M0` 个槽位，写侧只在尾部追加并 release 发布计数，
 * 读侧 acquire 读计数后逐槽读取，无需加锁、也不会读到被 realloc 释放的内存。
 * 第 1 层及以上节点只占约 1/M，仍用 per-node vector，用节点锁保护。
 *
 * ## 并发模型
 *
 * - `knn`：可并发调用，持结构共享锁；
 * - `insert`：可并发调用（各自占不同 id），只有扩容时短暂独占；
 * - `erase` / `compact` / `reserve`：独占，会阻塞并发读写；
 * - 双向加边永远只持有一把条带锁（先写自己、再逐条写邻居），因此不存在
 *   两把锁的获取顺序问题，天然无死锁。
 *
 * `build_parallel` 是批量构建入口：它先单线程预分配 id 与层级，
 * 再让多线程并发建边。注意并发构建的图与顺序构建立图**不保证逐位一致**。
 */
class HnswIndex {
public:
  static constexpr uint32_t kInvalidId = 0xFFFFFFFFu;
  static constexpr uint32_t kInvalidNeighbor = 0xFFFFFFFFu;

  /**
   * @param config 维度必须 > 0；M/M0/ef_construction 为 0 时回落到默认值
   * @throws std::invalid_argument 维度为 0
   */
  explicit HnswIndex(const HnswConfig &config);

  HnswIndex(const HnswIndex &) = delete;
  HnswIndex &operator=(const HnswIndex &) = delete;

  /**
   * @brief 插入一个向量
   * @param vec 长度必须等于 config().dim
   * @return 内部 id；参数非法时返回 kInvalidId
   */
  uint32_t insert(const float *vec);

  /**
   * @brief 删除一个向量（只打 Tombstone，不动物理结构）
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

  /**
   * @brief 预留节点容量，避免后续插入触发扩容
   *
   * 会阻塞并发读写。`build_parallel` 内部会自行预留。
   */
  void reserve(size_t node_count);

  /**
   * @brief 批量并行构建
   *
   * @param flat  连续存放的 count 个向量（长度 count * dim）
   * @param count 向量个数
   * @param threads 工作线程数，0 或 1 表示退化为单线程
   *
   * 调用方保证构建期间没有其他写入；构建期间并发查询是安全的（可能看到部分图）。
   */
  void build_parallel(const float *flat, size_t count, size_t threads);

  /** @brief 取某个 id 的向量（调用方保证 id < capacity()） */
  const float *vector_of(uint32_t id) const;

  const HnswConfig &config() const { return config_; }

  size_t dim() const { return config_.dim; }

  size_t size() const { return live_.load(std::memory_order_relaxed); } ///< 存活节点数

  size_t capacity() const { return next_id_.load(std::memory_order_relaxed); }

  size_t tombstone_count() const { return capacity() - size(); }

  int max_level() const { return max_level_.load(std::memory_order_relaxed); }

  uint32_t entry_point() const { return entry_point_.load(std::memory_order_relaxed); }

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
   * 独占执行。id 保持不变，因此上层的 key→id 映射无需重建。
   */
  void compact();

private:
  static constexpr size_t kStripeCount = 1024;

  std::mutex &stripe_for(uint32_t node) const {
    return stripes_[node & (kStripeCount - 1)];
  }

  size_t max_degree(int level) const { return level == 0 ? config_.M0 : config_.M; }

  float distance(const float *a, const float *b) const;
  float distance_to(uint32_t id, const float *query) const;

  bool alive(uint32_t id) const { return deleted_[id] == 0; }

  int random_level();
  uint32_t greedy_descend(uint32_t entry, const float *query, int level) const;
  std::vector<AnnHit> search_layer(const float *query,
                                   uint32_t entry,
                                   size_t ef,
                                   int level) const;
  std::vector<uint32_t> select_neighbors(const std::vector<AnnHit> &candidates,
                                         size_t m) const;

  /// 仅为 id 分配数据结构，不建边。调用方需持结构锁。
  void write_node_locked(uint32_t id, const float *vec, int level);
  void ensure_capacity_locked(size_t needed);
  /// 建边：包括贪心下降、逐层选邻、双向加边。调用方需持结构共享锁。
  void link_node(uint32_t id);
  /// 空图时把 id 立为入口。
  bool claim_entry_if_empty(uint32_t id);

  void write_links_locked(uint32_t node, int level, const std::vector<uint32_t> &ids);
  void add_back_link(uint32_t owner, int level, uint32_t id);
  void prune_locked(uint32_t owner, int level, std::vector<uint32_t> candidates);
  std::vector<uint32_t> read_links(uint32_t node, int level) const;

  HnswConfig config_;
  double level_multiplier_ = 1.0; ///< 1 / ln(M)

  mutable std::shared_mutex structure_mu_; ///< 保护下面的数组容量与生命周期
  mutable std::mutex entry_mu_;            ///< 保护 entry_point_ / max_level_

  std::vector<float> data_; ///< id * dim 连续存放的向量副本
  std::vector<int> levels_; ///< 每个节点的最高层
  std::vector<uint8_t> deleted_;
  size_t capacity_ = 0; ///< 已分配的节点槽位数（受 structure_mu_ 保护）

  std::unique_ptr<std::atomic<uint32_t>[]> l0_slots_;  ///< [node * M0 + i]
  std::unique_ptr<std::atomic<uint32_t>[]> l0_counts_; ///< 每节点当前邻居数
  std::vector<std::vector<std::vector<uint32_t>>> upper_links_; ///< [node][level-1]

  std::unique_ptr<std::mutex[]> stripes_; ///< 按 node & (kStripeCount-1) 取

  std::atomic<size_t> next_id_ {0};
  std::atomic<size_t> live_ {0};
  std::atomic<uint32_t> entry_point_ {kInvalidId};
  std::atomic<int> max_level_ {-1};
  uint64_t rng_state_;
};

#ifdef HNSW_TRACE_DISTANCE
/**
 * @brief Distance evaluations performed by the calling thread.
 *
 * Compiled in only with -DHNSW_TRACE_DISTANCE (benchmark instrumentation).
 * Counting is thread-local: graph construction on worker threads does not
 * pollute the numbers measured around single-threaded queries.
 */
uint64_t trace_distance_calls();
void trace_reset_distance_calls();
#endif

} // namespace vector
} // namespace minkv
