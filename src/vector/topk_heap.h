#pragma once

#include <algorithm>
#include <queue>
#include <string>
#include <vector>

namespace minkv {
namespace vector {

struct SearchResult {
  std::string key;
  float score;
  std::string metadata;

  // 用于最小堆（分数越大越好）
  bool operator<(const SearchResult &other) const {
    return score > other.score; // 反向比较，维护最小堆
  }
};

class TopKHeap {
public:
  explicit TopKHeap(size_t k) : k_(k) {}

  // 尝试插入结果
  void push(const SearchResult &result) {
    if (heap_.size() < k_) {
      heap_.push(result);
    } else if (result.score > heap_.top().score) {
      heap_.pop();
      heap_.push(result);
    }
  }

  // 获取 Top-K 结果（按分数降序）
  std::vector<SearchResult> get_results() {
    std::vector<SearchResult> results;
    results.reserve(heap_.size());

    while (!heap_.empty()) {
      results.push_back(heap_.top());
      heap_.pop();
    }

    // 反转为降序
    std::reverse(results.begin(), results.end());
    return results;
  }

  size_t size() const { return heap_.size(); }

  bool empty() const { return heap_.empty(); }

private:
  size_t k_;
  std::priority_queue<SearchResult> heap_; // 最小堆
};

} // namespace vector
} // namespace minkv
