#pragma once

/**
 * ThreadPool — 固定大小线程池
 *
 * 设计要点：
 *   1. 预创建 N 个 worker 线程，消除 std::async 每次创建线程的开销（50~200μs）
 *   2. 任务队列用 std::queue + std::mutex + std::condition_variable 实现
 *      生产者（submit）push 任务并 notify_one，消费者（worker）wait 后 pop 执行
 *   3. submit 返回 std::future<T>，调用方可以 .get() 等待结果，与 std::async
 * 接口一致
 *   4. 析构时设置 stop_ 标志并 notify_all，所有 worker 优雅退出
 *
 * 线程安全：
 *   - 任务队列的 push/pop 由 queue_mutex_ 保护
 *   - stop_ 标志由同一把锁保护，避免 data race
 *   - worker 线程只做纯读操作（BFS），不需要额外同步
 *
 * 典型用法：
 *   ThreadPool pool(std::thread::hardware_concurrency());
 *   auto fut = pool.submit([](){ return compute(); });
 *   auto result = fut.get();
 */

#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <vector>

namespace minkv {
namespace base {

class ThreadPool {
public:
  /**
   * 构造函数：启动 n_threads 个 worker 线程
   * 推荐值：std::thread::hardware_concurrency()
   */
  explicit ThreadPool(size_t n_threads) : stop_(false) {
    workers_.reserve(n_threads);
    for (size_t i = 0; i < n_threads; ++i) {
      workers_.emplace_back([this] { worker_loop(); });
    }
  }

  // 禁止拷贝和移动（线程不可复制）
  ThreadPool(const ThreadPool &) = delete;
  ThreadPool &operator=(const ThreadPool &) = delete;

  /**
   * 析构函数：通知所有 worker 退出，等待它们完成当前任务
   * RAII 保证：即使抛异常也能正确清理线程
   */
  ~ThreadPool() {
    {
      std::unique_lock<std::mutex> lock(queue_mutex_);
      stop_ = true;
    }
    cv_.notify_all(); // 唤醒所有在 wait 的 worker
    for (auto &t : workers_) {
      if (t.joinable())
        t.join();
    }
  }

  /**
   * 提交任务到线程池
   *
   * @param fn  可调用对象（lambda、函数指针等）
   * @param args 参数包
   * @return std::future<返回值类型>，调用 .get() 获取结果或等待完成
   *
   * 实现细节：
   *   - 用 std::packaged_task 包装 fn，从中取出 future
   *   - 将 packaged_task 包在 shared_ptr 里（因为 std::function 要求可拷贝）
   *   - push 到队列后 notify_one 唤醒一个 worker
   */
  template <typename F, typename... Args>
  auto submit(F &&fn, Args &&...args)
      -> std::future<typename std::invoke_result<F, Args...>::type> {
    using ReturnType = typename std::invoke_result<F, Args...>::type;

    // 绑定参数，得到无参可调用对象
    auto bound = std::bind(std::forward<F>(fn), std::forward<Args>(args)...);

    // packaged_task 持有任务和 promise，future 从它这里取
    auto task =
        std::make_shared<std::packaged_task<ReturnType()>>(std::move(bound));
    std::future<ReturnType> fut = task->get_future();

    {
      std::unique_lock<std::mutex> lock(queue_mutex_);
      if (stop_) {
        throw std::runtime_error("ThreadPool: submit after shutdown");
      }
      // 包成 std::function<void()> 推入队列
      tasks_.push([task]() { (*task)(); });
    }
    cv_.notify_one(); // 唤醒一个空闲 worker

    return fut;
  }

  /** 返回线程池中的线程数 */
  size_t size() const { return workers_.size(); }

private:
  /**
   * worker 线程的主循环
   * 逻辑：等待任务 → 取出任务 → 执行 → 循环
   * 退出条件：stop_ == true 且队列为空
   */
  void worker_loop() {
    while (true) {
      std::function<void()> task;
      {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        // 等待：有任务 或 需要停止
        cv_.wait(lock, [this] { return stop_ || !tasks_.empty(); });

        // stop_ 且队列空 → 退出
        if (stop_ && tasks_.empty())
          return;

        task = std::move(tasks_.front());
        tasks_.pop();
      }
      // 锁已释放，执行任务（不持锁，允许其他 worker 并发取任务）
      task();
    }
  }

  std::vector<std::thread> workers_;        // 预创建的 worker 线程
  std::queue<std::function<void()>> tasks_; // 待执行的任务队列
  std::mutex queue_mutex_;                  // 保护 tasks_ 和 stop_
  std::condition_variable cv_;              // worker 等待/唤醒
  bool stop_;                               // 析构标志
};

} // namespace base
} // namespace minkv
