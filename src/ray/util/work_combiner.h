#ifndef RAY_UTIL_WORKER_COMBINER_H
#define RAY_UTIL_WORKER_COMBINER_H

#include <boost/asio.hpp>
#include <deque>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"

namespace ray {

/// Work batcher that improves the submission performance of asio::thread_pool by
/// over an order of magnitude. It reduces the submission latency of work items by
/// (1) asynchronously posting work items to the pool, and (2) batching work items
/// to reduce the number of post operations. It can also reduce the post latency
/// of asio::io_service, but to a lesser extent (~2-3x).
class WorkCombiner {
 public:
  WorkCombiner(boost::asio::executor executor, int max_batch_size)
      : executor_(executor), max_batch_size_(max_batch_size){};

  /// Post the given function asynchronously to the executor. It may be combined
  /// with other work units in a batch post.
  void post(std::function<void()> fn) {
    absl::MutexLock lock(&mu_);
    pending_.push_back(fn);
    TryStartCombiner();
  }

 private:
  void TryStartCombiner() EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    if (combiner_active_) {
      return;
    }
    combiner_active_ = true;
    // Posts a combiner op. This op runs until there are no more events left
    // to process, which could be indefinitely if there is a continuous
    // stream of posts to this combiner.
    boost::asio::post(executor_, [this]() { CombineEvents(); });
  }

  /// Combiner op that runs until the pending queue is exhausted. While there
  /// are remaining work items, it posts batches of them to the executor.
  void CombineEvents() LOCKS_EXCLUDED(mu_) {
    while (true) {
      std::vector<std::function<void()>> event_batch;
      {
        absl::MutexLock lock(&mu_);
        while (!pending_.empty() && event_batch.size() < max_batch_size_) {
          event_batch.push_back(pending_.front());
          pending_.pop_front();
        }
        if (event_batch.empty()) {
          combiner_active_ = false;
          break;  // Idle, yield this thread.
        }
      }
      // Post the entire batch as one event.
      boost::asio::post(executor_, [this, event_batch]() {
        for (auto &fn : event_batch) {
          fn();
        }
      });
    }
  }

  /// The underlying executor that can run work items.
  boost::asio::executor executor_;

  /// The max batch size of work to post to the executor.
  int max_batch_size_;

  /// Protects combiner state below.
  absl::Mutex mu_;

  /// The list of work items that have been posted.
  std::deque<std::function<void()>> pending_ GUARDED_BY(mu_);

  /// Whether the combiner op is currently running.
  bool combiner_active_ GUARDED_BY(mu_) = false;
};
}  // namespace ray

#endif  // RAY_UTIL_WORK_COMBINER_H
