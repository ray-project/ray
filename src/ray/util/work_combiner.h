#ifndef RAY_UTIL_WORKER_COMBINER_H
#define RAY_UTIL_WORKER_COMBINER_H

#include <boost/asio.hpp>
#include <deque>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"

namespace ray {

class WorkCombiner {
 public:
  WorkCombiner(boost::asio::thread_pool &pool, int max_batch_size)
      : executor_(pool.get_executor()), max_batch_size_(max_batch_size){};

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
    boost::asio::post(executor_, [this]() {
      CombineEvents();
    });
  }

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

  int max_batch_size_;
  boost::asio::executor executor_;
  absl::Mutex mu_;
  std::deque<std::function<void()>> pending_ GUARDED_BY(mu_);
  bool combiner_active_ GUARDED_BY(mu_) = false;
};
}  // namespace ray

#endif  // RAY_UTIL_WORK_COMBINER_H
