#ifndef RAY_UTIL_SEQUENCER_H_
#define RAY_UTIL_SEQUENCER_H_

#include <deque>
#include <unordered_map>
#include "absl/synchronization/mutex.h"

namespace ray {

/// \class Sequencer
/// Sequencer guarantees that all operations with the same ordering key are sequenced.
/// This class is thread safe.
template <class KEY>
class Sequencer {
 public:
  void execute_ordered(KEY key, std::function<void()> operation) {
    mutex_.Lock();
    pending_operations_[key].push_back(operation);
    int queue_size = pending_operations_[key].size();
    mutex_.Unlock();
    if (1 == queue_size) {
      operation();
    }
  }

  void post_execute(KEY key) {
    mutex_.Lock();
    pending_operations_[key].pop_front();
    if (pending_operations_[key].empty()) {
      pending_operations_.erase(key);
      mutex_.Unlock();
    } else {
      auto operation = pending_operations_[key].front();
      mutex_.Unlock();
      operation();
    }
  }

 private:
  // Mutex to protect the pending_operations_ field.
  absl::Mutex mutex_;

  std::unordered_map<KEY, std::deque<std::function<void()>>> pending_operations_
      GUARDED_BY(mutex_);
};

}  // namespace ray

#endif  // RAY_UTIL_SEQUENCER_H_
