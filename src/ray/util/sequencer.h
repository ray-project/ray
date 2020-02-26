#ifndef RAY_UTIL_SEQUENCER_H_
#define RAY_UTIL_SEQUENCER_H_

#include <deque>
#include <functional>
#include <unordered_map>
#include "absl/synchronization/mutex.h"

namespace ray {

/// This callback is used to notify when a operation completes.
using SequencerDoneCallback = std::function<void()>;

/// \class Sequencer
/// Sequencer guarantees that all operations with the same key are sequenced.
/// This class is thread safe.
template <class KEY>
class Sequencer {
 public:
  /// This function is used to ask the sequencer to execute the given operation.
  /// The sequencer guarantees that all operations with the same key are sequenced.
  ///
  /// \param key The key of operation.
  /// \param operation The operation to be called.
  void Post(KEY key, std::function<void(SequencerDoneCallback done_callback)> operation) {
    mutex_.Lock();
    pending_operations_[key].push_back(operation);
    int queue_size = pending_operations_[key].size();
    mutex_.Unlock();

    if (1 == queue_size) {
      auto done_callback = [this, key]() { PostExecute(key); };
      operation(done_callback);
    }
  }

 private:
  /// This function is used when a operation completes.
  /// If the sequencer has operations with the same key, we will execute next operation.
  ///
  /// \param key The key of operation.
  void PostExecute(const KEY key) {
    mutex_.Lock();
    pending_operations_[key].pop_front();
    if (pending_operations_[key].empty()) {
      pending_operations_.erase(key);
      mutex_.Unlock();
    } else {
      auto operation = pending_operations_[key].front();
      mutex_.Unlock();

      auto done_callback = [this, key]() { PostExecute(key); };
      operation(done_callback);
    }
  }

  // Mutex to protect the pending_operations_ field.
  absl::Mutex mutex_;

  std::unordered_map<KEY,
                     std::deque<std::function<void(SequencerDoneCallback done_callback)>>>
      pending_operations_ GUARDED_BY(mutex_);
};

}  // namespace ray

#endif  // RAY_UTIL_SEQUENCER_H_
