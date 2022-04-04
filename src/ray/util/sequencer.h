// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <deque>
#include <functional>

#include "absl/container/flat_hash_map.h"
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

  absl::flat_hash_map<
      KEY,
      std::deque<std::function<void(SequencerDoneCallback done_callback)>>>
      pending_operations_ GUARDED_BY(mutex_);
};

}  // namespace ray
