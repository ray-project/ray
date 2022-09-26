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

#include <list>

#include "absl/container/flat_hash_map.h"
#include "ray/util/logging.h"

/// \class CounterMap
///
/// This container implements counter behavior on top of an absl hash table. CounterMap
/// entries will be automatically cleaned up when they fall back to zero. CounterMap
/// entries are not allowed to be negative. A callback can be set to run when any
/// counter entry changes.
///
/// For example, this can be used to track the number of running tasks broken down
/// by their function name, or track the number of tasks by (name, state) pairs.
///
/// This class is *not* thread-safe.
template <typename K>
class CounterMap {
 public:
  CounterMap(){};

  CounterMap(const CounterMap &other) = delete;

  CounterMap &operator=(const CounterMap &other) = delete;

  /// Set a function `f((key, count))` to run when the count for the key changes.
  void SetOnChangeCallback(std::function<void(const K &, int64_t)> on_change) {
    on_change_ = on_change;
  }

  /// Increment the specified key by one.
  void Increment(const K &key) {
    counters_[key] += 1;
    total_ += 1;
    if (on_change_ != nullptr) {
      on_change_(key, counters_[key]);
    }
  }

  /// Decrement the specified key by one. If the count for the key drops to zero,
  /// the entry for the key is erased from the counter. It is not allowed for
  /// the count to be decremented below zero.
  void Decrement(const K &key) {
    auto it = counters_.find(key);
    RAY_CHECK(it != counters_.end());
    it->second -= 1;
    total_ -= 1;
    int64_t new_value = it->second;
    if (new_value <= 0) {
      counters_.erase(it);
    }
    if (on_change_ != nullptr) {
      on_change_(key, new_value);
    }
  }

  /// Get the current count for the key, or zero if not tracked.
  int64_t Get(const K &key) const {
    auto it = counters_.find(key);
    if (it == counters_.end()) {
      return 0;
    } else {
      RAY_CHECK(it->second >= 0) << "CounterMap values cannot be negative.";
      return it->second;
    }
  }

  /// Decrement `old_key` by one and increment `new_key` by one.
  void Swap(const K &old_key, const K &new_key) {
    if (old_key != new_key) {
      Decrement(old_key);
      Increment(new_key);
    }
  }

  /// Return the number of non-zero keys tracked in this counter.
  size_t Size() const { return counters_.size(); }

  /// Return the total count across all keys in this counter.
  size_t Total() const { return total_; }

  /// Run the given function `f((key, count))` for every tracked entry.
  void ForEachEntry(std::function<void(const K &, int64_t)> callback) const {
    for (const auto &it : counters_) {
      callback(it.first, it.second);
    }
  }

 private:
  absl::flat_hash_map<K, int64_t> counters_;
  std::function<void(const K &, int64_t)> on_change_;
  size_t total_ = 0;
};
