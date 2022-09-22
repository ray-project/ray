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

// TODO: add docs, unit test
#include "ray/util/macros.h"
#include "absl/container/flat_hash_map.h"
/// \class Counter
///
/// This container implements counter behavior on top of an absl hash table. Counter
/// entries will be automatically cleaned up when they fall back to zero.
template <typename K>
class Counter {
 public:
  Counter() {};

  Counter(const Counter &other) = delete;

  Counter &operator=(const Counter &other) = delete;

  void SetOnChangeCallback(std::function<void(const K&, int64_t)> on_change) {
    on_change_ = on_change;
  }

  void Increment(const K &key) {
    counters_[key] += 1;
    if (on_change_ != nullptr) {
      on_change_(key, counters_[key]);
    }
  }

  void Decrement(const K &key) {
    auto it = counters_.find(key);
    RAY_CHECK(it != counters_.end());
    it->second -= 1;
    int64_t new_value = it->second;
    if (new_value <= 0) {
      counters_.erase(it);
    }
    if (on_change_ != nullptr) {
      on_change_(key, new_value);
    }
  }

  int64_t Get(const K &key) const {
    auto it = counters_.find(key);
    if (it == counters_.end()) {
      return 0;
    } else {
      RAY_CHECK(it->second >= 0) << "Counters values cannot be negative.";
      return it->second;
    }
  }

  void Swap(const K &old_key, const K &new_key) {
    if (old_key != new_key) {
      Increment(new_key);
      Decrement(old_key);
    }
  }

  size_t Size() const {
    return counters_.size();
  }

  void ForEachEntry(std::function<void(const K&, int64_t)> callback) {
    for (const auto& it : counters_) {
      callback(it->first, it->second);
    }
  }

 private:
  absl::flat_hash_map<K, int64_t> counters_;
  std::function<void(const K&, int64_t)> on_change_;
};
