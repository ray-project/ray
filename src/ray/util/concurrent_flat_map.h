// Copyright 2025 The Ray Authors.
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

#include <optional>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/types/span.h"
#include "ray/util/mutex_protected.h"

namespace ray {

// A flat map that's protected by a single mutex with read/write locks.
//
// Use when expecting one of the following:
// - mininal thread contention
// - trivially copyable values
// - majority reads
// If these don't describe your use case, prefer boost::concurrent_flat_map.
template <typename KeyType, typename ValueType>
class ConcurrentFlatMap {
 public:
  template <typename... Args>
  explicit ConcurrentFlatMap(Args &&...args) : map_(std::forward(args)...) {}

  ConcurrentFlatMap() = default;

  void Reserve(size_t size) {
    auto write_lock = map_.LockForWrite();
    write_lock.Get().reserve(size);
  }

  template <typename KeyLike>
  std::optional<ValueType> Get(const KeyLike &key) const {
    std::optional<ValueType> result;
    auto read_lock = map_.LockForRead();
    const auto &map = read_lock.Get();
    const auto it = map.find(key);
    if (it != map.end()) {
      result.emplace(it->second);
    }
    return result;
  }

  // Calls visit on all of the keys in the span.
  // Visitor is called under a write lock so should not be heavy, otherwise prefer Get
  // followed by InsertOrAssign.
  template <typename KeyLike, typename Visitor>
  void WriteVisit(absl::Span<KeyLike> keys, const Visitor &visitor) {
    auto write_lock = map_.LockForWrite();
    auto &map = write_lock.Get();
    for (const auto &key : keys) {
      auto it = map.find(key);
      if (it != map.end()) {
        visitor(it->first, it->second);
      }
    }
  }

  // Calls visit on all of the keys in the span.
  // Calls function under read lock so should not be heavy, otherwise prefer Get to copy
  // out.
  template <typename KeyLike, typename Visitor>
  void ReadVisit(absl::Span<KeyLike> keys, const Visitor &visitor) const {
    auto read_lock = map_.LockForRead();
    const auto &map = read_lock.Get();
    for (const auto &key : keys) {
      auto it = map.find(key);
      if (it != map.end()) {
        visitor(it->first, it->second);
      }
    }
  }

  // Applies visitor to all values in the map.
  // Calls under read lock so should not be heavy, otherwise prefer GetMapClone to copy.
  template <typename Visitor>
  void ReadVisitAll(const Visitor &visitor) const {
    auto read_lock = map_.LockForRead();
    const auto &map = read_lock.Get();
    for (const auto &[key, value] : map) {
      visitor(key, value);
    }
  }

  template <typename KeyLike>
  bool Contains(const KeyLike &key) const {
    auto read_lock = map_.LockForRead();
    return read_lock.Get().contains(key);
  }

  // Will insert the key value pair if the key does not exist.
  // Will replace the value if the key already exists.
  // Returns true if insertion happened, false if assignment happened.
  template <typename KeyLike, typename... Args>
  bool InsertOrAssign(KeyLike &&key, Args &&...args) {
    auto value = ValueType(std::forward<Args>(args)...);
    auto write_lock = map_.LockForWrite();
    auto [_, new_inserted] =
        write_lock.Get().insert_or_assign(std::forward<KeyLike>(key), std::move(value));
    return new_inserted;
  }

  // Returns a bool for whether the key/value was emplaced.
  // Note: This will not overwrite an existing key.
  template <typename KeyLike, typename... Args>
  bool Emplace(KeyLike &&key, Args &&...args) {
    auto value = ValueType(std::forward<Args>(args)...);
    auto write_lock = map_.LockForWrite();
    const auto [_, inserted] =
        write_lock.Get().emplace(std::forward<KeyLike>(key), std::move(value));
    return inserted;
  }

  // Returns a bool identifying whether the key was found and erased.
  template <typename KeyLike>
  bool Erase(const KeyLike &key) {
    auto write_lock = map_.LockForWrite();
    return write_lock.Get().erase(key) > 0;
  }

  // Returns the number of keys erased.
  template <typename KeyLike>
  int64_t EraseKeys(absl::Span<KeyLike> keys) {
    auto write_lock = map_.LockForWrite();
    auto &map = write_lock.Get();
    int64_t num_erased = 0;
    for (const auto &key : keys) {
      num_erased += map.erase(key);
    }
    return num_erased;
  }

  // Returns a copy of the underlying flat_hash_map.
  absl::flat_hash_map<KeyType, ValueType> GetMapClone() const {
    auto read_lock = map_.LockForRead();
    return read_lock.Get();
  }

 private:
  MutexProtected<absl::flat_hash_map<KeyType, ValueType>> map_;
};

}  // namespace ray
