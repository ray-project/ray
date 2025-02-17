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

#include "absl/container/flat_hash_map.h"
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

  // Returns true if key existed and visitor was called.
  // Write-only overload.
  template <typename KeyLike>
  bool WriteVisit(const KeyLike &key, const std::function<void(ValueType &)> &visitor) {
    auto write_lock = map_.LockForWrite();
    auto &map = write_lock.Get();
    const auto it = map.find(key);
    if (it != map.end()) {
      visitor(it->second);
      return true;
    }
    return false;
  }

  // Returns true if key existed and visitor was called.
  template <typename KeyLike>
  bool ReadVisit(const KeyLike &key,
                 const std::function<void(const ValueType &)> &visitor) const {
    auto read_lock = map_.LockForRead();
    const auto &map = read_lock.Get();
    auto it = map.find(key);
    if (it != map.end()) {
      visitor(it->second);
      return true;
    }
    return false;
  }

  // Applies visitor to all values in the map.
  void ReadVisitAll(
      const std::function<void(const KeyType &, const ValueType &)> &visitor) const {
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

  /// Will insert the key value pair if the key does not exist.
  /// Will replace the value if the key already exists.
  template <typename KeyLike, typename... Args>
  void InsertOrAssign(KeyLike &&key, Args &&...args) {
    auto write_lock = map_.LockForWrite();
    write_lock.Get().insert_or_assign(std::forward<KeyLike>(key),
                                      ValueType(std::forward<Args>(args)...));
  }

  /// Returns a bool for whether the key/value was emplaced.
  /// Note: This will not overwrite an existing key.
  template <typename KeyLike, typename... Args>
  bool Emplace(KeyLike &&key, Args &&...args) {
    auto write_lock = map_.LockForWrite();
    const auto [_, inserted] = write_lock.Get().emplace(
        std::forward<KeyLike>(key), ValueType(std::forward<Args>(args)...));
    return inserted;
  }

  /// Returns a bool identifying whether the key was found and erased.
  template <typename KeyLike>
  bool Erase(const KeyLike &key) {
    auto write_lock = map_.LockForWrite();
    return write_lock.Get().erase(key) > 0;
  }

  /// Returns the number of keys erased.
  int64_t EraseKeys(const absl::Span<KeyType> &keys) {
    auto write_lock = map_.LockForWrite();
    int64_t num_erased = 0;
    for (const auto &key : keys) {
      num_erased += write_lock.Get().erase(key);
    }
    return num_erased;
  }

  /// Returns a copy of the underlying flat_hash_map.
  absl::flat_hash_map<KeyType, ValueType> GetMapClone() const {
    auto read_lock = map_.LockForRead();
    return read_lock.Get();
  }

 private:
  MutexProtected<absl::flat_hash_map<KeyType, ValueType>> map_;
};

}  // namespace ray
