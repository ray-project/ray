// Copyright 2024 The Ray Authors.
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
//
// SharedLruCache is a LRU cache, with all entries shared, which means a single entry
// could be accessed by multiple getters. When `Get`, a copy of the value is returned, so
// for heavy-loaded types it's suggested to wrap with `std::shared_ptr<>`.
//
// Example usage:
// SharedLruCache<std::string, std::string> cache{cap};
// // Put a key-value pair into cache.
// cache.Put("key", "val");
//
// // Get a key-value pair from cache.
// auto val = ache.Get("key");
// // Check and consume `val`.
//
// TODO(hjiang): Write a wrapper around KeyHash and KeyEq, which takes
// std::reference_wrapper<Key>, so we could store keys only in std::list, and reference in
// absl::flat_hash_map.

#pragma once

#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/container/internal/hash_function_defaults.h"

namespace ray::utils::container {

// TODO(hjiang): `absl::container_internal::hash_default_` has been promoted to stable
// public interface in the latest abseil, update after we bump up abseil version.
template <typename Key,
          typename Val,
          typename KeyHash = absl::container_internal::hash_default_hash<Key>,
          typename KeyEq = absl::container_internal::hash_default_eq<Key>>
class SharedLruCache final {
 public:
  using key_type = Key;
  using mapped_type = Val;
  using hasher = KeyHash;
  using key_equal = KeyEq;

  // A `max_entries` of 0 means that there is no limit on the number of entries
  // in the cache.
  explicit SharedLruCache(size_t max_entries) : max_entries_(max_entries) {}

  SharedLruCache(const SharedLruCache &) = delete;
  SharedLruCache &operator=(const SharedLruCache &) = delete;

  ~SharedLruCache() = default;

  // Insert `value` with key `key`. This will replace any previous entry with
  // the same key.
  void Put(Key key, Val value) {
    lru_list_.emplace_front(key);
    Entry new_entry{std::move(value), lru_list_.begin()};
    cache_[std::move(key)] = std::move(new_entry);

    if (max_entries_ > 0 && lru_list_.size() > max_entries_) {
      const auto &stale_key = lru_list_.back();
      cache_.erase(stale_key);
      lru_list_.pop_back();
    }
  }

  // Delete the entry with key `key`. Return true if the entry was found for
  // `key`, false if the entry was not found. In both cases, there is no entry
  // with key `key` existed after the call.
  template <typename KeyLike>
  bool Delete(KeyLike &&key) {
    auto it = cache_.find(key);
    if (it == cache_.end()) {
      return false;
    }
    lru_list_.erase(it->second.lru_iterator);
    cache_.erase(it);
    return true;
  }

  // Look up the entry with key `key`. Return std::nullopt if key doesn't exist.
  // If found, return a copy for the value.
  template <typename KeyLike>
  std::optional<Val> Get(KeyLike &&key) {
    const auto cache_iter = cache_.find(key);
    if (cache_iter == cache_.end()) {
      return std::nullopt;
    }
    Val value = std::move(cache_iter->second.value);
    lru_list_.erase(cache_iter->second.lru_iterator);
    cache_.erase(cache_iter);

    // Re-insert into the cache, no need to check capacity.
    lru_list_.emplace_front(static_cast<Key>(key));
    Entry new_entry{value, lru_list_.begin()};
    cache_[static_cast<Key>(std::forward<KeyLike>(key))] = std::move(new_entry);

    return value;
  }

  // Clear the cache.
  void Clear() {
    cache_.clear();
    lru_list_.clear();
  }

  // Accessors for cache parameters.
  size_t max_entries() const { return max_entries_; }

 private:
  struct Entry {
    // The entry's value.
    Val value;

    // A list iterator pointing to the entry's position in the LRU list.
    typename std::list<Key>::iterator lru_iterator;
  };

  using EntryMap = absl::flat_hash_map<Key, Entry, KeyHash, KeyEq>;

  // The maximum number of entries in the cache. A value of 0 means there is no
  // limit on entry count.
  const size_t max_entries_;

  // All keys are stored as refernce (`std::reference_wrapper`), and the
  // ownership lies in `lru_list_`.
  EntryMap cache_;

  // The LRU list of entries. The front of the list identifies the most
  // recently accessed entry.
  std::list<Key> lru_list_;
};

// Same interfaces as `SharedLruCache`, but all cached values are
// `const`-specified to avoid concurrent updates.
template <typename K,
          typename V,
          typename KeyHash = absl::container_internal::hash_default_hash<K>,
          typename KeyEq = absl::container_internal::hash_default_eq<K>>
using SharedLruConstCache = SharedLruCache<K, const V, KeyHash, KeyEq>;

// Same interface and functionality as `SharedLruCache`, but thread-safe version.
template <typename Key,
          typename Val,
          typename KeyHash = absl::container_internal::hash_default_hash<Key>,
          typename KeyEq = absl::container_internal::hash_default_eq<Key>>
class ThreadSafeSharedLruCache final {
 public:
  using key_type = Key;
  using mapped_type = Val;
  using hasher = KeyHash;
  using key_equal = KeyEq;

  // A `max_entries` of 0 means that there is no limit on the number of entries
  // in the cache.
  explicit ThreadSafeSharedLruCache(size_t max_entries) : cache_(max_entries) {}

  ThreadSafeSharedLruCache(const ThreadSafeSharedLruCache &) = delete;
  ThreadSafeSharedLruCache &operator=(const ThreadSafeSharedLruCache &) = delete;

  ~ThreadSafeSharedLruCache() = default;

  // Insert `value` with key `key`. This will replace any previous entry with
  // the same key.
  void Put(Key key, Val value) {
    std::lock_guard lck(mtx_);
    cache_.Put(std::move(key), std::move(value));
  }

  // Delete the entry with key `key`. Return true if the entry was found for
  // `key`, false if the entry was not found. In both cases, there is no entry
  // with key `key` existed after the call.
  template <typename KeyLike>
  bool Delete(KeyLike &&key) {
    std::lock_guard lck(mtx_);
    return cache_.Delete(std::forward<KeyLike>(key));
  }

  // Look up the entry with key `key`. Return std::nullopt if key doesn't exist.
  // If found, return a copy for the value.
  template <typename KeyLike>
  std::optional<Val> Get(KeyLike &&key) {
    std::lock_guard lck(mtx_);
    return cache_.Get(std::forward<KeyLike>(key));
  }

  // Clear the cache.
  void Clear() {
    std::lock_guard lck(mtx_);
    cache_.Clear();
  }

  // Accessors for cache parameters.
  size_t max_entries() const { return cache_.max_entries(); }

 private:
  std::mutex mtx_;
  SharedLruCache<Key, Val, KeyHash, KeyEq> cache_;
};

// Same interfaces as `SharedLruCache`, but all cached values are
// `const`-specified to avoid concurrent updates.
template <typename K,
          typename V,
          typename KeyHash = absl::container_internal::hash_default_hash<K>,
          typename KeyEq = absl::container_internal::hash_default_eq<K>>
using ThreadSafeSharedLruConstCache =
    ThreadSafeSharedLruCache<K, const V, KeyHash, KeyEq>;

}  // namespace ray::utils::container
