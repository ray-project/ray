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
// could be accessed by multiple getters. All values are wrapped with shared pointer to
// avoid copy at get operation, meanwhile also useful to maintain memory validity at any
// time.
//
// Example usage:
// SharedLruCache<std::string, std::string> cache{cap};
// // Put a key-value pair into cache.
// cache.Put("key", std::make_shared<std::string>("val"));
//
// // Get a key-value pair from cache.
// auto val = cache.Get("key");
// // Check and consume `val`.
//
// TODO(hjiang):
// For thread-safe cache, add a sharded container wrapper to reduce lock contention.

#pragma once

#include <condition_variable>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "ray/util/logging.h"
#include "ray/util/map_utils.h"

namespace ray::utils::container {

template <typename Key, typename Val>
class SharedLruCache final {
 public:
  using key_type = Key;
  using mapped_type = Val;

  // A `max_entries` of 0 means that there is no limit on the number of entries
  // in the cache.
  explicit SharedLruCache(size_t max_entries) : max_entries_(max_entries) {}

  SharedLruCache(const SharedLruCache &) = delete;
  SharedLruCache &operator=(const SharedLruCache &) = delete;

  ~SharedLruCache() = default;

  // Insert `value` with key `key`. This will replace any previous entry with
  // the same key.
  void Put(Key key, std::shared_ptr<Val> value) {
    RAY_CHECK(value != nullptr);
    auto iter = cache_.find(key);
    if (iter != cache_.end()) {
      lru_list_.splice(lru_list_.begin(), lru_list_, iter->second.lru_iterator);
      iter->second.value = std::move(value);
      return;
    }

    lru_list_.emplace_front(key);
    Entry new_entry{std::move(value), lru_list_.begin()};
    cache_[std::cref(lru_list_.front())] = std::move(new_entry);

    if (max_entries_ > 0 && lru_list_.size() > max_entries_) {
      const auto &stale_key = lru_list_.back();
      cache_.erase(stale_key);
      lru_list_.pop_back();
    }

    RAY_CHECK_EQ(lru_list_.size(), cache_.size());
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
  std::shared_ptr<Val> Get(KeyLike &&key) {
    const auto cache_iter = cache_.find(key);
    if (cache_iter == cache_.end()) {
      return nullptr;
    }
    lru_list_.splice(lru_list_.begin(), lru_list_, cache_iter->second.lru_iterator);
    return cache_iter->second.value;
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
    std::shared_ptr<Val> value;

    // A list iterator pointing to the entry's position in the LRU list.
    typename std::list<Key>::iterator lru_iterator;
  };

  // TODO(hjiang): These two internal type alias has been consolidated into stable header
  // in later versions, update after we bump up abseil.
  using KeyHash = absl::container_internal::hash_default_hash<Key>;
  using KeyEqual = absl::container_internal::hash_default_eq<Key>;

  using KeyConstRef = std::reference_wrapper<const Key>;
  using EntryMap =
      absl::flat_hash_map<KeyConstRef, Entry, RefHash<KeyHash>, RefEq<KeyEqual>>;

  // The maximum number of entries in the cache. A value of 0 means there is no
  // limit on entry count.
  const size_t max_entries_;

  // Stores key-value pairs.
  EntryMap cache_;

  // The LRU list of entries. The front of the list identifies the most
  // recently accessed entry.
  std::list<Key> lru_list_;
};

// Same interfaces as `SharedLruCache`, but all cached values are
// `const`-specified to avoid concurrent updates.
template <typename K, typename V>
using SharedLruConstCache = SharedLruCache<K, const V>;

// Same interface and functionality as `SharedLruCache`, but thread-safe version.
template <typename Key, typename Val>
class ThreadSafeSharedLruCache final {
 public:
  using key_type = Key;
  using mapped_type = Val;

  // A `max_entries` of 0 means that there is no limit on the number of entries
  // in the cache.
  explicit ThreadSafeSharedLruCache(size_t max_entries) : cache_(max_entries) {}

  ThreadSafeSharedLruCache(const ThreadSafeSharedLruCache &) = delete;
  ThreadSafeSharedLruCache &operator=(const ThreadSafeSharedLruCache &) = delete;

  ~ThreadSafeSharedLruCache() = default;

  // Insert `value` with key `key`. This will replace any previous entry with
  // the same key.
  void Put(Key key, std::shared_ptr<Val> value) {
    std::lock_guard lck(mu_);
    cache_.Put(std::move(key), std::move(value));
  }

  // Delete the entry with key `key`. Return true if the entry was found for
  // `key`, false if the entry was not found. In both cases, there is no entry
  // with key `key` existed after the call.
  template <typename KeyLike>
  bool Delete(KeyLike &&key) {
    std::lock_guard lck(mu_);
    return cache_.Delete(std::forward<KeyLike>(key));
  }

  // Look up the entry with key `key`. Return std::nullopt if key doesn't exist.
  // If found, return a copy for the value.
  template <typename KeyLike>
  std::shared_ptr<Val> Get(KeyLike &&key) {
    std::lock_guard lck(mu_);
    return cache_.Get(std::forward<KeyLike>(key));
  }

  // Get or creation for cached key-value pairs.
  //
  // WARNING: Currently factory cannot have exception thrown.
  // TODO(hjiang): [factory] should support template.
  template <typename KeyLike>
  std::shared_ptr<Val> GetOrCreate(
      KeyLike &&key, std::function<std::shared_ptr<Val>(const Key &)> factory) {
    std::shared_ptr<CreationToken> creation_token;

    {
      std::unique_lock lck(mu_);
      auto cached_val = cache_.Get(key);
      if (cached_val != nullptr) {
        return cached_val;
      }

      auto creation_iter = ongoing_creation_.find(key);

      // Another thread has requested for the same key-value pair, simply wait for its
      // completion.
      if (creation_iter != ongoing_creation_.end()) {
        creation_token = creation_iter->second;
        ++creation_token->count;
        creation_token->cv.wait(lck, [creation_token = creation_token.get()]() {
          return creation_token->val != nullptr;
        });

        // Creation finished.
        --creation_token->count;
        if (creation_token->count == 0) {
          // [creation_iter] could be invalidated here due to new insertion/deletion.
          ongoing_creation_.erase(key);
        }
        return creation_token->val;
      }

      // Current thread is the first one to request for the key-value pair, perform
      // factory function.
      creation_iter =
          ongoing_creation_.emplace(key, std::make_shared<CreationToken>()).first;
      creation_token = creation_iter->second;
      creation_token->count = 1;
    }

    // Place factory out of critical section.
    std::shared_ptr<Val> val = factory(key);

    {
      std::lock_guard lck(mu_);
      cache_.Put(key, val);
      creation_token->val = val;
      creation_token->cv.notify_all();
      int new_count = --creation_token->count;
      if (new_count == 0) {
        // [creation_iter] could be invalidated here due to new insertion/deletion.
        ongoing_creation_.erase(key);
      }
    }

    return val;
  }

  // Clear the cache.
  void Clear() {
    std::lock_guard lck(mu_);
    cache_.Clear();
  }

  // Accessors for cache parameters.
  size_t max_entries() const { return cache_.max_entries(); }

 private:
  struct CreationToken {
    std::condition_variable cv;
    // Nullptr indicate creation unfinished.
    std::shared_ptr<Val> val;
    // Counter for ongoing creation.
    int count = 0;
  };

  std::mutex mu_;
  SharedLruCache<Key, Val> cache_;

  // Ongoing creation.
  absl::flat_hash_map<Key, std::shared_ptr<CreationToken>> ongoing_creation_;
};

// Same interfaces as `SharedLruCache`, but all cached values are
// `const`-specified to avoid concurrent updates.
template <typename K, typename V>
using ThreadSafeSharedLruConstCache = ThreadSafeSharedLruCache<K, const V>;

}  // namespace ray::utils::container
