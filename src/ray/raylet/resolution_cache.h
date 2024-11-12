#pragma once

#include "absl/container/flat_hash_map.h"
#include <mutex>

namespace ray {

template <typename Key, typename Value>
class ResolutionCache {
 public:
  std::optional<Value> Get(const Key& key) {
    std::lock_guard lck(mtx_);
    auto iter = map_.find(key);
    if (iter == map_.end()) {
        return std::nullopt;
    }
    return iter->second;
  }

  std::optional<Value> GetAndPop(const Key& key) {
    std::lock_guard lck(mtx_);
    auto iter = map_.find(key);
    if (iter == map_.end()) {
        return std::nullopt;
    }
    auto value = std::move(iter->second);
    map_.erase(iter);
    return value;
  }

  void Put(Key key, Value val) {
    std::lock_guard lck(mtx_);
    map_.emplace(std::move(key), std::move(val));
  }

  std::mutex mtx_;
  absl::flat_hash_map<Key, Value> map_; 
};

}  // namespace ray
