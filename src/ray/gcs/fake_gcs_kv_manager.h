// Copyright The Ray Authors.
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
#include <string>
#include <unordered_map>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "ray/gcs/gcs_kv_manager.h"

namespace ray {
namespace gcs {

// Hand-written fake for InternalKVInterface that stores keys and values in an
// in-memory map. Supports all operations: Get, MultiGet, Put, Del, Exists, Keys.
// Warning: Naively prepends the namespace to the key, so e.g. the
// (namespace, key) pairs ("a", "bc") and ("ab", "c") will collide which is a bug.
class FakeInternalKV : public InternalKVInterface {
 public:
  FakeInternalKV() = default;

  // The in-memory store.
  std::unordered_map<std::string, std::string> kv_store_;

  void Get(const std::string &ns,
           const std::string &key,
           Postable<void(std::optional<std::string>)> callback) override {
    std::string full_key = ns + key;
    auto it = kv_store_.find(full_key);
    if (it == kv_store_.end()) {
      std::move(callback).Post("FakeInternalKV.Get.notfound", std::nullopt);
    } else {
      std::move(callback).Post("FakeInternalKV.Get.found", it->second);
    }
  }

  void MultiGet(
      const std::string &ns,
      const std::vector<std::string> &keys,
      Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) override {
    absl::flat_hash_map<std::string, std::string> result;
    for (const auto &key : keys) {
      std::string full_key = ns + key;
      auto it = kv_store_.find(full_key);
      if (it != kv_store_.end()) {
        result[key] = it->second;
      }
    }
    std::move(callback).Post("FakeInternalKV.MultiGet.result", result);
  }

  void Put(const std::string &ns,
           const std::string &key,
           std::string value,
           bool overwrite,
           Postable<void(bool)> callback) override {
    std::string full_key = ns + key;
    if (kv_store_.find(full_key) != kv_store_.end() && !overwrite) {
      std::move(callback).Post("FakeInternalKV.Put.false", false);
    } else {
      kv_store_[full_key] = value;
      std::move(callback).Post("FakeInternalKV.Put.true", true);
    }
  }

  void Del(const std::string &ns,
           const std::string &key,
           bool del_by_prefix,
           Postable<void(int64_t)> callback) override {
    int64_t deleted_count = 0;
    if (del_by_prefix) {
      std::string prefix = ns + key;
      for (auto it = kv_store_.begin(); it != kv_store_.end();) {
        if (it->first.find(prefix) == 0) {
          it = kv_store_.erase(it);
          ++deleted_count;
        } else {
          ++it;
        }
      }
    } else {
      std::string full_key = ns + key;
      auto it = kv_store_.find(full_key);
      if (it != kv_store_.end()) {
        kv_store_.erase(it);
        deleted_count = 1;
      }
    }
    std::move(callback).Post("FakeInternalKV.Del.result", deleted_count);
  }

  void Exists(const std::string &ns,
              const std::string &key,
              Postable<void(bool)> callback) override {
    std::string full_key = ns + key;
    bool exists = kv_store_.find(full_key) != kv_store_.end();
    std::move(callback).Post("FakeInternalKV.Exists.result", exists);
  }

  void Keys(const std::string &ns,
            const std::string &prefix,
            Postable<void(std::vector<std::string>)> callback) override {
    std::vector<std::string> result;
    std::string search_prefix = ns + prefix;
    for (const auto &pair : kv_store_) {
      if (pair.first.find(search_prefix) == 0) {
        result.push_back(pair.first.substr(ns.length()));
      }
    }
    std::move(callback).Post("FakeInternalKV.Keys.result", result);
  }
};

}  // namespace gcs
}  // namespace ray
