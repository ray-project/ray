// Copyright  The Ray Authors.
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

#include "gmock/gmock.h"
#include "ray/gcs/gcs_server/gcs_kv_manager.h"

namespace ray {
namespace gcs {

class MockInternalKVInterface : public ray::gcs::InternalKVInterface {
 public:
  MockInternalKVInterface() {}

  MOCK_METHOD(void,
              Get,
              (const std::string &ns,
               const std::string &key,
               Postable<void(std::optional<std::string>)> callback),
              (override));
  MOCK_METHOD(void,
              MultiGet,
              (const std::string &ns,
               const std::vector<std::string> &keys,
               Postable<void(absl::flat_hash_map<std::string, std::string>)> callback),
              (override));
  MOCK_METHOD(void,
              Put,
              (const std::string &ns,
               const std::string &key,
               std::string value,
               bool overwrite,
               Postable<void(bool)> callback),
              (override));
  MOCK_METHOD(void,
              Del,
              (const std::string &ns,
               const std::string &key,
               bool del_by_prefix,
               Postable<void(int64_t)> callback),
              (override));
  MOCK_METHOD(void,
              Exists,
              (const std::string &ns,
               const std::string &key,
               Postable<void(bool)> callback),
              (override));
  MOCK_METHOD(void,
              Keys,
              (const std::string &ns,
               const std::string &prefix,
               Postable<void(std::vector<std::string>)> callback),
              (override));
};

// Fake internal KV interface that simply stores keys and values in a C++ map.
// Only supports Put and Get.
// Warning: Naively prepends the namespace to the key, so e.g.
// the (namespace, key) pairs ("a", "bc") and ("ab", "c") will collide which is a bug.

class FakeInternalKVInterface : public ray::gcs::InternalKVInterface {
 public:
  FakeInternalKVInterface() = default;

  // The C++ map.
  std::unordered_map<std::string, std::string> kv_store_;

  void Get(const std::string &ns,
           const std::string &key,
           Postable<void(std::optional<std::string>)> callback) override {
    std::string full_key = ns + key;
    auto it = kv_store_.find(full_key);
    if (it == kv_store_.end()) {
      std::move(callback).Post("FakeInternalKVInterface.Get.notfound", std::nullopt);
    } else {
      std::move(callback).Post("FakeInternalKVInterface.Get.found", it->second);
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
    std::move(callback).Post("FakeInternalKVInterface.MultiGet.result", result);
  }

  void Put(const std::string &ns,
           const std::string &key,
           std::string value,
           bool overwrite,
           Postable<void(bool)> callback) override {
    std::string full_key = ns + key;
    if (kv_store_.find(full_key) != kv_store_.end() && !overwrite) {
      std::move(callback).Post("FakeInternalKVInterface.Put.false", false);
    } else {
      kv_store_[full_key] = value;
      std::move(callback).Post("FakeInternalKVInterface.Put.true", true);
    }
  }

  MOCK_METHOD(void,
              Del,
              (const std::string &ns,
               const std::string &key,
               bool del_by_prefix,
               Postable<void(int64_t)> callback),
              (override));
  MOCK_METHOD(void,
              Exists,
              (const std::string &ns,
               const std::string &key,
               Postable<void(bool)> callback),
              (override));
  MOCK_METHOD(void,
              Keys,
              (const std::string &ns,
               const std::string &prefix,
               Postable<void(std::vector<std::string>)> callback),
              (override));
};

}  // namespace gcs
}  // namespace ray
