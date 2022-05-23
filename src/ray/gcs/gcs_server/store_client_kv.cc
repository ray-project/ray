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

#include "ray/gcs/gcs_server/store_client_kv.h"

#include <memory>

namespace ray {
namespace gcs {

StoreClientInternalKV::StoreClientInternalKV(std::unique_ptr<StoreClient> store_client)
    : delegate_(std::move(store_client)) {}

void StoreClientInternalKV::Get(
    const std::string &table_name,
    const std::string &key,
    std::function<void(std::optional<std::string>)> callback) {
  if (!callback) {
    callback = [](auto) {};
  }
  RAY_CHECK_OK(delegate_->AsyncGet(
      table_name, key, [callback = std::move(callback)](auto status, auto result) {
        callback(result.has_value() ? std::optional<std::string>(result.value())
                                    : std::optional<std::string>());
      }));
}

void StoreClientInternalKV::Put(const std::string &table_name,
                                const std::string &key,
                                const std::string &value,
                                bool overwrite,
                                std::function<void(bool)> callback) {
  if (!callback) {
    callback = [](auto) {};
  }
  RAY_CHECK_OK(delegate_->AsyncPut(table_name, key, value, overwrite, callback));
}

void StoreClientInternalKV::Del(const std::string &table_name,
                                const std::string &key,
                                bool del_by_prefix,
                                std::function<void(int64_t)> callback) {
  if (!callback) {
    callback = [](auto) {};
  }
  if (!del_by_prefix) {
    RAY_CHECK_OK(delegate_->AsyncDelete(
        table_name, key, [callback = std::move(callback)](bool deleted) {
          callback(deleted ? 1 : 0);
        }));
    return;
  }

  RAY_CHECK_OK(delegate_->AsyncGetKeys(
      table_name, key, [this, table_name, callback = std::move(callback)](auto keys) {
        if (keys.empty()) {
          callback(0);
          return;
        }
        RAY_CHECK_OK(delegate_->AsyncBatchDelete(table_name, keys, std::move(callback)));
      }));
}

void StoreClientInternalKV::Exists(const std::string &table_name,
                                   const std::string &key,
                                   std::function<void(bool)> callback) {
  if (!callback) {
    callback = [](auto) {};
  }
  RAY_CHECK_OK(delegate_->AsyncExists(table_name, key, std::move(callback)));
}

void StoreClientInternalKV::Keys(const std::string &table_name,
                                 const std::string &prefix,
                                 std::function<void(std::vector<std::string>)> callback) {
  if (!callback) {
    callback = [](auto) {};
  }
  RAY_CHECK_OK(delegate_->AsyncGetKeys(table_name, prefix, std::move(callback)));
}

}  // namespace gcs
}  // namespace ray
