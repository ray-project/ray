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

#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

namespace {

constexpr std::string_view kNamespacePrefix = "@namespace_";
constexpr std::string_view kNamespaceSep = ":";

std::string MakeKey(const std::string &ns, const std::string &key) {
  if (ns.empty()) {
    return key;
  }
  return absl::StrCat(kNamespacePrefix, ns, kNamespaceSep, key);
}

std::string ExtractKey(const std::string &key) {
  auto view = std::string_view(key);
  if (absl::StartsWith(view, kNamespacePrefix)) {
    std::vector<std::string> parts =
        absl::StrSplit(key, absl::MaxSplits(kNamespaceSep, 1));
    RAY_CHECK(parts.size() == 2) << "Invalid key: " << key;

    return parts[1];
  }
  return std::string(view.begin(), view.end());
}

}  // namespace

StoreClientInternalKV::StoreClientInternalKV(std::unique_ptr<StoreClient> store_client)
    : delegate_(std::move(store_client)),
      table_name_(TablePrefix_Name(TablePrefix::KV)) {}

void StoreClientInternalKV::Get(const std::string &ns,
                                const std::string &key,
                                Dispatchable<void(std::optional<std::string>)> callback) {
  RAY_CHECK_OK(delegate_->AsyncGet(
      table_name_,
      MakeKey(ns, key),
      [callback = std::move(callback)](auto status, auto result) {
        callback.DispatchIfNonNull("StoreClientInternalKV::Get",
                                   result.has_value()
                                       ? std::optional<std::string>(result.value())
                                       : std::optional<std::string>());
      }));
}

void StoreClientInternalKV::MultiGet(
    const std::string &ns,
    const std::vector<std::string> &keys,
    Dispatchable<void(std::unordered_map<std::string, std::string>)> callback) {
  std::vector<std::string> prefixed_keys;
  prefixed_keys.reserve(keys.size());
  for (const auto &key : keys) {
    prefixed_keys.emplace_back(MakeKey(ns, key));
  }
  RAY_CHECK_OK(delegate_->AsyncMultiGet(
      table_name_, prefixed_keys, [callback = std::move(callback)](auto result) {
        std::unordered_map<std::string, std::string> ret;
        for (const auto &item : result) {
          ret.emplace(ExtractKey(item.first), item.second);
        }
        callback.DispatchIfNonNull("StoreClientInternalKV::MultiGet", std::move(ret));
      }));
}

void StoreClientInternalKV::Put(const std::string &ns,
                                const std::string &key,
                                const std::string &value,
                                bool overwrite,
                                Dispatchable<void(bool)> callback) {
  RAY_CHECK_OK(delegate_->AsyncPut(table_name_,
                                   MakeKey(ns, key),
                                   value,
                                   overwrite,
                                   [callback = std::move(callback)](bool success) {
                                     callback.DispatchIfNonNull(
                                         "StoreClientInternalKV::Put", success);
                                   }));
}

void StoreClientInternalKV::Del(const std::string &ns,
                                const std::string &key,
                                bool del_by_prefix,
                                Dispatchable<void(int64_t)> callback) {
  auto dispatch_and_call =
      std::move(callback).AsDispatchedFunction("StoreClientInternalKV::Del");
  if (!del_by_prefix) {
    RAY_CHECK_OK(delegate_->AsyncDelete(
        table_name_,
        MakeKey(ns, key),
        [dispatch_and_call = std::move(dispatch_and_call)](bool deleted) {
          dispatch_and_call(deleted ? 1 : 0);
        }));
    return;
  }

  RAY_CHECK_OK(delegate_->AsyncGetKeys(
      table_name_,
      MakeKey(ns, key),
      [this, ns, dispatch_and_call = std::move(dispatch_and_call)](auto keys) {
        if (keys.empty()) {
          dispatch_and_call(0);
          return;
        }
        RAY_CHECK_OK(
            delegate_->AsyncBatchDelete(table_name_, keys, std::move(dispatch_and_call)));
      }));
}

void StoreClientInternalKV::Exists(const std::string &ns,
                                   const std::string &key,
                                   Dispatchable<void(bool)> callback) {
  RAY_CHECK_OK(delegate_->AsyncExists(
      table_name_,
      MakeKey(ns, key),
      std::move(callback).AsDispatchedFunction("StoreClientInternalKV::Exists")));
}

void StoreClientInternalKV::Keys(const std::string &ns,
                                 const std::string &prefix,
                                 Dispatchable<void(std::vector<std::string>)> callback) {
  RAY_CHECK_OK(delegate_->AsyncGetKeys(
      table_name_,
      MakeKey(ns, prefix),
      [callback = std::move(callback)](std::vector<std::string> keys) {
        std::vector<std::string> true_keys;
        true_keys.reserve(keys.size());
        for (auto &key : keys) {
          true_keys.emplace_back(ExtractKey(key));
        }
        callback.DispatchIfNonNull("StoreClientInternalKV::Keys", std::move(true_keys));
      }));
}

}  // namespace gcs
}  // namespace ray
