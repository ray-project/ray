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
  if (absl::StartsWith(key, kNamespacePrefix)) {
    std::vector<std::string> parts =
        absl::StrSplit(key, absl::MaxSplits(kNamespaceSep, 1));
    RAY_CHECK(parts.size() == 2) << "Invalid key: " << key;

    return parts[1];
  }
  return key;
}

}  // namespace

StoreClientInternalKV::StoreClientInternalKV(std::unique_ptr<StoreClient> store_client,
                                             instrumented_io_context &io_context)
    : delegate_(std::move(store_client)),
      table_name_(TablePrefix_Name(TablePrefix::KV)),
      io_context_(io_context) {}

void StoreClientInternalKV::Get(
    const std::string &ns,
    const std::string &key,
    std::function<void(std::optional<std::string>)> callback) {
  if (!callback) {
    callback = [](auto) {};
  }
  RAY_CHECK_OK(delegate_->AsyncGet(
      table_name_,
      MakeKey(ns, key),
      {[callback = std::move(callback)](auto status, auto result) {
         RAY_CHECK(status.ok()) << "Fails to get key from storage " << status;
         callback(std::move(result));
       },
       io_context_}));
}

void StoreClientInternalKV::MultiGet(
    const std::string &ns,
    const std::vector<std::string> &keys,
    std::function<void(absl::flat_hash_map<std::string, std::string>)> callback) {
  if (!callback) {
    callback = [](auto) {};
  }
  std::vector<std::string> prefixed_keys;
  prefixed_keys.reserve(keys.size());
  for (const auto &key : keys) {
    prefixed_keys.emplace_back(MakeKey(ns, key));
  }
  RAY_CHECK_OK(delegate_->AsyncMultiGet(
      table_name_,
      prefixed_keys,
      {[callback = std::move(callback)](auto result) {
         absl::flat_hash_map<std::string, std::string> ret;
         ret.reserve(result.size());
         for (auto &&item : std::move(result)) {
           ret.emplace(ExtractKey(item.first), std::move(item.second));
         }
         callback(std::move(ret));
       },
       io_context_}));
}

void StoreClientInternalKV::Put(const std::string &ns,
                                const std::string &key,
                                std::string value,
                                bool overwrite,
                                std::function<void(bool)> callback) {
  if (!callback) {
    callback = [](auto) {};
  }
  RAY_CHECK_OK(delegate_->AsyncPut(table_name_,
                                   MakeKey(ns, key),
                                   std::move(value),
                                   overwrite,
                                   {callback, io_context_}));
}

void StoreClientInternalKV::Del(const std::string &ns,
                                const std::string &key,
                                bool del_by_prefix,
                                std::function<void(int64_t)> callback) {
  if (!callback) {
    callback = [](auto) {};
  }
  if (!del_by_prefix) {
    RAY_CHECK_OK(delegate_->AsyncDelete(
        table_name_,
        MakeKey(ns, key),
        {[callback = std::move(callback)](bool deleted) { callback(deleted ? 1 : 0); },
         io_context_}));
    return;
  }

  RAY_CHECK_OK(delegate_->AsyncGetKeys(
      table_name_,
      MakeKey(ns, key),
      {[this, ns, callback = std::move(callback)](auto keys) mutable {
         if (keys.empty()) {
           callback(0);
           return;
         }
         RAY_CHECK_OK(delegate_->AsyncBatchDelete(
             table_name_, keys, {std::move(callback), io_context_}));
       },
       io_context_}));
}

void StoreClientInternalKV::Exists(const std::string &ns,
                                   const std::string &key,
                                   std::function<void(bool)> callback) {
  if (!callback) {
    callback = [](auto) {};
  }

  RAY_CHECK_OK(delegate_->AsyncExists(
      table_name_, MakeKey(ns, key), {std::move(callback), io_context_}));
}

void StoreClientInternalKV::Keys(const std::string &ns,
                                 const std::string &prefix,
                                 std::function<void(std::vector<std::string>)> callback) {
  if (!callback) {
    callback = [](auto) {};
  }
  RAY_CHECK_OK(delegate_->AsyncGetKeys(
      table_name_,
      MakeKey(ns, prefix),
      {[callback = std::move(callback)](std::vector<std::string> keys) {
         std::vector<std::string> true_keys;
         true_keys.reserve(keys.size());
         for (auto &key : keys) {
           true_keys.emplace_back(ExtractKey(key));
         }
         callback(std::move(true_keys));
       },
       io_context_}));
}

}  // namespace gcs
}  // namespace ray
