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

#include "ray/gcs/store_client_kv.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

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
    std::vector<std::string_view> parts =
        absl::StrSplit(key, absl::MaxSplits(kNamespaceSep, 1));
    RAY_CHECK(parts.size() == 2) << "Invalid key: " << key;

    return std::string{parts[1]};
  }
  return key;
}

}  // namespace

StoreClientInternalKV::StoreClientInternalKV(std::unique_ptr<StoreClient> store_client)
    : delegate_(std::move(store_client)),
      table_name_(TablePrefix_Name(rpc::TablePrefix::KV)) {}

void StoreClientInternalKV::Get(const std::string &ns,
                                const std::string &key,
                                Postable<void(std::optional<std::string>)> callback) {
  delegate_->AsyncGet(
      table_name_,
      MakeKey(ns, key),
      std::move(callback).TransformArg(
          [](ray::Status status,
             std::optional<std::string> result) -> std::optional<std::string> {
            RAY_CHECK(status.ok()) << "Fails to get key from storage " << status;
            return result;
          }));
}

void StoreClientInternalKV::MultiGet(
    const std::string &ns,
    const std::vector<std::string> &keys,
    Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) {
  std::vector<std::string> prefixed_keys;
  prefixed_keys.reserve(keys.size());
  for (const auto &key : keys) {
    prefixed_keys.emplace_back(MakeKey(ns, key));
  }
  delegate_->AsyncMultiGet(
      table_name_,
      prefixed_keys,
      std::move(callback).TransformArg(
          [](absl::flat_hash_map<std::string, std::string> before_extract) {
            absl::flat_hash_map<std::string, std::string> ret;
            ret.reserve(before_extract.size());
            for (auto &&item : std::move(before_extract)) {
              ret.emplace(ExtractKey(item.first), std::move(item.second));
            }
            return ret;
          }));
}

void StoreClientInternalKV::Put(const std::string &ns,
                                const std::string &key,
                                std::string value,
                                bool overwrite,
                                Postable<void(bool)> callback) {
  delegate_->AsyncPut(
      table_name_, MakeKey(ns, key), std::move(value), overwrite, std::move(callback));
}

void StoreClientInternalKV::Del(const std::string &ns,
                                const std::string &key,
                                bool del_by_prefix,
                                Postable<void(int64_t)> callback) {
  if (!del_by_prefix) {
    delegate_->AsyncDelete(table_name_,
                           MakeKey(ns, key),
                           std::move(callback).TransformArg(
                               [](bool deleted) -> int64_t { return deleted ? 1 : 0; }));
    return;
  }

  instrumented_io_context &io_context = callback.io_context();

  delegate_->AsyncGetKeys(
      table_name_,
      MakeKey(ns, key),
      {[this, ns, callback = std::move(callback)](auto keys) mutable {
         if (keys.empty()) {
           std::move(callback).Dispatch("StoreClientInternalKV.Del", 0);
           return;
         }
         delegate_->AsyncBatchDelete(table_name_, keys, std::move(callback));
       },
       io_context});
}

void StoreClientInternalKV::Exists(const std::string &ns,
                                   const std::string &key,
                                   Postable<void(bool)> callback) {
  delegate_->AsyncExists(table_name_, MakeKey(ns, key), std::move(callback));
}

void StoreClientInternalKV::Keys(const std::string &ns,
                                 const std::string &prefix,
                                 Postable<void(std::vector<std::string>)> callback) {
  delegate_->AsyncGetKeys(
      table_name_,
      MakeKey(ns, prefix),
      std::move(callback).TransformArg([](std::vector<std::string> keys) {
        std::vector<std::string> true_keys;
        true_keys.reserve(keys.size());
        for (auto &key : keys) {
          true_keys.emplace_back(ExtractKey(key));
        }
        return true_keys;
      }));
}

}  // namespace gcs
}  // namespace ray
