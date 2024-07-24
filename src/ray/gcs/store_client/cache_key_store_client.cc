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

#include "ray/gcs/store_client/cache_key_store_client.h"

#include <functional>
#include <regex>

#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "ray/gcs/redis_context.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

namespace {

const std::string_view kTableSeparator = ":";
const std::string_view kClusterSeparator = "@";

const int TABLE_COUNT = 8;
const std::string tables[TABLE_COUNT] = {
    TablePrefix_Name(TablePrefix::JOB),
    TablePrefix_Name(TablePrefix::ACTOR),
    TablePrefix_Name(TablePrefix::ACTOR_TASK_SPEC),
    TablePrefix_Name(TablePrefix::PLACEMENT_GROUP),
    TablePrefix_Name(TablePrefix::NODE),
    TablePrefix_Name(TablePrefix::WORKERS),
    TablePrefix_Name(TablePrefix::INTERNAL_CONFIG),
    TablePrefix_Name(TablePrefix::KV),
};

std::string GetKeyFromRedisKey(const std::string &external_storage_namespace,
                               const std::string &redis_key,
                               const std::string &table_name) {
  auto pos = external_storage_namespace.size() + kClusterSeparator.size() +
             table_name.size() + kTableSeparator.size();
  return redis_key.substr(pos, redis_key.size() - pos);
}

}  // namespace

CacheKeyStoreClient::CacheKeyStoreClient(
    std::shared_ptr<RedisStoreClient> redis_store_client)
    : external_storage_namespace_(::RayConfig::instance().external_storage_namespace()),
      redis_store_client_(redis_store_client) {
  RAY_CHECK(!absl::StrContains(external_storage_namespace_, kClusterSeparator))
      << "Storage namespace (" << external_storage_namespace_ << ") shouldn't contain "
      << kClusterSeparator << ".";
  InitializeCache();
}

void CacheKeyStoreClient::InitializeCache(void) {
  std::string prefixs[TABLE_COUNT];
  for (int i = 0; i < TABLE_COUNT; i++) {
    prefixs[i] = absl::StrCat(
        external_storage_namespace_, kClusterSeparator, tables[i], kTableSeparator);
  }

  auto ctx = redis_store_client_->GetRedisClient()->GetPrimaryContext();
  auto cmd = std::vector<std::string>{"HKEYS", external_storage_namespace_};
  auto redis_reply = ctx->RunArgvSync(cmd);
  auto all_redis_keys = redis_reply->ReadAsStringArray();
  for (const auto &redis_key : all_redis_keys) {
    if (redis_key.has_value()) {
      for (int i = 0; i < TABLE_COUNT; i++) {
        if (absl::StartsWith(*redis_key, prefixs[i])) {
          std::string key =
              GetKeyFromRedisKey(external_storage_namespace_, *redis_key, tables[i]);
          table_keys_cache_[tables[i]].insert(key);
        }
      }
    }
  }
}

Status CacheKeyStoreClient::AsyncPut(const std::string &table_name,
                                     const std::string &key,
                                     const std::string &data,
                                     bool overwrite,
                                     std::function<void(bool)> callback) {
  auto put_callback = [this, table_name, key, callback](bool result) {
    if (result) {
      absl::MutexLock lock(&cache_mu_);
      table_keys_cache_[table_name].insert(key);
    }
    callback(result);
  };
  return redis_store_client_->AsyncPut(table_name, key, data, overwrite, put_callback);
}

Status CacheKeyStoreClient::AsyncGet(const std::string &table_name,
                                     const std::string &key,
                                     const OptionalItemCallback<std::string> &callback) {
  return redis_store_client_->AsyncGet(table_name, key, callback);
}

Status CacheKeyStoreClient::AsyncGetAll(
    const std::string &table_name,
    const MapCallback<std::string, std::string> &callback) {
  RAY_CHECK(callback);
  std::vector<std::string> qry_keys;
  {
    absl::MutexLock lock(&cache_mu_);
    for (const auto &key : table_keys_cache_[table_name]) {
      qry_keys.push_back(key);
    }
  }
  return redis_store_client_->AsyncMultiGet(table_name, qry_keys, callback);
}

Status CacheKeyStoreClient::AsyncDelete(const std::string &table_name,
                                        const std::string &key,
                                        std::function<void(bool)> callback) {
  return AsyncBatchDelete(table_name, {key}, [callback](int64_t cnt) {
    if (callback != nullptr) {
      callback(cnt > 0);
    }
  });
}

Status CacheKeyStoreClient::AsyncBatchDelete(const std::string &table_name,
                                             const std::vector<std::string> &keys,
                                             std::function<void(int64_t)> callback) {
  return redis_store_client_->AsyncBatchDelete(
      table_name, keys, [this, table_name, keys, callback](int64_t cnt) {
        if (callback != nullptr) {
          callback(cnt > 0);
        }
        absl::MutexLock lock(&cache_mu_);
        for (const auto &key : keys) {
          table_keys_cache_[table_name].erase(key);
        }
      });
}

Status CacheKeyStoreClient::AsyncMultiGet(
    const std::string &table_name,
    const std::vector<std::string> &keys,
    const MapCallback<std::string, std::string> &callback) {
  return redis_store_client_->AsyncMultiGet(table_name, keys, callback);
}

int CacheKeyStoreClient::GetNextJobID() { return redis_store_client_->GetNextJobID(); }

Status CacheKeyStoreClient::AsyncGetKeys(
    const std::string &table_name,
    const std::string &prefix,
    std::function<void(std::vector<std::string>)> callback) {
  std::vector<std::string> qry_keys;
  {
    absl::MutexLock lock(&cache_mu_);
    for (const auto &key : table_keys_cache_[table_name]) {
      if (absl::StartsWith(key, prefix)) {
        qry_keys.push_back(key);
      }
    }
  }
  auto on_done = [callback](auto redis_result) {
    std::vector<std::string> result;
    result.reserve(redis_result.size());
    for (const auto &[key, _] : redis_result) {
      result.push_back(key);
    }
    callback(std::move(result));
  };
  return redis_store_client_->AsyncMultiGet(table_name, qry_keys, on_done);
}

Status CacheKeyStoreClient::AsyncExists(const std::string &table_name,
                                        const std::string &key,
                                        std::function<void(bool)> callback) {
  return redis_store_client_->AsyncExists(table_name, key, callback);
}

}  // namespace gcs

}  // namespace ray
