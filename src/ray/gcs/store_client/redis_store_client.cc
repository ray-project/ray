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

#include "ray/gcs/store_client/redis_store_client.h"

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

// "[, ], -, ?, *, ^, \" are special chars in Redis pattern matching.
// escape them with / according to the doc:
// https://redis.io/commands/keys/
std::string EscapeMatchPattern(const std::string &s) {
  static std::regex kSpecialChars("\\[|\\]|-|\\?|\\*|\\^|\\\\");
  return std::regex_replace(s, kSpecialChars, "\\$&");
}

std::string GenRedisKey(const std::string &external_storage_namespace,
                        const std::string &table_name,
                        const std::string &key) {
  return absl::StrCat(
      external_storage_namespace, kClusterSeparator, table_name, kTableSeparator, key);
}

std::string GenKeyRedisMatchPattern(const std::string &external_storage_namespace,
                                    const std::string &table_name) {
  return absl::StrCat(EscapeMatchPattern(external_storage_namespace),
                      kClusterSeparator,
                      EscapeMatchPattern(table_name),
                      kTableSeparator,
                      "*");
}

std::string GenKeyRedisMatchPattern(const std::string &external_storage_namespace,
                                    const std::string &table_name,
                                    const std::string &key) {
  return absl::StrCat(EscapeMatchPattern(external_storage_namespace),
                      kClusterSeparator,
                      EscapeMatchPattern(table_name),
                      kTableSeparator,
                      EscapeMatchPattern(key),
                      "*");
}

std::string GetKeyFromRedisKey(const std::string &external_storage_namespace,
                               const std::string &redis_key,
                               const std::string &table_name) {
  auto pos = external_storage_namespace.size() + kClusterSeparator.size() +
             table_name.size() + kTableSeparator.size();
  return redis_key.substr(pos, redis_key.size() - pos);
}

absl::flat_hash_map<RedisContext *, std::list<std::vector<std::string>>>
GenCommandsByShards(const std::shared_ptr<RedisClient> &redis_client,
                    const std::string &command,
                    const std::string &hash_field,
                    const std::vector<std::string> &keys,
                    int *count) {
  absl::flat_hash_map<RedisContext *, std::list<std::vector<std::string>>>
      commands_by_shards;
  for (auto &key : keys) {
    auto shard_context = redis_client->GetShardContext(key).get();
    auto it = commands_by_shards.find(shard_context);
    if (it == commands_by_shards.end()) {
      auto key_vector = commands_by_shards[shard_context].emplace(
          commands_by_shards[shard_context].begin(), std::vector<std::string>());
      key_vector->push_back(command);
      key_vector->push_back(hash_field);
      key_vector->push_back(key);
      (*count)++;
    } else {
      // If the last batch is full, add a new batch.
      if (it->second.back().size() - 1 ==
          RayConfig::instance().maximum_gcs_storage_operation_batch_size()) {
        it->second.emplace_back(std::vector<std::string>());
        it->second.back().push_back(command);
        it->second.back().push_back(hash_field);
        (*count)++;
      }
      it->second.back().push_back(key);
    }
  }
  return commands_by_shards;
}

Status MGetValues(std::shared_ptr<RedisClient> redis_client,
                  const std::string &external_storage_namespace,
                  const std::string &table_name,
                  const std::vector<std::string> &keys,
                  const MapCallback<std::string, std::string> &callback) {
  // The `MGET` command for each shard.
  int total_count = 0;
  auto mget_commands_by_shards =
      GenCommandsByShards(redis_client, "HMGET", external_storage_namespace, keys, &total_count);
  auto finished_count = std::make_shared<int>(0);
  auto key_value_map = std::make_shared<absl::flat_hash_map<std::string, std::string>>();
  for (auto &command_list : mget_commands_by_shards) {
    for (auto &command : command_list.second) {
      auto mget_keys = std::move(command);
      auto mget_callback = [table_name,
                            external_storage_namespace,
                            finished_count,
                            total_count,
                            mget_keys,
                            callback,
                            key_value_map](const std::shared_ptr<CallbackReply> &reply) {
        if (!reply->IsNil()) {
          auto value = reply->ReadAsStringArray();
          // The 0 th element of mget_keys is "MGET", so we start from the 1 th
          // element.
          for (size_t index = 0; index < value.size(); ++index) {
            if (value[index].has_value()) {
              (*key_value_map)[GetKeyFromRedisKey(
                  external_storage_namespace, mget_keys[index + 1], table_name)] =
                  *(value[index]);
            }
          }
        }

        ++(*finished_count);
        if (*finished_count == total_count) {
          callback(std::move(*key_value_map));
        }
      };
      RAY_CHECK_OK(command_list.first->RunArgvAsync(mget_keys, mget_callback));
    }
  }
  return Status::OK();
}

}  // namespace

RedisStoreClient::RedisStoreClient(std::shared_ptr<RedisClient> redis_client)
    : external_storage_namespace_(::RayConfig::instance().external_storage_namespace()),
      redis_client_(std::move(redis_client)) {
  RAY_CHECK(!absl::StrContains(external_storage_namespace_, kClusterSeparator))
      << "Storage namespace (" << external_storage_namespace_ << ") shouldn't contain "
      << kClusterSeparator << ".";
}

Status RedisStoreClient::AsyncPut(const std::string &table_name,
                                  const std::string &key,
                                  const std::string &data,
                                  bool overwrite,
                                  std::function<void(bool)> callback) {
  return DoPut(GenRedisKey(external_storage_namespace_, table_name, key),
               data,
               overwrite,
               callback);
}

Status RedisStoreClient::AsyncGet(const std::string &table_name,
                                  const std::string &key,
                                  const OptionalItemCallback<std::string> &callback) {
  RAY_CHECK(callback != nullptr);

  auto redis_callback = [callback](const std::shared_ptr<CallbackReply> &reply) {
    boost::optional<std::string> result;
    if (!reply->IsNil()) {
      std::string data = reply->ReadAsString();
      if (!data.empty()) {
        result = std::move(data);
      }
    }
    callback(Status::OK(), result);
  };

  std::string redis_key = GenRedisKey(external_storage_namespace_, table_name, key);
  std::vector<std::string> args = {"HGET", external_storage_namespace_, redis_key};

  auto shard_context = redis_client_->GetShardContext(redis_key);
  return shard_context->RunArgvAsync(args, redis_callback);
}

Status RedisStoreClient::AsyncGetAll(
    const std::string &table_name,
    const MapCallback<std::string, std::string> &callback) {
  RAY_CHECK(callback);
  std::string match_pattern =
      GenKeyRedisMatchPattern(external_storage_namespace_, table_name);
  auto scanner = std::make_shared<RedisScanner>(
      redis_client_, external_storage_namespace_, table_name);
  auto on_done = [callback,
                  scanner](absl::flat_hash_map<std::string, std::string> &&result) {
    callback(std::move(result));
  };
  return scanner->ScanKeysAndValues(match_pattern, on_done);
}

Status RedisStoreClient::AsyncDelete(const std::string &table_name,
                                     const std::string &key,
                                     std::function<void(bool)> callback) {
  RedisCallback delete_callback = nullptr;
  if (callback) {
    delete_callback = [callback](const std::shared_ptr<CallbackReply> &reply) {
      callback(reply->ReadAsInteger() == 1);
    };
  }

  std::string redis_key = GenRedisKey(external_storage_namespace_, table_name, key);
  // We always replace `DEL` with `UNLINK`.
  std::vector<std::string> args = {"HDEL", external_storage_namespace_, redis_key};

  auto shard_context = redis_client_->GetShardContext(redis_key);
  return shard_context->RunArgvAsync(args, delete_callback);
}

Status RedisStoreClient::AsyncBatchDelete(const std::string &table_name,
                                          const std::vector<std::string> &keys,
                                          std::function<void(int64_t)> callback) {
  std::vector<std::string> redis_keys;
  redis_keys.reserve(keys.size());
  for (auto &key : keys) {
    redis_keys.push_back(GenRedisKey(external_storage_namespace_, table_name, key));
  }
  return DeleteByKeys(redis_keys, callback);
}

Status RedisStoreClient::AsyncMultiGet(
    const std::string &table_name,
    const std::vector<std::string> &keys,
    const MapCallback<std::string, std::string> &callback) {
  RAY_CHECK(callback);
  if (keys.empty()) {
    callback({});
  }
  std::vector<std::string> true_keys;
  for (auto &key : keys) {
    true_keys.push_back(GenRedisKey(external_storage_namespace_, table_name, key));
  }
  RAY_CHECK_OK(MGetValues(
      redis_client_, external_storage_namespace_, table_name, true_keys, callback));
  return Status::OK();
}

Status RedisStoreClient::DoPut(const std::string &key,
                               const std::string &data,
                               bool overwrite,
                               std::function<void(bool)> callback) {
  std::vector<std::string> args = {overwrite ? "HSET" : "HSETNX", external_storage_namespace_, key, data};
  RedisCallback write_callback = nullptr;
  if (callback) {
    write_callback = [callback = std::move(callback),
                      overwrite](const std::shared_ptr<CallbackReply> &reply) {
      auto added_num = reply->ReadAsInteger();
      callback(added_num != 0);
    };
  }

  auto shard_context = redis_client_->GetShardContext(key);
  return shard_context->RunArgvAsync(args, write_callback);
}

Status RedisStoreClient::DeleteByKeys(const std::vector<std::string> &keys,
                                      std::function<void(int64_t)> callback) {
  // Delete for each shard.
  // We always replace `DEL` with `UNLINK`.
  int total_count = 0;
  auto del_commands_by_shards =
      GenCommandsByShards(redis_client_, "HDEL", external_storage_namespace_, keys, &total_count);

  auto finished_count = std::make_shared<int>(0);
  auto num_deleted = std::make_shared<int64_t>(0);

  for (auto &command_list : del_commands_by_shards) {
    for (auto &command : command_list.second) {
      auto delete_callback = [num_deleted, finished_count, total_count, callback](
                                 const std::shared_ptr<CallbackReply> &reply) {
        (*num_deleted) += reply->ReadAsInteger();
        ++(*finished_count);
        if (*finished_count == total_count) {
          if (callback) {
            callback(*num_deleted);
          }
        }
      };
      RAY_CHECK_OK(command_list.first->RunArgvAsync(command, delete_callback));
    }
  }
  return Status::OK();
}

RedisStoreClient::RedisScanner::RedisScanner(
    std::shared_ptr<RedisClient> redis_client,
    const std::string &external_storage_namespace,
    const std::string &table_name)
    : table_name_(table_name),
      external_storage_namespace_(external_storage_namespace),
      redis_client_(std::move(redis_client)) {
  for (size_t index = 0; index < redis_client_->GetShardContexts().size(); ++index) {
    shard_to_cursor_[index] = 0;
  }
}

Status RedisStoreClient::RedisScanner::ScanKeysAndValues(
    const std::string &match_pattern,
    const MapCallback<std::string, std::string> &callback) {
  auto on_done = [this, callback](const Status &status,
                                  const std::vector<std::string> &result) {
    if (result.empty()) {
      callback(absl::flat_hash_map<std::string, std::string>());
    } else {
      RAY_CHECK_OK(MGetValues(
          redis_client_, external_storage_namespace_, table_name_, result, callback));
    }
  };
  return ScanKeys(match_pattern, on_done);
}

Status RedisStoreClient::RedisScanner::ScanKeys(
    const std::string &match_pattern, const MultiItemCallback<std::string> &callback) {
  auto on_done = [this, callback](const Status &status) {
    std::vector<std::string> result;
    result.insert(result.begin(), keys_.begin(), keys_.end());
    callback(status, std::move(result));
  };
  Scan(match_pattern, on_done);
  return Status::OK();
}

void RedisStoreClient::RedisScanner::Scan(const std::string &match_pattern,
                                          const StatusCallback &callback) {
  // This lock guards the iterator over shard_to_cursor_ because the callbacks
  // can remove items from the shard_to_cursor_ map. If performance is a concern,
  // we should consider using a reader-writer lock.
  absl::MutexLock lock(&mutex_);
  if (shard_to_cursor_.empty()) {
    callback(Status::OK());
    return;
  }

  size_t batch_count = RayConfig::instance().maximum_gcs_storage_operation_batch_size();
  for (const auto &item : shard_to_cursor_) {
    ++pending_request_count_;

    size_t shard_index = item.first;
    size_t cursor = item.second;

    auto scan_callback = [this, match_pattern, shard_index, callback](
                             const std::shared_ptr<CallbackReply> &reply) {
      OnScanCallback(match_pattern, shard_index, reply, callback);
    };
    // Scan by prefix from Redis.
    std::vector<std::string> args = {
      "HSCAN",
      external_storage_namespace_,
      std::to_string(cursor),
      "MATCH",
      match_pattern,
      "COUNT",
      std::to_string(batch_count)};
    auto shard_context = redis_client_->GetShardContexts()[shard_index];
    Status status = shard_context->RunArgvAsync(args, scan_callback);
    if (!status.ok()) {
      RAY_LOG(FATAL) << "Scan failed, status " << status.ToString();
    }
  }
}

void RedisStoreClient::RedisScanner::OnScanCallback(
    const std::string &match_pattern,
    size_t shard_index,
    const std::shared_ptr<CallbackReply> &reply,
    const StatusCallback &callback) {
  RAY_CHECK(reply);
  std::vector<std::string> scan_result;
  size_t cursor = reply->ReadAsScanArray(&scan_result);
  // Update shard cursors and keys_.
  {
    absl::MutexLock lock(&mutex_);
    auto shard_it = shard_to_cursor_.find(shard_index);
    RAY_CHECK(shard_it != shard_to_cursor_.end());
    // If cursor is equal to 0, it means that the scan of this shard is finished, so we
    // erase it from shard_to_cursor_.
    if (cursor == 0) {
      shard_to_cursor_.erase(shard_it);
    } else {
      shard_it->second = cursor;
    }

    keys_.insert(scan_result.begin(), scan_result.end());
  }

  // If pending_request_count_ is equal to 0, it means that the scan of this batch is
  // completed and the next batch is started if any.
  if (--pending_request_count_ == 0) {
    Scan(match_pattern, callback);
  }
}

int RedisStoreClient::GetNextJobID() { return redis_client_->GetNextJobID(); }

Status RedisStoreClient::AsyncGetKeys(
    const std::string &table_name,
    const std::string &prefix,
    std::function<void(std::vector<std::string>)> callback) {
  std::string match_pattern =
      GenKeyRedisMatchPattern(external_storage_namespace_, table_name, prefix);
  auto scanner = std::make_shared<RedisScanner>(
      redis_client_, external_storage_namespace_, table_name);
  auto on_done = [this, table_name, callback, scanner](auto /* status*/, auto keys) {
    std::vector<std::string> result;
    result.reserve(keys.size());
    for (auto &key : keys) {
      result.push_back(
          GetKeyFromRedisKey(external_storage_namespace_, std::move(key), table_name));
    }
    callback(std::move(result));
  };
  return scanner->ScanKeys(match_pattern, on_done);
}

Status RedisStoreClient::AsyncExists(const std::string &table_name,
                                     const std::string &key,
                                     std::function<void(bool)> callback) {
  std::string redis_key = GenRedisKey(external_storage_namespace_, table_name, key);
  std::vector<std::string> args = {"HEXISTS", external_storage_namespace_, redis_key};

  auto shard_context = redis_client_->GetShardContext(redis_key);
  RAY_CHECK_OK(shard_context->RunArgvAsync(
      args,
      [callback = std::move(callback)](const std::shared_ptr<CallbackReply> &reply) {
        bool exists = reply->ReadAsInteger() > 0;
        callback(exists);
      }));
  return Status::OK();
}

}  // namespace gcs

}  // namespace ray
