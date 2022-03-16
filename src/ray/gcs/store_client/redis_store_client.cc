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

#include "ray/common/ray_config.h"
#include "ray/gcs/redis_context.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

std::string RedisStoreClient::table_separator_ = ":";
std::string RedisStoreClient::index_table_separator_ = "&";

Status RedisStoreClient::AsyncPut(const std::string &table_name,
                                  const std::string &key,
                                  const std::string &data,
                                  const StatusCallback &callback) {
  return DoPut(GenRedisKey(table_name, key), data, callback);
}

Status RedisStoreClient::AsyncPutWithIndex(const std::string &table_name,
                                           const std::string &key,
                                           const std::string &index_key,
                                           const std::string &data,
                                           const StatusCallback &callback) {
  // NOTE: To ensure the atomicity of `AsyncPutWithIndex`, we can't write data to Redis in
  // the callback function of index writing.
  // Write index to Redis.
  const auto &index_table_key = GenRedisKey(table_name, key, index_key);
  RAY_CHECK_OK(DoPut(index_table_key, key, nullptr));

  // Write data to Redis.
  // The operation of redis client is executed in order, and it can ensure that index is
  // written first and then data is written. The index and data are decoupled, so we don't
  // need to write data in the callback function of index writing.
  const auto &status = DoPut(GenRedisKey(table_name, key), data, callback);
  if (!status.ok()) {
    // Run callback if failed.
    if (callback != nullptr) {
      callback(status);
    }
  }
  return status;
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

  std::string redis_key = GenRedisKey(table_name, key);
  std::vector<std::string> args = {"GET", redis_key};

  auto shard_context = redis_client_->GetShardContext(redis_key);
  return shard_context->RunArgvAsync(args, redis_callback);
}

Status RedisStoreClient::AsyncGetAll(
    const std::string &table_name,
    const MapCallback<std::string, std::string> &callback) {
  RAY_CHECK(callback);
  std::string match_pattern = GenRedisMatchPattern(table_name);
  auto scanner = std::make_shared<RedisScanner>(redis_client_, table_name);
  auto on_done = [callback,
                  scanner](absl::flat_hash_map<std::string, std::string> &&result) {
    callback(std::move(result));
  };
  return scanner->ScanKeysAndValues(match_pattern, on_done);
}

Status RedisStoreClient::AsyncDelete(const std::string &table_name,
                                     const std::string &key,
                                     const StatusCallback &callback) {
  RedisCallback delete_callback = nullptr;
  if (callback) {
    delete_callback = [callback](const std::shared_ptr<CallbackReply> &reply) {
      callback(Status::OK());
    };
  }

  std::string redis_key = GenRedisKey(table_name, key);
  // We always replace `DEL` with `UNLINK`.
  std::vector<std::string> args = {"UNLINK", redis_key};

  auto shard_context = redis_client_->GetShardContext(redis_key);
  return shard_context->RunArgvAsync(args, delete_callback);
}

Status RedisStoreClient::AsyncDeleteWithIndex(const std::string &table_name,
                                              const std::string &key,
                                              const std::string &index_key,
                                              const StatusCallback &callback) {
  std::vector<std::string> redis_keys;
  redis_keys.reserve(2);
  redis_keys.push_back(GenRedisKey(table_name, key));
  redis_keys.push_back(GenRedisKey(table_name, key, index_key));

  return DeleteByKeys(redis_keys, callback);
}

Status RedisStoreClient::AsyncBatchDelete(const std::string &table_name,
                                          const std::vector<std::string> &keys,
                                          const StatusCallback &callback) {
  std::vector<std::string> redis_keys;
  redis_keys.reserve(keys.size());
  for (auto &key : keys) {
    redis_keys.push_back(GenRedisKey(table_name, key));
  }
  return DeleteByKeys(redis_keys, callback);
}

Status RedisStoreClient::AsyncBatchDeleteWithIndex(
    const std::string &table_name,
    const std::vector<std::string> &keys,
    const std::vector<std::string> &index_keys,
    const StatusCallback &callback) {
  RAY_CHECK(keys.size() == index_keys.size());

  std::vector<std::string> redis_keys;
  redis_keys.reserve(2 * keys.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    redis_keys.push_back(GenRedisKey(table_name, keys[i]));
    redis_keys.push_back(GenRedisKey(table_name, keys[i], index_keys[i]));
  }

  return DeleteByKeys(redis_keys, callback);
}

Status RedisStoreClient::AsyncGetByIndex(
    const std::string &table_name,
    const std::string &index_key,
    const MapCallback<std::string, std::string> &callback) {
  RAY_CHECK(callback);
  std::string match_pattern = GenRedisMatchPattern(table_name, index_key);
  auto scanner = std::make_shared<RedisScanner>(redis_client_, table_name);
  auto on_done = [this, callback, scanner, table_name, index_key](
                     const Status &status, const std::vector<std::string> &result) {
    if (!result.empty()) {
      std::vector<std::string> keys;
      keys.reserve(result.size());
      for (auto &item : result) {
        keys.push_back(
            GenRedisKey(table_name, GetKeyFromRedisKey(item, table_name, index_key)));
      }

      RAY_CHECK_OK(MGetValues(redis_client_, table_name, keys, callback));
    } else {
      callback(absl::flat_hash_map<std::string, std::string>());
    }
  };
  return scanner->ScanKeys(match_pattern, on_done);
}

Status RedisStoreClient::AsyncDeleteByIndex(const std::string &table_name,
                                            const std::string &index_key,
                                            const StatusCallback &callback) {
  std::string match_pattern = GenRedisMatchPattern(table_name, index_key);
  auto scanner = std::make_shared<RedisScanner>(redis_client_, table_name);
  auto on_done = [this, table_name, index_key, callback, scanner](
                     const Status &status, const std::vector<std::string> &result) {
    if (!result.empty()) {
      std::vector<std::string> keys;
      keys.reserve(result.size());
      for (auto &item : result) {
        keys.push_back(GetKeyFromRedisKey(item, table_name, index_key));
      }
      auto batch_delete_callback = [this, result, callback](const Status &status) {
        RAY_CHECK_OK(status);
        // Delete index keys.
        RAY_CHECK_OK(DeleteByKeys(result, callback));
      };
      RAY_CHECK_OK(AsyncBatchDelete(table_name, keys, batch_delete_callback));
    } else {
      if (callback) {
        callback(status);
      }
    }
  };

  return scanner->ScanKeys(match_pattern, on_done);
}

Status RedisStoreClient::DoPut(const std::string &key,
                               const std::string &data,
                               const StatusCallback &callback) {
  std::vector<std::string> args = {"SET", key, data};
  RedisCallback write_callback = nullptr;
  if (callback) {
    write_callback = [callback](const std::shared_ptr<CallbackReply> &reply) {
      auto status = reply->ReadAsStatus();
      callback(status);
    };
  }

  auto shard_context = redis_client_->GetShardContext(key);
  return shard_context->RunArgvAsync(args, write_callback);
}

Status RedisStoreClient::DeleteByKeys(const std::vector<std::string> &keys,
                                      const StatusCallback &callback) {
  // Delete for each shard.
  // We always replace `DEL` with `UNLINK`.
  int total_count = 0;
  auto del_commands_by_shards =
      GenCommandsByShards(redis_client_, "UNLINK", keys, &total_count);

  auto finished_count = std::make_shared<int>(0);

  for (auto &command_list : del_commands_by_shards) {
    for (auto &command : command_list.second) {
      auto delete_callback = [finished_count, total_count, callback](
                                 const std::shared_ptr<CallbackReply> &reply) {
        ++(*finished_count);
        if (*finished_count == total_count) {
          if (callback) {
            callback(Status::OK());
          }
        }
      };
      RAY_CHECK_OK(command_list.first->RunArgvAsync(command, delete_callback));
    }
  }
  return Status::OK();
}

absl::flat_hash_map<RedisContext *, std::list<std::vector<std::string>>>
RedisStoreClient::GenCommandsByShards(const std::shared_ptr<RedisClient> &redis_client,
                                      const std::string &command,
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
      key_vector->push_back(key);
      (*count)++;
    } else {
      // If the last batch is full, add a new batch.
      if (it->second.back().size() - 1 ==
          RayConfig::instance().maximum_gcs_storage_operation_batch_size()) {
        it->second.emplace_back(std::vector<std::string>());
        it->second.back().push_back(command);
        (*count)++;
      }
      it->second.back().push_back(key);
    }
  }
  return commands_by_shards;
}

std::string RedisStoreClient::GenRedisKey(const std::string &table_name,
                                          const std::string &key) {
  std::stringstream ss;
  ss << table_name << table_separator_ << key;
  return ss.str();
}

std::string RedisStoreClient::GenRedisKey(const std::string &table_name,
                                          const std::string &key,
                                          const std::string &index_key) {
  std::stringstream ss;
  ss << table_name << index_table_separator_ << index_key << index_table_separator_
     << key;
  return ss.str();
}

std::string RedisStoreClient::GenRedisMatchPattern(const std::string &table_name) {
  std::stringstream ss;
  ss << table_name << table_separator_ << "*";
  return ss.str();
}

std::string RedisStoreClient::GenRedisMatchPattern(const std::string &table_name,
                                                   const std::string &index_key) {
  std::stringstream ss;
  ss << table_name << index_table_separator_ << index_key << index_table_separator_
     << "*";
  return ss.str();
}

std::string RedisStoreClient::GetKeyFromRedisKey(const std::string &redis_key,
                                                 const std::string &table_name) {
  auto pos = table_name.size() + table_separator_.size();
  return redis_key.substr(pos, redis_key.size() - pos);
}

std::string RedisStoreClient::GetKeyFromRedisKey(const std::string &redis_key,
                                                 const std::string &table_name,
                                                 const std::string &index_key) {
  auto pos = table_name.size() + index_table_separator_.size() * 2 + index_key.size();
  return redis_key.substr(pos, redis_key.size() - pos);
}

Status RedisStoreClient::MGetValues(
    std::shared_ptr<RedisClient> redis_client,
    const std::string &table_name,
    const std::vector<std::string> &keys,
    const MapCallback<std::string, std::string> &callback) {
  // The `MGET` command for each shard.
  int total_count = 0;
  auto mget_commands_by_shards =
      GenCommandsByShards(redis_client, "MGET", keys, &total_count);
  auto finished_count = std::make_shared<int>(0);
  auto key_value_map = std::make_shared<absl::flat_hash_map<std::string, std::string>>();
  for (auto &command_list : mget_commands_by_shards) {
    for (auto &command : command_list.second) {
      auto mget_keys = std::move(command);
      auto mget_callback =
          [table_name, finished_count, total_count, mget_keys, callback, key_value_map](
              const std::shared_ptr<CallbackReply> &reply) {
            if (!reply->IsNil()) {
              auto value = reply->ReadAsStringArray();
              // The 0 th element of mget_keys is "MGET", so we start from the 1 th
              // element.
              for (size_t index = 0; index < value.size(); ++index) {
                if (value[index].has_value()) {
                  (*key_value_map)[GetKeyFromRedisKey(mget_keys[index + 1], table_name)] =
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

RedisStoreClient::RedisScanner::RedisScanner(std::shared_ptr<RedisClient> redis_client,
                                             const std::string &table_name)
    : table_name_(std::move(table_name)), redis_client_(std::move(redis_client)) {
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
      RAY_CHECK_OK(MGetValues(redis_client_, table_name_, result, callback));
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
    std::vector<std::string> args = {"SCAN",
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

}  // namespace gcs

}  // namespace ray
