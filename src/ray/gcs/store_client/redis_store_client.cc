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

std::list<std::vector<std::string>> GenCommands(const std::string &command,
                                                const std::string &hash_field,
                                                const std::vector<std::string> &keys) {
  std::list<std::vector<std::string>> commands_batch;
  static auto batch_size =
      RayConfig::instance().maximum_gcs_storage_operation_batch_size();
  for (auto &key : keys) {
    if (commands_batch.empty() || commands_batch.back().size() - 2 == batch_size) {
      commands_batch.push_back(std::vector<std::string>{command, hash_field});
    }
    commands_batch.back().push_back(key);
  }
  return commands_batch;
}

Status MGetValues(std::shared_ptr<RedisClient> redis_client,
                  const std::string &external_storage_namespace,
                  const std::string &table_name,
                  const std::vector<std::string> &keys,
                  const MapCallback<std::string, std::string> &callback) {
  // The `MGET` command
  auto mget_commands = GenCommands("HMGET", external_storage_namespace, keys);
  auto finished_count = std::make_shared<size_t>(0);
  auto key_value_map = std::make_shared<absl::flat_hash_map<std::string, std::string>>();
  auto context = redis_client->GetContext();
  for (auto &command : mget_commands) {
    auto mget_callback = [table_name,
                          external_storage_namespace,
                          finished_count,
                          total_count = mget_commands.size(),
                          command,
                          callback,
                          key_value_map](const std::shared_ptr<CallbackReply> &reply) {
      if (!reply->IsNil()) {
        auto value = reply->ReadAsStringArray();
        // The 0th element of command is "HMGET", and the 1st element of command is hashed
        // key. So we start from the 2nd one.
        for (size_t index = 0; index < value.size(); ++index) {
          if (value[index].has_value()) {
            (*key_value_map)[GetKeyFromRedisKey(
                external_storage_namespace, command[index + 2], table_name)] =
                *(value[index]);
          }
        }
      }

      ++(*finished_count);
      if (*finished_count == total_count) {
        callback(std::move(*key_value_map));
      }
    };

    RAY_CHECK_OK(context->RunArgvAsync(std::move(command), std::move(mget_callback)));
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
      result = reply->ReadAsString();
    }
    callback(Status::OK(), std::move(result));
  };

  std::string redis_key = GenRedisKey(external_storage_namespace_, table_name, key);
  std::vector<std::string> args = {"HGET", external_storage_namespace_, redis_key};

  auto context = redis_client_->GetContext();
  return context->RunArgvAsync(args, redis_callback);
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

  auto context = redis_client_->GetContext();
  return context->RunArgvAsync(args, delete_callback);
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
  std::vector<std::string> args = {
      overwrite ? "HSET" : "HSETNX", external_storage_namespace_, key, data};
  RedisCallback write_callback = nullptr;
  if (callback) {
    write_callback =
        [callback = std::move(callback)](const std::shared_ptr<CallbackReply> &reply) {
          auto added_num = reply->ReadAsInteger();
          callback(added_num != 0);
        };
  }

  auto context = redis_client_->GetContext();
  return context->RunArgvAsync(args, write_callback);
}

Status RedisStoreClient::DeleteByKeys(const std::vector<std::string> &keys,
                                      std::function<void(int64_t)> callback) {
  auto del_commands = GenCommands("HDEL", external_storage_namespace_, keys);

  auto finished_count = std::make_shared<size_t>(0);
  auto num_deleted = std::make_shared<int64_t>(0);
  auto delete_callback = [num_deleted,
                          finished_count,
                          total_count = del_commands.size(),
                          callback](const std::shared_ptr<CallbackReply> &reply) {
    (*num_deleted) += reply->ReadAsInteger();
    ++(*finished_count);
    if (*finished_count == total_count) {
      if (callback) {
        callback(*num_deleted);
      }
    }
  };
  auto context = redis_client_->GetContext();
  for (auto &command : del_commands) {
    RAY_CHECK_OK(context->RunArgvAsync(std::move(command), delete_callback));
  }
  return Status::OK();
}

RedisStoreClient::RedisScanner::RedisScanner(
    std::shared_ptr<RedisClient> redis_client,
    const std::string &external_storage_namespace,
    const std::string &table_name)
    : table_name_(table_name),
      external_storage_namespace_(external_storage_namespace),
      redis_client_(std::move(redis_client)) {}

Status RedisStoreClient::RedisScanner::ScanKeysAndValues(
    const std::string &match_pattern,
    const MapCallback<std::string, std::string> &callback) {
  auto on_done = [this, callback](const Status &status) {
    callback(std::move(results_));
  };
  Scan(match_pattern, on_done);
  return Status::OK();
}

void RedisStoreClient::RedisScanner::Scan(const std::string &match_pattern,
                                          const StatusCallback &callback) {
  static size_t batch_count =
      RayConfig::instance().maximum_gcs_storage_operation_batch_size();
  auto context = redis_client_->GetContext();
  auto scan_callback =
      [this, match_pattern, callback](const std::shared_ptr<CallbackReply> &reply) {
        OnScanCallback(match_pattern, reply, callback);
      };
  // Scan by prefix from Redis.
  std::vector<std::string> args = {"HSCAN",
                                   external_storage_namespace_,
                                   std::to_string(cursor_),
                                   "MATCH",
                                   match_pattern,
                                   "COUNT",
                                   std::to_string(batch_count)};
  Status status = context->RunArgvAsync(args, scan_callback);
  if (!status.ok()) {
    RAY_LOG(FATAL) << "Scan failed, status " << status.ToString();
  }
}

void RedisStoreClient::RedisScanner::OnScanCallback(
    const std::string &match_pattern,
    const std::shared_ptr<CallbackReply> &reply,
    const StatusCallback &callback) {
  RAY_CHECK(reply);
  std::vector<std::string> scan_result;
  cursor_ = reply->ReadAsScanArray(&scan_result);

  RAY_CHECK(scan_result.size() % 2 == 0);
  for (size_t i = 0; i < scan_result.size(); i += 2) {
    auto key = GetKeyFromRedisKey(
        external_storage_namespace_, std::move(scan_result[i]), table_name_);
    results_.emplace(std::move(key), std::move(scan_result[i + 1]));
  }
  if (cursor_ != 0) {
    Scan(match_pattern, callback);
  } else {
    callback(Status::OK());
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

  auto on_done = [table_name, callback, scanner](auto redis_result) {
    std::vector<std::string> result;
    result.reserve(redis_result.size());
    for (const auto &[key, _] : redis_result) {
      result.push_back(key);
    }
    callback(std::move(result));
  };
  return scanner->ScanKeysAndValues(match_pattern, on_done);
}

Status RedisStoreClient::AsyncExists(const std::string &table_name,
                                     const std::string &key,
                                     std::function<void(bool)> callback) {
  std::string redis_key = GenRedisKey(external_storage_namespace_, table_name, key);
  std::vector<std::string> args = {"HEXISTS", external_storage_namespace_, redis_key};

  auto context = redis_client_->GetContext();
  RAY_CHECK_OK(context->RunArgvAsync(
      args,
      [callback = std::move(callback)](const std::shared_ptr<CallbackReply> &reply) {
        bool exists = reply->ReadAsInteger() > 0;
        callback(exists);
      }));
  return Status::OK();
}

}  // namespace gcs

}  // namespace ray
