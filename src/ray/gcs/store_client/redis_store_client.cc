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

std::vector<std::vector<std::string>> GenCommandsBatched(
    const std::string &command,
    const std::string &hash_field,
    const std::vector<std::string> &keys) {
  std::vector<std::vector<std::string>> batched_requests;
  for (auto &key : keys) {
    // If it's empty or the last batch is full, add a new batch.
    if (batched_requests.empty() ||
        batched_requests.back().size() >=
            RayConfig::instance().maximum_gcs_storage_operation_batch_size() + 2) {
      batched_requests.emplace_back(std::vector<std::string>{command, hash_field});
    }
    batched_requests.back().push_back(key);
  }
  return batched_requests;
}

std::string GetKeyFromRedisKey(const std::string &external_storage_namespace,
                               const std::string &redis_key,
                               const std::string &table_name) {
  auto pos = external_storage_namespace.size() + kClusterSeparator.size() +
             table_name.size() + kTableSeparator.size();
  return redis_key.substr(pos, redis_key.size() - pos);
}

}  // namespace

void RedisStoreClient::MGetValues(const std::string &table_name,
                                  const std::vector<std::string> &keys,
                                  const MapCallback<std::string, std::string> &callback) {
  // The `MGET` command for each shard.
  auto batched_commands = GenCommandsBatched("HMGET", external_storage_namespace_, keys);
  auto total_count = batched_commands.size();
  auto finished_count = std::make_shared<size_t>(0);
  auto key_value_map = std::make_shared<absl::flat_hash_map<std::string, std::string>>();

  for (auto &command : batched_commands) {
    auto mget_keys = std::move(command);
    std::vector<std::string> partition_keys(mget_keys.begin() + 2, mget_keys.end());
    auto mget_callback = [this,
                          table_name,
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
                external_storage_namespace_, mget_keys[index + 2], table_name)] =
                *(value[index]);
          }
        }
      }

      ++(*finished_count);
      if (*finished_count == total_count) {
        callback(std::move(*key_value_map));
      }
    };
    SendRedisCmd(
        std::move(partition_keys), std::move(mget_keys), std::move(mget_callback));
  }
}

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
    RAY_CHECK(!reply->IsError())
        << "Failed to get from Redis with status: " << reply->ReadAsStatus();
    callback(Status::OK(), std::move(result));
  };

  std::string redis_key = GenRedisKey(external_storage_namespace_, table_name, key);
  std::vector<std::string> args = {"HGET", external_storage_namespace_, redis_key};
  SendRedisCmd({redis_key}, std::move(args), std::move(redis_callback));
  return Status::OK();
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
  return AsyncBatchDelete(table_name, {key}, [callback](int64_t cnt) {
    if (callback != nullptr) {
      callback(cnt > 0);
    }
  });
}

Status RedisStoreClient::AsyncBatchDelete(const std::string &table_name,
                                          const std::vector<std::string> &keys,
                                          std::function<void(int64_t)> callback) {
  if (keys.empty()) {
    if (callback) {
      callback(0);
    }
    return Status::OK();
  }
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
    return Status::OK();
  }
  std::vector<std::string> true_keys;
  for (auto &key : keys) {
    true_keys.push_back(GenRedisKey(external_storage_namespace_, table_name, key));
  }
  MGetValues(table_name, true_keys, callback);
  return Status::OK();
}

size_t RedisStoreClient::PushToSendingQueue(const std::vector<std::string> &keys,
                                            std::function<void()> send_request) {
  size_t queue_added = 0;
  for (const auto &key : keys) {
    auto [op_iter, added] =
        pending_redis_request_by_key_.emplace(key, std::queue<std::function<void()>>());
    if (added) {
      queue_added++;
    }
    if (added) {
      // As an optimization, if there is no in-flight request in this queue, we
      // don't need to store the actual send_request in the queue but just need
      // a placeholder (to indicate there are pending requests). This is because either
      // the send_request will be fired immediately (if all the depending queues are
      // empty). otherwise the send_request in the last queue with pending in-flight
      // requests will be called. In either case, the send_request will not be called in
      // this queue.
      op_iter->second.push(nullptr);
    } else {
      op_iter->second.push(send_request);
    }
  }
  return queue_added;
}

std::vector<std::function<void()>> RedisStoreClient::TakeRequestsFromSendingQueue(
    const std::vector<std::string> &keys) {
  std::vector<std::function<void()>> send_requests;
  for (const auto &key : keys) {
    auto [op_iter, added] =
        pending_redis_request_by_key_.emplace(key, std::queue<std::function<void()>>());
    RAY_CHECK(added == false) << "Pop from a queue doesn't exist: " << key;
    RAY_CHECK(op_iter->second.front() == nullptr);
    op_iter->second.pop();
    if (op_iter->second.empty()) {
      pending_redis_request_by_key_.erase(op_iter);
    } else {
      send_requests.emplace_back(std::move(op_iter->second.front()));
    }
  }
  return send_requests;
}

void RedisStoreClient::SendRedisCmd(std::vector<std::string> keys,
                                    std::vector<std::string> args,
                                    RedisCallback redis_callback) {
  RAY_CHECK(!keys.empty());
  // The number of keys that's ready for this request.
  // For a query reading or writing multiple keys, we need a counter
  // to check whether all existing requests for this keys have been
  // processed.
  auto num_ready_keys = std::make_shared<size_t>(0);
  std::function<void()> send_redis = [this,
                                      num_ready_keys = num_ready_keys,
                                      keys,
                                      args = std::move(args),
                                      redis_callback =
                                          std::move(redis_callback)]() mutable {
    {
      absl::MutexLock lock(&mu_);
      *num_ready_keys += 1;
      RAY_CHECK(*num_ready_keys <= keys.size());
      // There are still pending requests for these keys.
      if (*num_ready_keys != keys.size()) {
        return;
      }
    }
    // Send the actual request
    auto cxt = redis_client_->GetPrimaryContext();
    cxt->RunArgvAsync(std::move(args),
                      [this,
                       keys = std::move(keys),
                       redis_callback = std::move(redis_callback)](auto reply) {
                        std::vector<std::function<void()>> requests;
                        {
                          absl::MutexLock lock(&mu_);
                          requests = TakeRequestsFromSendingQueue(keys);
                        }
                        for (auto &request : requests) {
                          request();
                        }
                        if (redis_callback) {
                          redis_callback(reply);
                        }
                      });
  };

  {
    absl::MutexLock lock(&mu_);
    auto keys_ready = PushToSendingQueue(keys, send_redis);
    *num_ready_keys += keys_ready;
    // If all queues are empty for each key this request depends on
    // we are safe to fire the request immediately.
    if (*num_ready_keys == keys.size()) {
      *num_ready_keys = keys.size() - 1;
    } else {
      send_redis = nullptr;
    }
  }
  if (send_redis) {
    send_redis();
  }
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
  SendRedisCmd({key}, std::move(args), std::move(write_callback));
  return Status::OK();
}

Status RedisStoreClient::DeleteByKeys(const std::vector<std::string> &keys,
                                      std::function<void(int64_t)> callback) {
  auto del_cmds = GenCommandsBatched("HDEL", external_storage_namespace_, keys);
  auto total_count = del_cmds.size();
  auto finished_count = std::make_shared<size_t>(0);
  auto num_deleted = std::make_shared<int64_t>(0);
  auto context = redis_client_->GetPrimaryContext();
  for (auto &command : del_cmds) {
    std::vector<std::string> partition_keys(command.begin() + 2, command.end());
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
    SendRedisCmd(
        std::move(partition_keys), std::move(command), std::move(delete_callback));
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
  cursor_ = 0;
}

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
  // This lock guards cursor_ because the callbacks
  // can modify cursor_. If performance is a concern,
  // we should consider using a reader-writer lock.
  absl::MutexLock lock(&mutex_);
  if (!cursor_.has_value()) {
    callback(Status::OK());
    return;
  }

  size_t batch_count = RayConfig::instance().maximum_gcs_storage_operation_batch_size();
  ++pending_request_count_;

  auto scan_callback =
      [this, match_pattern, callback](const std::shared_ptr<CallbackReply> &reply) {
        OnScanCallback(match_pattern, reply, callback);
      };
  // Scan by prefix from Redis.
  std::vector<std::string> args = {"HSCAN",
                                   external_storage_namespace_,
                                   std::to_string(cursor_.value()),
                                   "MATCH",
                                   match_pattern,
                                   "COUNT",
                                   std::to_string(batch_count)};
  auto primary_context = redis_client_->GetPrimaryContext();
  primary_context->RunArgvAsync(args, scan_callback);
}

void RedisStoreClient::RedisScanner::OnScanCallback(
    const std::string &match_pattern,
    const std::shared_ptr<CallbackReply> &reply,
    const StatusCallback &callback) {
  RAY_CHECK(reply);
  std::vector<std::string> scan_result;
  size_t cursor = reply->ReadAsScanArray(&scan_result);
  // Update cursor and results_.
  {
    absl::MutexLock lock(&mutex_);
    // If cursor is equal to 0, it means that the scan is finished, so we
    // reset cursor_.
    if (cursor == 0) {
      cursor_.reset();
    } else {
      cursor_ = cursor;
    }
    RAY_CHECK(scan_result.size() % 2 == 0);
    for (size_t i = 0; i < scan_result.size(); i += 2) {
      auto key = GetKeyFromRedisKey(
          external_storage_namespace_, std::move(scan_result[i]), table_name_);
      results_.emplace(std::move(key), std::move(scan_result[i + 1]));
    }
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
  SendRedisCmd(
      {redis_key},
      std::move(args),
      [callback = std::move(callback)](const std::shared_ptr<CallbackReply> &reply) {
        bool exists = reply->ReadAsInteger() > 0;
        callback(exists);
      });
  return Status::OK();
}

}  // namespace gcs

}  // namespace ray
