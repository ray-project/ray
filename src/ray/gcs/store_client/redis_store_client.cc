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
#include <thread>
#include <utility>

#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "ray/gcs/redis_context.h"
#include "ray/util/logging.h"

namespace ray {
namespace gcs {

namespace {

const std::string_view kClusterSeparator = "@";

// "[, ], -, ?, *, ^, \" are special chars in Redis pattern matching.
// escape them with / according to the doc:
// https://redis.io/commands/keys/
std::string EscapeMatchPattern(const std::string &s) {
  static std::regex kSpecialChars(R"(\[|\]|-|\?|\*|\^|\\)");
  return std::regex_replace(s, kSpecialChars, "\\$&");
}

// Assume `command` can take arbitary number of keys. Chunk the args into multiple
// commands with the same command name and the same redis_key. Each chunk has at most
// `maximum_gcs_storage_operation_batch_size` keys.
std::vector<RedisCommand> GenCommandsBatched(const std::string &command,
                                             const RedisKey &redis_key,
                                             const std::vector<std::string> &args) {
  std::vector<RedisCommand> batched_requests;
  for (auto &arg : args) {
    // If it's empty or the last batch is full, add a new batch.
    if (batched_requests.empty() ||
        batched_requests.back().args.size() >=
            RayConfig::instance().maximum_gcs_storage_operation_batch_size()) {
      batched_requests.emplace_back(RedisCommand{command, redis_key, {}});
    }
    batched_requests.back().args.push_back(arg);
  }
  return batched_requests;
}

}  // namespace

RedisKey::RedisKey(absl::string_view external_storage_namespace,
                   absl::string_view table_name)
    : redis_key(absl::StrCat(
          "RAY", external_storage_namespace, kClusterSeparator, table_name)) {}

RedisMatchPattern RedisMatchPattern::Prefix(const std::string &prefix) {
  return RedisMatchPattern(absl::StrCat(EscapeMatchPattern(prefix), "*"));
}

void RedisStoreClient::MGetValues(const std::string &table_name,
                                  const std::vector<std::string> &keys,
                                  const MapCallback<std::string, std::string> &callback) {
  // The `HMGET` command for each shard.
  auto batched_commands = GenCommandsBatched(
      "HMGET", RedisKey(external_storage_namespace_, table_name), keys);
  auto total_count = batched_commands.size();
  auto finished_count = std::make_shared<size_t>(0);
  auto key_value_map = std::make_shared<absl::flat_hash_map<std::string, std::string>>();

  for (auto &command : batched_commands) {
    auto mget_callback = [finished_count,
                          total_count,
                          // Copies!
                          args = command.args,
                          callback,
                          key_value_map](const std::shared_ptr<CallbackReply> &reply) {
      if (!reply->IsNil()) {
        auto value = reply->ReadAsStringArray();
        for (size_t index = 0; index < value.size(); ++index) {
          if (value[index].has_value()) {
            (*key_value_map)[args[index]] = *(value[index]);
          }
        }
      }

      ++(*finished_count);
      if (*finished_count == total_count) {
        callback(std::move(*key_value_map));
      }
    };
    SendRedisCmd(command.args, std::move(command), std::move(mget_callback));
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
  RedisCommand command{.command = overwrite ? "HSET" : "HSETNX",
                       .redis_key = RedisKey(external_storage_namespace_, table_name),
                       .args = {key, data}};
  RedisCallback write_callback = nullptr;
  if (callback) {
    write_callback =
        [callback = std::move(callback)](const std::shared_ptr<CallbackReply> &reply) {
          auto added_num = reply->ReadAsInteger();
          callback(added_num != 0);
        };
  }
  SendRedisCmd({key}, std::move(command), std::move(write_callback));
  return Status::OK();
}

Status RedisStoreClient::AsyncGet(const std::string &table_name,
                                  const std::string &key,
                                  const OptionalItemCallback<std::string> &callback) {
  RAY_CHECK(callback != nullptr);

  auto redis_callback = [callback](const std::shared_ptr<CallbackReply> &reply) {
    std::optional<std::string> result;
    if (!reply->IsNil()) {
      result = reply->ReadAsString();
    }
    RAY_CHECK(!reply->IsError())
        << "Failed to get from Redis with status: " << reply->ReadAsStatus();
    callback(Status::OK(), result);
  };

  RedisCommand command{.command = "HGET",
                       .redis_key = RedisKey(external_storage_namespace_, table_name),
                       .args = {key}};
  SendRedisCmd({key}, std::move(command), std::move(redis_callback));
  return Status::OK();
}

Status RedisStoreClient::AsyncGetAll(
    const std::string &table_name,
    const MapCallback<std::string, std::string> &callback) {
  RAY_CHECK(callback);
  RedisScanner::ScanKeysAndValues(redis_client_,
                                  RedisKey(external_storage_namespace_, table_name),
                                  RedisMatchPattern::Any(),
                                  callback);
  return Status::OK();
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
  return DeleteByKeys(table_name, keys, callback);
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
  MGetValues(table_name, keys, callback);
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
      op_iter->second.push(std::move(send_request));
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
                                    RedisCommand command,
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
                                      command = std::move(command),
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
    cxt->RunArgvAsync(command.ToRedisArgs(),
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

Status RedisStoreClient::DeleteByKeys(const std::string &table,
                                      const std::vector<std::string> &keys,
                                      std::function<void(int64_t)> callback) {
  auto del_cmds =
      GenCommandsBatched("HDEL", RedisKey(external_storage_namespace_, table), keys);
  auto total_count = del_cmds.size();
  auto finished_count = std::make_shared<size_t>(0);
  auto num_deleted = std::make_shared<int64_t>(0);
  auto context = redis_client_->GetPrimaryContext();
  for (auto &command : del_cmds) {
    auto delete_callback =
        [num_deleted, finished_count, total_count, callback = std::move(callback)](
            const std::shared_ptr<CallbackReply> &reply) {
          (*num_deleted) += reply->ReadAsInteger();
          ++(*finished_count);
          if (*finished_count == total_count) {
            if (callback) {
              callback(*num_deleted);
            }
          }
        };
    SendRedisCmd(command.args, std::move(command), std::move(delete_callback));
  }
  return Status::OK();
}

RedisStoreClient::RedisScanner::RedisScanner(
    PrivateCtorTag ctor_tag,
    std::shared_ptr<RedisClient> redis_client,
    RedisKey redis_key,
    RedisMatchPattern match_pattern,
    MapCallback<std::string, std::string> callback)
    : redis_key_(std::move(redis_key)),
      match_pattern_(std::move(match_pattern)),
      redis_client_(std::move(redis_client)),
      callback_(std::move(callback)) {
  cursor_ = 0;
  pending_request_count_ = 0;
}

void RedisStoreClient::RedisScanner::ScanKeysAndValues(
    std::shared_ptr<RedisClient> redis_client,
    RedisKey redis_key,
    RedisMatchPattern match_pattern,
    MapCallback<std::string, std::string> callback) {
  auto scanner = std::make_shared<RedisScanner>(PrivateCtorTag(),
                                                std::move(redis_client),
                                                std::move(redis_key),
                                                std::move(match_pattern),
                                                std::move(callback));
  scanner->Scan();
}

void RedisStoreClient::RedisScanner::Scan() {
  // This lock guards cursor_ because the callbacks
  // can modify cursor_. If performance is a concern,
  // we should consider using a reader-writer lock.
  absl::MutexLock lock(&mutex_);
  if (!cursor_.has_value()) {
    callback_(std::move(results_));
  }

  size_t batch_count = RayConfig::instance().maximum_gcs_storage_operation_batch_size();
  ++pending_request_count_;

  // Scan by prefix from Redis.
  RedisCommand command = {"HSCAN", redis_key_, {std::to_string(cursor_.value())}};
  if (match_pattern_.escaped != "*") {
    command.args.push_back("MATCH");
    command.args.push_back(match_pattern_.escaped);
  }
  command.args.push_back("COUNT");
  command.args.push_back(std::to_string(batch_count));
  auto primary_context = redis_client_->GetPrimaryContext();
  primary_context->RunArgvAsync(
      command.ToRedisArgs(),
      [this,
       // keeps myself from being destructed waiting for async.
       shared_this = shared_from_this()](const std::shared_ptr<CallbackReply> &reply) {
        OnScanCallback(reply);
      });
}

void RedisStoreClient::RedisScanner::OnScanCallback(
    const std::shared_ptr<CallbackReply> &reply) {
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
    // Result is an array of key-value pairs.
    // scan_result[i] = key, scan_result[i+1] = value
    // Example req: HSCAN hash_with_cluster_id_for_Jobs
    // scan_result = job1 job1_value job2 job2_value
    RAY_CHECK(scan_result.size() % 2 == 0);
    for (size_t i = 0; i < scan_result.size(); i += 2) {
      results_.emplace(std::move(scan_result[i]), std::move(scan_result[i + 1]));
    }
  }

  // If pending_request_count_ is equal to 0, it means that the scan of this batch is
  // completed and the next batch is started if any.
  if (--pending_request_count_ == 0) {
    Scan();
  }
}

int RedisStoreClient::GetNextJobID() {
  // Note: This is not a HASH! It's a simple key-value pair.
  // Key: "RAYexternal_storage_namespace@JobCounter"
  // Value: The next job ID.
  RedisCommand command = {
      "INCRBY", RedisKey(external_storage_namespace_, "JobCounter"), {"1"}};

  auto cxt = redis_client_->GetPrimaryContext();
  auto reply = cxt->RunArgvSync(command.ToRedisArgs());
  return static_cast<int>(reply->ReadAsInteger());
}

Status RedisStoreClient::AsyncGetKeys(
    const std::string &table_name,
    const std::string &prefix,
    std::function<void(std::vector<std::string>)> callback) {
  RedisScanner::ScanKeysAndValues(
      redis_client_,
      RedisKey(external_storage_namespace_, table_name),
      RedisMatchPattern::Prefix(prefix),
      [callback](absl::flat_hash_map<std::string, std::string> &&result) {
        std::vector<std::string> keys;
        keys.reserve(result.size());
        for (const auto &[k, v] : result) {
          keys.push_back(k);
        }
        callback(std::move(keys));
      });
  return Status::OK();
}

Status RedisStoreClient::AsyncExists(const std::string &table_name,
                                     const std::string &key,
                                     std::function<void(bool)> callback) {
  RedisCommand command = {
      "HEXISTS", RedisKey(external_storage_namespace_, table_name), {key}};
  SendRedisCmd(
      {key},
      std::move(command),
      [callback = std::move(callback)](const std::shared_ptr<CallbackReply> &reply) {
        bool exists = reply->ReadAsInteger() > 0;
        callback(exists);
      });
  return Status::OK();
}

class Cleanup {
 public:
  explicit Cleanup(std::function<void()> f) : f_(std::move(f)) {}
  ~Cleanup() { f_(); }

 private:
  std::function<void()> f_;
};

// Returns True if at least 1 key is deleted, False otherwise.
bool RedisDelKeyPrefixSync(const std::string &host,
                           int32_t port,
                           const std::string &password,
                           bool use_ssl,
                           const std::string &key_prefix) {
  RedisClientOptions options(host, port, password, use_ssl);
  auto cli = std::make_unique<RedisClient>(options);

  instrumented_io_context io_service;

  auto thread = std::make_unique<std::thread>([&]() {
    boost::asio::io_service::work work(io_service);
    io_service.run();
  });

  Cleanup _([&]() {
    io_service.stop();
    thread->join();
  });

  auto status = cli->Connect(io_service);
  RAY_CHECK(status.ok()) << "Failed to connect to redis: " << status.ToString();

  auto context = cli->GetPrimaryContext();
  std::vector<std::string> cmd{"KEYS", RedisMatchPattern::Prefix(key_prefix).escaped};
  auto reply = context->RunArgvSync(cmd);
  const auto &keys = reply->ReadAsStringArray();
  if (keys.empty()) {
    RAY_LOG(INFO) << "No keys found for prefix " << key_prefix;
    return true;
  }
  auto del_cmd = std::vector<std::string>{"DEL"};
  for (const auto &key : keys) {
    if (key.has_value()) {
      del_cmd.push_back(key.value());
    }
  }
  auto del_reply = context->RunArgvSync(del_cmd);
  if (del_reply->ReadAsInteger() > 0) {
    RAY_LOG(INFO) << "Successfully deleted keys with prefix " << key_prefix;
    return true;
  }
  RAY_LOG(ERROR) << "Failed to delete keys with prefix " << key_prefix;
  return false;
}

}  // namespace gcs

}  // namespace ray
