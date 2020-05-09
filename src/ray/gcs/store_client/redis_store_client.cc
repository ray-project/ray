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

Status RedisStoreClient::AsyncPut(const std::string &table_name, const std::string &key,
                                  const std::string &data,
                                  const StatusCallback &callback) {
  std::string full_key = table_name + key;
  return DoPut(full_key, data, callback);
}

Status RedisStoreClient::AsyncPutWithIndex(const std::string &table_name,
                                           const std::string &key,
                                           const std::string &index_key,
                                           const std::string &data,
                                           const StatusCallback &callback) {
  auto write_callback = [this, table_name, key, data, callback](Status status) {
    if (!status.ok()) {
      // Run callback if failed.
      if (callback != nullptr) {
        callback(status);
      }
      return;
    }

    // Write data to Redis.
    std::string full_key = table_name + key;
    status = DoPut(full_key, data, callback);

    if (!status.ok()) {
      // Run callback if failed.
      if (callback != nullptr) {
        callback(status);
      }
    }
  };

  // Write index to Redis.
  std::string index_table_key = index_key + table_name + key;
  return DoPut(index_table_key, key, write_callback);
}

Status RedisStoreClient::DoPut(const std::string &key, const std::string &data,
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

Status RedisStoreClient::AsyncGet(const std::string &table_name, const std::string &key,
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

  std::string full_key = table_name + key;
  std::vector<std::string> args = {"GET", full_key};

  auto shard_context = redis_client_->GetShardContext(full_key);
  return shard_context->RunArgvAsync(args, redis_callback);
}

Status RedisStoreClient::AsyncGetAll(
    const std::string &table_name,
    const SegmentedCallback<std::pair<std::string, std::string>> &callback) {
  RAY_CHECK(0) << "Not implemented! Will implement this function in next PR.";
  return Status::OK();
}

Status RedisStoreClient::AsyncDelete(const std::string &table_name,
                                     const std::string &key,
                                     const StatusCallback &callback) {
  RedisCallback delete_callback = nullptr;
  if (callback) {
    delete_callback = [callback](std::shared_ptr<CallbackReply> reply) {
      int64_t deleted_count = reply->ReadAsInteger();
      RAY_LOG(DEBUG) << "Delete done, total delete count " << deleted_count;
      callback(Status::OK());
    };
  }

  std::string full_key = table_name + key;
  std::vector<std::string> args = {"DEL", full_key};

  auto shard_context = redis_client_->GetShardContext(full_key);
  return shard_context->RunArgvAsync(args, delete_callback);
}

Status RedisStoreClient::AsyncDeleteByIndex(const std::string &table_name,
                                            const std::string &index_key,
                                            const StatusCallback &callback) {
  RAY_CHECK(0) << "Not implemented! Will implement this function in next PR.";
  return Status::OK();
}

RedisStoreClient::RedisScanner::RedisScanner(std::shared_ptr<RedisClient> redis_client)
    : redis_client_(std::move(redis_client)) {
  for (size_t index = 0; index < redis_client_->GetShardContexts().size(); ++index) {
    shard_to_cursor_.emplace(index, /*cursor*/ 0);
  }
}

void RedisStoreClient::RedisScanner::DoScan() {
  bool is_scan_done = false;
  {
    absl::MutexLock lock(&mutex_);
    is_scan_done = shard_to_cursor_.empty();
  }

  if (is_scan_done) {
    RAY_CHECK(pending_request_count_ == 0);
    OnScanDone();
    return;
  }

  {
    size_t batch_count = RayConfig::instance().maximum_gcs_scan_batch_size();
    absl::MutexLock lock(&mutex_);
    for (const auto &item : shard_to_cursor_) {
      ++pending_request_count_;

      size_t shard_index = item.first;
      size_t cursor = item.second;
      auto scan_callback = [this, shard_index](std::shared_ptr<CallbackReply> reply) {
        OnScanCallback(shard_index, reply);
      };

      // Scan by prefix from Redis.
      std::vector<std::string> args = {"SCAN",  std::to_string(cursor),
                                       "MATCH", match_pattern_,
                                       "COUNT", std::to_string(batch_count)};
      auto shard_context = redis_client_->GetShardContexts()[shard_index];
      Status status = shard_context->RunArgvAsync(args, scan_callback);

      if (!status.ok()) {
        is_failed_ = true;
        if (--pending_request_count_ == 0) {
          OnScanDone();
        }
        RAY_LOG(INFO) << "Scan failed, status " << status.ToString();
        return;
      }
    }
  }
}

void RedisStoreClient::RedisScanner::OnScanDone() {
  Status status = is_failed_ ? Status::RedisError("Redis Error.") : Status::OK();
  scan_partial_rows_callback_(status, /* has_more */ false, rows_);
}

void RedisStoreClient::RedisScanner::OnScanCallback(
    size_t shard_index, std::shared_ptr<CallbackReply> reply) {
  RAY_CHECK(reply);
  bool pending_done = (--pending_request_count_ == 0);

  if (is_failed_) {
    if (pending_done) {
      OnScanDone();
    }
    return;
  }

  std::vector<std::string> keys;
  size_t cursor = reply->ReadAsScanArray(&keys);
  ProcessScanResult(shard_index, cursor, keys, pending_done);
}

void RedisStoreClient::RedisScanner::ProcessScanResult(
    size_t shard_index, size_t cursor, const std::vector<std::string> &scan_result,
    bool pending_done) {
  {
    absl::MutexLock lock(&mutex_);
    // Update shard cursors.
    auto shard_it = shard_to_cursor_.find(shard_index);
    RAY_CHECK(shard_it != shard_to_cursor_.end());
    if (cursor == 0) {
      shard_to_cursor_.erase(shard_it);
    } else {
      shard_it->second = cursor;
    }
  }

  // Deduplicate keys.
  auto deduped_result = Deduplicate(scan_result);
  // Save scan result.
  size_t total_count = UpdateResult(deduped_result);

  if (!pending_done) {
    // Waiting for all pending scan command return.
    return;
  }
}

}  // namespace gcs

}  // namespace ray
