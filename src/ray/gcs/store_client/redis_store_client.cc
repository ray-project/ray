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
#include <utility>
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
    const MultiItemCallback<std::pair<std::string, std::string>> &callback) {
  RAY_CHECK(callback);
  std::string match_pattern = table_name + "*";
  redis_scanner_ =
      std::make_shared<RedisScanner>(redis_client_, table_name, match_pattern);
  return redis_scanner_->ScanRows(callback);
}

Status RedisStoreClient::AsyncDelete(const std::string &table_name,
                                     const std::string &key,
                                     const StatusCallback &callback) {
  RedisCallback delete_callback = nullptr;
  if (callback) {
    delete_callback = [callback](const std::shared_ptr<CallbackReply> &reply) {
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

Status RedisStoreClient::AsyncBatchDelete(const std::string &table_name,
                                          const std::vector<std::string> &keys,
                                          const StatusCallback &callback) {
  auto finished_count = std::make_shared<int>(0);
  int size = keys.size();
  for (auto &key : keys) {
    auto done = [finished_count, size, callback](const Status &status) {
      ++(*finished_count);
      if (*finished_count == size) {
        callback(Status::OK());
      }
    };
    RAY_CHECK_OK(AsyncDelete(table_name, key, done));
  }
  return Status::OK();
}

Status RedisStoreClient::AsyncDeleteByIndex(const std::string &table_name,
                                            const std::string &index_key,
                                            const StatusCallback &callback) {
  std::string match_pattern = index_key + table_name + "*";
  auto scanner = std::make_shared<RedisScanner>(redis_client_, table_name, match_pattern);
  auto on_done = [this, table_name, callback](const Status &status,
                                              const std::vector<std::string> &result) {
    if (!result.empty()) {
      auto finished_count = std::make_shared<int>(0);
      int size = result.size();
      for (auto &key : result) {
        auto on_delete_done = [finished_count, size, callback](const Status &status) {
          ++(*finished_count);
          if (*finished_count == size) {
            callback(Status::OK());
          }
        };

        RAY_CHECK_OK(AsyncDelete(table_name, key, on_delete_done));
      }
    } else {
      callback(status);
    }
  };
  return scanner->ScanKeys(on_done);
}

RedisStoreClient::RedisScanner::RedisScanner(std::shared_ptr<RedisClient> redis_client,
                                             std::string table_name,
                                             std::string match_pattern)
    : table_name_(std::move(table_name)),
      match_pattern_(std::move(match_pattern)),
      redis_client_(std::move(redis_client)) {
  RAY_LOG(INFO) << "RedisScanner GetShardContexts size = "
                << redis_client_->GetShardContexts().size();
  for (size_t index = 0; index < redis_client_->GetShardContexts().size(); ++index) {
    shard_to_cursor_[index] = 0;
  }
  RAY_LOG(INFO) << "RedisScanner shard_to_cursor_ size = " << shard_to_cursor_.size();
}

Status RedisStoreClient::RedisScanner::ScanRows(
    const MultiItemCallback<std::pair<std::string, std::string>> &callback) {
  callback_ = [this, callback](const Status &status) {
    RAY_LOG(INFO) << "RedisStoreClient::RedisScanner::ScanRows................";
    callback(status, rows_);
  };
  Scan();
  return Status::OK();
}

Status RedisStoreClient::RedisScanner::ScanKeys(
    const MultiItemCallback<std::string> &callback) {
  callback_ = [this, callback](const Status &status) {
    std::vector<std::string> result;
    //    absl::MutexLock lock(&mutex_);
    result.insert(result.begin(), keys_.begin(), keys_.end());
    callback(status, result);
  };
  Scan();
  return Status::OK();
}

void RedisStoreClient::RedisScanner::Scan() {
  if (shard_to_cursor_.empty()) {
    OnScanDone();
    return;
  }

  size_t batch_count = RayConfig::instance().maximum_gcs_scan_batch_size();
  for (const auto &item : shard_to_cursor_) {
    ++pending_request_count_;

    size_t shard_index = item.first;
    size_t cursor = item.second;

    auto scan_callback = [this,
                          shard_index](const std::shared_ptr<CallbackReply> &reply) {
      RAY_LOG(INFO) << "RedisScanner::Scan shard_to_cursor_ size = "
                    << this->shard_to_cursor_.size() << ", shard_index = " << shard_index
                    << ", this = " << this;
      OnScanCallback(shard_index, reply);
    };

    // Scan by prefix from Redis.
    std::vector<std::string> args = {"SCAN",  std::to_string(cursor),
                                     "MATCH", match_pattern_,
                                     "COUNT", std::to_string(batch_count)};
    auto shard_context = redis_client_->GetShardContexts()[shard_index];
    Status status = shard_context->RunArgvAsync(args, scan_callback);

    if (!status.ok()) {
      RAY_LOG(WARNING) << "Scan failed, status " << status.ToString();
      is_failed_ = true;
      if (--pending_request_count_ == 0) {
        OnScanDone();
      }
      return;
    }
  }
}

void RedisStoreClient::RedisScanner::OnScanDone() {
  RAY_LOG(INFO) << "RedisStoreClient::RedisScanner::OnScanDone()............";
  if (is_failed_) {
    callback_(Status::RedisError("Redis Error."));
  } else {
    ReadRows(keys_);
  }
}

void RedisStoreClient::RedisScanner::OnScanCallback(
    size_t shard_index, const std::shared_ptr<CallbackReply> &reply) {
  RAY_CHECK(reply);
  std::vector<std::string> scan_result;
  size_t cursor = reply->ReadAsScanArray(&scan_result);

  // Update shard cursors and keys_.
  {
    absl::MutexLock lock(&mutex_);
    RAY_LOG(INFO) << "OnScanCallback shard_index = " << shard_index
                  << ", shard_to_cursor_ size = " << shard_to_cursor_.size()
                  << ", scan_result size = " << scan_result.size();
    auto shard_it = shard_to_cursor_.find(shard_index);
    RAY_CHECK(shard_it != shard_to_cursor_.end());
    if (cursor == 0) {
      shard_to_cursor_.erase(shard_it);
    } else {
      shard_it->second = cursor;
    }

    keys_.insert(scan_result.begin(), scan_result.end());
  }

  // Scan result is empty, continue scan.
  if (--pending_request_count_ == 0) {
    Scan();
  }
}

void RedisStoreClient::RedisScanner::ReadRows(
    const absl::flat_hash_set<std::string> &keys) {
  for (const auto &key : keys) {
    ++pending_read_count_;

    std::vector<std::string> args = {"GET", key};
    auto read_callback = [this, key](const std::shared_ptr<CallbackReply> &reply) {
      std::string value;
      if (!reply->IsNil()) {
        value = reply->ReadAsString();
        {
          absl::MutexLock lock(&mutex_);
          rows_.emplace_back(
              key.substr(table_name_.size(), key.size() - table_name_.size()), value);
        }
      }

      if (--pending_read_count_ == 0) {
        if (!is_failed_) {
          RAY_LOG(INFO)
              << "RedisStoreClient::RedisScanner::ReadRows.............1111111111";
          callback_(Status::OK());
        } else {
          RAY_LOG(INFO)
              << "RedisStoreClient::RedisScanner::ReadRows.............222222222222";
          callback_(Status::RedisError("Redis return failed."));
        }
      }
    };

    auto shard_context = redis_client_->GetShardContext(key);
    Status status = shard_context->RunArgvAsync(args, read_callback);
    if (!status.ok()) {
      RAY_LOG(WARNING) << "Read key " << key << " failed, status " << status.ToString();
      is_failed_ = true;
      break;
    }
  }
}

}  // namespace gcs

}  // namespace ray
