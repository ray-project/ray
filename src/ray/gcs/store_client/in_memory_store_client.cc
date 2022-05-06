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

#include "ray/gcs/store_client/in_memory_store_client.h"

namespace ray {

namespace gcs {

Status InMemoryStoreClient::AsyncPut(const std::string &table_name,
                                     const std::string &key,
                                     const std::string &data,
                                     bool overwrite,
                                     std::function<void(bool)> callback) {
  auto table = GetOrCreateTable(table_name);
  absl::MutexLock lock(&(table->mutex_));
  auto it = table->records_.find(key);
  bool inserted = false;
  if (it != table->records_.end()) {
    if (overwrite) {
      it->second = data;
    }
  } else {
    table->records_[key] = data;
    inserted = true;
  }
  if (callback != nullptr) {
    main_io_service_.post([callback, inserted]() { callback(inserted); },
                          "GcsInMemoryStore.Put");
  }
  return Status::OK();
}

Status InMemoryStoreClient::AsyncGet(const std::string &table_name,
                                     const std::string &key,
                                     const OptionalItemCallback<std::string> &callback) {
  RAY_CHECK(callback != nullptr);
  auto table = GetOrCreateTable(table_name);
  absl::MutexLock lock(&(table->mutex_));
  auto iter = table->records_.find(key);
  boost::optional<std::string> data;
  if (iter != table->records_.end()) {
    data = iter->second;
  }

  main_io_service_.post(
      [callback, data = std::move(data)]() { callback(Status::OK(), data); },
      "GcsInMemoryStore.Get");

  return Status::OK();
}

Status InMemoryStoreClient::AsyncGetAll(
    const std::string &table_name,
    const MapCallback<std::string, std::string> &callback) {
  RAY_CHECK(callback);
  auto table = GetOrCreateTable(table_name);
  absl::MutexLock lock(&(table->mutex_));
  auto result = absl::flat_hash_map<std::string, std::string>();
  result.insert(table->records_.begin(), table->records_.end());
  main_io_service_.post(
      [result = std::move(result), callback]() mutable { callback(std::move(result)); },
      "GcsInMemoryStore.GetAll");
  return Status::OK();
}

Status InMemoryStoreClient::AsyncMultiGet(
    const std::string &table_name,
    const std::vector<std::string> &keys,
    const MapCallback<std::string, std::string> &callback) {
  RAY_CHECK(callback);
  auto table = GetOrCreateTable(table_name);
  absl::MutexLock lock(&(table->mutex_));
  auto result = absl::flat_hash_map<std::string, std::string>();
  for (auto &key : keys) {
    auto it = table->records_.find(key);
    if (it == table->records_.end()) {
      continue;
    }
    result[key] = it->second;
  }
  main_io_service_.post(
      [result = std::move(result), callback]() mutable { callback(std::move(result)); },
      "GcsInMemoryStore.GetAll");
  return Status::OK();
}

Status InMemoryStoreClient::AsyncDelete(const std::string &table_name,
                                        const std::string &key,
                                        std::function<void(bool)> callback) {
  auto table = GetOrCreateTable(table_name);
  absl::MutexLock lock(&(table->mutex_));
  auto num = table->records_.erase(key);
  if (callback != nullptr) {
    main_io_service_.post([callback, num]() { callback(num > 0); },
                          "GcsInMemoryStore.Delete");
  }
  return Status::OK();
}

Status InMemoryStoreClient::AsyncBatchDelete(const std::string &table_name,
                                             const std::vector<std::string> &keys,
                                             std::function<void(int64_t)> callback) {
  auto table = GetOrCreateTable(table_name);
  absl::MutexLock lock(&(table->mutex_));
  int64_t num = 0;
  for (auto &key : keys) {
    num += table->records_.erase(key);
  }
  if (callback != nullptr) {
    main_io_service_.post([callback, num]() { callback(num); },
                          "GcsInMemoryStore.BatchDelete");
  }
  return Status::OK();
}

int InMemoryStoreClient::GetNextJobID() {
  absl::MutexLock lock(&mutex_);
  job_id_ += 1;
  return job_id_;
}

std::shared_ptr<InMemoryStoreClient::InMemoryTable> InMemoryStoreClient::GetOrCreateTable(
    const std::string &table_name) {
  absl::MutexLock lock(&mutex_);
  auto iter = tables_.find(table_name);
  if (iter != tables_.end()) {
    return iter->second;
  } else {
    auto table = std::make_shared<InMemoryTable>();
    tables_[table_name] = table;
    return table;
  }
}

Status InMemoryStoreClient::AsyncGetKeys(
    const std::string &table_name,
    const std::string &prefix,
    std::function<void(std::vector<std::string>)> callback) {
  RAY_CHECK(callback);
  auto table = GetOrCreateTable(table_name);
  std::vector<std::string> result;
  absl::MutexLock lock(&(table->mutex_));
  for (auto &pair : table->records_) {
    if (pair.first.find(prefix) == 0) {
      result.push_back(pair.first);
    }
  }
  main_io_service_.post(
      [result = std::move(result), callback]() mutable { callback(std::move(result)); },
      "GcsInMemoryStore.Keys");
  return Status::OK();
}

Status InMemoryStoreClient::AsyncExists(const std::string &table_name,
                                        const std::string &key,
                                        std::function<void(bool)> callback) {
  RAY_CHECK(callback);
  auto table = GetOrCreateTable(table_name);
  absl::MutexLock lock(&(table->mutex_));
  bool result = table->records_.contains(key);
  main_io_service_.post([result, callback]() mutable { callback(result); },
                        "GcsInMemoryStore.Exists");
  return Status::OK();
}

}  // namespace gcs

}  // namespace ray
