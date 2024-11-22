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
                                     Postable<void(bool)> callback) {
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
  callback.Post("GcsInMemoryStore.Put", inserted);
  return Status::OK();
}

Status InMemoryStoreClient::AsyncGet(
    const std::string &table_name,
    const std::string &key,
    ToPostable<OptionalItemCallback<std::string>> callback) {
  auto table = GetOrCreateTable(table_name);
  absl::MutexLock lock(&(table->mutex_));
  auto iter = table->records_.find(key);
  std::optional<std::string> data;
  if (iter != table->records_.end()) {
    data = iter->second;
  }
  callback.Post("GcsInMemoryStore.Get", Status::OK(), std::move(data));
  return Status::OK();
}

Status InMemoryStoreClient::AsyncGetAll(
    const std::string &table_name,
    ToPostable<MapCallback<std::string, std::string>> callback) {
  auto table = GetOrCreateTable(table_name);
  absl::MutexLock lock(&(table->mutex_));
  auto result = absl::flat_hash_map<std::string, std::string>();
  result.insert(table->records_.begin(), table->records_.end());
  callback.Post("GcsInMemoryStore.GetAll", std::move(result));
  return Status::OK();
}

Status InMemoryStoreClient::AsyncMultiGet(
    const std::string &table_name,
    const std::vector<std::string> &keys,
    ToPostable<MapCallback<std::string, std::string>> callback) {
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
  callback.Post("GcsInMemoryStore.GetAll", std::move(result));
  return Status::OK();
}

Status InMemoryStoreClient::AsyncDelete(const std::string &table_name,
                                        const std::string &key,
                                        Postable<void(bool)> callback) {
  auto table = GetOrCreateTable(table_name);
  absl::MutexLock lock(&(table->mutex_));
  auto num = table->records_.erase(key);
  callback.Post("GcsInMemoryStore.Delete", num > 0);
  return Status::OK();
}

Status InMemoryStoreClient::AsyncBatchDelete(const std::string &table_name,
                                             const std::vector<std::string> &keys,
                                             Postable<void(int64_t)> callback) {
  auto table = GetOrCreateTable(table_name);
  absl::MutexLock lock(&(table->mutex_));
  int64_t num = 0;
  for (auto &key : keys) {
    num += table->records_.erase(key);
  }
  callback.Post("GcsInMemoryStore.BatchDelete", num);
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
    Postable<void(std::vector<std::string>)> callback) {
  auto table = GetOrCreateTable(table_name);
  std::vector<std::string> result;
  absl::MutexLock lock(&(table->mutex_));
  for (auto &pair : table->records_) {
    if (pair.first.find(prefix) == 0) {
      result.push_back(pair.first);
    }
  }
  callback.Post("GcsInMemoryStore.Keys", std::move(result));
  return Status::OK();
}

Status InMemoryStoreClient::AsyncExists(const std::string &table_name,
                                        const std::string &key,
                                        Postable<void(bool)> callback) {
  auto table = GetOrCreateTable(table_name);
  absl::MutexLock lock(&(table->mutex_));
  bool result = table->records_.contains(key);
  callback.Post("GcsInMemoryStore.Exists", result);
  return Status::OK();
}

}  // namespace gcs

}  // namespace ray
