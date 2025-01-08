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

namespace ray::gcs {

Status InMemoryStoreClient::AsyncPut(const std::string &table_name,
                                     const std::string &key,
                                     std::string data,
                                     bool overwrite,
                                     Postable<void(bool)> callback) {
  auto &table = GetOrCreateMutableTable(table_name);
  absl::WriterMutexLock lock(&(table.mutex_));
  auto it = table.records_.find(key);
  bool inserted = false;
  if (it != table.records_.end()) {
    if (overwrite) {
      it->second = std::move(data);
    }
  } else {
    table.records_[key] = std::move(data);
    inserted = true;
  }
  std::move(callback).Post("GcsInMemoryStore.Put", inserted);
  return Status::OK();
}

Status InMemoryStoreClient::AsyncGet(
    const std::string &table_name,
    const std::string &key,
    ToPostable<OptionalItemCallback<std::string>> callback) {
  auto table = GetTable(table_name);
  std::optional<std::string> data;
  if (table != nullptr) {
    absl::ReaderMutexLock lock(&(table->mutex_));
    auto iter = table->records_.find(key);
    if (iter != table->records_.end()) {
      data = iter->second;
    }
  }
  std::move(callback).Post("GcsInMemoryStore.Get", Status::OK(), std::move(data));
  return Status::OK();
}

Status InMemoryStoreClient::AsyncGetAll(
    const std::string &table_name,
    Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) {
  auto result = absl::flat_hash_map<std::string, std::string>();
  auto table = GetTable(table_name);
  if (table != nullptr) {
    absl::ReaderMutexLock lock(&(table->mutex_));
    result = table->records_;
  }
  std::move(callback).Post("GcsInMemoryStore.GetAll", std::move(result));
  return Status::OK();
}

Status InMemoryStoreClient::AsyncMultiGet(
    const std::string &table_name,
    const std::vector<std::string> &keys,
    Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) {
  auto result = absl::flat_hash_map<std::string, std::string>();
  auto table = GetTable(table_name);
  if (table != nullptr) {
    absl::ReaderMutexLock lock(&(table->mutex_));
    for (const auto &key : keys) {
      auto it = table->records_.find(key);
      if (it == table->records_.end()) {
        continue;
      }
      result.emplace(key, it->second);
    }
  }
  std::move(callback).Post("GcsInMemoryStore.GetAll", std::move(result));
  return Status::OK();
}

Status InMemoryStoreClient::AsyncDelete(const std::string &table_name,
                                        const std::string &key,
                                        Postable<void(bool)> callback) {
  auto &table = GetOrCreateMutableTable(table_name);
  absl::WriterMutexLock lock(&(table.mutex_));
  auto num = table.records_.erase(key);
  std::move(callback).Post("GcsInMemoryStore.Delete", num > 0);
  return Status::OK();
}

Status InMemoryStoreClient::AsyncBatchDelete(const std::string &table_name,
                                             const std::vector<std::string> &keys,
                                             Postable<void(int64_t)> callback) {
  auto &table = GetOrCreateMutableTable(table_name);
  absl::WriterMutexLock lock(&(table.mutex_));
  int64_t num = 0;
  for (auto &key : keys) {
    num += table.records_.erase(key);
  }
  std::move(callback).Post("GcsInMemoryStore.BatchDelete", num);
  return Status::OK();
}

int InMemoryStoreClient::GetNextJobID() {
  return job_id_.fetch_add(1, std::memory_order_acq_rel);
}

InMemoryStoreClient::InMemoryTable &InMemoryStoreClient::GetOrCreateMutableTable(
    const std::string &table_name) {
  absl::WriterMutexLock lock(&mutex_);
  auto iter = tables_.find(table_name);
  if (iter != tables_.end()) {
    return iter->second;
  }
  return tables_[table_name];
}

const InMemoryStoreClient::InMemoryTable *InMemoryStoreClient::GetTable(
    const std::string &table_name) {
  absl::ReaderMutexLock lock(&mutex_);
  auto iter = tables_.find(table_name);
  if (iter != tables_.end()) {
    return &iter->second;
  }
  return nullptr;
}

Status InMemoryStoreClient::AsyncGetKeys(
    const std::string &table_name,
    const std::string &prefix,
    Postable<void(std::vector<std::string>)> callback) {
  std::vector<std::string> result;
  auto table = GetTable(table_name);
  if (table != nullptr) {
    absl::ReaderMutexLock lock(&(table->mutex_));
    for (const auto &[key, value] : table->records_) {
      if (absl::StartsWith(key, prefix)) {
        result.push_back(key);
      }
    }
  }
  std::move(callback).Post("GcsInMemoryStore.Keys", std::move(result));

  return Status::OK();
}

Status InMemoryStoreClient::AsyncExists(const std::string &table_name,
                                        const std::string &key,
                                        Postable<void(bool)> callback) {
  bool result = false;
  auto table = GetTable(table_name);
  if (table != nullptr) {
    absl::ReaderMutexLock lock(&(table->mutex_));
    result = table->records_.contains(key);
  }
  std::move(callback).Post("GcsInMemoryStore.Exists", result);
  return Status::OK();
}

}  // namespace ray::gcs
