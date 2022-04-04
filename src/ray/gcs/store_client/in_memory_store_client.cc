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
                                     const StatusCallback &callback) {
  auto table = GetOrCreateTable(table_name);
  absl::MutexLock lock(&(table->mutex_));
  table->records_[key] = data;
  if (callback != nullptr) {
    main_io_service_.post([callback]() { callback(Status::OK()); },
                          "GcsInMemoryStore.Put");
  }
  return Status::OK();
}

Status InMemoryStoreClient::AsyncPutWithIndex(const std::string &table_name,
                                              const std::string &key,
                                              const std::string &index_key,
                                              const std::string &data,
                                              const StatusCallback &callback) {
  auto table = GetOrCreateTable(table_name);
  absl::MutexLock lock(&(table->mutex_));
  table->records_[key] = data;
  table->index_keys_[index_key].emplace_back(key);
  if (callback != nullptr) {
    main_io_service_.post([callback]() { callback(Status::OK()); },
                          "GcsInMemoryStore.PutWithIndex");
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

Status InMemoryStoreClient::AsyncDelete(const std::string &table_name,
                                        const std::string &key,
                                        const StatusCallback &callback) {
  auto table = GetOrCreateTable(table_name);
  absl::MutexLock lock(&(table->mutex_));
  table->records_.erase(key);
  if (callback != nullptr) {
    main_io_service_.post([callback]() { callback(Status::OK()); },
                          "GcsInMemoryStore.Delete");
  }
  return Status::OK();
}

Status InMemoryStoreClient::AsyncDeleteWithIndex(const std::string &table_name,
                                                 const std::string &key,
                                                 const std::string &index_key,
                                                 const StatusCallback &callback) {
  auto table = GetOrCreateTable(table_name);
  absl::MutexLock lock(&(table->mutex_));
  // Remove key-value data.
  table->records_.erase(key);

  // Remove index-key data.
  auto iter = table->index_keys_.find(index_key);
  if (iter != table->index_keys_.end()) {
    auto it = std::find(iter->second.begin(), iter->second.end(), key);
    if (it != iter->second.end()) {
      iter->second.erase(it);
      if (iter->second.size() == 0) {
        table->index_keys_.erase(iter);
      }
    }
  }
  if (callback != nullptr) {
    main_io_service_.post([callback]() { callback(Status::OK()); },
                          "GcsInMemoryStore.DeleteWithIndex");
  }
  return Status::OK();
}

Status InMemoryStoreClient::AsyncBatchDelete(const std::string &table_name,
                                             const std::vector<std::string> &keys,
                                             const StatusCallback &callback) {
  auto table = GetOrCreateTable(table_name);
  absl::MutexLock lock(&(table->mutex_));
  for (auto &key : keys) {
    table->records_.erase(key);
  }
  if (callback != nullptr) {
    main_io_service_.post([callback]() { callback(Status::OK()); },
                          "GcsInMemoryStore.BatchDelete");
  }
  return Status::OK();
}

Status InMemoryStoreClient::AsyncBatchDeleteWithIndex(
    const std::string &table_name,
    const std::vector<std::string> &keys,
    const std::vector<std::string> &index_keys,
    const StatusCallback &callback) {
  RAY_CHECK(keys.size() == index_keys.size());

  auto table = GetOrCreateTable(table_name);
  absl::MutexLock lock(&(table->mutex_));

  for (size_t i = 0; i < keys.size(); ++i) {
    const std::string &key = keys[i];
    const std::string &index_key = index_keys[i];
    table->records_.erase(key);

    auto iter = table->index_keys_.find(index_key);
    if (iter != table->index_keys_.end()) {
      auto it = std::find(iter->second.begin(), iter->second.end(), key);
      if (it != iter->second.end()) {
        iter->second.erase(it);
        if (iter->second.size() == 0) {
          table->index_keys_.erase(iter);
        }
      }
    }
  }

  if (callback != nullptr) {
    main_io_service_.post([callback]() { callback(Status::OK()); },
                          "GcsInMemoryStore.BatchDeleteWithIndex");
  }

  return Status::OK();
}

Status InMemoryStoreClient::AsyncGetByIndex(
    const std::string &table_name,
    const std::string &index_key,
    const MapCallback<std::string, std::string> &callback) {
  RAY_CHECK(callback);
  auto table = GetOrCreateTable(table_name);
  absl::MutexLock lock(&(table->mutex_));
  auto iter = table->index_keys_.find(index_key);
  auto result = absl::flat_hash_map<std::string, std::string>();
  if (iter != table->index_keys_.end()) {
    for (auto &key : iter->second) {
      auto kv_iter = table->records_.find(key);
      if (kv_iter != table->records_.end()) {
        result[kv_iter->first] = kv_iter->second;
      }
    }
  }
  main_io_service_.post(
      [result = std::move(result), callback]() mutable { callback(std::move(result)); },
      "GcsInMemoryStore.GetByIndex");

  return Status::OK();
}

Status InMemoryStoreClient::AsyncDeleteByIndex(const std::string &table_name,
                                               const std::string &index_key,
                                               const StatusCallback &callback) {
  auto table = GetOrCreateTable(table_name);
  absl::MutexLock lock(&(table->mutex_));
  auto iter = table->index_keys_.find(index_key);
  if (iter != table->index_keys_.end()) {
    for (auto &key : iter->second) {
      table->records_.erase(key);
    }
    table->index_keys_.erase(iter);
  }
  if (callback != nullptr) {
    main_io_service_.post([callback]() { callback(Status::OK()); },
                          "GcsInMemoryStore.DeleteByIndex");
  }
  return Status::OK();
}

int InMemoryStoreClient::GetNextJobID() {
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

}  // namespace gcs

}  // namespace ray
