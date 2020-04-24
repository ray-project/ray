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

void InMemoryTable::Put(const std::string &key, const std::string &data) {
  records_[key] = data;
}
void InMemoryTable::PutWithIndex(const std::string &key, const std::string &index_key,
                                 const std::string &data) {
  records_[key] = data;
  index_keys_[index_key].emplace_back(key);
}

boost::optional<std::string> InMemoryTable::Get(const std::string &key) const {
  auto iter = records_.find(key);
  return iter == records_.end() ? boost::optional<std::string>() : iter->second;
}

const absl::flat_hash_map<std::string, std::string> &InMemoryTable::GetAll() const {
  return records_;
}

void InMemoryTable::Delete(const std::string &key) { records_.erase(key); }

void InMemoryTable::DeleteByIndex(const std::string &index_key) {
  auto iter = index_keys_.find(index_key);
  if (iter != index_keys_.end()) {
    for (auto &key : iter->second) {
      records_.erase(key);
    }
    index_keys_.erase(iter);
  }
}

Status InMemoryStoreClient::AsyncPut(const std::string &table_name,
                                     const std::string &key, const std::string &data,
                                     const StatusCallback &callback) {
  auto io_service = io_service_pool_->Get(std::hash<std::string>()(table_name));
  io_service->post([this, table_name, key, data, callback]() {
    auto table = GetOrCreateTable(table_name);
    table->Put(key, data);
    main_io_service_.post([callback]() { callback(Status::OK()); });
  });
  return Status::OK();
}

Status InMemoryStoreClient::AsyncPutWithIndex(const std::string &table_name,
                                              const std::string &key,
                                              const std::string &index_key,
                                              const std::string &data,
                                              const StatusCallback &callback) {
  auto io_service = io_service_pool_->Get(std::hash<std::string>()(table_name));
  io_service->post([this, table_name, key, index_key, data, callback]() {
    auto table = GetOrCreateTable(table_name);
    table->PutWithIndex(key, index_key, data);
    main_io_service_.post([callback]() { callback(Status::OK()); });
  });
  return Status::OK();
}

Status InMemoryStoreClient::AsyncGet(const std::string &table_name,
                                     const std::string &key,
                                     const OptionalItemCallback<std::string> &callback) {
  auto io_service = io_service_pool_->Get(std::hash<std::string>()(table_name));
  io_service->post([this, table_name, key, callback]() {
    auto table = GetOrCreateTable(table_name);
    auto data = table->Get(key);
    if (data) {
      main_io_service_.post([callback, data]() { callback(Status::OK(), data); });
    } else {
      main_io_service_.post([callback]() { callback(Status::OK(), boost::none); });
    }
  });
  return Status::OK();
}

Status InMemoryStoreClient::AsyncGetAll(
    const std::string &table_name,
    const SegmentedCallback<std::pair<std::string, std::string>> &callback) {
  auto io_service = io_service_pool_->Get(std::hash<std::string>()(table_name));
  io_service->post([this, table_name, callback]() {
    auto table = GetOrCreateTable(table_name);
    auto records = table->GetAll();
    std::vector<std::pair<std::string, std::string>> result;
    for (auto &record : records) {
      result.emplace_back(std::make_pair(record.first, record.second));
    }
    main_io_service_.post(
        [result, callback]() { callback(Status::OK(), false, result); });
  });
  return Status::OK();
}

Status InMemoryStoreClient::AsyncDelete(const std::string &table_name,
                                        const std::string &key,
                                        const StatusCallback &callback) {
  auto io_service = io_service_pool_->Get(std::hash<std::string>()(table_name));
  io_service->post([this, table_name, key, callback]() {
    auto table = GetOrCreateTable(table_name);
    table->Delete(key);
    main_io_service_.post([callback]() { callback(Status::OK()); });
  });
  return Status::OK();
}

Status InMemoryStoreClient::AsyncDeleteByIndex(const std::string &table_name,
                                               const std::string &index_key,
                                               const StatusCallback &callback) {
  auto io_service = io_service_pool_->Get(std::hash<std::string>()(table_name));
  io_service->post([this, table_name, index_key, callback]() {
    auto table = GetOrCreateTable(table_name);
    table->DeleteByIndex(index_key);
    main_io_service_.post([callback]() { callback(Status::OK()); });
  });
  return Status::OK();
}

std::shared_ptr<InMemoryTable> InMemoryStoreClient::GetOrCreateTable(
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
