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

#include "ray/gcs/store_client/observable_store_client.h"

#include "absl/time/time.h"
#include "ray/stats/metric_defs.h"

namespace ray {
namespace gcs {

using namespace ray::stats;

Status ObservableStoreClient::AsyncPut(const std::string &table_name,
                                       const std::string &key,
                                       std::string data,
                                       bool overwrite,
                                       Postable<void(bool)> callback) {
  auto start = absl::GetCurrentTimeNanos();
  STATS_gcs_storage_operation_count.Record(
      1, {{"Operation", "Put"}, {"TableName", table_name}});
  return delegate_->AsyncPut(
      table_name,
      key,
      data,
      overwrite,
      std::move(callback).OnInvocation([start, table_name]() {
        auto end = absl::GetCurrentTimeNanos();
        STATS_gcs_storage_operation_latency_ms.Record(
            absl::ToDoubleMilliseconds(absl::Nanoseconds(end - start)),
            {{"Operation", "Put"}, {"TableName", table_name}});
      }));
}

Status ObservableStoreClient::AsyncGet(
    const std::string &table_name,
    const std::string &key,
    ToPostable<OptionalItemCallback<std::string>> callback) {
  auto start = absl::GetCurrentTimeNanos();
  STATS_gcs_storage_operation_count.Record(
      1, {{"Operation", "Get"}, {"TableName", table_name}});
  return delegate_->AsyncGet(
      table_name, key, std::move(callback).OnInvocation([start, table_name]() {
        auto end = absl::GetCurrentTimeNanos();
        STATS_gcs_storage_operation_latency_ms.Record(
            absl::ToDoubleMilliseconds(absl::Nanoseconds(end - start)),
            {{"Operation", "Get"}, {"TableName", table_name}});
      }));
}

Status ObservableStoreClient::AsyncGetAll(
    const std::string &table_name,
    Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) {
  auto start = absl::GetCurrentTimeNanos();
  STATS_gcs_storage_operation_count.Record(
      1, {{"Operation", "GetAll"}, {"TableName", table_name}});
  return delegate_->AsyncGetAll(
      table_name, std::move(callback).OnInvocation([start, table_name]() {
        auto end = absl::GetCurrentTimeNanos();
        STATS_gcs_storage_operation_latency_ms.Record(
            absl::ToDoubleMilliseconds(absl::Nanoseconds(end - start)),
            {{"Operation", "GetAll"}, {"TableName", table_name}});
      }));
}

Status ObservableStoreClient::AsyncMultiGet(
    const std::string &table_name,
    const std::vector<std::string> &keys,
    Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) {
  auto start = absl::GetCurrentTimeNanos();
  STATS_gcs_storage_operation_count.Record(
      1, {{"Operation", "MultiGet"}, {"TableName", table_name}});
  return delegate_->AsyncMultiGet(
      table_name, keys, std::move(callback).OnInvocation([start, table_name]() {
        auto end = absl::GetCurrentTimeNanos();
        STATS_gcs_storage_operation_latency_ms.Record(
            absl::ToDoubleMilliseconds(absl::Nanoseconds(end - start)),
            {{"Operation", "MultiGet"}, {"TableName", table_name}});
      }));
}

Status ObservableStoreClient::AsyncDelete(const std::string &table_name,
                                          const std::string &key,
                                          Postable<void(bool)> callback) {
  auto start = absl::GetCurrentTimeNanos();
  STATS_gcs_storage_operation_count.Record(
      1, {{"Operation", "Delete"}, {"TableName", table_name}});
  return delegate_->AsyncDelete(
      table_name, key, std::move(callback).OnInvocation([start, table_name]() {
        auto end = absl::GetCurrentTimeNanos();
        STATS_gcs_storage_operation_latency_ms.Record(
            absl::ToDoubleMilliseconds(absl::Nanoseconds(end - start)),
            {{"Operation", "Delete"}, {"TableName", table_name}});
      }));
}

Status ObservableStoreClient::AsyncBatchDelete(const std::string &table_name,
                                               const std::vector<std::string> &keys,
                                               Postable<void(int64_t)> callback) {
  auto start = absl::GetCurrentTimeNanos();
  STATS_gcs_storage_operation_count.Record(
      1, {{"Operation", "BatchDelete"}, {"TableName", table_name}});
  return delegate_->AsyncBatchDelete(
      table_name, keys, std::move(callback).OnInvocation([start, table_name]() {
        auto end = absl::GetCurrentTimeNanos();
        STATS_gcs_storage_operation_latency_ms.Record(
            absl::ToDoubleMilliseconds(absl::Nanoseconds(end - start)),
            {{"Operation", "BatchDelete"}, {"TableName", table_name}});
      }));
}

Status ObservableStoreClient::AsyncGetNextJobID(Postable<void(int)> callback) {
  return delegate_->AsyncGetNextJobID(std::move(callback));
}

Status ObservableStoreClient::AsyncGetKeys(
    const std::string &table_name,
    const std::string &prefix,
    Postable<void(std::vector<std::string>)> callback) {
  auto start = absl::GetCurrentTimeNanos();
  STATS_gcs_storage_operation_count.Record(
      1, {{"Operation", "GetKeys"}, {"TableName", table_name}});
  return delegate_->AsyncGetKeys(
      table_name, prefix, std::move(callback).OnInvocation([start, table_name]() {
        auto end = absl::GetCurrentTimeNanos();
        STATS_gcs_storage_operation_latency_ms.Record(
            absl::ToDoubleMilliseconds(absl::Nanoseconds(end - start)),
            {{"Operation", "GetKeys"}, {"TableName", table_name}});
      }));
}

Status ObservableStoreClient::AsyncExists(const std::string &table_name,
                                          const std::string &key,
                                          Postable<void(bool)> callback) {
  auto start = absl::GetCurrentTimeNanos();
  STATS_gcs_storage_operation_count.Record(
      1, {{"Operation", "Exists"}, {"TableName", table_name}});
  return delegate_->AsyncExists(
      table_name, key, std::move(callback).OnInvocation([start, table_name]() {
        auto end = absl::GetCurrentTimeNanos();
        STATS_gcs_storage_operation_latency_ms.Record(
            absl::ToDoubleMilliseconds(absl::Nanoseconds(end - start)),
            {{"Operation", "Exists"}, {"TableName", table_name}});
      }));
}

}  // namespace gcs

}  // namespace ray
