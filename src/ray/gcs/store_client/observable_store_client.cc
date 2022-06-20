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
                                       const std::string &data,
                                       bool overwrite,
                                       std::function<void(bool)> callback) {
  auto start = absl::GetCurrentTimeNanos();
  STATS_gcs_storage_operation_count.Record(1, "Put");
  return delegate_->AsyncPut(table_name,
                             key,
                             data,
                             overwrite,
                             [start, callback = std::move(callback)](auto result) {
                               auto end = absl::GetCurrentTimeNanos();
                               STATS_gcs_storage_operation_latency_ms.Record(
                                   absl::Nanoseconds(end - start) / absl::Milliseconds(1),
                                   "Put");
                               if (callback) {
                                 callback(std::move(result));
                               }
                             });
}

Status ObservableStoreClient::AsyncGet(
    const std::string &table_name,
    const std::string &key,
    const OptionalItemCallback<std::string> &callback) {
  auto start = absl::GetCurrentTimeNanos();
  STATS_gcs_storage_operation_count.Record(1, "Get");
  return delegate_->AsyncGet(
      table_name, key, [start, callback](auto status, auto result) {
        auto end = absl::GetCurrentTimeNanos();
        STATS_gcs_storage_operation_latency_ms.Record(
            absl::Nanoseconds(end - start) / absl::Milliseconds(1), "Get");
        if (callback) {
          callback(status, std::move(result));
        }
      });
}

Status ObservableStoreClient::AsyncGetAll(
    const std::string &table_name,
    const MapCallback<std::string, std::string> &callback) {
  auto start = absl::GetCurrentTimeNanos();
  STATS_gcs_storage_operation_count.Record(1, "GetAll");
  return delegate_->AsyncGetAll(table_name, [start, callback](auto result) {
    auto end = absl::GetCurrentTimeNanos();
    STATS_gcs_storage_operation_latency_ms.Record(
        absl::Nanoseconds(end - start) / absl::Milliseconds(1), "GetAll");
    if (callback) {
      callback(std::move(result));
    }
  });
}
Status ObservableStoreClient::AsyncMultiGet(
    const std::string &table_name,
    const std::vector<std::string> &keys,
    const MapCallback<std::string, std::string> &callback) {
  auto start = absl::GetCurrentTimeNanos();
  STATS_gcs_storage_operation_count.Record(1, "MultiGet");
  return delegate_->AsyncMultiGet(table_name, keys, [start, callback](auto result) {
    auto end = absl::GetCurrentTimeNanos();
    STATS_gcs_storage_operation_latency_ms.Record(
        absl::Nanoseconds(end - start) / absl::Milliseconds(1), "MultiGet");
    if (callback) {
      callback(std::move(result));
    }
  });
}

Status ObservableStoreClient::AsyncDelete(const std::string &table_name,
                                          const std::string &key,
                                          std::function<void(bool)> callback) {
  auto start = absl::GetCurrentTimeNanos();
  STATS_gcs_storage_operation_count.Record(1, "Delete");
  return delegate_->AsyncDelete(
      table_name, key, [start, callback = std::move(callback)](auto result) {
        auto end = absl::GetCurrentTimeNanos();
        STATS_gcs_storage_operation_latency_ms.Record(
            absl::Nanoseconds(end - start) / absl::Milliseconds(1), "Delete");
        if (callback) {
          callback(std::move(result));
        }
      });
}

Status ObservableStoreClient::AsyncBatchDelete(const std::string &table_name,
                                               const std::vector<std::string> &keys,
                                               std::function<void(int64_t)> callback) {
  auto start = absl::GetCurrentTimeNanos();
  STATS_gcs_storage_operation_count.Record(1, "BatchDelete");
  return delegate_->AsyncBatchDelete(
      table_name, keys, [start, callback = std::move(callback)](auto result) {
        auto end = absl::GetCurrentTimeNanos();
        STATS_gcs_storage_operation_latency_ms.Record(
            absl::Nanoseconds(end - start) / absl::Milliseconds(1), "BatchDelete");
        if (callback) {
          callback(std::move(result));
        }
      });
}

int ObservableStoreClient::GetNextJobID() { return delegate_->GetNextJobID(); }

Status ObservableStoreClient::AsyncGetKeys(
    const std::string &table_name,
    const std::string &prefix,
    std::function<void(std::vector<std::string>)> callback) {
  auto start = absl::GetCurrentTimeNanos();
  STATS_gcs_storage_operation_count.Record(1, "GetKeys");
  return delegate_->AsyncGetKeys(
      table_name, prefix, [start, callback = std::move(callback)](auto result) {
        auto end = absl::GetCurrentTimeNanos();
        STATS_gcs_storage_operation_latency_ms.Record(
            absl::Nanoseconds(end - start) / absl::Milliseconds(1), "GetKeys");
        if (callback) {
          callback(std::move(result));
        }
      });
}

Status ObservableStoreClient::AsyncExists(const std::string &table_name,
                                          const std::string &key,
                                          std::function<void(bool)> callback) {
  auto start = absl::GetCurrentTimeNanos();
  STATS_gcs_storage_operation_count.Record(1, "Exists");
  return delegate_->AsyncExists(
      table_name, key, [start, callback = std::move(callback)](auto result) {
        auto end = absl::GetCurrentTimeNanos();
        STATS_gcs_storage_operation_latency_ms.Record(
            absl::Nanoseconds(end - start) / absl::Milliseconds(1), "Exists");
        if (callback) {
          callback(std::move(result));
        }
      });
}

}  // namespace gcs

}  // namespace ray
