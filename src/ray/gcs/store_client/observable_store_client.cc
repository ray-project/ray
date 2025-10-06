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

#include <string>
#include <utility>
#include <vector>

#include "absl/time/time.h"
#include "ray/stats/metric_defs.h"

using std::literals::operator""sv;

namespace ray {
namespace gcs {

void ObservableStoreClient::AsyncPut(const std::string &table_name,
                                     const std::string &key,
                                     std::string data,
                                     bool overwrite,
                                     Postable<void(bool)> callback) {
  auto start = absl::GetCurrentTimeNanos();
  storage_operation_count_counter_.Record(1, {{"Operation"sv, "Put"}});
  delegate_->AsyncPut(table_name,
                      key,
                      std::move(data),
                      overwrite,
                      std::move(callback).OnInvocation([this, start]() {
                        auto end = absl::GetCurrentTimeNanos();
                        storage_operation_latency_in_ms_histogram_.Record(
                            absl::ToDoubleMilliseconds(absl::Nanoseconds(end - start)),
                            {{"Operation"sv, "Put"}});
                      }));
}

void ObservableStoreClient::AsyncGet(
    const std::string &table_name,
    const std::string &key,
    ToPostable<OptionalItemCallback<std::string>> callback) {
  auto start = absl::GetCurrentTimeNanos();
  storage_operation_count_counter_.Record(1, {{"Operation"sv, "Get"}});
  delegate_->AsyncGet(table_name, key, std::move(callback).OnInvocation([this, start]() {
    auto end = absl::GetCurrentTimeNanos();
    storage_operation_latency_in_ms_histogram_.Record(
        absl::ToDoubleMilliseconds(absl::Nanoseconds(end - start)),
        {{"Operation"sv, "Get"}});
  }));
}

void ObservableStoreClient::AsyncGetAll(
    const std::string &table_name,
    Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) {
  auto start = absl::GetCurrentTimeNanos();
  storage_operation_count_counter_.Record(1, {{"Operation"sv, "GetAll"}});
  delegate_->AsyncGetAll(table_name, std::move(callback).OnInvocation([this, start]() {
    auto end = absl::GetCurrentTimeNanos();
    storage_operation_latency_in_ms_histogram_.Record(
        absl::ToDoubleMilliseconds(absl::Nanoseconds(end - start)),
        {{"Operation"sv, "GetAll"}});
  }));
}

void ObservableStoreClient::AsyncMultiGet(
    const std::string &table_name,
    const std::vector<std::string> &keys,
    Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) {
  auto start = absl::GetCurrentTimeNanos();
  storage_operation_count_counter_.Record(1, {{"Operation"sv, "MultiGet"}});
  delegate_->AsyncMultiGet(
      table_name, keys, std::move(callback).OnInvocation([this, start]() {
        auto end = absl::GetCurrentTimeNanos();
        storage_operation_latency_in_ms_histogram_.Record(
            absl::ToDoubleMilliseconds(absl::Nanoseconds(end - start)),
            {{"Operation"sv, "MultiGet"}});
      }));
}

void ObservableStoreClient::AsyncDelete(const std::string &table_name,
                                        const std::string &key,
                                        Postable<void(bool)> callback) {
  auto start = absl::GetCurrentTimeNanos();
  storage_operation_count_counter_.Record(1, {{"Operation"sv, "Delete"}});
  delegate_->AsyncDelete(
      table_name, key, std::move(callback).OnInvocation([this, start]() {
        auto end = absl::GetCurrentTimeNanos();
        storage_operation_latency_in_ms_histogram_.Record(
            absl::ToDoubleMilliseconds(absl::Nanoseconds(end - start)),
            {{"Operation"sv, "Delete"}});
      }));
}

void ObservableStoreClient::AsyncBatchDelete(const std::string &table_name,
                                             const std::vector<std::string> &keys,
                                             Postable<void(int64_t)> callback) {
  auto start = absl::GetCurrentTimeNanos();
  storage_operation_count_counter_.Record(1, {{"Operation"sv, "BatchDelete"}});
  delegate_->AsyncBatchDelete(
      table_name, keys, std::move(callback).OnInvocation([this, start]() {
        auto end = absl::GetCurrentTimeNanos();
        storage_operation_latency_in_ms_histogram_.Record(
            absl::ToDoubleMilliseconds(absl::Nanoseconds(end - start)),
            {{"Operation"sv, "BatchDelete"}});
      }));
}

void ObservableStoreClient::AsyncGetNextJobID(Postable<void(int)> callback) {
  delegate_->AsyncGetNextJobID(std::move(callback));
}

void ObservableStoreClient::AsyncGetKeys(
    const std::string &table_name,
    const std::string &prefix,
    Postable<void(std::vector<std::string>)> callback) {
  auto start = absl::GetCurrentTimeNanos();
  storage_operation_count_counter_.Record(1, {{"Operation"sv, "GetKeys"}});
  delegate_->AsyncGetKeys(
      table_name, prefix, std::move(callback).OnInvocation([this, start]() {
        auto end = absl::GetCurrentTimeNanos();
        storage_operation_latency_in_ms_histogram_.Record(
            absl::ToDoubleMilliseconds(absl::Nanoseconds(end - start)),
            {{"Operation"sv, "GetKeys"}});
      }));
}

void ObservableStoreClient::AsyncExists(const std::string &table_name,
                                        const std::string &key,
                                        Postable<void(bool)> callback) {
  auto start = absl::GetCurrentTimeNanos();
  storage_operation_count_counter_.Record(1, {{"Operation"sv, "Exists"}});
  delegate_->AsyncExists(
      table_name, key, std::move(callback).OnInvocation([this, start]() {
        auto end = absl::GetCurrentTimeNanos();
        storage_operation_latency_in_ms_histogram_.Record(
            absl::ToDoubleMilliseconds(absl::Nanoseconds(end - start)),
            {{"Operation"sv, "Exists"}});
      }));
}

}  // namespace gcs

}  // namespace ray
