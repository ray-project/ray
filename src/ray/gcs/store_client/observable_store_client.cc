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

#include <chrono>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "ray/gcs/store_client/table_name_label.h"

namespace ray {
namespace gcs {

namespace {

// Both tags for one recording site. `table` must already be normalized (see
// NormalizeTableNameLabel) so that TableName stays inside a bounded domain.
//
// MetricInterface::Record takes the tag value as an owned std::string, so the
// copy here is unavoidable. Every table name a StoreClient caller can actually
// produce is at most 15 characters (PLACEMENT_GROUP and ACTOR_TASK_SPEC are the
// longest), which fits libstdc++'s small-string buffer, so in practice it does
// not allocate. The label domain is wider than that -- NormalizeTableNameLabel
// accepts every rpc::TablePrefix name, and four of those exceed 15 characters,
// PLACEMENT_GROUP_SCHEDULE being the longest at 24 -- but none of them is
// reachable from any current caller, and reaching one would cost a small
// allocation rather than a wrong label. TableNameLabelTest pins the
// reachable-name bound so this comment cannot rot silently.
std::vector<std::pair<std::string_view, std::string>> MetricTags(
    std::string_view operation, std::string_view table) {
  return {{"Operation", std::string(operation)}, {"TableName", std::string(table)}};
}

// Milliseconds elapsed since `start`. Durations are measured on the monotonic
// clock, per the ClockInterface contract: Now()/NowUnixNanos() is wall time and
// can jump with NTP -- backwards, which would record a negative latency, or
// forwards, which would inflate one -- while SteadyNow() never goes backwards.
double ElapsedMs(ClockInterface &clock, SteadyTimePoint start) {
  return std::chrono::duration<double, std::milli>(clock.SteadyNow() - start).count();
}

}  // namespace

std::shared_ptr<StoreClient> MaybeObserve(
    std::shared_ptr<StoreClient> delegate,
    bool enabled,
    ray::observability::MetricInterface &storage_operation_latency_in_ms_histogram,
    ray::observability::MetricInterface &storage_operation_count_counter,
    ClockInterface &clock) {
  if (!enabled) {
    return delegate;
  }
  return std::make_shared<ObservableStoreClient>(
      std::move(delegate),
      storage_operation_latency_in_ms_histogram,
      storage_operation_count_counter,
      clock);
}

void ObservableStoreClient::AsyncPut(const std::string &table_name,
                                     const std::string &key,
                                     std::string data,
                                     bool overwrite,
                                     Postable<void(bool)> callback) {
  const auto start = clock_.SteadyNow();
  // Normalized once per operation and captured as a view, not as a string: the
  // observer below runs on the io_context after this call has returned, so a
  // view borrowed from `table_name` could outlive the caller's string and
  // dangle. NormalizeTableNameLabel returns a view into protobuf's static
  // descriptor storage instead, which is why capturing it costs no allocation
  // and carries no lifetime risk.
  const std::string_view table = NormalizeTableNameLabel(table_name);
  storage_operation_count_counter_.Record(1, MetricTags("Put", table));
  delegate_->AsyncPut(table_name,
                      key,
                      std::move(data),
                      overwrite,
                      std::move(callback).OnInvocation([this, start, table]() {
                        storage_operation_latency_in_ms_histogram_.Record(
                            ElapsedMs(clock_, start), MetricTags("Put", table));
                      }));
}

void ObservableStoreClient::AsyncGet(
    const std::string &table_name,
    const std::string &key,
    ToPostable<rpc::OptionalItemCallback<std::string>> callback) {
  const auto start = clock_.SteadyNow();
  const std::string_view table = NormalizeTableNameLabel(table_name);
  storage_operation_count_counter_.Record(1, MetricTags("Get", table));
  delegate_->AsyncGet(
      table_name, key, std::move(callback).OnInvocation([this, start, table]() {
        storage_operation_latency_in_ms_histogram_.Record(ElapsedMs(clock_, start),
                                                          MetricTags("Get", table));
      }));
}

void ObservableStoreClient::AsyncGetAll(
    const std::string &table_name,
    Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) {
  const auto start = clock_.SteadyNow();
  const std::string_view table = NormalizeTableNameLabel(table_name);
  storage_operation_count_counter_.Record(1, MetricTags("GetAll", table));
  delegate_->AsyncGetAll(table_name,
                         std::move(callback).OnInvocation([this, start, table]() {
                           storage_operation_latency_in_ms_histogram_.Record(
                               ElapsedMs(clock_, start), MetricTags("GetAll", table));
                         }));
}

void ObservableStoreClient::AsyncMultiGet(
    const std::string &table_name,
    const std::vector<std::string> &keys,
    Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) {
  const auto start = clock_.SteadyNow();
  const std::string_view table = NormalizeTableNameLabel(table_name);
  storage_operation_count_counter_.Record(1, MetricTags("MultiGet", table));
  delegate_->AsyncMultiGet(
      table_name, keys, std::move(callback).OnInvocation([this, start, table]() {
        storage_operation_latency_in_ms_histogram_.Record(ElapsedMs(clock_, start),
                                                          MetricTags("MultiGet", table));
      }));
}

void ObservableStoreClient::AsyncDelete(const std::string &table_name,
                                        const std::string &key,
                                        Postable<void(bool)> callback) {
  const auto start = clock_.SteadyNow();
  const std::string_view table = NormalizeTableNameLabel(table_name);
  storage_operation_count_counter_.Record(1, MetricTags("Delete", table));
  delegate_->AsyncDelete(
      table_name, key, std::move(callback).OnInvocation([this, start, table]() {
        storage_operation_latency_in_ms_histogram_.Record(ElapsedMs(clock_, start),
                                                          MetricTags("Delete", table));
      }));
}

void ObservableStoreClient::AsyncBatchDelete(const std::string &table_name,
                                             const std::vector<std::string> &keys,
                                             Postable<void(int64_t)> callback) {
  const auto start = clock_.SteadyNow();
  const std::string_view table = NormalizeTableNameLabel(table_name);
  storage_operation_count_counter_.Record(1, MetricTags("BatchDelete", table));
  delegate_->AsyncBatchDelete(
      table_name, keys, std::move(callback).OnInvocation([this, start, table]() {
        storage_operation_latency_in_ms_histogram_.Record(
            ElapsedMs(clock_, start), MetricTags("BatchDelete", table));
      }));
}

void ObservableStoreClient::AsyncGetNextJobID(Postable<void(int)> callback) {
  const auto start = clock_.SteadyNow();
  // This operation addresses a plain counter key rather than a GCS table, so it
  // carries the explicit kJobCounterTable sentinel. It is labeled like every
  // other site because both metric stacks export a declared tag that a site
  // does not record with an empty value rather than omitting it, and an
  // unexplained TableName="" is worse on a dashboard than a named sentinel.
  storage_operation_count_counter_.Record(1,
                                          MetricTags("GetNextJobID", kJobCounterTable));
  delegate_->AsyncGetNextJobID(std::move(callback).OnInvocation([this, start]() {
    storage_operation_latency_in_ms_histogram_.Record(
        ElapsedMs(clock_, start), MetricTags("GetNextJobID", kJobCounterTable));
  }));
}

void ObservableStoreClient::AsyncGetKeys(
    const std::string &table_name,
    const std::string &prefix,
    Postable<void(std::vector<std::string>)> callback) {
  const auto start = clock_.SteadyNow();
  const std::string_view table = NormalizeTableNameLabel(table_name);
  storage_operation_count_counter_.Record(1, MetricTags("GetKeys", table));
  delegate_->AsyncGetKeys(
      table_name, prefix, std::move(callback).OnInvocation([this, start, table]() {
        storage_operation_latency_in_ms_histogram_.Record(ElapsedMs(clock_, start),
                                                          MetricTags("GetKeys", table));
      }));
}

void ObservableStoreClient::AsyncExists(const std::string &table_name,
                                        const std::string &key,
                                        Postable<void(bool)> callback) {
  const auto start = clock_.SteadyNow();
  const std::string_view table = NormalizeTableNameLabel(table_name);
  storage_operation_count_counter_.Record(1, MetricTags("Exists", table));
  delegate_->AsyncExists(
      table_name, key, std::move(callback).OnInvocation([this, start, table]() {
        storage_operation_latency_in_ms_histogram_.Record(ElapsedMs(clock_, start),
                                                          MetricTags("Exists", table));
      }));
}

}  // namespace gcs

}  // namespace ray
