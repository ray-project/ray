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

#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ray/gcs/store_client/store_client.h"
#include "ray/observability/metric_interface.h"
#include "ray/util/clock.h"

namespace ray {

namespace gcs {

/// Wraps `delegate` in an ObservableStoreClient when `enabled`, and returns it
/// unchanged otherwise.
///
/// Exists as a named function, and takes `enabled` rather than reading
/// RayConfig itself, so that both GCS construction sites share one branch and
/// so that the branch is testable without standing up a GcsServer.
///
/// \param delegate The store client to wrap.
/// \param enabled Whether to observe. GcsServer passes
/// RayConfig::gcs_redis_storage_metrics_enabled() for the Redis backend; the
/// in-memory and RocksDB backends are always observed and do not call this.
/// \param storage_operation_latency_in_ms_histogram Sink for per-operation
/// latency, recorded when each operation completes.
/// \param storage_operation_count_counter Sink for per-operation counts,
/// recorded when each operation is issued.
/// \param clock Clock used to measure operation latency.
/// \return Either an ObservableStoreClient owning `delegate`, or `delegate`.
std::shared_ptr<StoreClient> MaybeObserve(
    std::shared_ptr<StoreClient> delegate,
    bool enabled,
    ray::observability::MetricInterface &storage_operation_latency_in_ms_histogram,
    ray::observability::MetricInterface &storage_operation_count_counter,
    ClockInterface &clock);

/// Wraps around a StoreClient instance and observe the metrics.
class ObservableStoreClient : public StoreClient {
 public:
  /// \param delegate The store client to observe. Taken as a shared_ptr rather
  /// than a unique_ptr because the Redis backend has to be reachable
  /// concretely as well: AsyncCheckHealth is declared on RedisStoreClient and
  /// not on this interface, so GcsServer keeps its own pointer for the periodic
  /// health check while handing a copy here. std::unique_ptr converts
  /// implicitly, so the in-memory and RocksDB construction sites are unchanged.
  /// \param storage_operation_latency_in_ms_histogram Sink for per-operation
  /// latency, recorded when each operation completes.
  /// \param storage_operation_count_counter Sink for per-operation counts,
  /// recorded when each operation is issued.
  /// \param clock Clock used to measure operation latency.
  explicit ObservableStoreClient(
      std::shared_ptr<StoreClient> delegate,
      ray::observability::MetricInterface &storage_operation_latency_in_ms_histogram,
      ray::observability::MetricInterface &storage_operation_count_counter,
      ClockInterface &clock)
      : delegate_(std::move(delegate)),
        storage_operation_latency_in_ms_histogram_(
            storage_operation_latency_in_ms_histogram),
        storage_operation_count_counter_(storage_operation_count_counter),
        clock_(clock) {}

  void AsyncPut(const std::string &table_name,
                const std::string &key,
                std::string data,
                bool overwrite,
                Postable<void(bool)> callback) override;

  void AsyncGet(const std::string &table_name,
                const std::string &key,
                ToPostable<rpc::OptionalItemCallback<std::string>> callback) override;

  void AsyncGetAll(
      const std::string &table_name,
      Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) override;

  void AsyncMultiGet(
      const std::string &table_name,
      const std::vector<std::string> &keys,
      Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) override;

  void AsyncDelete(const std::string &table_name,
                   const std::string &key,
                   Postable<void(bool)> callback) override;

  void AsyncBatchDelete(const std::string &table_name,
                        const std::vector<std::string> &keys,
                        Postable<void(int64_t)> callback) override;

  void AsyncGetNextJobID(Postable<void(int)> callback) override;

  void AsyncGetKeys(const std::string &table_name,
                    const std::string &prefix,
                    Postable<void(std::vector<std::string>)> callback) override;

  void AsyncExists(const std::string &table_name,
                   const std::string &key,
                   Postable<void(bool)> callback) override;

 private:
  std::shared_ptr<StoreClient> delegate_;
  ray::observability::MetricInterface &storage_operation_latency_in_ms_histogram_;
  ray::observability::MetricInterface &storage_operation_count_counter_;
  ClockInterface &clock_;
};

}  // namespace gcs

}  // namespace ray
