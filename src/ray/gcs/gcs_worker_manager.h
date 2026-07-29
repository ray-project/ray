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

#include <array>
#include <deque>
#include <vector>

#include "ray/gcs/gcs_kv_manager.h"
#include "ray/gcs/gcs_table_storage.h"
#include "ray/gcs/grpc_service_interfaces.h"
#include "ray/gcs/usage_stats_client.h"
#include "ray/pubsub/gcs_publisher.h"
#include "ray/stats/metric.h"

namespace ray {
namespace gcs {

class GcsInitData;

class GcsWorkerManager : public rpc::WorkerInfoGcsServiceHandler {
 public:
  GcsWorkerManager(gcs::GcsTableStorage &gcs_table_storage,
                   instrumented_io_context &io_context,
                   pubsub::GcsPublisher &gcs_publisher)
      : gcs_table_storage_(gcs_table_storage),
        io_context_(io_context),
        gcs_publisher_(gcs_publisher) {}

  void HandleReportWorkerFailure(rpc::ReportWorkerFailureRequest request,
                                 rpc::ReportWorkerFailureReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetWorkerInfo(rpc::GetWorkerInfoRequest request,
                           rpc::GetWorkerInfoReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetAllWorkerInfo(rpc::GetAllWorkerInfoRequest request,
                              rpc::GetAllWorkerInfoReply *reply,
                              rpc::SendReplyCallback send_reply_callback) override;

  void HandleAddWorkerInfo(rpc::AddWorkerInfoRequest request,
                           rpc::AddWorkerInfoReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;

  void HandleUpdateWorkerDebuggerPort(
      rpc::UpdateWorkerDebuggerPortRequest request,
      rpc::UpdateWorkerDebuggerPortReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  void HandleUpdateWorkerNumPausedThreads(
      rpc::UpdateWorkerNumPausedThreadsRequest request,
      rpc::UpdateWorkerNumPausedThreadsReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  void AddWorkerDeadListener(
      std::function<void(std::shared_ptr<rpc::WorkerTableData>)> listener);

  void SetUsageStatsClient(UsageStatsClient *usage_stats_client) {
    usage_stats_client_ = usage_stats_client;
  }

  /**
   * @brief Rebuilds the dead-worker id queue from the worker table on GCS startup and
   * trims it to the retention cap.
   *
   * @param gcs_init_data Metadata loaded from the store at startup, providing the worker
   * table snapshot to rebuild the queue from.
   */
  void RestoreDeadWorkerIdsQueue(const GcsInitData &gcs_init_data);

 private:
  void GetWorkerInfo(const WorkerID &worker_id,
                     Postable<void(std::optional<rpc::WorkerTableData>)> callback) const;

  /**
   * @brief Records a newly dead worker in its priority tier and, when the retention cap
   * is exceeded, evicts the oldest worker from the lowest-priority tier first
   *
   * @param worker_id The id of the worker that just died.
   * @param exit_type How the worker exited; determines its retention priority tier.
   */
  void TrimDeadWorkers(const WorkerID &worker_id, rpc::WorkerExitType exit_type);

  gcs::GcsTableStorage &gcs_table_storage_;
  instrumented_io_context &io_context_;
  pubsub::GcsPublisher &gcs_publisher_;
  UsageStatsClient *usage_stats_client_;

  /// Only listens for unexpected worker deaths not expected like node death.
  std::vector<std::function<void(std::shared_ptr<rpc::WorkerTableData>)>>
      worker_dead_listeners_;

  /// Tracks the number of occurrences of worker crash due to system error
  int32_t worker_crash_system_error_count_ = 0;

  /// Tracks the number of occurrences of worker crash due to OOM
  int32_t worker_crash_oom_count_ = 0;

  /// Ray metrics
  ray::stats::Count ray_metric_unintentional_worker_failures_{
      /*name=*/"unintentional_worker_failures_total",
      /*description=*/
      "Number of worker failures that are not intentional. For example, worker failures "
      "due to system related errors.",
      /*unit=*/""};

  /// Total dead workers retained across all priority tiers
  size_t TotalDeadWorkers() const {
    size_t total = 0;
    for (const auto &tier : dead_workers_by_tier_) {
      total += tier.size();
    }
    return total;
  }

  /// Number of dead-worker retention priority tiers; lower index is evicted first
  /// Right now, we only have two tiers:
  //    0: INTENDED_SYSTEM_EXIT, INTENDED_USER_EXIT
  //    1: USER_ERROR, SYSTEM_ERROR, NODE_OUT_OF_MEMORY
  static constexpr size_t kNumDeadWorkerTiers = 2;

  /// Dead worker ids bucketed by retention priority tier; each tier is FIFO (oldest at
  /// front). Bounds retention in the worker table. Only accessed on io_context_.
  std::array<std::deque<WorkerID>, kNumDeadWorkerTiers> dead_workers_by_tier_;
};

}  // namespace gcs
}  // namespace ray
