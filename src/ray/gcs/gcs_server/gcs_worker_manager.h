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

#include "ray/gcs/gcs_server/gcs_kv_manager.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/gcs_server/usage_stats_client.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"
#include "ray/util/counter_map.h"
#include "ray/gcs/gcs_server/gcs_init_data.h"

namespace ray {
namespace gcs {

/// This implementation class of `WorkerInfoHandler`.
class GcsWorkerManager : public rpc::WorkerInfoHandler {
 public:
  explicit GcsWorkerManager(size_t max_num_worker_events,
                            gcs::GcsTableStorage &gcs_table_storage,
                            GcsPublisher &gcs_publisher)
      : gcs_table_storage_(gcs_table_storage),
        gcs_publisher_(gcs_publisher),
        max_num_dead_workers_(max_num_dead_workers) {}

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

  /// Initialize with the gcs tables data synchronously.
  /// This should be called when GCS server restarts after a failure.
  ///
  /// \param gcs_init_data.
  void Initialize(const GcsInitData &gcs_init_data);

  void AddWorkerDeadListener(
      std::function<void(std::shared_ptr<WorkerTableData>)> listener);

  void SetUsageStatsClient(UsageStatsClient *usage_stats_client) {
    usage_stats_client_ = usage_stats_client;
  }

 private:
  void GetWorkerInfo(
      const WorkerID &worker_id,
      std::function<void(const std::optional<WorkerTableData> &)> callback) const;

  gcs::GcsTableStorage &gcs_table_storage_;
  GcsPublisher &gcs_publisher_;
  UsageStatsClient *usage_stats_client_;
  std::vector<std::function<void(std::shared_ptr<WorkerTableData>)>>
      worker_dead_listeners_;

  /// A deque where workers are store as pairs of (WorkerID, Timestamp).
  /// The workers are sorted according to the timestamp, and the oldest is at the head of the deque.
  /// @note The pair consists of:
  ///   - first: WorkerID (identifier for the worker)
  ///   - second: Timestamp (time when the worker was last updated or added)
  std::deque<std::pair<WorkerID, int64_t>> sorted_dead_worker_deque_;

  /// Max number of dead workers allowed in the storage.
  size_t max_num_dead_workers_;

  /// Tracks the number of occurences of worker crash due to system error
  int32_t worker_crash_system_error_count_ = 0;

  /// Tracks the number of occurences of worker crash due to OOM
  int32_t worker_crash_oom_count_ = 0;
};

}  // namespace gcs
}  // namespace ray
