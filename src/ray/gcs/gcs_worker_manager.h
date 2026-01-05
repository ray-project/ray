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

#include <vector>

#include "ray/gcs/gcs_kv_manager.h"
#include "ray/gcs/gcs_table_storage.h"
#include "ray/gcs/grpc_service_interfaces.h"
#include "ray/gcs/usage_stats_client.h"
#include "ray/pubsub/gcs_publisher.h"
#include "ray/stats/metric.h"

namespace ray {
namespace gcs {

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

 private:
  void GetWorkerInfo(const WorkerID &worker_id,
                     Postable<void(std::optional<rpc::WorkerTableData>)> callback) const;

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
};

}  // namespace gcs
}  // namespace ray
