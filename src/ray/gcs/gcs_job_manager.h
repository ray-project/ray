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

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "ray/common/runtime_env_manager.h"
#include "ray/core_worker_rpc_client/core_worker_client_pool.h"
#include "ray/gcs/gcs_function_manager.h"
#include "ray/gcs/gcs_init_data.h"
#include "ray/gcs/gcs_kv_manager.h"
#include "ray/gcs/gcs_table_storage.h"
#include "ray/gcs/grpc_service_interfaces.h"
#include "ray/observability/ray_event_recorder_interface.h"
#include "ray/pubsub/gcs_publisher.h"
#include "ray/util/event.h"
#include "ray/util/thread_checker.h"

namespace ray {
namespace gcs {

// Please keep this in sync with the definition in ray_constants.py.
const std::string kRayInternalNamespacePrefix = "_ray_internal_";  // NOLINT

// Please keep these in sync with the definition in dashboard/modules/job/common.py.
// NOLINTNEXTLINE
const std::string kJobDataKeyPrefix = kRayInternalNamespacePrefix + "job_info_";
inline std::string JobDataKey(const std::string &submission_id) {
  return kJobDataKeyPrefix + submission_id;
}

using JobFinishListenerCallback =
    rpc::JobInfoGcsServiceHandler::JobFinishListenerCallback;

class GcsJobManager : public rpc::JobInfoGcsServiceHandler {
 public:
  explicit GcsJobManager(GcsTableStorage &gcs_table_storage,
                         pubsub::GcsPublisher &gcs_publisher,
                         RuntimeEnvManager &runtime_env_manager,
                         GCSFunctionManager &function_manager,
                         InternalKVInterface &internal_kv,
                         instrumented_io_context &io_context,
                         rpc::CoreWorkerClientPool &worker_client_pool,
                         observability::RayEventRecorderInterface &ray_event_recorder,
                         const std::string &session_name)
      : gcs_table_storage_(gcs_table_storage),
        gcs_publisher_(gcs_publisher),
        runtime_env_manager_(runtime_env_manager),
        function_manager_(function_manager),
        internal_kv_(internal_kv),
        io_context_(io_context),
        worker_client_pool_(worker_client_pool),
        ray_event_recorder_(ray_event_recorder),
        session_name_(session_name),
        export_event_write_enabled_(IsExportAPIEnabledDriverJob()) {}

  void Initialize(const GcsInitData &gcs_init_data);

  void HandleAddJob(rpc::AddJobRequest request,
                    rpc::AddJobReply *reply,
                    rpc::SendReplyCallback send_reply_callback) override;

  void HandleMarkJobFinished(rpc::MarkJobFinishedRequest request,
                             rpc::MarkJobFinishedReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetAllJobInfo(rpc::GetAllJobInfoRequest request,
                           rpc::GetAllJobInfoReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;

  void HandleReportJobError(rpc::ReportJobErrorRequest request,
                            rpc::ReportJobErrorReply *reply,
                            rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetNextJobID(rpc::GetNextJobIDRequest request,
                          rpc::GetNextJobIDReply *reply,
                          rpc::SendReplyCallback send_reply_callback) override;

  void AddJobFinishedListener(JobFinishListenerCallback listener) override;

  std::shared_ptr<rpc::JobConfig> GetJobConfig(const JobID &job_id) const;

  /// Handle a node death. This will marks all jobs associated with the
  /// specified node id as finished.
  ///
  /// \param node_id The specified node id.
  void OnNodeDead(const NodeID &node_id);

  void WriteDriverJobExportEvent(rpc::JobTableData job_data,
                                 rpc::events::DriverJobLifecycleEvent::State state) const;

  // Verify if export events should be written for EXPORT_DRIVER_JOB source types
  bool IsExportAPIEnabledDriverJob() const {
    return IsExportAPIEnabledSourceType(
        "EXPORT_DRIVER_JOB",
        RayConfig::instance().enable_export_api_write(),
        RayConfig::instance().enable_export_api_write_config());
  }

  /// Record metrics.
  /// For job manager, (1) running jobs count gauge and (2) new finished jobs (whether
  /// succeed or fail) will be reported periodically.
  void RecordMetrics();

 private:
  void ClearJobInfos(const rpc::JobTableData &job_data);

  void MarkJobAsFinished(rpc::JobTableData job_table_data,
                         std::function<void(Status)> done_callback);

  // Used to validate invariants for threading; for example, all callbacks are executed on
  // the same thread.
  ThreadChecker thread_checker_;

  // Running Job Start Times, used to report metrics.
  // Maps JobID to job start time in milliseconds since epoch.
  absl::flat_hash_map<JobID, int64_t> running_job_start_times_;

  // Number of finished jobs since start of this GCS Server, used to report metrics.
  int64_t finished_jobs_count_ = 0;

  GcsTableStorage &gcs_table_storage_;
  pubsub::GcsPublisher &gcs_publisher_;

  /// Listeners which monitors the finish of jobs.
  std::vector<JobFinishListenerCallback> job_finished_listeners_;

  /// A cached mapping from job id to job config.
  absl::flat_hash_map<JobID, std::shared_ptr<rpc::JobConfig>> cached_job_configs_;

  ray::RuntimeEnvManager &runtime_env_manager_;
  GCSFunctionManager &function_manager_;
  InternalKVInterface &internal_kv_;
  instrumented_io_context &io_context_;
  rpc::CoreWorkerClientPool &worker_client_pool_;
  observability::RayEventRecorderInterface &ray_event_recorder_;
  std::string session_name_;

  /// If true, driver job events are exported for Export API
  bool export_event_write_enabled_ = false;
};

}  // namespace gcs
}  // namespace ray
