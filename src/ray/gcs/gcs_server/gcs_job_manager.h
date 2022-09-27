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

#include "ray/common/runtime_env_manager.h"
#include "ray/gcs/gcs_server/gcs_function_manager.h"
#include "ray/gcs/gcs_server/gcs_init_data.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace gcs {

/// This implementation class of `JobInfoHandler`.
class GcsJobManager : public rpc::JobInfoHandler {
 public:
  explicit GcsJobManager(std::shared_ptr<GcsTableStorage> gcs_table_storage,
                         std::shared_ptr<GcsPublisher> gcs_publisher,
                         RuntimeEnvManager &runtime_env_manager,
                         GcsFunctionManager &function_manager)
      : gcs_table_storage_(std::move(gcs_table_storage)),
        gcs_publisher_(std::move(gcs_publisher)),
        runtime_env_manager_(runtime_env_manager),
        function_manager_(function_manager) {}

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

  void AddJobFinishedListener(
      std::function<void(std::shared_ptr<JobID>)> listener) override;

  std::shared_ptr<rpc::JobConfig> GetJobConfig(const JobID &job_id) const;

 private:
  std::shared_ptr<GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<GcsPublisher> gcs_publisher_;

  /// Listeners which monitors the finish of jobs.
  std::vector<std::function<void(std::shared_ptr<JobID>)>> job_finished_listeners_;

  /// A cached mapping from job id to job config.
  absl::flat_hash_map<JobID, std::shared_ptr<rpc::JobConfig>> cached_job_configs_;

  ray::RuntimeEnvManager &runtime_env_manager_;
  GcsFunctionManager &function_manager_;
  void ClearJobInfos(const JobID &job_id);

  void MarkJobAsFinished(rpc::JobTableData job_table_data,
                         std::function<void(Status)> done_callback);
};

}  // namespace gcs
}  // namespace ray
