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
#include "ray/gcs/gcs_server/gcs_object_manager.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace gcs {

/// This implementation class of `JobInfoHandler`.
class GcsJobManager : public rpc::JobInfoHandler {
 public:
  explicit GcsJobManager(std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
                         std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub,
                         RuntimeEnvManager &runtime_env_manager)
      : gcs_table_storage_(std::move(gcs_table_storage)),
        gcs_pub_sub_(std::move(gcs_pub_sub)),
        runtime_env_manager_(runtime_env_manager) {}

  void Initialize(const GcsInitData &gcs_init_data);

  void HandleAddJob(const rpc::AddJobRequest &request, rpc::AddJobReply *reply,
                    rpc::SendReplyCallback send_reply_callback) override;

  void HandleMarkJobFinished(const rpc::MarkJobFinishedRequest &request,
                             rpc::MarkJobFinishedReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetAllJobInfo(const rpc::GetAllJobInfoRequest &request,
                           rpc::GetAllJobInfoReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;

  void HandleReportJobError(const rpc::ReportJobErrorRequest &request,
                            rpc::ReportJobErrorReply *reply,
                            rpc::SendReplyCallback send_reply_callback) override;

  void AddJobFinishedListener(
      std::function<void(std::shared_ptr<JobID>)> listener) override;

  std::string GetRayNamespace(const JobID &job_id) const;

 private:
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub_;

  /// Listeners which monitors the finish of jobs.
  std::vector<std::function<void(std::shared_ptr<JobID>)>> job_finished_listeners_;

  /// A cached mapping from job id to namespace.
  std::unordered_map<JobID, std::string> ray_namespaces_;

  ray::RuntimeEnvManager &runtime_env_manager_;
  void ClearJobInfos(const JobID &job_id);
};

}  // namespace gcs
}  // namespace ray
