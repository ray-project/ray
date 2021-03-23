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
                         std::shared_ptr<gcs::GcsNodeManager> gcs_node_manager)
      : gcs_table_storage_(std::move(gcs_table_storage)),
        gcs_pub_sub_(std::move(gcs_pub_sub)),
        gcs_node_manager_(std::move(gcs_node_manager)) {}

  /// Initialize with the gcs tables data synchronously.
  /// This should be called when GCS server restarts after a failure.
  ///
  /// \param gcs_init_data.
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

  void HandleSubmitJob(const rpc::SubmitJobRequest &request, rpc::SubmitJobReply *reply,
                       rpc::SendReplyCallback send_reply_callback) override;

  void AddJobFinishedListener(
      std::function<void(std::shared_ptr<JobID>)> listener) override;

 private:
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub_;

  /// The shared pointer of GcsNodeManager to select driver.
  std::shared_ptr<gcs::GcsNodeManager> gcs_node_manager_;

  /// Map from job ID to the JobTableData.
  absl::flat_hash_map<JobID, std::shared_ptr<JobTableData>> jobs_;

  /// Map from driver node ID to the IDs of jobs associated with the driver.
  absl::flat_hash_map<NodeID, absl::flat_hash_set<JobID>> driver_node_to_jobs_;

  /// Listeners which monitors the finish of jobs.
  std::vector<std::function<void(std::shared_ptr<JobID>)>> job_finished_listeners_;

  void ClearJobInfos(const JobID &job_id);

  absl::optional<std::shared_ptr<rpc::GcsNodeInfo>> SelectDriver(
      const rpc::JobTableData &job_data) const;

  Status UpdateJobStateToDead(std::shared_ptr<JobTableData> job_table_data,
                              const ray::gcs::StatusCallback &callback);
};

}  // namespace gcs
}  // namespace ray
