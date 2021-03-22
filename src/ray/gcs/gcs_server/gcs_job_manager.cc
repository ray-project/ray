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

#include "ray/gcs/gcs_server/gcs_job_manager.h"

#include "ray/gcs/pb_util.h"

namespace ray {
namespace gcs {

void GcsJobManager::Initialize(const GcsInitData &gcs_init_data) {
  for (auto &item : gcs_init_data.Jobs()) {
    auto job_data = std::make_shared<JobTableData>(item.second);
    // Add job data to local cache.
    jobs_.emplace(item.first, job_data);
  }
}

void GcsJobManager::HandleAddJob(const rpc::AddJobRequest &request,
                                 rpc::AddJobReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) {
  JobID job_id = JobID::FromBinary(request.data().job_id());
  auto driver_pid = request.data().driver_pid();
  RAY_LOG(INFO) << "Adding job, job id = " << job_id << ", driver pid = " << driver_pid
                << ", config is:\n"
                << request.data().config().DebugString();

  std::shared_ptr<JobTableData> job_table_data;

  auto iter = jobs_.find(job_id);
  if (iter != jobs_.end()) {
    // The job state should be RUNNING or SUBMITTED.
    const auto state = iter->second->state();
    if (iter->second->timestamp() == request.data().timestamp() &&
        state == rpc::JobTableData::RUNNING) {
      // It is a duplicated message, just reply ok.
      GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
      return;
    } else if (state != rpc::JobTableData::SUBMITTED) {
      std::ostringstream ostr;
      ostr << "Failed to add job " << job_id
           << " as job id conflicts or state is unexpected.";
      RAY_LOG(WARNING) << ostr.str();
      GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::Invalid(ostr.str()));
      return;
    }
    RAY_CHECK(state == rpc::JobTableData::RUNNING ||
              state == rpc::JobTableData::SUBMITTED);
    job_table_data = iter->second;
  } else {
    job_table_data = std::make_shared<JobTableData>();
  }

  // Reserve these fields that may be filled when submitting.
  auto is_submitted = job_table_data->is_submitted();
  auto job_payload = job_table_data->job_payload();

  // Just use the job_table_data come from raylet.
  job_table_data->CopyFrom(request.data());
  job_table_data->set_state(rpc::JobTableData::RUNNING);

  // Recover reserved fields.
  job_table_data->set_is_submitted(is_submitted);
  job_table_data->set_job_payload(job_payload);

  auto on_done = [this, job_table_data, driver_pid, reply,
                  send_reply_callback](const Status &status) {
    RAY_CHECK_OK(status);
    auto job_id = JobID::FromBinary(job_table_data->job_id());
    jobs_.emplace(job_id, job_table_data);
    RAY_CHECK_OK(gcs_pub_sub_->Publish(JOB_CHANNEL, job_id.Hex(),
                                       job_table_data->SerializeAsString(), nullptr));
    RAY_LOG(INFO) << "Finished adding job, job id = " << job_id
                  << ", driver pid = " << driver_pid;
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };
  RAY_CHECK_OK(gcs_table_storage_->JobTable().Put(job_id, *job_table_data, on_done));
}

void GcsJobManager::HandleMarkJobFinished(const rpc::MarkJobFinishedRequest &request,
                                          rpc::MarkJobFinishedReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  JobID job_id = JobID::FromBinary(request.job_id());
  RAY_LOG(INFO) << "Received driver exit notification, job id = " << job_id;
  auto iter = jobs_.find(job_id);
  if (iter == jobs_.end()) {
    RAY_LOG(WARNING) << "Failed to handle the notification of driver exit. job id = "
                     << job_id;
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::Invalid("Invalid job id."));
    return;
  }
  auto job_table_data = iter->second;
  if (job_table_data->is_dead()) {
    RAY_LOG(INFO) << "Job is already dead, just ignore this notification, job id = "
                  << job_id;
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
    return;
  }

  RAY_CHECK_OK(UpdateJobStateToDead(
      job_table_data, [send_reply_callback, reply](const Status &status) {
        RAY_CHECK_OK(status);
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
      }));
}

void GcsJobManager::ClearJobInfos(const JobID &job_id) {
  // Notify all listeners.
  for (auto &listener : job_finished_listeners_) {
    listener(std::make_shared<JobID>(job_id));
  }
}

/// Add listener to monitor the add action of nodes.
///
/// \param listener The handler which process the add of nodes.
void GcsJobManager::AddJobFinishedListener(
    std::function<void(std::shared_ptr<JobID>)> listener) {
  RAY_CHECK(listener);
  job_finished_listeners_.emplace_back(std::move(listener));
}

void GcsJobManager::HandleGetAllJobInfo(const rpc::GetAllJobInfoRequest &request,
                                        rpc::GetAllJobInfoReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(INFO) << "Getting all job info.";
  auto on_done = [reply, send_reply_callback](
                     const std::unordered_map<JobID, JobTableData> &result) {
    for (auto &data : result) {
      reply->add_job_info_list()->CopyFrom(data.second);
    }
    RAY_LOG(INFO) << "Finished getting all job info.";
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  };
  Status status = gcs_table_storage_->JobTable().GetAll(on_done);
  if (!status.ok()) {
    on_done(std::unordered_map<JobID, JobTableData>());
  }
}

void GcsJobManager::HandleReportJobError(const rpc::ReportJobErrorRequest &request,
                                         rpc::ReportJobErrorReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  auto job_id = JobID::FromBinary(request.job_error().job_id());
  RAY_CHECK_OK(gcs_pub_sub_->Publish(ERROR_INFO_CHANNEL, job_id.Hex(),
                                     request.job_error().SerializeAsString(), nullptr));
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void GcsJobManager::HandleSubmitJob(const rpc::SubmitJobRequest &request,
                                    rpc::SubmitJobReply *reply,
                                    rpc::SendReplyCallback send_reply_callback) {
  auto on_done = [reply, send_reply_callback](const Status &status) {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };
  auto status = SubmitJob(request, on_done);
  if (!status.ok()) {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  }
}

Status GcsJobManager::SubmitJob(const ray::rpc::SubmitJobRequest &request,
                                const ray::gcs::StatusCallback &callback) {
  auto job_id = JobID::FromBinary(request.job_id());

  RAY_LOG(INFO) << "Submitting job " << job_id;

  auto it = jobs_.find(job_id);
  if (it != jobs_.end()) {
    RAY_LOG(ERROR) << "Failed to submit job " << job_id << ", job id conflicts.";
    std::ostringstream ss;
    ss << "Job id conflicts: " << job_id;
    return Status::Invalid(ss.str());
  }

  // Fill in as much information as possible for the dashboard to
  // show the job info in SUBMITTED state.
  auto job_table_data = std::make_shared<rpc::JobTableData>();
  job_table_data->set_state(rpc::JobTableData::SUBMITTED);
  job_table_data->set_job_id(request.job_id());
  job_table_data->set_language(request.language());
  // Set the job payload (the json submitted from dashboard).
  job_table_data->set_job_payload(request.job_payload());
  // Mark the job is submitted, this field determines whether to
  // initialize the job environement or not.
  job_table_data->set_is_submitted(true);

  auto driver_client_id = SelectDriver(*job_table_data);
  if (driver_client_id.IsNil()) {
    RAY_LOG(ERROR) << "Failed to submit job " << job_id
                   << ", can't select a node for driver.";
    std::ostringstream ss;
    ss << "Can't select a node for driver, job id: " << job_id;
    return Status::Invalid(ss.str());
  }

  job_table_data->set_raylet_id(driver_client_id.Binary());
  auto maybe_node = gcs_node_manager_->GetAliveNode(driver_client_id);
  if (maybe_node.has_value()) {
    auto node = maybe_node.value();
    // The hostname and ip address of the node are the same as the driver.
    job_table_data->set_driver_hostname(node->node_manager_hostname());
    job_table_data->set_driver_ip_address(node->node_manager_address());
  }

  RAY_LOG(INFO) << "Submitting job, job id = " << job_id << ", config is "
                << job_table_data->config().DebugString();
  auto on_done = [this, driver_client_id, job_id, job_table_data,
                  callback](Status status) {
    RAY_CHECK(jobs_.emplace(job_id, job_table_data).second);
    RAY_CHECK_OK(gcs_pub_sub_->Publish(JOB_CHANNEL, job_id.Hex(),
                                       job_table_data->SerializeAsString(), nullptr));
    driver_node_to_jobs_[driver_client_id].emplace(job_id);
    if (callback) {
      callback(status);
    }
    RAY_LOG(INFO) << "Finished submitting job, job id = " << job_id;
  };
  return gcs_table_storage_->JobTable().Put(job_id, *job_table_data, on_done);
}

NodeID GcsJobManager::SelectDriver(const rpc::JobTableData &job_data) const {
  std::vector<NodeID> alive_nodes;
  for (auto &entry : gcs_node_manager_->GetAllAliveNodes()) {
    alive_nodes.emplace_back(entry.first);
  }

  if (alive_nodes.empty()) {
    return NodeID::Nil();
  }

  static std::mt19937_64 gen_(
      std::chrono::high_resolution_clock::now().time_since_epoch().count());
  std::uniform_int_distribution<int> distribution(0, alive_nodes.size() - 1);
  return alive_nodes[distribution(gen_)];
}

Status GcsJobManager::UpdateJobStateToDead(std::shared_ptr<JobTableData> job_table_data,
                                           const ray::gcs::StatusCallback &callback) {
  // Update job state.
  job_table_data->set_state(rpc::JobTableData::FINISHED);
  JobID job_id = JobID::FromBinary(job_table_data->job_id());
  RAY_LOG(INFO) << "Updating job state to "
                << rpc::JobTableData_JobState_Name(job_table_data->state())
                << ", job id = " << job_id;
  job_table_data->set_is_dead(true);
  auto on_done = [this, callback, job_id, job_table_data](const Status &status) {
    RAY_CHECK_OK(status);
    RAY_CHECK_OK(gcs_pub_sub_->Publish(JOB_CHANNEL, job_id.Hex(),
                                       job_table_data->SerializeAsString(), nullptr));
    ClearJobInfos(job_id);
    if (callback) {
      callback(status);
    }
    RAY_LOG(INFO) << "Finished updating job state to "
                  << rpc::JobTableData_JobState_Name(job_table_data->state())
                  << ", job id = " << job_id;
  };
  return gcs_table_storage_->JobTable().Put(job_id, *job_table_data, on_done);
}

}  // namespace gcs
}  // namespace ray
