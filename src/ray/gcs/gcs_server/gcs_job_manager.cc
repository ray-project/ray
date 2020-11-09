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

void GcsJobManager::HandleAddJob(const rpc::AddJobRequest &request,
                                 rpc::AddJobReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) {
  JobID job_id = JobID::FromBinary(request.data().job_id());
  RAY_LOG(INFO) << "Adding job, job id = " << job_id
                << ", driver pid = " << request.data().driver_pid();
  auto on_done = [this, job_id, request, reply,
                  send_reply_callback](const Status &status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to add job, job id = " << job_id
                     << ", driver pid = " << request.data().driver_pid();
    } else {
      RAY_CHECK_OK(gcs_pub_sub_->Publish(JOB_CHANNEL, job_id.Binary(),
                                         request.data().SerializeAsString(), nullptr));
      RAY_LOG(INFO) << "Finished adding job, job id = " << job_id
                    << ", driver pid = " << request.data().driver_pid();
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status status = gcs_table_storage_->JobTable().Put(job_id, request.data(), on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

void GcsJobManager::HandleMarkJobFinished(const rpc::MarkJobFinishedRequest &request,
                                          rpc::MarkJobFinishedReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  JobID job_id = JobID::FromBinary(request.job_id());
  RAY_LOG(INFO) << "Marking job state, job id = " << job_id;
  auto job_table_data =
      gcs::CreateJobTableData(job_id, /*is_dead*/ true, std::time(nullptr), "", -1);
  auto on_done = [this, job_id, job_table_data, reply,
                  send_reply_callback](const Status &status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to mark job state, job id = " << job_id;
    } else {
      RAY_CHECK_OK(gcs_pub_sub_->Publish(JOB_CHANNEL, job_id.Binary(),
                                         job_table_data->SerializeAsString(), nullptr));
      ClearJobInfos(job_id);
      RAY_LOG(INFO) << "Finished marking job state, job id = " << job_id;
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status status = gcs_table_storage_->JobTable().Put(job_id, *job_table_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
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

void GcsJobManager::HandleDropJob(const rpc::DropJobRequest &request,
                                  rpc::DropJobReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) {
  auto on_done = [reply, send_reply_callback](const Status &status) {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };
  auto status = DropJob(request, on_done);
  if (!status.ok()) {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  }
}

Status GcsJobManager::SubmitJob(const ray::rpc::SubmitJobRequest &request,
                                const ray::gcs::StatusCallback &callback) {
  auto job_id = JobID::FromBinary(request.job_id());
  const auto &namespace_id = request.namespace_id();

  RAY_LOG(INFO) << "Starting register job " << job_id
                << " with namespace id = " << namespace_id;

  auto it = jobs_.find(job_id);
  if (it != jobs_.end()) {
    RAY_LOG(ERROR) << "Failed to register job " << job_id << ", job id conflict.";
    std::ostringstream ss;
    ss << "Job id conflict: " << job_id;
    return Status::Invalid(ss.str());
  }

  auto job_table_data = std::make_shared<rpc::JobTableData>();
  job_table_data->set_job_id(request.job_id());
  job_table_data->set_namespace_id(request.namespace_id());
  job_table_data->set_language(request.language());
  job_table_data->set_job_payload(request.job_payload());
  job_table_data->set_state(rpc::JobTableData_JobState_SUBMITTED);

  auto driver_client_id = SelectDriver(*job_table_data);
  if (driver_client_id.IsNil()) {
    RAY_LOG(ERROR) << "Failed to init job " << job_id
                   << " with namespace id = " << namespace_id;
    std::ostringstream ss;
    ss << "Insufficient resources, job id: " << job_id
       << ", namespace id: " << namespace_id;
    return Status::Invalid(ss.str());
  }

  auto node_info = gcs_node_manager_->GetAliveNode(driver_client_id);
  job_table_data->set_driver_hostname(node_info->node_manager_hostname());
  job_table_data->set_driver_ip_address(node_info->node_manager_address());
  job_table_data->set_raylet_id(driver_client_id.Binary());

  RAY_LOG(INFO) << "Submitting job, job id = " << job_id << ", config is "
                << job_table_data->config().DebugString();
  auto on_done = [this, driver_client_id, job_id, job_table_data,
                  callback](Status status) {
    RAY_CHECK(jobs_.emplace(job_id, job_table_data).second);
    RAY_CHECK_OK(gcs_pub_sub_->Publish(JOB_CHANNEL, job_id.Binary(),
                                       job_table_data->SerializeAsString(), nullptr));
    driver_node_to_jobs_[driver_client_id].emplace(job_id);
    if (callback) {
      callback(status);
    }
    RAY_LOG(INFO) << "Finished submitting job, job id = " << job_id
                  << " with namespace id " << job_table_data->namespace_id();
  };
  return gcs_table_storage_->JobTable().Put(job_id, *job_table_data, on_done);
}

Status GcsJobManager::DropJob(const ray::rpc::DropJobRequest &request,
                              const ray::gcs::StatusCallback &callback) {
  auto job_id = JobID::FromBinary(request.job_id());
  RAY_LOG(INFO) << "Starting drop job, job id = " << job_id;

  auto it = jobs_.find(job_id);
  if (it == jobs_.end() || it->second->is_dead()) {
    return Status::Invalid("Job is invalid or is already dead.");
  }

  auto job_table_data = it->second;
  auto driver_node_id = NodeID::FromBinary(job_table_data->raylet_id());
  if (!driver_node_id.IsNil()) {
    auto iter = driver_node_to_jobs_.find(driver_node_id);
    if (iter != driver_node_to_jobs_.end() && iter->second.erase(job_id)) {
      if (iter->second.empty()) {
        driver_node_to_jobs_.erase(iter);
      }
    }
  }

  RAY_LOG(INFO) << "Dropping job, job id = " << job_id;
  job_table_data->set_is_dropped(true);
  return UpdateJobStateToDead(job_table_data, [job_id, callback](const Status &status) {
    RAY_CHECK_OK(status);
    callback(status);
    RAY_LOG(INFO) << "Finished dropping job, job id = " << job_id;
  });
}

void GcsJobManager::OnNodeDead(const NodeID &node_id) {
  // Notify the job manager that the driver node is removed.
  auto driver_node_to_jobs_iter = driver_node_to_jobs_.find(node_id);
  if (driver_node_to_jobs_iter != driver_node_to_jobs_.end()) {
    for (auto &job_id : driver_node_to_jobs_iter->second) {
      OnDriverNodeRemoved(job_id, node_id);
    }
    driver_node_to_jobs_.erase(node_id);
  }
}

NodeID GcsJobManager::SelectDriver(const rpc::JobTableData &job_data) const {
  std::vector<NodeID> alive_nodes;
  for (auto &entry : gcs_node_manager_->GetAllAliveNodes()) {
    alive_nodes.emplace_back(entry.first);
  }

  if (alive_nodes.empty()) {
    return NodeID::Nil();
  }

  // TODO(Shanly): Check if the resources is enough.

  static std::mt19937_64 gen_(
      std::chrono::high_resolution_clock::now().time_since_epoch().count());
  std::uniform_int_distribution<int> distribution(0, alive_nodes.size() - 1);
  return alive_nodes[distribution(gen_)];
}

void GcsJobManager::OnDriverNodeRemoved(const JobID &job_id, const NodeID &node_id) {
  RAY_LOG(WARNING) << "Driver node " << node_id << " is removed with job id = " << job_id;
  auto iter = jobs_.find(job_id);
  if (iter == jobs_.end()) {
    RAY_LOG(WARNING) << "Ignore this event as the job is not submitted, job id = "
                     << job_id;
    return;
  }

  auto job_table_data = iter->second;
  if (job_table_data->is_dead()) {
    RAY_LOG(INFO) << "Job is already dead, just ignore this event. job id = " << job_id;
    return;
  }

  if (job_table_data->config().long_running() &&
      job_table_data->driver_exit_state() == rpc::JobTableData_DriverExitState_OK) {
    RAY_LOG(INFO) << "This is a long-running job and it already exits gracefully, so "
                     "the job state sould not be changed. job id = "
                  << job_id;
    return;
  }

  RAY_CHECK_OK(UpdateJobStateToDead(job_table_data, nullptr));
}

Status GcsJobManager::UpdateJobStateToDead(std::shared_ptr<JobTableData> job_table_data,
                                           const ray::gcs::StatusCallback &callback) {
  // Update job state.
  if (job_table_data->is_dropped() ||
      job_table_data->driver_exit_state() == rpc::JobTableData_DriverExitState_OK) {
    job_table_data->set_state(rpc::JobTableData_JobState_FINISHED);
  } else {
    job_table_data->set_state(rpc::JobTableData_JobState_FAILED);
  }
  JobID job_id = JobID::FromBinary(job_table_data->job_id());
  RAY_LOG(INFO) << "Updating job state to "
                << rpc::JobTableData_JobState_Name(job_table_data->state())
                << ", job id = " << job_id << ", driver exit state = "
                << rpc::JobTableData_DriverExitState_Name(
                       job_table_data->driver_exit_state())
                << ", is dropped = " << job_table_data->is_dropped();
  job_table_data->set_is_dead(true);
  auto on_done = [this, callback, job_id, job_table_data](const Status &status) {
    RAY_CHECK_OK(status);
    RAY_CHECK_OK(gcs_pub_sub_->Publish(JOB_CHANNEL, job_id.Binary(),
                                       job_table_data->SerializeAsString(), nullptr));
    ClearJobInfos(job_id);
    if (callback) {
      callback(status);
    }
    RAY_LOG(INFO) << "Finished updating job state to "
                  << rpc::JobTableData_JobState_Name(job_table_data->state())
                  << ", job id = " << job_id << ", driver exit state = "
                  << rpc::JobTableData_DriverExitState_Name(
                         job_table_data->driver_exit_state())
                  << ", is dropped = " << job_table_data->is_dropped();
  };
  return gcs_table_storage_->JobTable().Put(job_id, *job_table_data, on_done);
}

}  // namespace gcs
}  // namespace ray
