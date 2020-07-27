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

#include "ray/gcs/gcs_server/task_info_handler_impl.h"

namespace ray {
namespace rpc {

void DefaultTaskInfoHandler::HandleAddTask(const AddTaskRequest &request,
                                           AddTaskReply *reply,
                                           SendReplyCallback send_reply_callback) {
  JobID job_id = JobID::FromBinary(request.task_data().task().task_spec().job_id());
  TaskID task_id = TaskID::FromBinary(request.task_data().task().task_spec().task_id());
  RAY_LOG(DEBUG) << "Adding task, job id = " << job_id << ", task id = " << task_id;
  auto on_done = [this, job_id, task_id, request, reply,
                  send_reply_callback](const Status &status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to add task, job id = " << job_id
                     << ", task id = " << task_id;
    } else {
      RAY_CHECK_OK(gcs_pub_sub_->Publish(
          TASK_CHANNEL, task_id.Hex(), request.task_data().SerializeAsString(), nullptr));
      RAY_LOG(DEBUG) << "Finished adding task, job id = " << job_id
                     << ", task id = " << task_id;
      GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
    }
  };

  Status status =
      gcs_table_storage_->TaskTable().Put(task_id, request.task_data(), on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

void DefaultTaskInfoHandler::HandleGetTask(const GetTaskRequest &request,
                                           GetTaskReply *reply,
                                           SendReplyCallback send_reply_callback) {
  TaskID task_id = TaskID::FromBinary(request.task_id());
  RAY_LOG(DEBUG) << "Getting task, job id = " << task_id.JobId()
                 << ", task id = " << task_id;
  auto on_done = [task_id, request, reply, send_reply_callback](
                     const Status &status, const boost::optional<TaskTableData> &result) {
    if (status.ok() && result) {
      reply->mutable_task_data()->CopyFrom(*result);
    }
    RAY_LOG(DEBUG) << "Finished getting task, job id = " << task_id.JobId()
                   << ", task id = " << task_id << ", status = " << status.ToString();
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  };

  Status status = gcs_table_storage_->TaskTable().Get(task_id, on_done);
  if (!status.ok()) {
    on_done(status, boost::none);
  }
}

void DefaultTaskInfoHandler::HandleDeleteTasks(const DeleteTasksRequest &request,
                                               DeleteTasksReply *reply,
                                               SendReplyCallback send_reply_callback) {
  std::vector<TaskID> task_ids = IdVectorFromProtobuf<TaskID>(request.task_id_list());
  JobID job_id = task_ids.empty() ? JobID::Nil() : task_ids[0].JobId();
  RAY_LOG(DEBUG) << "Deleting tasks, job id = " << job_id
                 << ", task id list size = " << task_ids.size();
  auto on_done = [job_id, task_ids, request, reply, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to delete tasks, job id = " << job_id
                     << ", task id list size = " << task_ids.size();
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status status = gcs_table_storage_->TaskTable().BatchDelete(task_ids, on_done);
  if (!status.ok()) {
    on_done(status);
  }
  RAY_LOG(DEBUG) << "Finished deleting tasks, job id = " << job_id
                 << ", task id list size = " << task_ids.size();
}

void DefaultTaskInfoHandler::HandleAddTaskLease(const AddTaskLeaseRequest &request,
                                                AddTaskLeaseReply *reply,
                                                SendReplyCallback send_reply_callback) {
  TaskID task_id = TaskID::FromBinary(request.task_lease_data().task_id());
  ClientID node_id = ClientID::FromBinary(request.task_lease_data().node_manager_id());
  RAY_LOG(DEBUG) << "Adding task lease, job id = " << task_id.JobId()
                 << ", task id = " << task_id << ", node id = " << node_id;
  auto on_done = [this, task_id, node_id, request, reply,
                  send_reply_callback](const Status &status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to add task lease, job id = " << task_id.JobId()
                     << ", task id = " << task_id << ", node id = " << node_id;
    } else {
      RAY_CHECK_OK(gcs_pub_sub_->Publish(TASK_LEASE_CHANNEL, task_id.Hex(),
                                         request.task_lease_data().SerializeAsString(),
                                         nullptr));
      RAY_LOG(DEBUG) << "Finished adding task lease, job id = " << task_id.JobId()
                     << ", task id = " << task_id << ", node id = " << node_id;
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status status = gcs_table_storage_->TaskLeaseTable().Put(
      task_id, request.task_lease_data(), on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

void DefaultTaskInfoHandler::HandleGetTaskLease(const GetTaskLeaseRequest &request,
                                                GetTaskLeaseReply *reply,
                                                SendReplyCallback send_reply_callback) {
  TaskID task_id = TaskID::FromBinary(request.task_id());
  RAY_LOG(DEBUG) << "Getting task lease, job id = " << task_id.JobId()
                 << ", task id = " << task_id;
  auto on_done = [task_id, request, reply, send_reply_callback](
                     const Status &status, const boost::optional<TaskLeaseData> &result) {
    if (status.ok() && result) {
      reply->mutable_task_lease_data()->CopyFrom(*result);
    }
    RAY_LOG(DEBUG) << "Finished getting task lease, job id = " << task_id.JobId()
                   << ", task id = " << task_id << ", status = " << status.ToString();
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  };

  Status status = gcs_table_storage_->TaskLeaseTable().Get(task_id, on_done);
  if (!status.ok()) {
    on_done(status, boost::none);
  }
}

void DefaultTaskInfoHandler::HandleAttemptTaskReconstruction(
    const AttemptTaskReconstructionRequest &request,
    AttemptTaskReconstructionReply *reply, SendReplyCallback send_reply_callback) {
  TaskID task_id = TaskID::FromBinary(request.task_reconstruction().task_id());
  ClientID node_id =
      ClientID::FromBinary(request.task_reconstruction().node_manager_id());
  RAY_LOG(DEBUG) << "Reconstructing task, job id = " << task_id.JobId()
                 << ", task id = " << task_id << ", reconstructions num = "
                 << request.task_reconstruction().num_reconstructions()
                 << ", node id = " << node_id;
  auto on_done = [task_id, node_id, request, reply,
                  send_reply_callback](const Status &status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to reconstruct task, job id = " << task_id.JobId()
                     << ", task id = " << task_id << ", reconstructions num = "
                     << request.task_reconstruction().num_reconstructions()
                     << ", node id = " << node_id;
    } else {
      RAY_LOG(DEBUG) << "Finished reconstructing task, job id = " << task_id.JobId()
                     << ", task id = " << task_id << ", reconstructions num = "
                     << request.task_reconstruction().num_reconstructions()
                     << ", node id = " << node_id;
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status status = gcs_table_storage_->TaskReconstructionTable().Put(
      task_id, request.task_reconstruction(), on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

}  // namespace rpc
}  // namespace ray
