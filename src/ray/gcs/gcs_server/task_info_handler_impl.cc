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

#include "task_info_handler_impl.h"

namespace ray {
namespace rpc {

void DefaultTaskInfoHandler::HandleAddTask(const AddTaskRequest &request,
                                           AddTaskReply *reply,
                                           SendReplyCallback send_reply_callback) {
  JobID job_id = JobID::FromBinary(request.task_data().task().task_spec().job_id());
  TaskID task_id = TaskID::FromBinary(request.task_data().task().task_spec().task_id());
  RAY_LOG(DEBUG) << "Adding task, task id = " << task_id << ", job id = " << job_id;
  auto task_table_data = std::make_shared<TaskTableData>();
  task_table_data->CopyFrom(request.task_data());
  auto on_done = [job_id, task_id, request, reply, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to add task, task id = " << task_id
                     << ", job id = " << job_id;
    }
    reply->set_status(status.ToString());
    send_reply_callback(Status::OK(), nullptr, nullptr);
  };

  Status status = gcs_client_.Tasks().AsyncAdd(task_table_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
  RAY_LOG(DEBUG) << "Finished adding task, task id = " << task_id
                 << ", job id = " << job_id;
}

void DefaultTaskInfoHandler::HandleGetTask(const GetTaskRequest &request,
                                           GetTaskReply *reply,
                                           SendReplyCallback send_reply_callback) {
  TaskID task_id = TaskID::FromBinary(request.task_id());
  RAY_LOG(DEBUG) << "Getting task, task id = " << task_id;
  auto on_done = [task_id, request, reply, send_reply_callback](
                     Status status, const boost::optional<TaskTableData> &result) {
    if (status.ok()) {
      RAY_DCHECK(result);
      reply->mutable_task_data()->CopyFrom(*result);
    } else {
      RAY_LOG(ERROR) << "Failed to get task, task id = " << task_id;
    }
    reply->set_status(status.ToString());
    send_reply_callback(Status::OK(), nullptr, nullptr);
  };

  Status status = gcs_client_.Tasks().AsyncGet(task_id, on_done);
  if (!status.ok()) {
    on_done(status, boost::none);
  }
  RAY_LOG(DEBUG) << "Finished getting task, task id = " << task_id;
}

void DefaultTaskInfoHandler::HandleDeleteTasks(const DeleteTasksRequest &request,
                                               DeleteTasksReply *reply,
                                               SendReplyCallback send_reply_callback) {
  std::vector<TaskID> task_ids = IdVectorFromProtobuf<TaskID>(request.task_id_list());
  RAY_LOG(DEBUG) << "Deleting tasks, task id list size = " << task_ids.size();
  auto on_done = [task_ids, request, reply, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to delete tasks, task id list size = " << task_ids.size();
    }
    reply->set_status(status.ToString());
    send_reply_callback(Status::OK(), nullptr, nullptr);
  };

  Status status = gcs_client_.Tasks().AsyncDelete(task_ids, on_done);
  if (!status.ok()) {
    on_done(status);
  }
  RAY_LOG(DEBUG) << "Finished deleting tasks, task id list size = " << task_ids.size();
}

void DefaultTaskInfoHandler::HandleAddTaskLease(const AddTaskLeaseRequest &request,
                                                AddTaskLeaseReply *reply,
                                                SendReplyCallback send_reply_callback) {
  TaskID task_id = TaskID::FromBinary(request.task_lease_data().task_id());
  ClientID node_id = ClientID::FromBinary(request.task_lease_data().node_manager_id());
  RAY_LOG(DEBUG) << "Adding task lease, task id = " << task_id
                 << ", node id = " << node_id;
  auto task_lease_data = std::make_shared<TaskLeaseData>();
  task_lease_data->CopyFrom(request.task_lease_data());
  auto on_done = [task_id, node_id, request, reply, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to add task lease, task id = " << task_id
                     << ", node id = " << node_id;
    }
    reply->set_status(status.ToString());
    send_reply_callback(Status::OK(), nullptr, nullptr);
  };

  Status status = gcs_client_.Tasks().AsyncAddTaskLease(task_lease_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
  RAY_LOG(DEBUG) << "Finished adding task lease, task id = " << task_id
                 << ", node id = " << node_id;
}

void DefaultTaskInfoHandler::HandleAttemptTaskReconstruction(
    const AttemptTaskReconstructionRequest &request,
    AttemptTaskReconstructionReply *reply, SendReplyCallback send_reply_callback) {
  ClientID node_id =
      ClientID::FromBinary(request.task_reconstruction().node_manager_id());
  RAY_LOG(DEBUG) << "Reconstructing task, reconstructions num = "
                 << request.task_reconstruction().num_reconstructions()
                 << ", node id = " << node_id;
  auto task_reconstruction_data = std::make_shared<TaskReconstructionData>();
  task_reconstruction_data->CopyFrom(request.task_reconstruction());
  auto on_done = [node_id, request, reply, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to reconstruct task, reconstructions num = "
                     << request.task_reconstruction().num_reconstructions()
                     << ", node id = " << node_id;
    }
    reply->set_status(status.ToString());
    send_reply_callback(Status::OK(), nullptr, nullptr);
  };

  Status status =
      gcs_client_.Tasks().AttemptTaskReconstruction(task_reconstruction_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
  RAY_LOG(DEBUG) << "Finished reconstructing task, reconstructions num = "
                 << request.task_reconstruction().num_reconstructions()
                 << ", node id = " << node_id;
}

}  // namespace rpc
}  // namespace ray
