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
  auto on_done = [job_id, task_id, request, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to add task, task id = " << task_id
                     << ", job id = " << job_id;
    }
    send_reply_callback(status, nullptr, nullptr);
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
    send_reply_callback(status, nullptr, nullptr);
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
  auto on_done = [task_ids, request, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to delete tasks, task id list size = " << task_ids.size();
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  Status status = gcs_client_.Tasks().AsyncDelete(task_ids, on_done);
  if (!status.ok()) {
    on_done(status);
  }
  RAY_LOG(DEBUG) << "Finished deleting tasks, task id list size = " << task_ids.size();
}

}  // namespace rpc
}  // namespace ray
