#include "ray/gcs/task_state_accessor.h"
#include "ray/gcs/redis_gcs_client.h"

namespace ray {

namespace gcs {

TaskStateAccessor::TaskStateAccessor(RedisGcsClient &client_impl)
    : client_impl_(client_impl), task_sub_executor_(client_impl.raylet_task_table()) {}

Status TaskStateAccessor::AsyncRegister(const std::shared_ptr<TaskTableData> &data_ptr,
                                        const StatusCallback &callback) {
  raylet::TaskTable::WriteCallback on_done = nullptr;
  if (callback != nullptr) {
    on_done = [callback](RedisGcsClient *client, const TaskID &task_id,
                         const TaskTableData &data) { callback(Status::OK()); };
  }

  TaskID task_id = TaskID::FromBinary(data_ptr->task().task_spec().task_id());
  raylet::TaskTable &task_table = client_impl_.raylet_task_table();
  return task_table.Add(JobID::Nil(), task_id, data_ptr, on_done);
}

Status TaskStateAccessor::AsyncGet(const TaskID &task_id,
                                   const OptionalItemCallback<TaskTableData> &callback) {
  RAY_CHECK(callback != nullptr);
  auto on_success = [callback](RedisGcsClient *client, const TaskID &task_id,
                               const TaskTableData &data) {
    boost::optional<TaskTableData> result(data);
    callback(Status::OK(), result);
  };

  auto on_failure = [callback](RedisGcsClient *client, const TaskID &task_id) {
    boost::optional<TaskTableData> result;
    callback(Status::Invalid("Task not exist."), result);
  };

  raylet::TaskTable &task_table = client_impl_.raylet_task_table();
  return task_table.Lookup(JobID::Nil(), task_id, on_success, on_failure);
}

Status TaskStateAccessor::AsyncDelete(const std::vector<TaskID> &task_ids,
                                      const StatusCallback &callback) {
  raylet::TaskTable &task_table = client_impl_.raylet_task_table();
  task_table.Delete(JobID::Nil(), task_ids);
  // TODO(micafan) Always return OK here.
  // Confirm if we need to handle the deletion failure and how to handle it.
  return Status::OK();
}

Status TaskStateAccessor::AsyncSubscribe(
    const TaskID &task_id, const SubscribePairCallback<TaskID, TaskTableData> &subscribe,
    const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  ClientTable &client_table = client_impl_.client_table();
  const ClientID &client_id = client_table.GetLocalClientId();
  return task_sub_executor_.AsyncSubscribe(client_id, task_id, subscribe, done);
}

Status TaskStateAccessor::AsyncUnsubscribe(const TaskID &task_id,
                                           const StatusCallback &done) {
  ClientTable &client_table = client_impl_.client_table();
  const ClientID &client_id = client_table.GetLocalClientId();
  return task_sub_executor_.AsyncUnsubscribe(client_id, task_id, done);
}

}  // namespace gcs

}  // namespace ray
