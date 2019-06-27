#include "ray/gcs/task_state_accessor.h"
#include "ray/gcs/client.h"
#include "ray/gcs/gcs_client_impl.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

TaskStateAccessor::TaskStateAccessor(GcsClientImpl &client_impl)
    : client_impl_(client_impl) {}

Status TaskStateAccessor::AsyncAdd(const DriverID &driver_id, const TaskID &task_id,
                                   std::shared_ptr<TaskTableData> data,
                                   const StatusCallback &callback) {
  // TODO(micafan) callback no data
  raylet::TaskTable &task_table = client_impl_.AsyncClient().raylet_task_table();
  if (callback != nullptr) {
    auto on_done = [callback](AsyncGcsClient *client, const TaskID &task_id,
                              const TaskTableData &data) { callback(Status::OK()); };

    return task_table.Add(driver_id, task_id, data, on_done);
  }

  return task_table.Add(driver_id, task_id, data, nullptr);
}

Status TaskStateAccessor::AsyncGet(const DriverID &driver_id, const TaskID &task_id,
                                   const OptionalItemCallback<TaskTableData> &callback) {
  RAY_DCHECK(callback != nullptr);
  auto on_success = [callback](AsyncGcsClient *client, const TaskID &task_id,
                               const TaskTableData &data) {
    boost::optional<TaskTableData> result(data);
    callback(Status::OK(), std::move(result));
  };

  auto on_failure = [callback](AsyncGcsClient *client, const TaskID &task_id) {
    boost::optional<TaskTableData> result;
    callback(Status::KeyError("DriverID or TaskID not exist."), std::move(result));
  };

  raylet::TaskTable &task_table = client_impl_.AsyncClient().raylet_task_table();
  return task_table.Lookup(driver_id, task_id, on_success, on_failure);
}

Status TaskStateAccessor::AsyncSubscribe(
    const DriverID &driver_id, const ClientID &client_id,
    const SubscribeCallback<TaskID, TaskTableData> &subscribe,
    const StatusCallback &done) {
  RAY_DCHECK(subscribe != nullptr);
  auto on_subscribe = [subscribe](AsyncGcsClient *client, const TaskID &task_id,
                                  const TaskTableData &data) {
    std::vector<TaskTableData> datas;
    datas.emplace_back(data);
    subscribe(task_id, std::move(datas));
  };

  raylet::TaskTable &task_table = client_impl_.AsyncClient().raylet_task_table();
  if (done != nullptr) {
    auto on_failure = [done](AsyncGcsClient *client, const TaskID &task_id) {
      done(Status::RedisError("GCS error."));
    };

    auto on_success = [done](AsyncGcsClient *client) { done(Status::OK()); };

    return task_table.Subscribe(driver_id, client_id, on_subscribe, on_failure,
                                on_success);
  }

  return task_table.Subscribe(driver_id, client_id, on_subscribe, nullptr, nullptr);
}

Status TaskStateAccessor::RequestNotifications(const DriverID &driver_id,
                                               const TaskID &task_id,
                                               const ClientID &client_id) {
  raylet::TaskTable &task_table = client_impl_.AsyncClient().raylet_task_table();
  task_table.RequestNotifications(driver_id, task_id, client_id);
  return Status::OK();
}

Status TaskStateAccessor::CancelNotifications(const DriverID &driver_id,
                                              const TaskID &task_id,
                                              const ClientID &client_id) {
  raylet::TaskTable &task_table = client_impl_.AsyncClient().raylet_task_table();
  task_table.CancelNotifications(driver_id, task_id, client_id);
  return Status::OK();
}

Status TaskStateAccessor::Delete(const DriverID &driver_id,
                                 const std::vector<TaskID> &task_ids) {
  raylet::TaskTable &task_table = client_impl_.AsyncClient().raylet_task_table();
  task_table.Delete(driver_id, task_ids);
  return Status::OK();
}

}  // namespace gcs

}  // namespace ray
