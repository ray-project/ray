#include "ray/gcs/task_state_accessor.h"
#include "ray/gcs/client.h"
#include "ray/gcs/gcs_client_impl.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

TaskStateAccessor::TaskStateAccessor(GcsClientImpl &client_impl)
    : client_impl_(client_impl) {}

Status TaskStateAccessor::AsyncGet(const DriverID &driver_id, const TaskID &task_id,
                                   DatumCallback<TaskTableData>::OptionalItem callback) {
  auto on_successed = [callback](AsyncGcsClient *client, const TaskID &task_id,
                                 const TaskTableData &task_spec) {
    boost::optional<TaskTableData> result(task_spec);
    callback(Status::OK(), std::move(result));
  };

  auto on_failed = [callback](AsyncGcsClient *client, const TaskID &task_id) {
    boost::optional<TaskTableData> result;
    callback(Status::KeyError("DriverID or TaskID not exist."), std::move(result));
  };

  raylet::TaskTable &task_table = client_impl_.AsyncClient().raylet_task_table();
  return task_table.Lookup(driver_id, task_id, on_successed, on_failed);
}

}  // namespace gcs

}  // namespace ray
