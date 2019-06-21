#include "ray/gcs/task_state_accessor.h"
#include "ray/gcs/client.h"
#include "ray/gcs/tables.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

TaskStateAccessor::TaskStateAccessor(AsyncGcsClient *client_impl)
    : client_impl_(client_impl) {
}

Status TaskStateAccessor::AsyncGet(const DriverID &driver_id, const TaskID &task_id,
    DatumCallback<ray::protocol::TaskT>::SingleItem callback) {
  auto on_successed = [callback](AsyncGcsClient *client, const TaskID &task_id,
                                 const ray::protocol::TaskT &task_spec) {
    // Because TaskT's member task_execution_spec is unique_ptr,
    // can't passed by value or ref, so make copy of ray::protocol::TaskT
    ray::protocol::TaskT copy_task_spec;
    copy_task_spec.task_specification = task_spec.task_specification;
    copy_task_spec.task_execution_spec.reset(
      new ray::protocol::TaskExecutionSpecificationT(*task_spec.task_execution_spec));
    boost::optional<ray::protocol::TaskT> result(std::move(copy_task_spec));
    callback(Status::OK(), std::move(result));
  };

  auto on_failed = [callback](AsyncGcsClient *client,
                              const TaskID &task_id) {
    boost::optional<ray::protocol::TaskT> result;
    callback(Status::KeyError("DriverID or TaskID not exist."), std::move(result));
  };

  raylet::TaskTable &task_table = client_impl_->raylet_task_table();
  return task_table.Lookup(driver_id, task_id, on_successed, on_failed);
}

}  // namespace gcs

}  // namespace ray
