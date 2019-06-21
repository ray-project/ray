#ifndef RAY_GCS_TASK_STATE_ACCESSOR_H
#define RAY_GCS_TASK_STATE_ACCESSOR_H

#include "ray/common/id.h"
#include "ray/gcs/call_back.h"
#include "ray/gcs/format/gcs_generated.h"
// TODO(micafan) delete
#include "ray/raylet/format/node_manager_generated.h"

namespace ray {

namespace gcs {

class AsyncGcsClient;

class TaskStateAccessor {
 public:
  TaskStateAccessor(AsyncGcsClient *client_impl);

  ~TaskStateAccessor() {}

  Status AsyncAdd(const std::vector<ray::protocol::TaskT> &task_specs,
                  StatusCallback callback);

  Status AsyncGet(const DriverID &driver_id, const TaskID &task_id,
                  DatumCallback<ray::protocol::TaskT>::SingleItem callback);

 private:
  AsyncGcsClient *client_impl_{nullptr};
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_TASK_STATE_ACCESSOR_H
