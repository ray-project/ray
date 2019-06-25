#ifndef RAY_GCS_TASK_STATE_ACCESSOR_H
#define RAY_GCS_TASK_STATE_ACCESSOR_H

#include "ray/common/id.h"
#include "ray/gcs/call_back.h"
#include "ray/gcs/tables.h"

namespace ray {

namespace gcs {

class GcsClientImpl;

class TaskStateAccessor {
 public:
  TaskStateAccessor(GcsClientImpl &client_impl);

  ~TaskStateAccessor() {}

  Status AsyncAdd(const std::vector<TaskTableData> &task_specs, StatusCallback callback);

  Status AsyncGet(const DriverID &driver_id, const TaskID &task_id,
                  DatumCallback<TaskTableData>::OptionalItem callback);

 private:
  GcsClientImpl &client_impl_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_TASK_STATE_ACCESSOR_H
