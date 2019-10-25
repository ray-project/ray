#ifndef RAY_CORE_WORKER_TRANSPORT_H
#define RAY_CORE_WORKER_TRANSPORT_H

#include <list>

#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/common/task/task_spec.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/store_provider/store_provider.h"

namespace ray {

/// This class receives tasks for execution.
class CoreWorkerTaskReceiver {
 public:
  using TaskHandler = std::function<Status(
      const TaskSpecification &task_spec, const ResourceMappingType &resource_ids,
      std::vector<std::shared_ptr<RayObject>> *results)>;

  virtual ~CoreWorkerTaskReceiver() {}
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_TRANSPORT_H
