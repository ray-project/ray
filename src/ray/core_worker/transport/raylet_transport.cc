
#include "ray/core_worker/transport/raylet_transport.h"

namespace ray {

CoreWorkerRayletTaskSubmitter::CoreWorkerRayletTaskSubmitter(
    RayClient &ray_client)
    : ray_client_(ray_client) {}

Status CoreWorkerRayletTaskSubmitter::SubmitTask(const TaskSpec &task) {
  return ray_client_.raylet_client_->SubmitTask(task.GetDependencies(),
                                                task.GetTaskSpecification());
}

CoreWorkerRayletTaskReceiver::CoreWorkerRayletTaskReceiver(
    RayClient &ray_client)
    : ray_client_(ray_client) {}

Status CoreWorkerRayletTaskReceiver::GetTasks(std::vector<TaskSpec> *tasks) {
  std::unique_ptr<raylet::TaskSpecification> task_spec;
  auto status = ray_client_.raylet_client_->GetTask(&task_spec);
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Get task from raylet failed with error: "
                   << ray::Status::IOError(status.message());
    return status;
  }

  std::vector<ObjectID> dependencies;
  (*tasks).clear();
  (*tasks).emplace_back(*task_spec, dependencies);

  return Status::OK();
}

}  // namespace ray
