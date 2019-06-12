
#include "ray/core_worker/task_provider/raylet_task_provider.h"

namespace ray {

CoreWorkerRayletTaskSubmissionProvider::CoreWorkerRayletTaskSubmissionProvider(
    RayClient &ray_client)
    : ray_client_(ray_client) {}

Status CoreWorkerRayletTaskSubmissionProvider::SubmitTask(const TaskSpec &task) {

  return ray_client_.raylet_client_->SubmitTask(task.GetDependencies(), task.GetTaskSpecification());
}

CoreWorkerRayletTaskExecutionProvider::CoreWorkerRayletTaskExecutionProvider(
    RayClient &ray_client)
    : ray_client_(ray_client) {}

Status CoreWorkerRayletTaskExecutionProvider::GetTasks(std::vector<TaskSpec> *tasks) {
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
