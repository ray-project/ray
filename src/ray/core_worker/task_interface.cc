#include "task_interface.h"

namespace ray {

Status CoreWorkerTaskInterface::SubmitTask(const RayFunction &function,
                                           const std::vector<TaskArg> &args,
                                           const TaskOptions &task_options,
                                           std::vector<ObjectID> *return_ids) {
  return Status::OK();
}

Status CoreWorkerTaskInterface::CreateActor(
    const RayFunction &function, const std::vector<TaskArg> &args,
    const ActorCreationOptions &actor_creation_options, ActorHandle *actor_handle) {
  return Status::OK();
}

Status CoreWorkerTaskInterface::SubmitActorTask(ActorHandle &actor_handle,
                                                const RayFunction &function,
                                                const std::vector<TaskArg> &args,
                                                const TaskOptions &task_options,
                                                std::vector<ObjectID> *return_ids) {
  return Status::OK();
}

}  // namespace ray
