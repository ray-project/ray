#include "task_interface.h"

namespace ray {

Status CoreWorkerTaskInterface::CallTask(const RayFunction &function,
                                         const std::vector<Arg> args,
                                         const CallOptions &call_options,
                                         std::vector<ObjectID> *return_ids) {
  return Status::OK();
}

Status CoreWorkerTaskInterface::CreateActor(
    const RayFunction &function, const std::vector<Arg> args,
    const ActorCreationOptions &actor_creation_options, ActorHandle *actor_handle) {
  return Status::OK();
}

Status CoreWorkerTaskInterface::CallActorTask(ActorHandle &actor_handle,
                                              const RayFunction &function,
                                              const std::vector<Arg> args,
                                              const CallOptions &call_options,
                                              std::vector<ObjectID> *return_ids) {
  return Status::OK();
}

}  // namespace ray
