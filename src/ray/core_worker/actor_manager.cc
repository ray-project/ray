#include "ray/core_worker/actor_manager.h"
#include "ray/gcs/pb_util.h"
#include "ray/gcs/redis_accessor.h"

namespace ray {

void ActorManager::PublishTerminatedActor(const TaskSpecification &actor_creation_task) {
  auto actor_id = actor_creation_task.ActorCreationId();
  auto data = gcs::CreateActorTableData(actor_creation_task, rpc::Address(),
                                        rpc::ActorTableData::DEAD, 0);

  auto update_callback = [actor_id](Status status) {
    if (!status.ok()) {
      // Only one node at a time should succeed at creating or updating the actor.
      RAY_LOG(ERROR) << "Failed to update state to DEAD for actor " << actor_id
                     << ", error: " << status.ToString();
    }
  };
  RAY_CHECK_OK(actor_accessor_.AsyncRegister(data, update_callback));
}

}  // namespace ray
