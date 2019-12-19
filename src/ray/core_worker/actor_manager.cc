#include "ray/core_worker/actor_manager.h"
#include "ray/gcs/redis_actor_info_accessor.h"

namespace ray {

void ActorManager::PublishCreatedActor(const TaskSpecification &actor_creation_task,
                                       const rpc::Address &address) {
  auto actor_id = actor_creation_task.ActorCreationId();
  auto data = gcs::CreateActorTableData(actor_creation_task, address,
                                        gcs::ActorTableData::ALIVE, 0);
  RAY_CHECK_OK(global_actor_table_.Append(JobID::Nil(), actor_id, data, nullptr));
}

void ActorManager::PublishTerminatedActor(const TaskSpecification &actor_creation_task) {
  auto actor_id = actor_creation_task.ActorCreationId();
  auto data = gcs::CreateActorTableData(actor_creation_task, rpc::Address(),
                                        gcs::ActorTableData::DEAD, 0);
  RAY_CHECK_OK(global_actor_table_.Append(JobID::Nil(), actor_id, data, nullptr));
}

}  // namespace ray
