#ifndef RAY_CORE_WORKER_ACTOR_MANAGER_H
#define RAY_CORE_WORKER_ACTOR_MANAGER_H

#include "ray/core_worker/actor_handle.h"
#include "ray/core_worker/transport/direct_actor_transport.h"

namespace ray {

/// Class to manage lifetimes of actors that we create (actor children).
class ActorManager {
 public:
  ActorManager(gcs::DirectActorTable &global_actor_table)
      : global_actor_table_(global_actor_table) {}

  /// Called when an actor creation task that we submitted finishes.
  void PublishCreatedActor(const TaskSpecification &actor_creation_task,
                           const rpc::Address &address);

  /// Called when an actor that we own can no longer be restarted.
  void PublishTerminatedActor(const TaskSpecification &actor_creation_task);

  ///// Add a reference to an actor.
  // bool AddActorHandle(std::unique_ptr<ActorHandle> actor_handle);

 private:
  /// Global database of actors.
  gcs::DirectActorTable &global_actor_table_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_ACTOR_MANAGER_H
