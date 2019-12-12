#ifndef RAY_CORE_WORKER_ACTOR_MANAGER_H
#define RAY_CORE_WORKER_ACTOR_MANAGER_H

#include "ray/core_worker/actor_handle.h"
#include "ray/gcs/redis_gcs_client.h"

namespace ray {

// Interface for testing.
class ActorManagerInterface {
 public:
  virtual void PublishCreatedActor(const TaskSpecification &actor_creation_task,
                                   const rpc::Address &address) = 0;

  virtual void PublishTerminatedActor(const TaskSpecification &actor_creation_task) = 0;

  virtual ~ActorManagerInterface() {}
};

/// Class to manage lifetimes of actors that we create (actor children).
class ActorManager : public ActorManagerInterface {
 public:
  ActorManager(gcs::DirectActorTable &global_actor_table)
      : global_actor_table_(global_actor_table) {}

  /// Called when an actor creation task that we submitted finishes.
  void PublishCreatedActor(const TaskSpecification &actor_creation_task,
                           const rpc::Address &address) override;

  /// Called when an actor that we own can no longer be restarted.
  void PublishTerminatedActor(const TaskSpecification &actor_creation_task) override;

 private:
  /// Global database of actors.
  gcs::DirectActorTable &global_actor_table_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_ACTOR_MANAGER_H
