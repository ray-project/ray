#ifndef RAY_CORE_WORKER_ACTOR_MANAGER_H
#define RAY_CORE_WORKER_ACTOR_MANAGER_H

#include "ray/core_worker/actor_handle.h"
#include "ray/core_worker/transport/direct_actor_transport.h"

namespace ray {

/// Class to manage all actor references and lifetimes of actors that we
/// create (child actors).
class ActorManager {
 public:
  using ActorCreationCallback = const std::function<void(
      const ActorID &, const ray::TaskSpecification &, uint64_t)>;

  ActorManager(DirectActorClientsInterface &direct_actor_clients,
               const ActorCreationCallback &actor_creation_callback)
      : direct_actor_clients_(direct_actor_clients),
        actor_creation_callback_(actor_creation_callback) {}

  /// Metadata for an actor that we created.
  /// TODO(swang): Make this struct private. It's only public right now because
  /// the CoreWorker needs access to the children when handling node removal.
  struct ChildActor {
    ChildActor(const ray::TaskSpecification &spec) : actor_creation_spec(spec) {}
    /// The actor creation task spec. This is used to populate the actor table
    /// entry and restart the actor if the actor fails.
    const ray::TaskSpecification actor_creation_spec;
    /// How many times this actor has been alive before.
    uint64_t num_lifetimes = 0;

    bool CanRestart() const {
      return actor_creation_spec.MaxActorReconstructions() - num_lifetimes > 0;
    }
  };

  const absl::flat_hash_map<ActorID, ChildActor> &Children() const {
    return children_actors_;
  }

  const absl::flat_hash_map<ActorID, std::unique_ptr<ActorHandle>> &Handles() const {
    return actor_handles_;
  }

  /// Get a handle to an actor. This asserts that the worker actually has this
  /// handle.
  ///
  /// \param[in] actor_id The actor handle to get.
  /// \param[out] actor_handle A handle to the requested actor.
  /// \return Status::Invalid if we don't have this actor handle.
  Status GetActorHandle(const ActorID &actor_id, ActorHandle **actor_handle) const;

  void RegisterChildActor(const ActorID &actor_id, const ray::TaskSpecification &spec);

  bool AddActorHandle(std::unique_ptr<ActorHandle> actor_handle);

  /// Clear all state for actors. This should be called by non-actors when
  /// completing a task.
  void Clear();

  void OnActorLocationChanged(const ActorID &actor_id, const ClientID &node_id,
                              const std::string &ip_address, const int port);

  void OnActorFailed(const ActorID &actor_id);

  void OnActorDead(const ActorID &actor_id);

 private:
  DirectActorClientsInterface &direct_actor_clients_;
  const ActorCreationCallback actor_creation_callback_;
  /// Map of actors that we created.
  absl::flat_hash_map<ActorID, ChildActor> children_actors_;
  /// Map from actor ID to a handle to that actor.
  absl::flat_hash_map<ActorID, std::unique_ptr<ActorHandle>> actor_handles_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_ACTOR_MANAGER_H
