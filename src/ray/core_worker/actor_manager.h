#ifndef RAY_CORE_WORKER_ACTOR_MANAGER_H
#define RAY_CORE_WORKER_ACTOR_MANAGER_H

#include "ray/core_worker/actor_handle.h"
#include "ray/core_worker/transport/direct_actor_transport.h"

namespace ray {

/// Class to manage all actor references and lifetimes of actors that we
/// create (child actors).
class ActorManager {
 public:
  // Called if a failed actor needs to be restarted.
  ///
  /// \param[in] actor_id The ID of the actor to restart.
  /// \param[in] spec The spec of the actor creation task.
  /// \param[in] num_restarts The number of times the actor has been restarted.
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
    uint64_t num_restarts = 0;

    bool CanRestart() const {
      return num_restarts < actor_creation_spec.MaxActorReconstructions();
    }
  };

  const absl::flat_hash_map<ActorID, ChildActor> &Children() const {
    return children_actors_;
  }

  const absl::flat_hash_map<ActorID, std::unique_ptr<ActorHandle>> &ActorHandles() const {
    return actor_handles_;
  }

  /// Get a handle to an actor. This asserts that the worker actually has this
  /// handle.
  ///
  /// \param[in] actor_id The actor handle to get.
  /// \param[out] actor_handle A handle to the requested actor.
  /// \return Status::Invalid if we don't have this actor handle.
  Status GetActorHandle(const ActorID &actor_id, ActorHandle **actor_handle) const;

  /// Add a reference to an actor. We will track its state, e.g., alive, and
  /// its location.
  bool AddActorHandle(std::unique_ptr<ActorHandle> actor_handle);

  /// Register an actor that we created. In addition to the accounting done for
  /// normal actor references, we are also responsible for restarting these
  /// actors on failure (if max reconstructions is nonzero) and for marking
  /// these actors as dead on node failure.
  ///
  /// \param[in] spec The spec for the actor creation task. If the child actor
  /// dies and max reconstructions is nonzero, then this task should be
  /// resubmitted to recreate the actor via the ActorCreationCallback.
  /// \return Status::Invalid if we already had a handle to the same actor.
  Status RegisterChildActor(const ray::TaskSpecification &spec);

  /// Clear all state for actors. This should be called by non-actors when
  /// completing a task.
  void Clear();

  /// Handler for a location update of an actor that we have a reference to.
  ///
  /// \param[in] actor_id The ID of the actor.
  /// \param[in] node_id The ID of the node where the actor is now located.
  /// \param[in] ip_address The IP address where the actor is now located.
  /// \param[in] port The port that the actor is now listening on.
  void OnActorLocationChanged(const ActorID &actor_id, const ClientID &node_id,
                              const std::string &ip_address, const int port);

  /// Handler for failure of an actor that we have a reference to.
  ///
  /// Terminal failures are ones where the actor was explicitly terminated by
  /// the application. If the failure was not terminal, we own the actor, and
  /// we have not restarted the actor max reconstructions times yet, then we
  /// will try to restart the actor via the ActorCreationCallback.
  ///
  /// \param[in] actor_id The ID of the actor.
  /// \param[in] terminal Whether the failure was terminal.
  void OnActorFailed(const ActorID &actor_id, bool terminal);

 private:
  /// Interface to the class that maintains RPC clients to the actors.
  DirectActorClientsInterface &direct_actor_clients_;
  /// Callback that is called if a failed actor needs to be recreated.
  const ActorCreationCallback actor_creation_callback_;
  /// Map of actors that we created.
  absl::flat_hash_map<ActorID, ChildActor> children_actors_;
  /// Map from actor ID to a handle to that actor.
  absl::flat_hash_map<ActorID, std::unique_ptr<ActorHandle>> actor_handles_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_ACTOR_MANAGER_H
