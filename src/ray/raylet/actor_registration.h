#ifndef RAY_RAYLET_ACTOR_REGISTRATION_H
#define RAY_RAYLET_ACTOR_REGISTRATION_H

#include <unordered_map>

#include "ray/gcs/format/gcs_generated.h"
#include "ray/id.h"

namespace ray {

namespace raylet {

/// \class ActorRegistration
///
/// Information about an actor registered in the system. This includes the
/// actor's current node manager location, and if local, information about its
/// current execution state, used for reconstruction purposes, and whether the
/// actor is currently alive or not.
class ActorRegistration {
 public:
  /// Create an actor registration.
  ///
  /// \param actor_table_data Information from the global actor table about
  /// this actor. This includes the actor's node manager location.
  ActorRegistration(const ActorTableDataT &actor_table_data);

  /// Each actor may have multiple callers, or "handles". A frontier leaf
  /// represents the execution state of the actor with respect to a single
  /// handle.
  struct FrontierLeaf {
    /// The number of tasks submitted by this handle that have executed on the
    /// actor so far.
    int64_t task_counter;
    /// The execution dependency returned by the task submitted by this handle
    /// that most recently executed on the actor.
    ObjectID execution_dependency;
  };

  /// Get the actor's node manager location.
  ///
  /// \return The actor's node manager location. All tasks for the actor should
  /// be forwarded to this node.
  const ClientID GetNodeManagerId() const;

  /// Get the object that represents the actor's initial state. This is the
  /// execution dependency returned by this actor's creation task. If
  /// reconstructed, this will recreate the actor.
  ///
  /// \return The execution dependency returned by the actor's creation task.
  const ObjectID GetActorCreationDependency() const;

  /// TODO

  const int64_t GetMaxReconstructions() const;

  const int64_t GetRemainingReconstructions() const;

  /// Get the object that represents the actor's current state. This is the
  /// execution dependency returned by the task most recently executed on the
  /// actor. The next task to execute on the actor should be marked as
  /// execution-dependent on this object.
  ///
  /// \return The execution dependency returned by the most recently executed
  /// task.
  const ObjectID GetExecutionDependency() const;

  /// Get the execution frontier of the actor, indexed by handle. This captures
  /// the execution state of the actor, a summary of which tasks have executed
  /// so far.
  ///
  /// \return The actor frontier, a map from handle ID to execution state for
  /// that handle.
  const std::unordered_map<ActorHandleID, FrontierLeaf> &GetFrontier() const;

  /// Extend the frontier of the actor by a single task. This should be called
  /// whenever the actor executes a task.
  ///
  /// \param handle_id The ID of the handle that submitted the task.
  /// \param execution_dependency The object representing the actor's new
  /// state. This is the execution dependency returned by the task.
  void ExtendFrontier(const ActorHandleID &handle_id,
                      const ObjectID &execution_dependency);

  /// Return whether the actor is alive or not. This should only be called on
  /// local actors.
  ///
  /// \return True if the local actor is alive and false if it is dead.
  bool IsAlive() const;

  /// Mark the actor as dead.
  /// \return Void.
  void MarkDead();

 private:
  /// Information from the global actor table about this actor, including the
  /// node manager location.
  ActorTableDataT actor_table_data_;
  /// True if the actor is alive and false otherwise.
  bool alive_;
  /// The object representing the state following the actor's most recently
  /// executed task. The next task to execute on the actor should be marked as
  /// execution-dependent on this object.
  ObjectID execution_dependency_;
  /// The execution frontier of the actor, which represents which tasks have
  /// executed so far and which tasks may execute next, based on execution
  /// dependencies. This is indexed by handle.
  std::unordered_map<ActorHandleID, FrontierLeaf> frontier_;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_ACTOR_REGISTRATION_H
