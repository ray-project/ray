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

  /// Get the actor table data.
  ///
  /// \return The actor table data.
  const ActorTableDataT &GetTableData() const { return actor_table_data_; }

  /// Get the actor's current state (ALIVE or DEAD).
  ///
  /// \return The actor's current state.
  const ActorState &GetState() const { return actor_table_data_.state; }

  /// Update actor's state.
  void SetState(const ActorState &state) { actor_table_data_.state = state; }

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

  /// Get actor's driver ID.
  const DriverID GetDriverId() const;

  /// Get the max number of times this actor should be reconstructed.
  const int64_t GetMaxReconstructions() const;

  /// Get the remaining number of times this actor should be reconstructed.
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

  /// Get all the dummy objects of this actor's tasks.
  const std::unordered_map<ObjectID, int64_t> &GetDummyObjects() const {
    return dummy_objects_;
  }

  /// Extend the frontier of the actor by a single task. This should be called
  /// whenever the actor executes a task.
  ///
  /// \param handle_id The ID of the handle that submitted the task.
  /// \param execution_dependency The object representing the actor's new
  /// state. This is the execution dependency returned by the task.
  /// \return The dummy object that can be released as a result of the executed
  /// task. If no dummy object can be released, then this is nil.
  ObjectID ExtendFrontier(const ActorHandleID &handle_id,
                          const ObjectID &execution_dependency);

  /// Add a new handle to the actor frontier. This does nothing if the actor
  /// handle already exists.
  ///
  /// \param handle_id The ID of the handle to add.
  /// \param execution_dependency This is the expected execution dependency for
  /// the first task submitted on the new handle. If the new handle hasn't been
  /// seen yet, then this dependency will be added to the actor frontier and is
  /// not safe to release until the first task has been submitted.
  void AddHandle(const ActorHandleID &handle_id, const ObjectID &execution_dependency);

  /// Returns num handles to this actor entry.
  ///
  /// \return int.
  int NumHandles() const;

 private:
  /// Information from the global actor table about this actor, including the
  /// node manager location.
  ActorTableDataT actor_table_data_;
  /// The object representing the state following the actor's most recently
  /// executed task. The next task to execute on the actor should be marked as
  /// execution-dependent on this object.
  ObjectID execution_dependency_;
  /// The execution frontier of the actor, which represents which tasks have
  /// executed so far and which tasks may execute next, based on execution
  /// dependencies. This is indexed by handle.
  std::unordered_map<ActorHandleID, FrontierLeaf> frontier_;

  /// A map from all of the dummy object IDs stored for this actor to the
  /// number of actor handles that have the object ID in scope. Each actor
  /// handle will have one dummy object ID in scope, the execution dependency
  /// for the next task submitted by that actor handle to be executed. If a
  /// task submitted by this actor handle has been executed before, then this
  /// is also the dummy object returned by the last task executed. Otherwise,
  /// it's the dummy object returned by the last task submitted on the original
  /// actor handle from which this handle was forked.
  ///
  /// We release an actor handle's dummy object when a new task submitted by
  /// that handle finishes, since this means that the handle will never submit
  /// another task that depends on that object. We also acquire a reference to
  /// the dummy object returned by the finished task, since this means that the
  /// next task submitted by the handle will depend on that object. If new
  /// actor handles are created, these will be included in the next task
  /// submitted on the original actor handle and are given a reference to the
  /// same dummy object as the original handle.
  ///
  /// Once all handles have released a dummy object, it will be removed from
  /// this map. This object is safe to evict, since no handle will submit
  /// another method dependent on that object.
  std::unordered_map<ObjectID, int64_t> dummy_objects_;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_ACTOR_REGISTRATION_H
