// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <unordered_map>

#include "ray/common/id.h"
#include "ray/common/task/task.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {

namespace raylet {

using rpc::ActorTableData;
using ActorState = rpc::ActorTableData::ActorState;
using rpc::ActorCheckpointData;

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
  explicit ActorRegistration(const ActorTableData &actor_table_data);

  /// Recreate an actor's registration from a checkpoint.
  ///
  /// \param checkpoint_data The checkpoint used to restore the actor.
  ActorRegistration(const ActorTableData &actor_table_data,
                    const ActorCheckpointData &checkpoint_data);

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
  const ActorTableData &GetTableData() const { return actor_table_data_; }

  /// Get the actor's current state (ALIVE or DEAD).
  ///
  /// \return The actor's current state.
  const ActorState GetState() const { return actor_table_data_.state(); }

  /// Update actor's state.
  void SetState(const ActorState &state) { actor_table_data_.set_state(state); }

  /// Get the actor's node manager location.
  ///
  /// \return The actor's node manager location. All tasks for the actor should
  /// be forwarded to this node.
  const ClientID GetNodeManagerId() const;

  /// Get the object that represents the actor's initial state. This is the
  /// execution dependency returned by this actor's creation task. If
  /// restarted, this will recreate the actor.
  ///
  /// \return The execution dependency returned by the actor's creation task.
  const ObjectID GetActorCreationDependency() const;

  /// Get actor's job ID.
  const JobID GetJobId() const;

  /// Get the max number of times this actor should be restarted.
  const int64_t GetMaxRestarts() const;

  /// Get the remaining number of times this actor should be restarted.
  const int64_t GetRemainingRestarts() const;

  /// Get the number of times this actor has already been restarted
  const uint64_t GetNumRestarts() const;

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
  const std::unordered_map<TaskID, FrontierLeaf> &GetFrontier() const;

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
  ObjectID ExtendFrontier(const TaskID &caller_id, const ObjectID &execution_dependency);

  /// Returns num handles to this actor entry.
  ///
  /// \return int.
  int NumHandles() const;

  /// Generate checkpoint data based on actor's current state.
  ///
  /// \param actor_id ID of this actor.
  /// \param task The task that just finished on the actor. (nullptr when it's direct
  /// call.)
  /// \return A shared pointer to the generated checkpoint data.
  std::shared_ptr<ActorCheckpointData> GenerateCheckpointData(const ActorID &actor_id,
                                                              const Task *task);

 private:
  /// Information from the global actor table about this actor, including the
  /// node manager location.
  ActorTableData actor_table_data_;
  /// The object representing the state following the actor's most recently
  /// executed task. The next task to execute on the actor should be marked as
  /// execution-dependent on this object.
  ObjectID execution_dependency_;
  /// The execution frontier of the actor, which represents which tasks have
  /// executed so far and which tasks may execute next, based on execution
  /// dependencies. This is indexed by handle.
  std::unordered_map<TaskID, FrontierLeaf> frontier_;
  /// This map is used to track all the unreleased dummy objects for this
  /// actor.  The map key is the dummy object ID, and the map value is the
  /// number of actor handles that depend on that dummy object. When the map
  /// value decreases to 0, the dummy object is safe to release from the object
  /// manager, since this means that no actor handle will depend on that dummy
  /// object again.
  ///
  /// An actor handle depends on a dummy object when its next unfinished task
  /// depends on the dummy object. For a given dummy object (say D) created by
  /// task (say T) that was submitted by an actor handle (say H), there could
  /// be 2 types of such actor handles:
  /// 1. T is the last task submitted by H that was executed. If the next task
  /// submitted by H hasn't finished yet, then H still depends on D since D
  /// will be in the next task's execution dependencies.
  /// 2. Any handles that were forked from H after T finished, and before T's
  /// next task finishes. Such handles depend on D until their first tasks
  /// finish since D will be their first tasks' execution dependencies.
  std::unordered_map<ObjectID, int64_t> dummy_objects_;
};

}  // namespace raylet

}  // namespace ray
