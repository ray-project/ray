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

#ifndef RAY_CORE_WORKER_ACTOR_MANAGER_H
#define RAY_CORE_WORKER_ACTOR_MANAGER_H

#include "absl/container/flat_hash_map.h"
#include "ray/core_worker/actor_handle.h"
#include "ray/gcs/redis_gcs_client.h"

namespace ray {

// Interface for testing.
class ActorManagerInterface {
 public:
  virtual void PublishTerminatedActor(const TaskSpecification &actor_creation_task) = 0;

  virtual ~ActorManagerInterface() {}
};

/// Class to manage lifetimes of actors that we create (actor children).
/// Currently this class is only used to publish actor DEAD event
/// for actor creation task failures. All other cases are managed
/// by raylet.
class ActorManager : public ActorManagerInterface {
 public:
  ActorManager(gcs::ActorInfoAccessor &actor_accessor)
      : actor_accessor_(actor_accessor) {}

  /// Called when an actor that we own can no longer be restarted.
  void PublishTerminatedActor(const TaskSpecification &actor_creation_task) override;

  // SANG-TODO Move it to actor manager
  /// Add an actor handle from a serialized string.
  ///
  /// This should be called when an actor handle is given to us by another task
  /// or actor. This may be called even if we already have a handle to the same
  /// actor.
  ///
  /// \param[in] serialized The serialized actor handle.
  /// \param[in] outer_object_id The object ID that contained the serialized
  /// actor handle, if any.
  /// \return The ActorID of the deserialized handle.
  ActorID DeserializeAndRegisterActorHandle(const std::string &serialized,
                                            const ObjectID &outer_object_id);

  // SANG-TODO Move it to actor manager
  /// Serialize an actor handle.
  ///
  /// This should be called when passing an actor handle to another task or
  /// actor.
  ///
  /// \param[in] actor_id The ID of the actor handle to serialize.
  /// \param[out] The serialized handle.
  /// \param[out] The ID used to track references to the actor handle. If the
  /// serialized actor handle in the language frontend is stored inside an
  /// object, then this must be recorded in the worker's ReferenceCounter.
  /// \return Status::Invalid if we don't have the specified handle.
  Status SerializeActorHandle(const ActorID &actor_id, std::string *output,
                              ObjectID *actor_handle_id) const;

  // SANG-TODO Move it to actor manager
  /// Get a handle to an actor.
  ///
  /// \param[in] actor_id The actor handle to get.
  /// \param[out] actor_handle A handle to the requested actor.
  /// \return Status::Invalid if we don't have this actor handle.
  Status GetActorHandle(const ActorID &actor_id, ActorHandle **actor_handle) const;

  // SANG-TODO Move it to actor manager
  /// Get a handle to a named actor.
  ///
  /// \param[in] name The name of the actor whose handle to get.
  /// \param[out] actor_handle A handle to the requested actor.
  /// \return Status::NotFound if an actor with the specified name wasn't found.
  Status GetNamedActorHandle(const std::string &name, ActorHandle **actor_handle);

  // SANG-TODO Move it to actor manager
  /// Give this worker a handle to an actor.
  ///
  /// This handle will remain as long as the current actor or task is
  /// executing, even if the Python handle goes out of scope. Tasks submitted
  /// through this handle are guaranteed to execute in the same order in which
  /// they are submitted.
  ///
  /// \param actor_handle The handle to the actor.
  /// \param is_owner_handle Whether this is the owner's handle to the actor.
  /// The owner is the creator of the actor and is responsible for telling the
  /// actor to disconnect once all handles are out of scope.
  /// \return True if the handle was added and False if we already had a handle
  /// to the same actor.
  bool AddActorHandle(std::unique_ptr<ActorHandle> actor_handle, bool is_owner_handle);

 private:
  /// Global database of actors.
  gcs::ActorInfoAccessor &actor_accessor_;

  // SANG-TODO Move it to actor manager
  // TODO(swang): Refactor to merge actor_handles_mutex_ and all fields that it
  // protects into the ActorManager.
  /// The `actor_handles_` field could be mutated concurrently due to multi-threading, we
  /// need a mutex to protect it.
  mutable absl::Mutex mutex_;

  // SANG-TODO Move it to actor manager
  /// Map from actor ID to a handle to that actor.
  absl::flat_hash_map<ActorID, std::unique_ptr<ActorHandle>> actor_handles_
      GUARDED_BY(mutex_);

  // SANG-TODO Move it to actor manager
  /// Map from actor ID to a callback to call when all local handles to that
  /// actor have gone out of scpoe.
  absl::flat_hash_map<ActorID, std::function<void(const ActorID &)>>
      actor_out_of_scope_callbacks_ GUARDED_BY(mutex_);
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_ACTOR_MANAGER_H
