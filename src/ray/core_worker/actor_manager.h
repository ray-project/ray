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
#include "ray/core_worker/reference_count.h"
#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/gcs/redis_gcs_client.h"

namespace ray {

/// Class to manage lifetimes of actors that we create (actor children).
/// Currently this class is only used to publish actor DEAD event
/// for actor creation task failures. All other cases are managed
/// by raylet.
class ActorManager {
 public:
  ActorManager(
      std::shared_ptr<gcs::GcsClient> gcs_client,
      std::shared_ptr<CoreWorkerDirectActorTaskSubmitterInterface> direct_actor_submitter,
      std::shared_ptr<ReferenceCounterInterface> reference_counter)
      : gcs_client_(gcs_client),
        direct_actor_submitter_(direct_actor_submitter),
        reference_counter_(reference_counter) {}

  ~ActorManager() {}

  /// Add an actor handle from a serialized string.
  ///
  /// This should be called when an actor handle is given to us by another task
  /// or actor. This may be called even if we already have a handle to the same
  /// actor.
  ///
  /// \param[in] serialized The serialized actor handle.
  /// \param[in] outer_object_id The object ID that contained the serialized
  /// actor handle, if any.
  /// \param[in] caller_id The caller's task ID
  /// \param[in] call_site The caller's site.
  /// \return The ActorID of the deserialized handle.
  ActorID DeserializeAndRegisterActorHandle(const std::string &serialized,
                                            const ObjectID &outer_object_id,
                                            const TaskID &caller_id,
                                            const std::string &call_site,
                                            const rpc::Address &caller_address);

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

  /// Get a handle to an actor.
  ///
  /// \param[in] actor_id The actor handle to get.
  /// \param[out] actor_handle A handle to the requested actor.
  /// \return Status::Invalid if we don't have this actor handle.
  Status GetActorHandle(const ActorID &actor_id, ActorHandle **actor_handle) const;

  /// Get a handle to a named actor.
  ///
  /// \param[in] name The name of the actor whose handle to get.
  /// \param[out] actor_handle A handle to the requested actor.
  /// \param[in] caller_id The caller's task ID
  /// \param[in] call_site The caller's site.
  /// \return Status::NotFound if an actor with the specified name wasn't found.
  Status GetNamedActorHandle(const std::string &name, ActorHandle **actor_handle,
                             const TaskID &caller_id, const std::string &call_site,
                             const rpc::Address &caller_address);

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
  /// \param[in] caller_id The caller's task ID
  /// \param[in] call_site The caller's site.
  /// \return True if the handle was added and False if we already had a handle
  /// to the same actor.
  bool AddActorHandle(std::shared_ptr<ActorHandle> actor_handle, bool is_owner_handle,
                      const TaskID &caller_id, const std::string &call_site,
                      const rpc::Address &caller_address);

  /// Add a callback that is called when an actor goes out of scope.
  ///
  /// \param actor_id The actor id that owns the callback.
  /// \param actor_out_of_scope_callbacks The callback function that will be called when
  /// an actor_id goes out of scope.
  void AddActorOutOfScopeCallback(
      const ActorID &actor_id,
      std::function<void(const ActorID &)> actor_out_of_scope_callbacks);

  /// Tell an actor to exit immediately, without completing outstanding work.
  ///
  /// \param[in] actor_id ID of the actor to kill.
  /// \param[in] no_restart If set to true, the killed actor will not be
  /// restarted anymore.
  /// \param[out] Status
  Status KillActor(const ActorID &actor_id, bool force_kill, bool no_restart);

  /// Handle actor state notification published from GCS.
  ///
  /// \param[in] actor_id The actor id of this notification.
  /// \param[in] actor_data The GCS actor data.
  void HandleActorStateNotification(const ActorID &actor_id,
                                    const gcs::ActorTableData &actor_data);

  /// Get a list of actor_ids from existing actor handles.
  /// This is used for debugging purpose.
  std::vector<ObjectID> GetActorHandleIDsFromHandles();

 private:
  /// GCS client
  std::shared_ptr<gcs::GcsClient> gcs_client_;

  /// Interface to submit tasks directly to other actors.
  std::shared_ptr<CoreWorkerDirectActorTaskSubmitterInterface> direct_actor_submitter_;

  /// Keeps track of object ID reference counts.
  std::shared_ptr<ReferenceCounterInterface> reference_counter_;

  mutable absl::Mutex mutex_;

  /// Map from actor ID to a handle to that actor.
  absl::flat_hash_map<ActorID, std::shared_ptr<ActorHandle>> actor_handles_
      GUARDED_BY(mutex_);

  /// Map from actor ID to a callback to call when all local handles to that
  /// actor have gone out of scpoe.
  absl::flat_hash_map<ActorID, std::function<void(const ActorID &)>>
      actor_out_of_scope_callbacks_ GUARDED_BY(mutex_);
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_ACTOR_MANAGER_H
