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

#include <gtest/gtest_prod.h>

#include "absl/container/flat_hash_map.h"
#include "ray/core_worker/actor_creator.h"
#include "ray/core_worker/actor_handle.h"
#include "ray/core_worker/reference_count.h"
#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/gcs/gcs_client/gcs_client.h"
namespace ray {
namespace core {

/// Class to manage lifetimes of actors that we create (actor children).
/// Currently this class is only used to publish actor DEAD event
/// for actor creation task failures. All other cases are managed
/// by raylet.
class ActorManager {
 public:
  explicit ActorManager(
      std::shared_ptr<gcs::GcsClient> gcs_client,
      std::shared_ptr<CoreWorkerDirectActorTaskSubmitterInterface> direct_actor_submitter,
      std::shared_ptr<ReferenceCounterInterface> reference_counter)
      : gcs_client_(gcs_client),
        direct_actor_submitter_(direct_actor_submitter),
        reference_counter_(reference_counter) {}

  ~ActorManager() = default;

  friend class ActorManagerTest;

  /// Register an actor handle.
  ///
  /// This should be called when an actor handle is given to us by another task
  /// or actor. This may be called even if we already have a handle to the same
  /// actor.
  ///
  /// \param[in] actor_handle The actor handle.
  /// \param[in] outer_object_id The object ID that contained the serialized
  /// actor handle, if any.
  /// \param[in] call_site The caller's site.
  /// \param[in] is_self Whether this handle is current actor's handle. If true, actor
  /// manager won't subscribe actor info from GCS.
  /// \return The ActorID of the deserialized handle.
  ActorID RegisterActorHandle(std::unique_ptr<ActorHandle> actor_handle,
                              const ObjectID &outer_object_id,
                              const std::string &call_site,
                              const rpc::Address &caller_address,
                              bool is_self = false);

  /// Get a handle to an actor.
  ///
  /// \param[in] actor_id The actor handle to get.
  /// \return reference to the actor_handle's pointer.
  /// NOTE: Returned actorHandle should not be stored anywhere.
  std::shared_ptr<ActorHandle> GetActorHandle(const ActorID &actor_id);

  /// Get actor handle by name.
  /// We cache <name, id> pair after getting the named actor from GCS, so that it can use
  /// local cache in next call.
  ///
  /// \param[in] name The actor name.
  /// \param[in] ray_namespace Namespace that actor belongs to.
  /// \param[in] call_site The caller's site.
  /// \param[in] caller_address The rpc address of the calling task.
  /// \return KV pair of actor handle pointer and status.
  std::pair<std::shared_ptr<const ActorHandle>, Status> GetNamedActorHandle(
      const std::string &name,
      const std::string &ray_namespace,
      const std::string &call_site,
      const rpc::Address &caller_address);

  /// Check if an actor handle that corresponds to an actor_id exists.
  /// \param[in] actor_id The actor id of a handle.
  /// \return True if the actor_handle for an actor_id exists. False otherwise.
  bool CheckActorHandleExists(const ActorID &actor_id);

  /// Give this worker a new handle to an actor.
  ///
  /// This handle will remain as long as the current actor or task is
  /// executing, even if the Python handle goes out of scope. Tasks submitted
  /// through this handle are guaranteed to execute in the same order in which
  /// they are submitted.
  ///
  /// NOTE: Getting an actor handle from GCS (named actor) is considered as adding a new
  /// actor handle.
  ///
  /// \param actor_handle The handle to the actor.
  /// \param[in] call_site The caller's site.
  /// \param[in] is_detached Whether or not the actor of a handle is detached (named)
  /// actor. \return True if the handle was added and False if we already had a handle to
  /// the same actor.
  bool AddNewActorHandle(std::unique_ptr<ActorHandle> actor_handle,
                         const std::string &call_site,
                         const rpc::Address &caller_address,
                         bool is_detached);

  /// Wait for actor out of scope.
  ///
  /// \param actor_id The actor id that owns the callback.
  /// \param actor_out_of_scope_callback The callback function that will be called when
  /// an actor_id goes out of scope.
  void WaitForActorOutOfScope(
      const ActorID &actor_id,
      std::function<void(const ActorID &)> actor_out_of_scope_callback);

  /// Get a list of actor_ids from existing actor handles.
  /// This is used for debugging purpose.
  std::vector<ObjectID> GetActorHandleIDsFromHandles();

  /// Function that's invoked when the actor is permanatly dead.
  ///
  /// \param actor_id The actor id of the handle that will be invalidated.
  void OnActorKilled(const ActorID &actor_id);

  /// Subscribe to the state of actor. This method is idempotent and will ensure the actor
  /// only be subscribed once.
  ///
  /// \param actor_id ID of the actor to be subscribed.
  void SubscribeActorState(const ActorID &actor_id);

 private:
  /// Give this worker a handle to an actor.
  ///
  /// This handle will remain as long as the current actor or task is
  /// executing, even if the Python handle goes out of scope. Tasks submitted
  /// through this handle are guaranteed to execute in the same order in which
  /// they are submitted.
  ///
  /// \param actor_handle The handle to the actor.
  /// \param[in] call_site The caller's site.
  /// \param[in] actor_id The id of an actor
  /// \param[in] actor_creation_return_id object id of this actor creation
  /// \param[in] is_self Whether this handle is current actor's handle. If true, actor
  /// to the same actor.
  /// manager won't subscribe actor info from GCS.
  /// \return True if the handle was added and False if we already had a handle
  /// to the same actor.
  bool AddActorHandle(std::unique_ptr<ActorHandle> actor_handle,
                      const std::string &call_site,
                      const rpc::Address &caller_address,
                      const ActorID &actor_id,
                      const ObjectID &actor_creation_return_id,
                      bool is_self = false);

  /// Check if named actor is cached locally.
  /// If it has been cached, core worker will not get actor id by name from GCS.
  ActorID GetCachedNamedActorID(const std::string &actor_name);

  /// Handle actor state notification published from GCS.
  ///
  /// \param[in] actor_id The actor id of this notification.
  /// \param[in] actor_data The GCS actor data.
  void HandleActorStateNotification(const ActorID &actor_id,
                                    const rpc::ActorTableData &actor_data);

  /// It should be invoked when the actor is killed or out of scope.
  /// After the actor is marked killed or out of scope, task submission to the actor will
  /// throw an exception.
  ///
  /// \param actor_handle The actor handle that will be marked as invalidate.
  void MarkActorKilledOrOutOfScope(std::shared_ptr<ActorHandle> actor_handle);

  /// Check if actor is valid.
  bool IsActorKilledOrOutOfScope(const ActorID &actor_id) const;

  /// GCS client.
  std::shared_ptr<gcs::GcsClient> gcs_client_;

  /// Interface to submit tasks directly to other actors.
  std::shared_ptr<CoreWorkerDirectActorTaskSubmitterInterface> direct_actor_submitter_;

  /// Used to keep track of actor handle reference counts.
  /// All actor handle related ref counting logic should be included here.
  std::shared_ptr<ReferenceCounterInterface> reference_counter_;

  mutable absl::Mutex mutex_;

  /// Map from actor ID to a handle to that actor.
  /// Actor handle is a logical abstraction that holds actor handle's states.
  absl::flat_hash_map<ActorID, std::shared_ptr<ActorHandle>> actor_handles_
      GUARDED_BY(mutex_);

  /// Protects access `cached_actor_name_to_ids_` and `subscribed_actors_`.
  mutable absl::Mutex cache_mutex_;

  /// The map to cache name and id of the named actors in this worker locally, to avoid
  /// getting them from GCS frequently.
  absl::flat_hash_map<std::string, ActorID> cached_actor_name_to_ids_
      GUARDED_BY(cache_mutex_);

  /// id -> is_killed_or_out_of_scope
  /// The state of actor is true When the actor is out of scope or is killed
  absl::flat_hash_map<ActorID, bool> subscribed_actors_ GUARDED_BY(cache_mutex_);

  FRIEND_TEST(ActorManagerTest, TestNamedActorIsKilledAfterSubscribeFinished);
  FRIEND_TEST(ActorManagerTest, TestNamedActorIsKilledBeforeSubscribeFinished);
};

}  // namespace core
}  // namespace ray
