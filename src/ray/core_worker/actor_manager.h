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

#include "absl/container/flat_hash_map.h"
#include "ray/core_worker/actor_handle.h"
#include "ray/core_worker/reference_count.h"
#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/gcs/gcs_client.h"

namespace ray {
namespace core {

class ActorCreatorInterface {
 public:
  virtual ~ActorCreatorInterface() = default;
  /// Register actor to GCS synchronously.
  ///
  /// \param task_spec The specification for the actor creation task.
  /// \return Status
  virtual Status RegisterActor(const TaskSpecification &task_spec) = 0;

  /// Asynchronously request GCS to create the actor.
  ///
  /// \param task_spec The specification for the actor creation task.
  /// \param callback Callback that will be called after the actor info is written to GCS.
  /// \return Status
  virtual Status AsyncCreateActor(const TaskSpecification &task_spec,
                                  const gcs::StatusCallback &callback) = 0;
};

class DefaultActorCreator : public ActorCreatorInterface {
 public:
  explicit DefaultActorCreator(std::shared_ptr<gcs::GcsClient> gcs_client)
      : gcs_client_(std::move(gcs_client)) {}

  Status RegisterActor(const TaskSpecification &task_spec) override {
    auto promise = std::make_shared<std::promise<void>>();
    auto status = gcs_client_->Actors().AsyncRegisterActor(
        task_spec, [promise](const Status &status) { promise->set_value(); });
    if (status.ok() &&
        promise->get_future().wait_for(std::chrono::seconds(
            ::RayConfig::instance().gcs_server_request_timeout_seconds())) !=
            std::future_status::ready) {
      std::ostringstream stream;
      stream << "There was timeout in registering an actor. It is probably "
                "because GCS server is dead or there's a high load there.";
      return Status::TimedOut(stream.str());
    }
    return status;
  }

  Status AsyncCreateActor(const TaskSpecification &task_spec,
                          const gcs::StatusCallback &callback) override {
    return gcs_client_->Actors().AsyncCreateActor(task_spec, callback);
  }

 private:
  std::shared_ptr<gcs::GcsClient> gcs_client_;
};

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
                              const rpc::Address &caller_address, bool is_self = false);

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
      const std::string &name, const std::string &ray_namespace,
      const std::string &call_site, const rpc::Address &caller_address);

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
                         const std::string &call_site, const rpc::Address &caller_address,
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

  /// Check if named actor is cached locally.
  /// If it has been cached, core worker will not get actor id by name from GCS.
  ActorID GetCachedNamedActorID(const std::string &actor_name);

 private:
  bool AddNewActorHandle(std::unique_ptr<ActorHandle> actor_handle,
                         const std::string &cached_actor_name,
                         const std::string &call_site, const rpc::Address &caller_address,
                         bool is_detached);

  /// Give this worker a handle to an actor.
  ///
  /// This handle will remain as long as the current actor or task is
  /// executing, even if the Python handle goes out of scope. Tasks submitted
  /// through this handle are guaranteed to execute in the same order in which
  /// they are submitted.
  ///
  /// \param actor_handle The handle to the actor.
  /// \param cached_actor_name Actor name used to cache named actor.
  /// \param is_owner_handle Whether this is the owner's handle to the actor.
  /// The owner is the creator of the actor and is responsible for telling the
  /// actor to disconnect once all handles are out of scope.
  /// \param[in] call_site The caller's site.
  /// \param[in] actor_id The id of an actor
  /// \param[in] actor_creation_return_id object id of this actor creation
  /// \param[in] is_self Whether this handle is current actor's handle. If true, actor
  /// to the same actor.
  /// manager won't subscribe actor info from GCS.
  /// \return True if the handle was added and False if we already had a handle
  /// to the same actor.
  bool AddActorHandle(std::unique_ptr<ActorHandle> actor_handle,
                      const std::string &cached_actor_name, bool is_owner_handle,
                      const std::string &call_site, const rpc::Address &caller_address,
                      const ActorID &actor_id, const ObjectID &actor_creation_return_id,
                      bool is_self = false);

  /// Handle actor state notification published from GCS.
  ///
  /// \param[in] actor_id The actor id of this notification.
  /// \param[in] actor_data The GCS actor data.
  void HandleActorStateNotification(const ActorID &actor_id,
                                    const rpc::ActorTableData &actor_data);

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

  /// Protects access `cached_actor_name_to_ids_`.
  absl::Mutex cache_mutex_;

  /// The map to cache name and id of the named actors in this worker locally, to avoid
  /// getting them from GCS frequently.
  absl::flat_hash_map<std::string, ActorID> cached_actor_name_to_ids_
      GUARDED_BY(cache_mutex_);
};

}  // namespace core
}  // namespace ray
