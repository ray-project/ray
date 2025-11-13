// Copyright 2025 The Ray Authors.
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

#include <functional>
#include <memory>

#include "absl/synchronization/mutex.h"
#include "ray/common/gcs_callback_types.h"
#include "ray/gcs_rpc_client/accessors/actor_info_accessor_interface.h"
#include "ray/gcs_rpc_client/gcs_client_context.h"
#include "ray/rpc/rpc_callback_types.h"
#include "ray/util/sequencer.h"

namespace ray {
namespace gcs {

using SubscribeOperation = std::function<void(const StatusCallback &done)>;
using FetchDataOperation = std::function<void(const StatusCallback &done)>;

/**
  @class ActorInfoAccessor

  Implementation of ActorInfoAccessorInterface that accesses actor information by querying
  the GCS.
 */
class ActorInfoAccessor : public ActorInfoAccessorInterface {
 public:
  ActorInfoAccessor() = default;
  explicit ActorInfoAccessor(GcsClientContext *context);
  virtual ~ActorInfoAccessor() = default;
  /**
    Get actor specification from GCS asynchronously.

    @param actor_id The ID of actor to look up in the GCS.
    @param callback Callback that will be called after lookup finishes.
   */
  void AsyncGet(const ActorID &actor_id,
                const OptionalItemCallback<rpc::ActorTableData> &callback) override;

  /**
    Get all actor specification from the GCS asynchronously.

    @param  actor_id To filter actors by actor_id.
    @param  job_id To filter actors by job_id.
    @param  actor_state_name To filter actors based on actor state.
    @param callback Callback that will be called after lookup finishes.
    @param timeout_ms request timeout, defaults to -1 for infinite timeout.
   */
  void AsyncGetAllByFilter(const std::optional<ActorID> &actor_id,
                           const std::optional<JobID> &job_id,
                           const std::optional<std::string> &actor_state_name,
                           const MultiItemCallback<rpc::ActorTableData> &callback,
                           int64_t timeout_ms = -1) override;

  /**
    Get actor specification for a named actor from the GCS asynchronously.

    @param name The name of the actor to look up in the GCS.
    @param ray_namespace The namespace to filter to.
    @param callback Callback that will be called after lookup finishes.
    @param timeout_ms RPC timeout in milliseconds. -1 means the default.
   */
  void AsyncGetByName(const std::string &name,
                      const std::string &ray_namespace,
                      const OptionalItemCallback<rpc::ActorTableData> &callback,
                      int64_t timeout_ms = -1) override;

  /**
    Get actor specification for a named actor from the GCS synchronously.
    The RPC will timeout after the default GCS RPC timeout is exceeded.

    @param name The name of the actor to look up in the GCS.
    @param ray_namespace The namespace to filter to. NotFound if the name doesn't exist.

    @return Status::OK
    @return Status::TimedOut if the method is timed out.
   */
  Status SyncGetByName(const std::string &name,
                       const std::string &ray_namespace,
                       rpc::ActorTableData &actor_table_data,
                       rpc::TaskSpec &task_spec) override;

  /**
    List all named actors from the GCS synchronously.

    The RPC will timeout after the default GCS RPC timeout is exceeded.

    @param all_namespaces Whether to include actors from all Ray namespaces.
    @param ray_namespace The namespace to filter to if all_namespaces is false.
    @param[out] actors The pair of list of named actors. Each pair includes the
    namespace and name of the actor.
    @return Status::OK
    @return Status::TimedOut if the method is timed out.
   */
  Status SyncListNamedActors(
      bool all_namespaces,
      const std::string &ray_namespace,
      std::vector<std::pair<std::string, std::string>> &actors) override;

  /**
    Report actor out of scope asynchronously.

    @param actor_id The ID of the actor.
    @param num_restarts_due_to_lineage_reconstruction Number of restarts due to lineage
    reconstruction.
    @param callback Callback that will be called after the operation completes.
    @param timeout_ms RPC timeout in milliseconds. -1 means the default.
   */
  void AsyncReportActorOutOfScope(const ActorID &actor_id,
                                  uint64_t num_restarts_due_to_lineage_reconstruction,
                                  const StatusCallback &callback,
                                  int64_t timeout_ms = -1) override;

  /**
    Register actor to GCS asynchronously.

    @param task_spec The specification for the actor creation task.
    @param callback Callback that will be called after the actor info is written to GCS.
    @param timeout_ms RPC timeout ms. -1 means there's no timeout.
   */
  void AsyncRegisterActor(const TaskSpecification &task_spec,
                          const StatusCallback &callback,
                          int64_t timeout_ms = -1) override;

  /**
    Restart actor for lineage reconstruction asynchronously.

    @param actor_id The ID of the actor.
    @param num_restarts_due_to_lineage_reconstructions Number of restarts due to lineage
    reconstructions.
    @param callback Callback that will be called after the operation completes.
    @param timeout_ms RPC timeout in milliseconds. -1 means the default.
   */
  void AsyncRestartActorForLineageReconstruction(
      const ActorID &actor_id,
      uint64_t num_restarts_due_to_lineage_reconstructions,
      const StatusCallback &callback,
      int64_t timeout_ms = -1) override;

  /**
    Register actor to GCS synchronously.

    The RPC will timeout after the default GCS RPC timeout is exceeded.

    @param task_spec The specification for the actor creation task.
    @return Status::OK
    @return Status. TimedOut if actor is not registered by the global
    GCS timeout.
   */
  Status SyncRegisterActor(const ray::TaskSpecification &task_spec) override;

  /**
    Kill actor via GCS asynchronously.

    @param actor_id The ID of actor to destroy.
    @param force_kill Whether to force kill an actor by killing the worker.
    @param no_restart If set to true, the killed actor will not be restarted anymore.
    @param callback Callback that will be called after the actor is destroyed.
    @param timeout_ms RPC timeout in milliseconds. -1 means infinite.
   */
  void AsyncKillActor(const ActorID &actor_id,
                      bool force_kill,
                      bool no_restart,
                      const StatusCallback &callback,
                      int64_t timeout_ms = -1) override;

  /**
    Asynchronously request GCS to create the actor.

    This should be called after the worker has resolved the actor dependencies.
    TODO(...): Currently this request will only reply after the actor is created.
    We should change it to reply immediately after GCS has persisted the actor
    dependencies in storage.

    @param task_spec The specification for the actor creation task.
    @param callback Callback that will be called after the actor info is written to GCS.
   */
  void AsyncCreateActor(
      const TaskSpecification &task_spec,
      const rpc::ClientCallback<rpc::CreateActorReply> &callback) override;

  /**
    Subscribe to any update operations of an actor.

    @param actor_id The ID of actor to be subscribed to.
    @param subscribe Callback that will be called each time when the actor is updated.
    @param done Callback that will be called when subscription is complete.
   */
  void AsyncSubscribe(const ActorID &actor_id,
                      const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
                      const StatusCallback &done) override;

  /**
    Cancel subscription to an actor.

    @param actor_id The ID of the actor to be unsubscribed to.
   */
  void AsyncUnsubscribe(const ActorID &actor_id) override;

  /**
    Reestablish subscription.

    This should be called when GCS server restarts from a failure.
    PubSub server restart will cause GCS server restart. In this case, we need to
    resubscribe from PubSub server, otherwise we only need to fetch data from GCS
    server.
   */
  void AsyncResubscribe() override;

  /**
    Check if the specified actor is unsubscribed.

    @param actor_id The ID of the actor.
    @return Whether the specified actor is unsubscribed.
   */
  bool IsActorUnsubscribed(const ActorID &actor_id) override;

 private:
  // Mutex to protect the resubscribe_operations_ field and fetch_data_operations_ field.
  absl::Mutex mutex_;

  /// Resubscribe operations for actors.
  absl::flat_hash_map<ActorID, SubscribeOperation> resubscribe_operations_
      ABSL_GUARDED_BY(mutex_);

  /// Save the fetch data operation of actors.
  absl::flat_hash_map<ActorID, FetchDataOperation> fetch_data_operations_
      ABSL_GUARDED_BY(mutex_);

  GcsClientContext *context_;
};

}  // namespace gcs
}  // namespace ray
