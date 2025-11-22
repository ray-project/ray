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

#include <optional>
#include <string>

#include "ray/common/gcs_callback_types.h"
#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"
#include "ray/rpc/rpc_callback_types.h"
#include "src/ray/protobuf/gcs.pb.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
namespace gcs {

/**
  @interface ActorInfoAccessorInterface

  Interface for accessing actor information and managing actor lifecycle
 */
class ActorInfoAccessorInterface {
 public:
  virtual ~ActorInfoAccessorInterface() = default;

  /**
    Get actor specification asynchronously.

    @param actor_id The ID of actor to look up.
    @param callback Callback that will be called after lookup finishes.
   */
  virtual void AsyncGet(const ActorID &actor_id,
                        const OptionalItemCallback<rpc::ActorTableData> &callback) = 0;

  /**
    Get all actor specifications asynchronously.

    @param  actor_id To filter actors by actor_id.
    @param  job_id To filter actors by job_id.
    @param  actor_state_name To filter actors based on actor state.
    @param callback Callback that will be called after lookup finishes.
    @param timeout_ms request timeout, defaults to -1 for infinite timeout.
   */
  virtual void AsyncGetAllByFilter(const std::optional<ActorID> &actor_id,
                                   const std::optional<JobID> &job_id,
                                   const std::optional<std::string> &actor_state_name,
                                   const MultiItemCallback<rpc::ActorTableData> &callback,
                                   int64_t timeout_ms = -1) = 0;

  /**
    Get actor specification for a named actor asynchronously.

    @param name The name of the actor to look up.
    @param ray_namespace The namespace to filter to.
    @param callback Callback that will be called after lookup finishes.
    @param timeout_ms RPC timeout in milliseconds. -1 means the default.
   */
  virtual void AsyncGetByName(const std::string &name,
                              const std::string &ray_namespace,
                              const OptionalItemCallback<rpc::ActorTableData> &callback,
                              int64_t timeout_ms = -1) = 0;

  /**
    Get actor specification for a named actor synchronously.

    @param name The name of the actor to look up.
    @param ray_namespace The namespace to filter to. NotFound if the name doesn't exist.
    @return Status::OK
    @return Status::TimedOut if the method is timed out.
   */
  virtual Status SyncGetByName(const std::string &name,
                               const std::string &ray_namespace,
                               rpc::ActorTableData &actor_table_data,
                               rpc::TaskSpec &task_spec) = 0;

  /**
    List all named actors synchronously.

    @param all_namespaces Whether to include actors from all Ray namespaces.
    @param ray_namespace The namespace to filter to if all_namespaces is false.
    @param[out] actors The pair of list of named actors. Each pair includes the
    namespace and name of the actor.
    @return Status::OK
    @return Status::TimedOut if the method is timed out.
   */
  virtual Status SyncListNamedActors(
      bool all_namespaces,
      const std::string &ray_namespace,
      std::vector<std::pair<std::string, std::string>> &actors) = 0;

  /**
  Report actor out of scope asynchronously.

  @param actor_id The ID of the actor.
  @param num_restarts_due_to_lineage_reconstruction Number of restarts due to lineage
  reconstruction.
  @param callback Callback that will be called after the operation completes.
  @param timeout_ms Timeout in milliseconds. -1 means the default.
 */
  virtual void AsyncReportActorOutOfScope(
      const ActorID &actor_id,
      uint64_t num_restarts_due_to_lineage_reconstruction,
      const StatusCallback &callback,
      int64_t timeout_ms = -1) = 0;

  /**
    Register actor asynchronously.

    @param task_spec The specification for the actor creation task.
    @param callback Callback that will be called after the actor info is written.
    @param timeout_ms Timeout ms. -1 means there's no timeout.
   */
  virtual void AsyncRegisterActor(const TaskSpecification &task_spec,
                                  const StatusCallback &callback,
                                  int64_t timeout_ms = -1) = 0;

  /**
  Restart actor for lineage reconstruction asynchronously.

  @param actor_id The ID of the actor.
  @param num_restarts_due_to_lineage_reconstructions Number of restarts due to lineage
  reconstructions.
  @param callback Callback that will be called after the operation completes.
  @param timeout_ms Timeout in milliseconds. -1 means the default.
 */
  virtual void AsyncRestartActorForLineageReconstruction(
      const ActorID &actor_id,
      uint64_t num_restarts_due_to_lineage_reconstructions,
      const StatusCallback &callback,
      int64_t timeout_ms = -1) = 0;

  /**
    Register actor to GCS synchronously.

    The RPC will timeout after the default GCS RPC timeout is exceeded.

    @param task_spec The specification for the actor creation task.
    @return Status::OK
    @return Status::TimedOut if actor is not registered by the global
    GCS timeout.
   */
  virtual Status SyncRegisterActor(const ray::TaskSpecification &task_spec) = 0;

  /**
    Kill actor asynchronously.

    @param actor_id The ID of actor to destroy.
    @param force_kill Whether to force kill an actor by killing the worker.
    @param no_restart If set to true, the killed actor will not be restarted anymore.
    @param callback Callback that will be called after the actor is destroyed.
    @param timeout_ms RPC timeout in milliseconds. -1 means infinite.
   */
  virtual void AsyncKillActor(const ActorID &actor_id,
                              bool force_kill,
                              bool no_restart,
                              const StatusCallback &callback,
                              int64_t timeout_ms = -1) = 0;

  /**
    Asynchronously request to create the actor.

    @param task_spec The specification for the actor creation task.
    @param callback Callback that will be called after the actor info is written.
   */
  virtual void AsyncCreateActor(
      const TaskSpecification &task_spec,
      const rpc::ClientCallback<rpc::CreateActorReply> &callback) = 0;

  /**
    Subscribe to any update operations of an actor.

    @param actor_id The ID of actor to be subscribed to.
    @param subscribe Callback that will be called each time when the actor is updated.
    @param done Callback that will be called when subscription is complete.
   */
  virtual void AsyncSubscribe(
      const ActorID &actor_id,
      const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
      const StatusCallback &done) = 0;

  /**
    Cancel subscription to an actor.

    @param actor_id The ID of the actor to be unsubscribed to.
   */
  virtual void AsyncUnsubscribe(const ActorID &actor_id) = 0;

  /**
    Reestablish subscription.
   */
  virtual void AsyncResubscribe() = 0;

  /**
    Check if the specified actor is unsubscribed.

    @param actor_id The ID of the actor.
    @return Whether the specified actor is unsubscribed.
   */
  virtual bool IsActorUnsubscribed(const ActorID &actor_id) = 0;
};

}  // namespace gcs
}  // namespace ray
