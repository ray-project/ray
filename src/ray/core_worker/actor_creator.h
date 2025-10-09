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

#include <memory>
#include <utility>
#include <vector>

#include "ray/gcs_rpc_client/accessor.h"
#include "ray/util/thread_utils.h"

namespace ray {
namespace core {

class ActorCreatorInterface {
 public:
  virtual ~ActorCreatorInterface() = default;
  /// Register actor to GCS synchronously.
  ///
  /// \param task_spec The specification for the actor creation task.
  /// \return Status
  virtual Status RegisterActor(const TaskSpecification &task_spec) const = 0;

  /// Asynchronously request GCS to register the actor.
  /// \param task_spec The specification for the actor creation task.
  /// \param callback Callback that will be called after the actor info is registered to
  /// GCS
  virtual void AsyncRegisterActor(const TaskSpecification &task_spec,
                                  gcs::StatusCallback callback) = 0;

  virtual void AsyncRestartActorForLineageReconstruction(
      const ActorID &actor_id,
      uint64_t num_restarts_due_to_lineage_reconstructions,
      gcs::StatusCallback callback) = 0;

  virtual void AsyncReportActorOutOfScope(
      const ActorID &actor_id,
      uint64_t num_restarts_due_to_lineage_reconstructions,
      gcs::StatusCallback callback) = 0;

  /// Asynchronously request GCS to create the actor.
  ///
  /// \param task_spec The specification for the actor creation task.
  /// \param callback Callback that will be called after the actor info is written to GCS.
  virtual void AsyncCreateActor(
      const TaskSpecification &task_spec,
      const rpc::ClientCallback<rpc::CreateActorReply> &callback) = 0;

  /// Asynchronously wait until actor is registered successfully
  ///
  /// \param actor_id The actor id to wait
  /// \param callback The callback that will be called after actor registered
  virtual void AsyncWaitForActorRegisterFinish(const ActorID &actor_id,
                                               gcs::StatusCallback callback) = 0;

  /// Check whether actor is activately under registering
  ///
  /// \param actor_id The actor id to check
  /// \return bool Boolean to indicate whether the actor is under registering
  virtual bool IsActorInRegistering(const ActorID &actor_id) const = 0;
};

class ActorCreator : public ActorCreatorInterface {
 public:
  explicit ActorCreator(gcs::ActorInfoAccessor &actor_client)
      : actor_client_(actor_client) {}

  Status RegisterActor(const TaskSpecification &task_spec) const override;

  void AsyncRegisterActor(const TaskSpecification &task_spec,
                          gcs::StatusCallback callback) override;

  void AsyncRestartActorForLineageReconstruction(
      const ActorID &actor_id,
      uint64_t num_restarts_due_to_lineage_reconstructions,
      gcs::StatusCallback callback) override;

  void AsyncReportActorOutOfScope(const ActorID &actor_id,
                                  uint64_t num_restarts_due_to_lineage_reconstruction,
                                  gcs::StatusCallback callback) override;

  bool IsActorInRegistering(const ActorID &actor_id) const override;

  void AsyncWaitForActorRegisterFinish(const ActorID &actor_id,
                                       gcs::StatusCallback callback) override;

  void AsyncCreateActor(
      const TaskSpecification &task_spec,
      const rpc::ClientCallback<rpc::CreateActorReply> &callback) override;

 private:
  gcs::ActorInfoAccessor &actor_client_;
  using RegisteringActorType =
      absl::flat_hash_map<ActorID, std::vector<ray::gcs::StatusCallback>>;
  ThreadPrivate<RegisteringActorType> registering_actors_;
};

}  // namespace core
}  // namespace ray
