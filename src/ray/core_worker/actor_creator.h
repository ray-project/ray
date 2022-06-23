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

#include "ray/common/ray_config.h"
#include "ray/gcs/gcs_client/gcs_client.h"

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
  /// \return Status
  virtual Status AsyncRegisterActor(const TaskSpecification &task_spec,
                                    gcs::StatusCallback callback) = 0;

  /// Asynchronously request GCS to create the actor.
  ///
  /// \param task_spec The specification for the actor creation task.
  /// \param callback Callback that will be called after the actor info is written to GCS.
  /// \return Status
  virtual Status AsyncCreateActor(
      const TaskSpecification &task_spec,
      const rpc::ClientCallback<rpc::CreateActorReply> &callback) = 0;

  /// Asynchronously wait until actor is registered successfully
  ///
  /// \param actor_id The actor id to wait
  /// \param callback The callback that will be called after actor registered
  /// \return void
  virtual void AsyncWaitForActorRegisterFinish(const ActorID &actor_id,
                                               gcs::StatusCallback callback) = 0;

  /// Check whether actor is activately under registering
  ///
  /// \param actor_id The actor id to check
  /// \return bool Boolean to indicate whether the actor is under registering
  virtual bool IsActorInRegistering(const ActorID &actor_id) const = 0;
};

class DefaultActorCreator : public ActorCreatorInterface {
 public:
  explicit DefaultActorCreator(std::shared_ptr<gcs::GcsClient> gcs_client)
      : gcs_client_(std::move(gcs_client)) {}

  Status RegisterActor(const TaskSpecification &task_spec) const override {
    const auto status = gcs_client_->Actors().SyncRegisterActor(task_spec);
    if (status.IsTimedOut()) {
      std::ostringstream stream;
      stream << "There was timeout in registering an actor. It is probably "
                "because GCS server is dead or there's a high load there.";
      return Status::TimedOut(stream.str());
    }
    return status;
  }

  Status AsyncRegisterActor(const TaskSpecification &task_spec,
                            gcs::StatusCallback callback) override {
    if (::RayConfig::instance().actor_register_async()) {
      auto actor_id = task_spec.ActorCreationId();
      (*registering_actors_)[actor_id] = {};
      if (callback != nullptr) {
        (*registering_actors_)[actor_id].emplace_back(std::move(callback));
      }
      return gcs_client_->Actors().AsyncRegisterActor(
          task_spec, [actor_id, this](Status status) {
            std::vector<ray::gcs::StatusCallback> cbs;
            cbs = std::move((*registering_actors_)[actor_id]);
            registering_actors_->erase(actor_id);
            for (auto &cb : cbs) {
              cb(status);
            }
          });
    } else {
      callback(RegisterActor(task_spec));
      return Status::OK();
    }
  }

  bool IsActorInRegistering(const ActorID &actor_id) const override {
    return registering_actors_->find(actor_id) != registering_actors_->end();
  }

  void AsyncWaitForActorRegisterFinish(const ActorID &actor_id,
                                       gcs::StatusCallback callback) override {
    auto iter = registering_actors_->find(actor_id);
    RAY_CHECK(iter != registering_actors_->end());
    iter->second.emplace_back(std::move(callback));
  }

  Status AsyncCreateActor(
      const TaskSpecification &task_spec,
      const rpc::ClientCallback<rpc::CreateActorReply> &callback) override {
    return gcs_client_->Actors().AsyncCreateActor(task_spec, callback);
  }

 private:
  std::shared_ptr<gcs::GcsClient> gcs_client_;
  using RegisteringActorType =
      absl::flat_hash_map<ActorID, std::vector<ray::gcs::StatusCallback>>;
  ThreadPrivate<RegisteringActorType> registering_actors_;
};

}  // namespace core
}  // namespace ray
