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

#include "ray/core_worker/actor_creator.h"

#include <memory>
#include <utility>
#include <vector>

namespace ray {
namespace core {

Status ActorCreator::RegisterActor(const TaskSpecification &task_spec) const {
  const auto status = actor_client_.SyncRegisterActor(task_spec);
  if (status.IsTimedOut()) {
    std::ostringstream stream;
    stream << "There was timeout in registering an actor. It is probably "
              "because GCS server is dead or there's a high load there.";
    return Status::TimedOut(stream.str());
  }
  return status;
}

void ActorCreator::AsyncRegisterActor(const TaskSpecification &task_spec,
                                      gcs::StatusCallback callback) {
  auto actor_id = task_spec.ActorCreationId();
  (*registering_actors_)[actor_id] = {};
  if (callback != nullptr) {
    (*registering_actors_)[actor_id].emplace_back(std::move(callback));
  }
  actor_client_.AsyncRegisterActor(task_spec, [actor_id, this](Status status) {
    std::vector<ray::gcs::StatusCallback> cbs;
    cbs = std::move((*registering_actors_)[actor_id]);
    registering_actors_->erase(actor_id);
    for (auto &cb : cbs) {
      cb(status);
    }
  });
}

void ActorCreator::AsyncRestartActorForLineageReconstruction(
    const ActorID &actor_id,
    uint64_t num_restarts_due_to_lineage_reconstructions,
    gcs::StatusCallback callback) {
  actor_client_.AsyncRestartActorForLineageReconstruction(
      actor_id, num_restarts_due_to_lineage_reconstructions, callback);
}

void ActorCreator::AsyncReportActorOutOfScope(
    const ActorID &actor_id,
    uint64_t num_restarts_due_to_lineage_reconstruction,
    gcs::StatusCallback callback) {
  actor_client_.AsyncReportActorOutOfScope(
      actor_id, num_restarts_due_to_lineage_reconstruction, callback);
}

bool ActorCreator::IsActorInRegistering(const ActorID &actor_id) const {
  return registering_actors_->find(actor_id) != registering_actors_->end();
}

void ActorCreator::AsyncWaitForActorRegisterFinish(const ActorID &actor_id,
                                                   gcs::StatusCallback callback) {
  auto iter = registering_actors_->find(actor_id);
  RAY_CHECK(iter != registering_actors_->end());
  iter->second.emplace_back(std::move(callback));
}

void ActorCreator::AsyncCreateActor(
    const TaskSpecification &task_spec,
    const rpc::ClientCallback<rpc::CreateActorReply> &callback) {
  actor_client_.AsyncCreateActor(task_spec, callback);
}

}  // namespace core
}  // namespace ray
