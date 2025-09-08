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

#include <list>
#include <memory>
#include <utility>
#include <vector>

#include "ray/core_worker/actor_creator.h"

namespace ray {
namespace core {

class FakeActorCreator : public ActorCreatorInterface {
 public:
  Status RegisterActor(const TaskSpecification &task_spec) const override {
    return Status::OK();
  };

  void AsyncRegisterActor(const TaskSpecification &task_spec,
                          gcs::StatusCallback callback) override {}

  void AsyncRestartActorForLineageReconstruction(
      const ActorID &actor_id,
      uint64_t num_restarts_due_to_lineage_reconstructions,
      gcs::StatusCallback callback) override {}

  void AsyncReportActorOutOfScope(const ActorID &actor_id,
                                  uint64_t num_restarts_due_to_lineage_reconstruction,
                                  gcs::StatusCallback callback) override {}

  void AsyncCreateActor(
      const TaskSpecification &task_spec,
      const rpc::ClientCallback<rpc::CreateActorReply> &callback) override {}

  void AsyncWaitForActorRegisterFinish(const ActorID &,
                                       gcs::StatusCallback callback) override {
    callbacks.push_back(callback);
  }

  [[nodiscard]] bool IsActorInRegistering(const ActorID &actor_id) const override {
    return actor_pending;
  }

  std::list<gcs::StatusCallback> callbacks;
  bool actor_pending = false;
};

}  // namespace core
}  // namespace ray
