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

#include "ray/core_worker/transport/actor_task_submitter.h"

namespace ray {
namespace core {

class FakeActorTaskSubmitter : public ActorTaskSubmitterInterface {
 public:
  FakeActorTaskSubmitter() : ActorTaskSubmitterInterface() {}

  void AddActorQueueIfNotExists(const ActorID &actor_id,
                                int32_t max_pending_calls,
                                bool execute_out_of_order,
                                bool fail_if_actor_unreachable,
                                bool owned) override {}

  void ConnectActor(const ActorID &actor_id,
                    const rpc::Address &address,
                    int64_t num_restarts) override {}

  void DisconnectActor(const ActorID &actor_id,
                       int64_t num_restarts,
                       bool dead,
                       const rpc::ActorDeathCause &death_cause,
                       bool is_restartable) override {}

  void CheckTimeoutTasks() override {}

  void SetPreempted(const ActorID &actor_id) override {}

  Status SubmitTask(TaskSpecification task_spec) override { return Status::OK(); }

  Status SubmitActorCreationTask(TaskSpecification task_spec) override {
    return Status::OK();
  }

  std::optional<rpc::Address> GetActorAddress(const ActorID &actor_id) const override {
    return std::nullopt;
  }

  bool CheckActorExists(const ActorID &actor_id) const override { return false; }

  bool PendingTasksFull(const ActorID &actor_id) const override { return false; }

  std::string DebugString(const ActorID &actor_id) const override { return ""; }

  size_t NumPendingTasks(const ActorID &actor_id) const override { return 0; }

  Status CancelTask(TaskSpecification task_spec, bool recursive) override {
    return Status::OK();
  }

  std::optional<rpc::ActorTableData::ActorState> GetLocalActorState(
      const ActorID &actor_id) const override {
    return std::nullopt;
  }

  bool QueueGeneratorForResubmit(const TaskSpecification &spec) override { return false; }

  virtual ~FakeActorTaskSubmitter() {}
};

}  // namespace core
}  // namespace ray
