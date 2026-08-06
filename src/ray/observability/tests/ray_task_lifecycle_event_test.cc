// Copyright 2026 The Ray Authors.
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

#include "ray/observability/ray_task_lifecycle_event.h"

#include <memory>
#include <optional>
#include <utility>

#include "gtest/gtest.h"
#include "ray/common/id.h"
#include "ray/observability/task_state_update.h"

namespace ray {
namespace observability {

class RayTaskLifecycleEventTest : public ::testing::Test {};

// Merging the status changes of one attempt produces a single event holding the whole
// state-transition series, and a later change does not clear the properties reported by
// an earlier one.
TEST_F(RayTaskLifecycleEventTest, TestMergeAndSerialize) {
  TaskID task_id = TaskID::FromRandom(JobID::FromInt(1));
  JobID job_id = JobID::FromInt(1);
  NodeID node_id = NodeID::FromRandom();
  WorkerID worker_id = WorkerID::FromRandom();

  auto event1 = std::make_unique<RayTaskLifecycleEvent>(
      task_id,
      job_id,
      /*task_attempt=*/0,
      rpc::TaskStatus::SUBMITTED_TO_WORKER,
      std::optional<const TaskStateUpdate>(TaskStateUpdate(node_id, worker_id)),
      "sess1",
      /*timestamp=*/1000);

  auto event2 = std::make_unique<RayTaskLifecycleEvent>(
      task_id,
      job_id,
      /*task_attempt=*/0,
      rpc::TaskStatus::RUNNING,
      std::optional<const TaskStateUpdate>(TaskStateUpdate(static_cast<uint32_t>(4321))),
      "sess1",
      /*timestamp=*/2000);

  ASSERT_EQ(event1->GetEntityId(), event2->GetEntityId());
  ASSERT_EQ(event1->GetTaskAttempt().first, task_id.Binary());
  ASSERT_EQ(event1->GetTaskAttempt().second, 0);

  event1->Merge(std::move(*event2));
  auto serialized_event = std::move(*event1).Serialize().value();

  ASSERT_EQ(serialized_event.source_type(), rpc::events::RayEvent::CORE_WORKER);
  ASSERT_EQ(serialized_event.event_type(), rpc::events::RayEvent::TASK_LIFECYCLE_EVENT);
  ASSERT_EQ(serialized_event.severity(), rpc::events::RayEvent::INFO);
  ASSERT_EQ(serialized_event.session_name(), "sess1");
  ASSERT_TRUE(serialized_event.has_task_lifecycle_event());

  const auto &lifecycle = serialized_event.task_lifecycle_event();
  ASSERT_EQ(lifecycle.task_id(), task_id.Binary());
  ASSERT_EQ(lifecycle.task_attempt(), 0);
  ASSERT_EQ(lifecycle.job_id(), job_id.Binary());

  ASSERT_EQ(lifecycle.state_transitions_size(), 2);
  ASSERT_EQ(lifecycle.state_transitions(0).state(), rpc::TaskStatus::SUBMITTED_TO_WORKER);
  ASSERT_EQ(lifecycle.state_transitions(0).timestamp().nanos(), 1000);
  ASSERT_EQ(lifecycle.state_transitions(1).state(), rpc::TaskStatus::RUNNING);
  ASSERT_EQ(lifecycle.state_transitions(1).timestamp().nanos(), 2000);

  // The node/worker reported by the first change survive the merge with a change that
  // does not carry them, and the second change's pid is applied.
  ASSERT_EQ(lifecycle.node_id(), node_id.Binary());
  ASSERT_EQ(lifecycle.worker_id(), worker_id.Binary());
  ASSERT_EQ(lifecycle.worker_pid(), 4321);
}

// A status change with no state update contributes only its state transition.
TEST_F(RayTaskLifecycleEventTest, TestSerializeWithoutStateUpdate) {
  TaskID task_id = TaskID::FromRandom(JobID::FromInt(2));

  RayTaskLifecycleEvent event(task_id,
                              JobID::FromInt(2),
                              /*task_attempt=*/3,
                              rpc::TaskStatus::FINISHED,
                              std::nullopt,
                              "sess2",
                              /*timestamp=*/5000);

  auto serialized_event = std::move(event).Serialize().value();
  const auto &lifecycle = serialized_event.task_lifecycle_event();
  ASSERT_EQ(lifecycle.task_attempt(), 3);
  ASSERT_EQ(lifecycle.state_transitions_size(), 1);
  ASSERT_EQ(lifecycle.state_transitions(0).state(), rpc::TaskStatus::FINISHED);
  ASSERT_EQ(lifecycle.state_transitions(0).timestamp().nanos(), 5000);
  ASSERT_TRUE(lifecycle.node_id().empty());
  ASSERT_EQ(lifecycle.worker_pid(), 0);
  ASSERT_FALSE(lifecycle.has_is_debugger_paused());
}

}  // namespace observability
}  // namespace ray
