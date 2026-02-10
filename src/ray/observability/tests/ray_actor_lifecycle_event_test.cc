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

#include "ray/observability/ray_actor_lifecycle_event.h"

#include "gtest/gtest.h"

namespace ray {
namespace observability {

class RayActorLifecycleEventTest : public ::testing::Test {};

TEST_F(RayActorLifecycleEventTest, TestMergeAndSerialize) {
  rpc::ActorTableData data;
  data.set_actor_id("test_actor_id");
  data.set_job_id("test_job_id");
  data.set_is_detached(false);
  data.set_name("MyActor");
  data.set_ray_namespace("test_ns");
  data.set_node_id("node-1");
  data.mutable_address()->set_worker_id("worker-123");
  data.mutable_address()->set_port(12345);
  data.set_pid(54321);

  auto event1 = std::make_unique<RayActorLifecycleEvent>(
      data, rpc::events::ActorLifecycleEvent::DEPENDENCIES_UNREADY, "sess1");

  // repr_name is only available after actor creation.
  data.set_repr_name("MyActor(id=123)");
  auto event2 = std::make_unique<RayActorLifecycleEvent>(
      data, rpc::events::ActorLifecycleEvent::ALIVE, "sess1");

  event1->Merge(std::move(*event2));
  auto serialized_event = std::move(*event1).Serialize();

  ASSERT_EQ(serialized_event.source_type(), rpc::events::RayEvent::GCS);
  ASSERT_EQ(serialized_event.session_name(), "sess1");
  ASSERT_EQ(serialized_event.event_type(), rpc::events::RayEvent::ACTOR_LIFECYCLE_EVENT);
  ASSERT_EQ(serialized_event.severity(), rpc::events::RayEvent::INFO);
  ASSERT_TRUE(serialized_event.has_actor_lifecycle_event());

  const auto &actor_life = serialized_event.actor_lifecycle_event();
  ASSERT_EQ(actor_life.actor_id(), "test_actor_id");
  ASSERT_EQ(actor_life.state_transitions_size(), 2);
  ASSERT_EQ(actor_life.state_transitions(0).state(),
            rpc::events::ActorLifecycleEvent::DEPENDENCIES_UNREADY);
  ASSERT_EQ(actor_life.state_transitions(1).state(),
            rpc::events::ActorLifecycleEvent::ALIVE);
  ASSERT_EQ(actor_life.state_transitions(1).node_id(), "node-1");
  ASSERT_EQ(actor_life.state_transitions(1).worker_id(), "worker-123");
  ASSERT_EQ(actor_life.state_transitions(0).repr_name(), "");
  ASSERT_EQ(actor_life.state_transitions(1).repr_name(), "MyActor(id=123)");
  ASSERT_EQ(actor_life.state_transitions(1).pid(), 54321);
  ASSERT_EQ(actor_life.state_transitions(1).port(), 12345);
}

struct RestartReasonTestCase {
  std::string name;
  bool preempted;
  rpc::events::ActorLifecycleEvent::RestartReason input_reason;
  rpc::events::ActorLifecycleEvent::RestartReason expected_reason;
};

class RayActorLifecycleEventRestartTest
    : public ::testing::TestWithParam<RestartReasonTestCase> {};

TEST_P(RayActorLifecycleEventRestartTest, TestRestartingReason) {
  const auto &test_case = GetParam();

  rpc::ActorTableData data;
  data.set_actor_id("test_actor_id");
  data.set_preempted(test_case.preempted);

  std::unique_ptr<RayActorLifecycleEvent> event;
  if (test_case.input_reason ==
      rpc::events::ActorLifecycleEvent::LINEAGE_RECONSTRUCTION) {
    // restart reason is explicitly passed in constructor only incase of lineage
    // reconstruction
    event = std::make_unique<RayActorLifecycleEvent>(
        data,
        rpc::events::ActorLifecycleEvent::RESTARTING,
        "sess1",
        test_case.input_reason);
  } else {
    event = std::make_unique<RayActorLifecycleEvent>(
        data, rpc::events::ActorLifecycleEvent::RESTARTING, "sess1");
  }

  auto serialized_event = std::move(*event).Serialize();
  const auto &actor_life = serialized_event.actor_lifecycle_event();

  ASSERT_EQ(actor_life.state_transitions_size(), 1);
  ASSERT_EQ(actor_life.state_transitions(0).state(),
            rpc::events::ActorLifecycleEvent::RESTARTING);
  ASSERT_EQ(actor_life.state_transitions(0).restart_reason(), test_case.expected_reason);
}

INSTANTIATE_TEST_SUITE_P(
    RestartReasons,
    RayActorLifecycleEventRestartTest,
    ::testing::Values(
        RestartReasonTestCase{"Preemption",
                              true,
                              rpc::events::ActorLifecycleEvent::ACTOR_FAILURE,
                              rpc::events::ActorLifecycleEvent::NODE_PREEMPTION},
        RestartReasonTestCase{"LineageReconstruction",
                              false,
                              rpc::events::ActorLifecycleEvent::LINEAGE_RECONSTRUCTION,
                              rpc::events::ActorLifecycleEvent::LINEAGE_RECONSTRUCTION},
        RestartReasonTestCase{"ActorFailure",
                              false,
                              rpc::events::ActorLifecycleEvent::ACTOR_FAILURE,
                              rpc::events::ActorLifecycleEvent::ACTOR_FAILURE}),
    [](const ::testing::TestParamInfo<RestartReasonTestCase> &info) {
      return info.param.name;
    });

}  // namespace observability
}  // namespace ray
