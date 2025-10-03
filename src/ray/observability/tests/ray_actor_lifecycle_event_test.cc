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

  auto event1 = std::make_unique<RayActorLifecycleEvent>(
      data, rpc::events::ActorLifecycleEvent::DEPENDENCIES_UNREADY, "sess1");
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
}

}  // namespace observability
}  // namespace ray
