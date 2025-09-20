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

#include "ray/observability/ray_node_lifecycle_event.h"

#include "gtest/gtest.h"

namespace ray {
namespace observability {

class RayNodeLifecycleEventTest : public ::testing::Test {};

TEST_F(RayNodeLifecycleEventTest, TestMerge) {
  rpc::GcsNodeInfo data;
  std::string session_name = "test_session_name";
  std::string node_id = "node";
  data.set_node_id(node_id);
  data.set_state(rpc::GcsNodeInfo::ALIVE);
  auto event = std::make_unique<RayNodeLifecycleEvent>(data, session_name);

  for (auto i = 0; i < 10; i++) {
    rpc::GcsNodeInfo data;
    data.set_node_id(node_id);
    data.set_state(rpc::GcsNodeInfo::ALIVE);
    data.mutable_state_snapshot()->set_state(rpc::NodeSnapshot::DRAINING);
    auto new_event = std::make_unique<RayNodeLifecycleEvent>(data, session_name);
    event->MergeSorted(std::move(*new_event));
  }

  // The state transitions should have only two states: ALIVE and DRAINING. This
  // is because the other states are frequent states and will be merged.
  auto serialized_event = std::move(*event).Serialize();
  ASSERT_EQ(serialized_event.node_lifecycle_event().state_transitions_size(), 2);
  ASSERT_EQ(serialized_event.node_lifecycle_event().state_transitions(0).state(),
            rpc::events::NodeLifecycleEvent::ALIVE);
  ASSERT_EQ(serialized_event.node_lifecycle_event().state_transitions(1).state(),
            rpc::events::NodeLifecycleEvent::DRAINING);
}

}  // namespace observability
}  // namespace ray
