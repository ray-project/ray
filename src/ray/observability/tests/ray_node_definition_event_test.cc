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

#include "ray/observability/ray_node_definition_event.h"

#include "gtest/gtest.h"

namespace ray {
namespace observability {

class RayNodeDefinitionEventTest : public ::testing::Test {};

TEST_F(RayNodeDefinitionEventTest, TestSerialize) {
  rpc::GcsNodeInfo node_info;
  node_info.set_node_id("test_node_id");
  node_info.set_node_manager_address("192.168.1.100");
  node_info.set_node_manager_hostname("test-hostname");
  node_info.set_node_name("my-test-node");
  node_info.set_instance_id("i-1234567890abcdef0");
  node_info.set_instance_type_name("m5.xlarge");
  node_info.set_start_time_ms(1234567890000);
  (*node_info.mutable_labels())["team"] = "core";
  (*node_info.mutable_labels())["env"] = "prod";

  auto event = std::make_unique<RayNodeDefinitionEvent>(node_info, "test_session");
  auto serialized_event = std::move(*event).Serialize();

  ASSERT_EQ(serialized_event.source_type(), rpc::events::RayEvent::GCS);
  ASSERT_EQ(serialized_event.session_name(), "test_session");
  ASSERT_EQ(serialized_event.event_type(), rpc::events::RayEvent::NODE_DEFINITION_EVENT);
  ASSERT_EQ(serialized_event.severity(), rpc::events::RayEvent::INFO);
  ASSERT_TRUE(serialized_event.has_node_definition_event());

  const auto &node_def = serialized_event.node_definition_event();
  ASSERT_EQ(node_def.node_id(), "test_node_id");
  ASSERT_EQ(node_def.node_ip_address(), "192.168.1.100");
  ASSERT_EQ(node_def.hostname(), "test-hostname");
  ASSERT_EQ(node_def.node_name(), "my-test-node");
  ASSERT_EQ(node_def.instance_id(), "i-1234567890abcdef0");
  ASSERT_EQ(node_def.instance_type_name(), "m5.xlarge");
  ASSERT_TRUE(node_def.has_start_timestamp());
  ASSERT_EQ(node_def.labels().at("team"), "core");
  ASSERT_EQ(node_def.labels().at("env"), "prod");
}

}  // namespace observability
}  // namespace ray
