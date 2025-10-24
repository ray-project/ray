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

#include "ray/observability/ray_actor_definition_event.h"

#include "gtest/gtest.h"

namespace ray {
namespace observability {

class RayActorDefinitionEventTest : public ::testing::Test {};

TEST_F(RayActorDefinitionEventTest, TestSerialize) {
  rpc::ActorTableData data;
  data.set_actor_id("test_actor_id");
  data.set_job_id("test_job_id");
  data.set_is_detached(true);
  data.set_name("MyActor");
  data.set_ray_namespace("test_ns");
  data.set_serialized_runtime_env("{\"pip\":[\"requests\"]}");
  data.set_class_name("MyClass");
  (*data.mutable_required_resources())["CPU"] = 1.0;
  (*data.mutable_required_resources())["GPU"] = 0.5;
  data.set_placement_group_id("pg_id");
  auto *team_constraint = data.mutable_label_selector()->add_label_constraints();
  team_constraint->set_label_key("team");
  team_constraint->set_operator_(ray::rpc::LABEL_OPERATOR_IN);
  team_constraint->add_label_values("core");
  auto *tier_constraint = data.mutable_label_selector()->add_label_constraints();
  tier_constraint->set_label_key("tier");
  tier_constraint->set_operator_(ray::rpc::LABEL_OPERATOR_IN);
  tier_constraint->add_label_values("prod");

  auto event = std::make_unique<RayActorDefinitionEvent>(data, "test_session_name");
  auto serialized_event = std::move(*event).Serialize();

  ASSERT_EQ(serialized_event.source_type(), rpc::events::RayEvent::GCS);
  ASSERT_EQ(serialized_event.session_name(), "test_session_name");
  ASSERT_EQ(serialized_event.event_type(), rpc::events::RayEvent::ACTOR_DEFINITION_EVENT);
  ASSERT_EQ(serialized_event.severity(), rpc::events::RayEvent::INFO);
  ASSERT_TRUE(serialized_event.has_actor_definition_event());

  const auto &actor_def = serialized_event.actor_definition_event();
  ASSERT_EQ(actor_def.actor_id(), "test_actor_id");
  ASSERT_EQ(actor_def.job_id(), "test_job_id");
  ASSERT_TRUE(actor_def.is_detached());
  ASSERT_EQ(actor_def.name(), "MyActor");
  ASSERT_EQ(actor_def.ray_namespace(), "test_ns");
  ASSERT_EQ(actor_def.serialized_runtime_env(), "{\"pip\":[\"requests\"]}");
  ASSERT_EQ(actor_def.class_name(), "MyClass");
  ASSERT_EQ(actor_def.required_resources().at("CPU"), 1.0);
  ASSERT_EQ(actor_def.required_resources().at("GPU"), 0.5);
  ASSERT_EQ(actor_def.placement_group_id(), "pg_id");
  const auto &read_team_constraint = actor_def.label_selector().label_constraints(0);
  ASSERT_EQ(read_team_constraint.label_key(), "team");
  ASSERT_EQ(read_team_constraint.operator_(), ray::rpc::LABEL_OPERATOR_IN);
  ASSERT_EQ(read_team_constraint.label_values(0), "core");
  const auto &read_tier_constraint = actor_def.label_selector().label_constraints(1);
  ASSERT_EQ(read_tier_constraint.label_key(), "tier");
  ASSERT_EQ(read_tier_constraint.operator_(), ray::rpc::LABEL_OPERATOR_IN);
  ASSERT_EQ(read_tier_constraint.label_values(0), "prod");
}

}  // namespace observability
}  // namespace ray
