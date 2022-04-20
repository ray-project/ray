// Copyright 2022 The Ray Authors.
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

#include "ray/common/task/task_spec.h"

#include "gtest/gtest.h"

namespace ray {
TEST(TaskSpecTest, TestSchedulingClassDescriptor) {
  FunctionDescriptor descriptor = FunctionDescriptorBuilder::BuildPython("a", "", "", "");
  ResourceSet resources(absl::flat_hash_map<std::string, double>({{"a", 1.0}}));
  rpc::SchedulingStrategy scheduling_strategy;
  scheduling_strategy.mutable_spread_scheduling_strategy();
  SchedulingClassDescriptor descriptor1(resources, descriptor, 0, scheduling_strategy);
  SchedulingClassDescriptor descriptor2(resources, descriptor, 1, scheduling_strategy);
  scheduling_strategy.mutable_default_scheduling_strategy();
  SchedulingClassDescriptor descriptor3(resources, descriptor, 0, scheduling_strategy);
  scheduling_strategy.mutable_node_affinity_scheduling_strategy()->set_node_id("x");
  scheduling_strategy.mutable_node_affinity_scheduling_strategy()->set_soft(true);
  SchedulingClassDescriptor descriptor4(resources, descriptor, 0, scheduling_strategy);
  scheduling_strategy.mutable_node_affinity_scheduling_strategy()->set_node_id("y");
  SchedulingClassDescriptor descriptor5(resources, descriptor, 0, scheduling_strategy);
  SchedulingClassDescriptor descriptor6(resources, descriptor, 0, scheduling_strategy);
  scheduling_strategy.mutable_placement_group_scheduling_strategy()
      ->set_placement_group_id("o");
  scheduling_strategy.mutable_placement_group_scheduling_strategy()
      ->set_placement_group_bundle_index(0);
  scheduling_strategy.mutable_placement_group_scheduling_strategy()
      ->set_placement_group_capture_child_tasks(true);
  SchedulingClassDescriptor descriptor7(resources, descriptor, 0, scheduling_strategy);
  scheduling_strategy.mutable_placement_group_scheduling_strategy()
      ->set_placement_group_bundle_index(1);
  SchedulingClassDescriptor descriptor8(resources, descriptor, 0, scheduling_strategy);
  scheduling_strategy.mutable_placement_group_scheduling_strategy()
      ->set_placement_group_bundle_index(0);
  SchedulingClassDescriptor descriptor9(resources, descriptor, 0, scheduling_strategy);
  ASSERT_TRUE(descriptor1 == descriptor1);
  ASSERT_TRUE(std::hash<SchedulingClassDescriptor>()(descriptor1) ==
              std::hash<SchedulingClassDescriptor>()(descriptor1));
  ASSERT_TRUE(TaskSpecification::GetSchedulingClass(descriptor1) ==
              TaskSpecification::GetSchedulingClass(descriptor1));

  ASSERT_FALSE(descriptor1 == descriptor2);
  ASSERT_FALSE(std::hash<SchedulingClassDescriptor>()(descriptor1) ==
               std::hash<SchedulingClassDescriptor>()(descriptor2));
  ASSERT_FALSE(TaskSpecification::GetSchedulingClass(descriptor1) ==
               TaskSpecification::GetSchedulingClass(descriptor2));

  ASSERT_FALSE(descriptor1 == descriptor3);
  ASSERT_FALSE(std::hash<SchedulingClassDescriptor>()(descriptor1) ==
               std::hash<SchedulingClassDescriptor>()(descriptor3));
  ASSERT_FALSE(TaskSpecification::GetSchedulingClass(descriptor1) ==
               TaskSpecification::GetSchedulingClass(descriptor3));

  ASSERT_FALSE(descriptor1 == descriptor4);
  ASSERT_FALSE(std::hash<SchedulingClassDescriptor>()(descriptor1) ==
               std::hash<SchedulingClassDescriptor>()(descriptor4));
  ASSERT_FALSE(TaskSpecification::GetSchedulingClass(descriptor1) ==
               TaskSpecification::GetSchedulingClass(descriptor4));

  ASSERT_FALSE(descriptor4 == descriptor5);
  ASSERT_FALSE(std::hash<SchedulingClassDescriptor>()(descriptor4) ==
               std::hash<SchedulingClassDescriptor>()(descriptor5));
  ASSERT_FALSE(TaskSpecification::GetSchedulingClass(descriptor4) ==
               TaskSpecification::GetSchedulingClass(descriptor5));

  ASSERT_TRUE(descriptor5 == descriptor6);
  ASSERT_TRUE(std::hash<SchedulingClassDescriptor>()(descriptor5) ==
              std::hash<SchedulingClassDescriptor>()(descriptor6));
  ASSERT_TRUE(TaskSpecification::GetSchedulingClass(descriptor5) ==
              TaskSpecification::GetSchedulingClass(descriptor6));

  ASSERT_FALSE(descriptor6 == descriptor7);
  ASSERT_FALSE(std::hash<SchedulingClassDescriptor>()(descriptor6) ==
               std::hash<SchedulingClassDescriptor>()(descriptor7));
  ASSERT_FALSE(TaskSpecification::GetSchedulingClass(descriptor6) ==
               TaskSpecification::GetSchedulingClass(descriptor7));

  ASSERT_FALSE(descriptor7 == descriptor8);
  ASSERT_FALSE(std::hash<SchedulingClassDescriptor>()(descriptor7) ==
               std::hash<SchedulingClassDescriptor>()(descriptor8));
  ASSERT_FALSE(TaskSpecification::GetSchedulingClass(descriptor7) ==
               TaskSpecification::GetSchedulingClass(descriptor8));

  ASSERT_TRUE(descriptor7 == descriptor9);
  ASSERT_TRUE(std::hash<SchedulingClassDescriptor>()(descriptor7) ==
              std::hash<SchedulingClassDescriptor>()(descriptor9));
  ASSERT_TRUE(TaskSpecification::GetSchedulingClass(descriptor7) ==
              TaskSpecification::GetSchedulingClass(descriptor9));
}

TEST(TaskSpecTest, TestTaskSpecification) {
  rpc::SchedulingStrategy scheduling_strategy;
  NodeID node_id = NodeID::FromRandom();
  scheduling_strategy.mutable_node_affinity_scheduling_strategy()->set_node_id(
      node_id.Binary());
  scheduling_strategy.mutable_node_affinity_scheduling_strategy()->set_soft(true);
  TaskSpecification task_spec;
  task_spec.GetMutableMessage().mutable_scheduling_strategy()->CopyFrom(
      scheduling_strategy);
  ASSERT_TRUE(task_spec.GetSchedulingStrategy() == scheduling_strategy);
  ASSERT_TRUE(task_spec.GetNodeAffinitySchedulingStrategySoft());
  ASSERT_TRUE(task_spec.GetNodeAffinitySchedulingStrategyNodeId() == node_id);
}
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}