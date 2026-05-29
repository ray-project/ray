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

#include <string>
#include <unordered_map>
#include <utility>

#include "gtest/gtest.h"
#include "ray/common/task/task_util.h"

namespace ray {
TEST(TaskSpecTest, TestSchedulingClassDescriptor) {
  FunctionDescriptor descriptor = FunctionDescriptorBuilder::BuildPython("a", "", "", "");
  ResourceSet resources(absl::flat_hash_map<std::string, double>({{"a", 1.0}}));
  rpc::SchedulingStrategy scheduling_strategy;
  scheduling_strategy.mutable_spread_scheduling_strategy();
  SchedulingClassDescriptor descriptor1(resources,
                                        LabelSelector(),
                                        descriptor,
                                        0,
                                        scheduling_strategy,
                                        std::vector<FallbackOption>());
  SchedulingClassDescriptor descriptor2(resources,
                                        LabelSelector(),
                                        descriptor,
                                        1,
                                        scheduling_strategy,
                                        std::vector<FallbackOption>());
  scheduling_strategy.mutable_default_scheduling_strategy();
  SchedulingClassDescriptor descriptor3(resources,
                                        LabelSelector(),
                                        descriptor,
                                        0,
                                        scheduling_strategy,
                                        std::vector<FallbackOption>());
  scheduling_strategy.mutable_node_affinity_scheduling_strategy()->set_node_id("x");
  scheduling_strategy.mutable_node_affinity_scheduling_strategy()->set_soft(true);
  SchedulingClassDescriptor descriptor4(resources,
                                        LabelSelector(),
                                        descriptor,
                                        0,
                                        scheduling_strategy,
                                        std::vector<FallbackOption>());
  scheduling_strategy.mutable_node_affinity_scheduling_strategy()->set_node_id("y");
  SchedulingClassDescriptor descriptor5(resources,
                                        LabelSelector(),
                                        descriptor,
                                        0,
                                        scheduling_strategy,
                                        std::vector<FallbackOption>());
  SchedulingClassDescriptor descriptor6(resources,
                                        LabelSelector(),
                                        descriptor,
                                        0,
                                        scheduling_strategy,
                                        std::vector<FallbackOption>());
  scheduling_strategy.mutable_node_affinity_scheduling_strategy()
      ->set_spill_on_unavailable(true);
  SchedulingClassDescriptor descriptor10(resources,
                                         LabelSelector(),
                                         descriptor,
                                         0,
                                         scheduling_strategy,
                                         std::vector<FallbackOption>());
  scheduling_strategy.mutable_placement_group_scheduling_strategy()
      ->set_placement_group_id("o");
  scheduling_strategy.mutable_placement_group_scheduling_strategy()
      ->set_placement_group_bundle_index(0);
  scheduling_strategy.mutable_placement_group_scheduling_strategy()
      ->set_placement_group_capture_child_tasks(true);
  SchedulingClassDescriptor descriptor7(resources,
                                        LabelSelector(),
                                        descriptor,
                                        0,
                                        scheduling_strategy,
                                        std::vector<FallbackOption>());
  scheduling_strategy.mutable_placement_group_scheduling_strategy()
      ->set_placement_group_bundle_index(1);
  SchedulingClassDescriptor descriptor8(resources,
                                        LabelSelector(),
                                        descriptor,
                                        0,
                                        scheduling_strategy,
                                        std::vector<FallbackOption>());
  scheduling_strategy.mutable_placement_group_scheduling_strategy()
      ->set_placement_group_bundle_index(0);
  SchedulingClassDescriptor descriptor9(resources,
                                        LabelSelector(),
                                        descriptor,
                                        0,
                                        scheduling_strategy,
                                        std::vector<FallbackOption>());
  ASSERT_TRUE(descriptor1 == descriptor1);
  ASSERT_TRUE(absl::Hash<SchedulingClassDescriptor>()(descriptor1) ==
              absl::Hash<SchedulingClassDescriptor>()(descriptor1));
  ASSERT_TRUE(SchedulingClassToIds::GetSchedulingClass(descriptor1) ==
              SchedulingClassToIds::GetSchedulingClass(descriptor1));

  ASSERT_FALSE(descriptor1 == descriptor2);
  ASSERT_FALSE(absl::Hash<SchedulingClassDescriptor>()(descriptor1) ==
               absl::Hash<SchedulingClassDescriptor>()(descriptor2));
  ASSERT_FALSE(SchedulingClassToIds::GetSchedulingClass(descriptor1) ==
               SchedulingClassToIds::GetSchedulingClass(descriptor2));

  ASSERT_FALSE(descriptor1 == descriptor3);
  ASSERT_FALSE(absl::Hash<SchedulingClassDescriptor>()(descriptor1) ==
               absl::Hash<SchedulingClassDescriptor>()(descriptor3));
  ASSERT_FALSE(SchedulingClassToIds::GetSchedulingClass(descriptor1) ==
               SchedulingClassToIds::GetSchedulingClass(descriptor3));

  ASSERT_FALSE(descriptor1 == descriptor4);
  ASSERT_FALSE(absl::Hash<SchedulingClassDescriptor>()(descriptor1) ==
               absl::Hash<SchedulingClassDescriptor>()(descriptor4));
  ASSERT_FALSE(SchedulingClassToIds::GetSchedulingClass(descriptor1) ==
               SchedulingClassToIds::GetSchedulingClass(descriptor4));

  ASSERT_FALSE(descriptor4 == descriptor5);
  ASSERT_FALSE(absl::Hash<SchedulingClassDescriptor>()(descriptor4) ==
               absl::Hash<SchedulingClassDescriptor>()(descriptor5));
  ASSERT_FALSE(SchedulingClassToIds::GetSchedulingClass(descriptor4) ==
               SchedulingClassToIds::GetSchedulingClass(descriptor5));

  ASSERT_TRUE(descriptor5 == descriptor6);
  ASSERT_TRUE(absl::Hash<SchedulingClassDescriptor>()(descriptor5) ==
              absl::Hash<SchedulingClassDescriptor>()(descriptor6));
  ASSERT_TRUE(SchedulingClassToIds::GetSchedulingClass(descriptor5) ==
              SchedulingClassToIds::GetSchedulingClass(descriptor6));

  ASSERT_FALSE(descriptor6 == descriptor10);
  ASSERT_FALSE(absl::Hash<SchedulingClassDescriptor>()(descriptor6) ==
               absl::Hash<SchedulingClassDescriptor>()(descriptor10));
  ASSERT_FALSE(SchedulingClassToIds::GetSchedulingClass(descriptor6) ==
               SchedulingClassToIds::GetSchedulingClass(descriptor10));

  ASSERT_FALSE(descriptor6 == descriptor7);
  ASSERT_FALSE(absl::Hash<SchedulingClassDescriptor>()(descriptor6) ==
               absl::Hash<SchedulingClassDescriptor>()(descriptor7));
  ASSERT_FALSE(SchedulingClassToIds::GetSchedulingClass(descriptor6) ==
               SchedulingClassToIds::GetSchedulingClass(descriptor7));

  ASSERT_FALSE(descriptor7 == descriptor8);
  ASSERT_FALSE(absl::Hash<SchedulingClassDescriptor>()(descriptor7) ==
               absl::Hash<SchedulingClassDescriptor>()(descriptor8));
  ASSERT_FALSE(SchedulingClassToIds::GetSchedulingClass(descriptor7) ==
               SchedulingClassToIds::GetSchedulingClass(descriptor8));

  ASSERT_TRUE(descriptor7 == descriptor9);
  ASSERT_TRUE(absl::Hash<SchedulingClassDescriptor>()(descriptor7) ==
              absl::Hash<SchedulingClassDescriptor>()(descriptor9));
  ASSERT_TRUE(SchedulingClassToIds::GetSchedulingClass(descriptor7) ==
              SchedulingClassToIds::GetSchedulingClass(descriptor9));
}

TEST(TaskSpecTest, TestActorSchedulingClass) {
  // This test ensures that an actor's lease request's scheduling class is
  // determined by the placement resources, not the regular resources.

  const std::unordered_map<std::string, double> one_cpu = {{"CPU", 1}};

  rpc::TaskSpec actor_task_spec_proto;
  actor_task_spec_proto.set_type(TaskType::ACTOR_CREATION_TASK);
  actor_task_spec_proto.mutable_required_placement_resources()->insert(one_cpu.begin(),
                                                                       one_cpu.end());

  TaskSpecification actor_task(actor_task_spec_proto);

  rpc::TaskSpec regular_task_spec_proto;
  regular_task_spec_proto.set_type(TaskType::NORMAL_TASK);
  regular_task_spec_proto.mutable_required_resources()->insert(one_cpu.begin(),
                                                               one_cpu.end());

  TaskSpecification regular_task(regular_task_spec_proto);

  ASSERT_EQ(regular_task.GetSchedulingClass(), actor_task.GetSchedulingClass());
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

TEST(TaskSpecTest, TestRootDetachedActorId) {
  ActorID actor_id =
      ActorID::Of(JobID::FromInt(1), TaskID::FromRandom(JobID::FromInt(1)), 0);
  TaskSpecification task_spec;
  ASSERT_TRUE(task_spec.RootDetachedActorId().IsNil());
  task_spec.GetMutableMessage().set_root_detached_actor_id(actor_id.Binary());
  ASSERT_EQ(task_spec.RootDetachedActorId(), actor_id);
}

TEST(TaskSpecTest, TestTaskSpecBuilderRootDetachedActorId) {
  ActorID actor_id =
      ActorID::Of(JobID::FromInt(1), TaskID::FromRandom(JobID::FromInt(1)), 0);

  {
    TaskSpecBuilder task_spec_builder;
    task_spec_builder.SetNormalTaskSpec(
        0, false, "", rpc::SchedulingStrategy(), ActorID::Nil());
    ASSERT_TRUE(
        std::move(task_spec_builder).ConsumeAndBuild().RootDetachedActorId().IsNil());
  }

  {
    TaskSpecBuilder task_spec_builder;
    task_spec_builder.SetNormalTaskSpec(
        0, false, "", rpc::SchedulingStrategy(), actor_id);
    ASSERT_EQ(std::move(task_spec_builder).ConsumeAndBuild().RootDetachedActorId(),
              actor_id);
  }

  {
    TaskSpecBuilder actor_spec_builder;
    actor_spec_builder.SetActorCreationTaskSpec(
        actor_id,
        /*serialized_actor_handle=*/"",
        rpc::SchedulingStrategy(),
        /*max_restarts=*/0,
        /*max_task_retries=*/0,
        /*dynamic_worker_options=*/{},
        /*max_concurrency=*/1,
        /*is_detached=*/false,
        /*name=*/"",
        /*ray_namespace=*/"",
        /*is_asyncio=*/false,
        /*concurrency_groups=*/{},
        /*extension_data=*/"",
        /*allow_out_of_order_execution=*/false,
        /*root_detached_actor_id=*/ActorID::Nil());
    ASSERT_TRUE(
        std::move(actor_spec_builder).ConsumeAndBuild().RootDetachedActorId().IsNil());
  }

  {
    TaskSpecBuilder actor_spec_builder;
    actor_spec_builder.SetActorCreationTaskSpec(actor_id,
                                                /*serialized_actor_handle=*/"",
                                                rpc::SchedulingStrategy(),
                                                /*max_restarts=*/0,
                                                /*max_task_retries=*/0,
                                                /*dynamic_worker_options=*/{},
                                                /*max_concurrency=*/1,
                                                /*is_detached=*/true,
                                                /*name=*/"",
                                                /*ray_namespace=*/"",
                                                /*is_asyncio=*/false,
                                                /*concurrency_groups=*/{},
                                                /*extension_data=*/"",
                                                /*allow_out_of_order_execution=*/false,
                                                /*root_detached_actor_id=*/actor_id);
    ASSERT_EQ(std::move(actor_spec_builder).ConsumeAndBuild().RootDetachedActorId(),
              actor_id);
  }
}

TEST(TaskSpecTest, TestCallerAddress) {
  rpc::Address caller_address;
  NodeID caller_node_id = NodeID::FromRandom();
  WorkerID caller_worker_id = WorkerID::FromRandom();
  caller_address.set_node_id(caller_node_id.Binary());
  caller_address.set_worker_id(caller_worker_id.Binary());
  TaskSpecBuilder task_spec_builder;
  task_spec_builder.SetCommonTaskSpec(
      TaskID::Nil(),
      "dummy_task",
      Language::PYTHON,
      FunctionDescriptorBuilder::BuildPython("", "", "", ""),
      JobID::Nil(),
      rpc::JobConfig(),
      TaskID::Nil(),
      0,
      TaskID::Nil(),
      caller_address,
      1,
      false,
      false,
      -1,
      {},
      {},
      "",
      0,
      TaskID::Nil(),
      "");
  task_spec_builder.SetNormalTaskSpec(
      0, false, "", rpc::SchedulingStrategy(), ActorID::Nil());
  TaskSpecification task_spec = std::move(task_spec_builder).ConsumeAndBuild();
  ASSERT_EQ(task_spec.CallerNodeId(), caller_node_id);
  ASSERT_EQ(task_spec.CallerWorkerId(), caller_worker_id);
}

TEST(TaskSpecTest, TestNodeLabelSchedulingStrategy) {
  rpc::SchedulingStrategy scheduling_strategy_1;
  auto expr_1 = scheduling_strategy_1.mutable_node_label_scheduling_strategy()
                    ->mutable_hard()
                    ->add_expressions();
  expr_1->set_key("key");
  expr_1->mutable_operator_()->mutable_label_in()->add_values("value1");

  rpc::SchedulingStrategy scheduling_strategy_2;
  auto expr_2 = scheduling_strategy_2.mutable_node_label_scheduling_strategy()
                    ->mutable_hard()
                    ->add_expressions();
  expr_2->set_key("key");
  expr_2->mutable_operator_()->mutable_label_in()->add_values("value1");

  ASSERT_TRUE(absl::Hash<rpc::SchedulingStrategy>()(scheduling_strategy_1) ==
              absl::Hash<rpc::SchedulingStrategy>()(scheduling_strategy_1));
  ASSERT_TRUE(absl::Hash<rpc::SchedulingStrategy>()(scheduling_strategy_1) ==
              absl::Hash<rpc::SchedulingStrategy>()(scheduling_strategy_2));

  rpc::SchedulingStrategy scheduling_strategy_3;
  auto expr_3 = scheduling_strategy_3.mutable_node_label_scheduling_strategy()
                    ->mutable_soft()
                    ->add_expressions();
  expr_3->set_key("key");
  expr_3->mutable_operator_()->mutable_label_in()->add_values("value1");
  ASSERT_FALSE(absl::Hash<rpc::SchedulingStrategy>()(scheduling_strategy_1) ==
               absl::Hash<rpc::SchedulingStrategy>()(scheduling_strategy_3));

  rpc::SchedulingStrategy scheduling_strategy_4;
  auto expr_4 = scheduling_strategy_4.mutable_node_label_scheduling_strategy()
                    ->mutable_hard()
                    ->add_expressions();
  expr_4->set_key("key");
  expr_4->mutable_operator_()->mutable_label_in()->add_values("value1");
  expr_4->mutable_operator_()->mutable_label_in()->add_values("value2");

  ASSERT_FALSE(absl::Hash<rpc::SchedulingStrategy>()(scheduling_strategy_1) ==
               absl::Hash<rpc::SchedulingStrategy>()(scheduling_strategy_4));

  rpc::SchedulingStrategy scheduling_strategy_5;
  auto expr_5 = scheduling_strategy_5.mutable_node_label_scheduling_strategy()
                    ->mutable_hard()
                    ->add_expressions();
  expr_5->set_key("key");
  expr_5->mutable_operator_()->mutable_label_not_in()->add_values("value1");

  ASSERT_FALSE(absl::Hash<rpc::SchedulingStrategy>()(scheduling_strategy_1) ==
               absl::Hash<rpc::SchedulingStrategy>()(scheduling_strategy_5));
}
}  // namespace ray
