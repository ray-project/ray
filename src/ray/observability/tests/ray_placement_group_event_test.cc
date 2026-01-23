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

#include "gtest/gtest.h"
#include "ray/observability/ray_placement_group_definition_event.h"
#include "ray/observability/ray_placement_group_lifecycle_event.h"

namespace ray {
namespace observability {

namespace {

rpc::PlacementGroupTableData CreateTestPlacementGroupTableData(
    rpc::PlacementStrategy strategy,
    rpc::PlacementGroupTableData::PlacementGroupState state,
    rpc::PlacementGroupStats::SchedulingState scheduling_state) {
  rpc::PlacementGroupTableData data;
  data.set_placement_group_id("test_pg_id");
  data.set_name("test_pg");
  data.set_ray_namespace("test_namespace");
  data.set_strategy(strategy);
  data.set_state(state);
  data.set_is_detached(true);
  data.set_creator_job_id("test_job_id");
  data.set_creator_actor_id("test_actor_id");
  data.set_soft_target_node_id("target_node_id");

  // Add bundles
  auto *bundle1 = data.add_bundles();
  bundle1->mutable_bundle_id()->set_placement_group_id("test_pg_id");
  bundle1->mutable_bundle_id()->set_bundle_index(0);
  (*bundle1->mutable_unit_resources())["CPU"] = 1.0;
  (*bundle1->mutable_label_selector())["team"] = "core";

  auto *bundle2 = data.add_bundles();
  bundle2->mutable_bundle_id()->set_placement_group_id("test_pg_id");
  bundle2->mutable_bundle_id()->set_bundle_index(1);
  (*bundle2->mutable_unit_resources())["GPU"] = 2.0;

  // Add stats
  auto *stats = data.mutable_stats();
  stats->set_scheduling_state(scheduling_state);
  stats->set_creation_request_received_ns(1000000000);
  stats->set_scheduling_started_time_ns(1000100000);
  stats->set_scheduling_attempt(3);
  stats->set_highest_retry_delay_ms(100.5);
  if (state == rpc::PlacementGroupTableData::CREATED) {
    stats->set_scheduling_latency_us(500);
    stats->set_end_to_end_creation_latency_us(1000);
  }

  return data;
}

}  // namespace

class RayPlacementGroupDefinitionEventTest : public ::testing::Test {};

// Parametrized test for different placement strategies
class DefinitionEventStrategyTest
    : public ::testing::TestWithParam<rpc::PlacementStrategy> {};

TEST_P(DefinitionEventStrategyTest, TestSerializeWithStrategy) {
  auto strategy = GetParam();
  auto data = CreateTestPlacementGroupTableData(
      strategy, rpc::PlacementGroupTableData::PENDING, rpc::PlacementGroupStats::QUEUED);

  auto event = std::make_unique<RayPlacementGroupDefinitionEvent>(data, "test_session");
  auto serialized_event = std::move(*event).Serialize();

  ASSERT_EQ(serialized_event.source_type(), rpc::events::RayEvent::GCS);
  ASSERT_EQ(serialized_event.session_name(), "test_session");
  ASSERT_EQ(serialized_event.event_type(),
            rpc::events::RayEvent::PLACEMENT_GROUP_DEFINITION_EVENT);
  ASSERT_EQ(serialized_event.severity(), rpc::events::RayEvent::INFO);
  ASSERT_TRUE(serialized_event.has_placement_group_definition_event());

  const auto &pg_def = serialized_event.placement_group_definition_event();
  ASSERT_EQ(pg_def.placement_group_id(), "test_pg_id");
  ASSERT_EQ(pg_def.name(), "test_pg");
  ASSERT_EQ(pg_def.ray_namespace(), "test_namespace");
  ASSERT_EQ(pg_def.strategy(), strategy);
  ASSERT_TRUE(pg_def.is_detached());
  ASSERT_EQ(pg_def.creator_job_id(), "test_job_id");
  ASSERT_EQ(pg_def.creator_actor_id(), "test_actor_id");
  ASSERT_EQ(pg_def.soft_target_node_id(), "target_node_id");
  ASSERT_EQ(pg_def.creation_request_received_ns(), 1000000000);

  // Verify bundles
  ASSERT_EQ(pg_def.bundles_size(), 2);
  ASSERT_EQ(pg_def.bundles(0).bundle_index(), 0);
  ASSERT_EQ(pg_def.bundles(0).unit_resources().at("CPU"), 1.0);
  ASSERT_EQ(pg_def.bundles(0).label_selector().at("team"), "core");
  ASSERT_EQ(pg_def.bundles(1).bundle_index(), 1);
  ASSERT_EQ(pg_def.bundles(1).unit_resources().at("GPU"), 2.0);
}

INSTANTIATE_TEST_SUITE_P(Strategies,
                         DefinitionEventStrategyTest,
                         ::testing::Values(rpc::PlacementStrategy::PACK,
                                           rpc::PlacementStrategy::SPREAD,
                                           rpc::PlacementStrategy::STRICT_PACK,
                                           rpc::PlacementStrategy::STRICT_SPREAD));

TEST_F(RayPlacementGroupDefinitionEventTest, TestMergeIsNoOp) {
  auto data1 = CreateTestPlacementGroupTableData(rpc::PlacementStrategy::PACK,
                                                 rpc::PlacementGroupTableData::PENDING,
                                                 rpc::PlacementGroupStats::QUEUED);
  auto data2 = CreateTestPlacementGroupTableData(rpc::PlacementStrategy::SPREAD,
                                                 rpc::PlacementGroupTableData::CREATED,
                                                 rpc::PlacementGroupStats::FINISHED);
  data2.set_name("modified_pg");

  auto event1 = std::make_unique<RayPlacementGroupDefinitionEvent>(data1, "test_session");
  auto event2 = std::make_unique<RayPlacementGroupDefinitionEvent>(data2, "test_session");

  event1->Merge(std::move(*event2));
  auto serialized_event = std::move(*event1).Serialize();

  const auto &pg_def = serialized_event.placement_group_definition_event();
  // Original data should be preserved, not merged
  ASSERT_EQ(pg_def.name(), "test_pg");
  ASSERT_EQ(pg_def.strategy(), rpc::PlacementStrategy::PACK);
}

class RayPlacementGroupLifecycleEventTest : public ::testing::Test {};

// Parametrized test for different lifecycle states
class LifecycleEventStateTest
    : public ::testing::TestWithParam<
          std::pair<rpc::PlacementGroupTableData::PlacementGroupState,
                    rpc::events::PlacementGroupLifecycleEvent::State>> {};

TEST_P(LifecycleEventStateTest, TestStateConversion) {
  auto [table_state, expected_event_state] = GetParam();
  auto data = CreateTestPlacementGroupTableData(
      rpc::PlacementStrategy::PACK, table_state, rpc::PlacementGroupStats::QUEUED);

  auto converted_state = RayPlacementGroupLifecycleEvent::ConvertState(table_state);
  ASSERT_EQ(converted_state, expected_event_state);

  auto event = std::make_unique<RayPlacementGroupLifecycleEvent>(
      data, converted_state, "test_session");
  auto serialized_event = std::move(*event).Serialize();

  ASSERT_TRUE(serialized_event.has_placement_group_lifecycle_event());
  const auto &pg_life = serialized_event.placement_group_lifecycle_event();
  ASSERT_EQ(pg_life.state_transitions_size(), 1);
  ASSERT_EQ(pg_life.state_transitions(0).state(), expected_event_state);
}

INSTANTIATE_TEST_SUITE_P(
    States,
    LifecycleEventStateTest,
    ::testing::Values(
        std::make_pair(rpc::PlacementGroupTableData::PENDING,
                       rpc::events::PlacementGroupLifecycleEvent::PENDING),
        std::make_pair(rpc::PlacementGroupTableData::PREPARED,
                       rpc::events::PlacementGroupLifecycleEvent::PREPARED),
        std::make_pair(rpc::PlacementGroupTableData::CREATED,
                       rpc::events::PlacementGroupLifecycleEvent::CREATED),
        std::make_pair(rpc::PlacementGroupTableData::REMOVED,
                       rpc::events::PlacementGroupLifecycleEvent::REMOVED),
        std::make_pair(rpc::PlacementGroupTableData::RESCHEDULING,
                       rpc::events::PlacementGroupLifecycleEvent::RESCHEDULING)));

// Parametrized test for different scheduling states
class LifecycleEventSchedulingStateTest
    : public ::testing::TestWithParam<
          std::pair<rpc::PlacementGroupStats::SchedulingState,
                    rpc::events::PlacementGroupLifecycleEvent::SchedulingState>> {};

TEST_P(LifecycleEventSchedulingStateTest, TestSchedulingStateConversion) {
  auto [stats_state, expected_event_state] = GetParam();
  auto data = CreateTestPlacementGroupTableData(
      rpc::PlacementStrategy::PACK, rpc::PlacementGroupTableData::PENDING, stats_state);

  auto converted_state =
      RayPlacementGroupLifecycleEvent::ConvertSchedulingState(stats_state);
  ASSERT_EQ(converted_state, expected_event_state);

  auto event = std::make_unique<RayPlacementGroupLifecycleEvent>(
      data, rpc::events::PlacementGroupLifecycleEvent::PENDING, "test_session");
  auto serialized_event = std::move(*event).Serialize();

  const auto &pg_life = serialized_event.placement_group_lifecycle_event();
  ASSERT_EQ(pg_life.state_transitions(0).scheduling_state(), expected_event_state);
}

INSTANTIATE_TEST_SUITE_P(
    SchedulingStates,
    LifecycleEventSchedulingStateTest,
    ::testing::Values(
        std::make_pair(rpc::PlacementGroupStats::QUEUED,
                       rpc::events::PlacementGroupLifecycleEvent::QUEUED),
        std::make_pair(rpc::PlacementGroupStats::REMOVED,
                       rpc::events::PlacementGroupLifecycleEvent::SCHEDULING_REMOVED),
        std::make_pair(rpc::PlacementGroupStats::SCHEDULING_STARTED,
                       rpc::events::PlacementGroupLifecycleEvent::SCHEDULING_STARTED),
        std::make_pair(rpc::PlacementGroupStats::NO_RESOURCES,
                       rpc::events::PlacementGroupLifecycleEvent::NO_RESOURCES),
        std::make_pair(rpc::PlacementGroupStats::INFEASIBLE,
                       rpc::events::PlacementGroupLifecycleEvent::INFEASIBLE),
        std::make_pair(
            rpc::PlacementGroupStats::FAILED_TO_COMMIT_RESOURCES,
            rpc::events::PlacementGroupLifecycleEvent::FAILED_TO_COMMIT_RESOURCES),
        std::make_pair(rpc::PlacementGroupStats::FINISHED,
                       rpc::events::PlacementGroupLifecycleEvent::FINISHED)));

// Parametrized test for merging different numbers of transitions
class EventMergingTest : public ::testing::TestWithParam<int> {};

TEST_P(EventMergingTest, TestMergeMultipleTransitions) {
  int num_transitions = GetParam();
  auto data = CreateTestPlacementGroupTableData(rpc::PlacementStrategy::PACK,
                                                rpc::PlacementGroupTableData::PENDING,
                                                rpc::PlacementGroupStats::QUEUED);

  std::vector<rpc::events::PlacementGroupLifecycleEvent::State> states = {
      rpc::events::PlacementGroupLifecycleEvent::PENDING,
      rpc::events::PlacementGroupLifecycleEvent::PREPARED,
      rpc::events::PlacementGroupLifecycleEvent::CREATED,
      rpc::events::PlacementGroupLifecycleEvent::RESCHEDULING,
      rpc::events::PlacementGroupLifecycleEvent::REMOVED};

  auto first_event =
      std::make_unique<RayPlacementGroupLifecycleEvent>(data, states[0], "test_session");

  for (int i = 1; i < num_transitions && i < static_cast<int>(states.size()); ++i) {
    auto next_event = std::make_unique<RayPlacementGroupLifecycleEvent>(
        data, states[i], "test_session");
    first_event->Merge(std::move(*next_event));
  }

  auto serialized_event = std::move(*first_event).Serialize();

  ASSERT_EQ(serialized_event.source_type(), rpc::events::RayEvent::GCS);
  ASSERT_EQ(serialized_event.event_type(),
            rpc::events::RayEvent::PLACEMENT_GROUP_LIFECYCLE_EVENT);

  const auto &pg_life = serialized_event.placement_group_lifecycle_event();
  ASSERT_EQ(pg_life.placement_group_id(), "test_pg_id");
  ASSERT_EQ(pg_life.state_transitions_size(),
            std::min(num_transitions, static_cast<int>(states.size())));

  for (int i = 0; i < pg_life.state_transitions_size(); ++i) {
    ASSERT_EQ(pg_life.state_transitions(i).state(), states[i]);
    ASSERT_TRUE(pg_life.state_transitions(i).has_timestamp());
  }
}

INSTANTIATE_TEST_SUITE_P(TransitionCounts, EventMergingTest, ::testing::Values(2, 3, 5));

TEST_F(RayPlacementGroupLifecycleEventTest, TestCreatedStateIncludesLatencies) {
  auto data = CreateTestPlacementGroupTableData(rpc::PlacementStrategy::PACK,
                                                rpc::PlacementGroupTableData::CREATED,
                                                rpc::PlacementGroupStats::FINISHED);

  auto event = std::make_unique<RayPlacementGroupLifecycleEvent>(
      data, rpc::events::PlacementGroupLifecycleEvent::CREATED, "test_session");
  auto serialized_event = std::move(*event).Serialize();

  const auto &pg_life = serialized_event.placement_group_lifecycle_event();
  const auto &transition = pg_life.state_transitions(0);

  ASSERT_EQ(transition.state(), rpc::events::PlacementGroupLifecycleEvent::CREATED);
  ASSERT_EQ(transition.scheduling_state(),
            rpc::events::PlacementGroupLifecycleEvent::FINISHED);
  ASSERT_EQ(transition.scheduling_latency_us(), 500);
  ASSERT_EQ(transition.end_to_end_creation_latency_us(), 1000);
  ASSERT_EQ(transition.scheduling_attempt(), 3);
  ASSERT_DOUBLE_EQ(transition.highest_retry_delay_ms(), 100.5);
  ASSERT_EQ(transition.scheduling_started_time_ns(), 1000100000);
}

TEST_F(RayPlacementGroupLifecycleEventTest, TestNonCreatedStateOmitsLatencies) {
  auto data = CreateTestPlacementGroupTableData(rpc::PlacementStrategy::PACK,
                                                rpc::PlacementGroupTableData::PENDING,
                                                rpc::PlacementGroupStats::NO_RESOURCES);

  auto event = std::make_unique<RayPlacementGroupLifecycleEvent>(
      data, rpc::events::PlacementGroupLifecycleEvent::PENDING, "test_session");
  auto serialized_event = std::move(*event).Serialize();

  const auto &pg_life = serialized_event.placement_group_lifecycle_event();
  const auto &transition = pg_life.state_transitions(0);

  ASSERT_EQ(transition.state(), rpc::events::PlacementGroupLifecycleEvent::PENDING);
  // Latencies should not be set for non-CREATED states
  ASSERT_EQ(transition.scheduling_latency_us(), 0);
  ASSERT_EQ(transition.end_to_end_creation_latency_us(), 0);
  // But other stats should still be present
  ASSERT_EQ(transition.scheduling_attempt(), 3);
  ASSERT_DOUBLE_EQ(transition.highest_retry_delay_ms(), 100.5);
}

}  // namespace observability
}  // namespace ray
