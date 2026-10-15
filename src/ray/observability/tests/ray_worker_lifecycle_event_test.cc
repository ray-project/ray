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

#include "ray/observability/ray_worker_lifecycle_event.h"

#include "gtest/gtest.h"

namespace ray {
namespace observability {

class RayWorkerLifecycleEventTest : public ::testing::Test {};

TEST_F(RayWorkerLifecycleEventTest, TestMergeAndSerialize) {
  // ALIVE transition.
  rpc::WorkerTableData alive;
  alive.set_is_alive(true);
  alive.set_worker_type(rpc::WorkerType::WORKER);
  alive.set_pid(54321);
  alive.set_worker_launch_time_ms(800);
  alive.set_worker_launched_time_ms(900);
  alive.set_start_time_ms(1000);
  alive.mutable_worker_address()->set_worker_id("worker-123");
  alive.mutable_worker_address()->set_node_id("node-1");
  auto event1 = std::make_unique<RayWorkerLifecycleEvent>(alive, "sess1");

  // DEAD transition (idle recycle).
  rpc::WorkerTableData dead;
  dead.set_is_alive(false);
  dead.set_worker_type(rpc::WorkerType::WORKER);
  dead.set_pid(54321);
  dead.set_end_time_ms(2000);
  dead.set_exit_type(rpc::WorkerExitType::NODE_OUT_OF_MEMORY);
  dead.set_exit_detail("out of memory");
  dead.set_memory_used_bytes_at_death(987654321);
  dead.set_job_id("job-1");
  dead.mutable_worker_address()->set_worker_id("worker-123");
  dead.mutable_worker_address()->set_node_id("node-1");
  auto event2 = std::make_unique<RayWorkerLifecycleEvent>(dead, "sess1");

  event1->Merge(std::move(*event2));
  auto serialized = std::move(*event1).Serialize().value();

  ASSERT_EQ(serialized.source_type(), rpc::events::RayEvent::GCS);
  ASSERT_EQ(serialized.session_name(), "sess1");
  ASSERT_EQ(serialized.event_type(), rpc::events::RayEvent::WORKER_LIFECYCLE_EVENT);
  ASSERT_EQ(serialized.severity(), rpc::events::RayEvent::INFO);
  ASSERT_TRUE(serialized.has_worker_lifecycle_event());

  const auto &w = serialized.worker_lifecycle_event();
  ASSERT_EQ(w.worker_id(), "worker-123");
  ASSERT_EQ(w.job_id(), "job-1");
  ASSERT_EQ(w.state_transitions_size(), 2);

  ASSERT_EQ(w.state_transitions(0).state(), rpc::events::WorkerLifecycleEvent::ALIVE);
  ASSERT_EQ(w.state_transitions(0).worker_launch_time_ms(), 800);
  ASSERT_EQ(w.state_transitions(0).worker_launched_time_ms(), 900);
  ASSERT_EQ(w.state_transitions(0).start_time_ms(), 1000);

  ASSERT_EQ(w.state_transitions(1).state(), rpc::events::WorkerLifecycleEvent::DEAD);
  ASSERT_EQ(w.state_transitions(1).end_time_ms(), 2000);
  ASSERT_EQ(w.state_transitions(1).death_info().exit_type(),
            rpc::WorkerExitType::NODE_OUT_OF_MEMORY);
  ASSERT_EQ(w.state_transitions(1).death_info().exit_detail(), "out of memory");
  ASSERT_TRUE(w.state_transitions(1).death_info().has_memory_used_bytes());
  ASSERT_EQ(w.state_transitions(1).death_info().memory_used_bytes(), 987654321);

  // A non-OOM death carries no memory reading so the event's memory_used_bytes must stay
  // absent rather than default to 0
  rpc::WorkerTableData non_oom_dead;
  non_oom_dead.set_is_alive(false);
  non_oom_dead.set_worker_type(rpc::WorkerType::WORKER);
  non_oom_dead.set_exit_type(rpc::WorkerExitType::INTENDED_SYSTEM_EXIT);
  non_oom_dead.set_exit_detail("idle recycle");
  // memory_used_bytes_at_death intentionally left unset.
  ASSERT_FALSE(non_oom_dead.has_memory_used_bytes_at_death());

  auto non_oom_event = std::make_unique<RayWorkerLifecycleEvent>(non_oom_dead, "sess1");
  auto non_oom_serialized = std::move(*non_oom_event).Serialize().value();
  const auto &death_info =
      non_oom_serialized.worker_lifecycle_event().state_transitions(0).death_info();
  ASSERT_FALSE(death_info.has_memory_used_bytes());
  ASSERT_EQ(death_info.memory_used_bytes(), 0);
}

}  // namespace observability
}  // namespace ray
