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

#include "ray/observability/ray_worker_definition_event.h"

#include "gtest/gtest.h"

namespace ray {
namespace observability {

class RayWorkerDefinitionEventTest : public ::testing::Test {};

TEST_F(RayWorkerDefinitionEventTest, TestSerialize) {
  rpc::WorkerTableData data;
  data.set_worker_type(rpc::WorkerType::SPILL_WORKER);
  data.set_pid(54321);
  data.mutable_worker_address()->set_worker_id("worker-123");
  data.mutable_worker_address()->set_node_id("node-1");
  auto event = std::make_unique<RayWorkerDefinitionEvent>(data, "sess1");

  ASSERT_EQ(event->GetEntityId(), "worker-123");

  auto serialized = std::move(*event).Serialize().value();
  ASSERT_EQ(serialized.source_type(), rpc::events::RayEvent::GCS);
  ASSERT_EQ(serialized.session_name(), "sess1");
  ASSERT_EQ(serialized.event_type(), rpc::events::RayEvent::WORKER_DEFINITION_EVENT);
  ASSERT_EQ(serialized.severity(), rpc::events::RayEvent::INFO);
  ASSERT_TRUE(serialized.has_worker_definition_event());

  const auto &w = serialized.worker_definition_event();
  ASSERT_EQ(w.worker_id(), "worker-123");
  ASSERT_EQ(w.node_id(), "node-1");
  ASSERT_EQ(w.worker_type(), rpc::WorkerType::SPILL_WORKER);
  ASSERT_EQ(w.pid(), 54321);
}

TEST_F(RayWorkerDefinitionEventTest, TestMergeIsStatic) {
  // Definition events are static: merging a second event must not change the data.
  rpc::WorkerTableData first;
  first.set_worker_type(rpc::WorkerType::WORKER);
  first.set_pid(1);
  first.mutable_worker_address()->set_worker_id("worker-123");
  first.mutable_worker_address()->set_node_id("node-1");
  auto event1 = std::make_unique<RayWorkerDefinitionEvent>(first, "sess1");

  rpc::WorkerTableData second;
  second.set_worker_type(rpc::WorkerType::WORKER);
  second.set_pid(2);
  second.mutable_worker_address()->set_worker_id("worker-123");
  second.mutable_worker_address()->set_node_id("node-2");
  auto event2 = std::make_unique<RayWorkerDefinitionEvent>(second, "sess1");

  event1->Merge(std::move(*event2));
  auto serialized = std::move(*event1).Serialize().value();
  const auto &w = serialized.worker_definition_event();
  ASSERT_EQ(w.pid(), 1);
  ASSERT_EQ(w.node_id(), "node-1");
}

}  // namespace observability
}  // namespace ray
