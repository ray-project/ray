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

// clang-format off
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/ray_syncer/ray_syncer.h"
#include "mock/ray/common/ray_syncer/ray_syncer.h"
// clang-format on


using namespace ray::syncer;

class RaySyncerTest : public ::testing::Test {
 protected:
  void SetUp() override {
  }

  void TearDown() override {
  }
};


TEST(NodeStatus, GetSnapshot) {
  auto node_status = std::make_unique<NodeStatus>();
  node_status->SetComponents(RayComponentId::RESOURCE_MANAGER, nullptr, nullptr);
  ASSERT_EQ(std::nullopt, node_status->GetSnapshot(RayComponentId::RESOURCE_MANAGER));
  ASSERT_EQ(std::nullopt, node_status->GetSnapshot(RayComponentId::SCHEDULER));

  auto reporter = std::make_unique<MockReporterInterface>();
  ASSERT_TRUE(node_status->SetComponents(RayComponentId::RESOURCE_MANAGER, reporter.get(), nullptr));

}
