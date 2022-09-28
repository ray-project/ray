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

#include "ray/raylet/scheduling/local_resource_manager.h"

#include "gtest/gtest.h"

namespace ray {

NodeResources CreateNodeResources(double available_cpu,
                                  double total_cpu,
                                  double available_custom_resource = 0,
                                  double total_custom_resource = 0,
                                  bool object_pulls_queued = false) {
  NodeResources resources;
  resources.available.Set(ResourceID::CPU(), available_cpu);
  resources.total.Set(ResourceID::CPU(), total_cpu);
  resources.available.Set(scheduling::ResourceID("CUSTOM"), available_custom_resource);
  resources.total.Set(scheduling::ResourceID("CUSTOM"), total_custom_resource);
  resources.object_pulls_queued = object_pulls_queued;
  return resources;
}

class LocalResourceManagerTest : public ::testing::Test {
  void SetUp() {
    ::testing::Test::SetUp();
    manager = nullptr;
  }

  scheduling::NodeID local_node_id = scheduling::NodeID(0);
  std::unique_ptr<LocalResourceManager> manager;
};

TEST_F(LocalResourceManagerTest, BasicMetricsTest) {
  // SANG-TODO Add unit tests
  manager = std::make_unique<LocalResourceManager>(
      local_node_id, CreateNodeResources(8, 8), nullptr, nullptr, nullptr);
}

}  // namespace ray
