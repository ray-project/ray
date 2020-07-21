// Copyright 2017 The Ray Authors.
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

#include <string>
#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/id.h"
#include "ray/raylet/scheduling/cluster_resource_scheduler.h"
#include "ray/raylet/scheduling/cluster_task_manager.h"
#include "ray/raylet/scheduling/scheduling_ids.h"

#ifdef UNORDERED_VS_ABSL_MAPS_EVALUATION
#include <chrono>

#include "absl/container/flat_hash_map.h"
#endif  // UNORDERED_VS_ABSL_MAPS_EVALUATION

// using namespace std;

namespace ray {

namespace raylet {

std::shared_ptr<ClusterResourceScheduler> CreateSingleNodeScheduler() {
  // Define node resources
  NodeResources node_resources;
  ResourceCapacity cpu_capacity;
  cpu_capacity.total = cpu_capacity.available = 8;
  node_resources.predefined_resources.push_back(cpu_capacity);
  ResourceCapacity mem_capacity;
  mem_capacity.total = mem_capacity.available = 256;
  node_resources.predefined_resources.push_back(mem_capacity);


  for (int i = node_resources.predefined_resources.size(); i < PredefinedResources_MAX; i++) {
    ResourceCapacity rc;
    rc.total = rc.available = 0;
    node_resources.predefined_resources.push_back(rc);
  }

  auto scheduler = std::make_shared<ClusterResourceScheduler>(ClusterResourceScheduler(1, node_resources));

  return scheduler;
}

std::shared_ptr<ClusterTaskManager> create_task_manager() {

  ClientID node_id = ClientID::FromBinary(1);
}

class ClusterTaskManagerTest : public ::testing::Test {
public:
  void SetUp() {}

  void Shutdown() {}
};

TEST_F(ClusterTaskManagerTest, SampleTest) {
  ASSERT_TRUE(true);
}

} // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
