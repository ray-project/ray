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

#include <memory>

#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/gcs_server/gcs_resource_scheduler.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {

using ::testing::_;

class GcsResourceSchedulerTest : public ::testing::Test {
 public:
  void SetUp() override {
    gcs_resource_manager_ =
        std::make_shared<gcs::GcsResourceManager>(io_service_, nullptr, nullptr, true);
    gcs_resource_scheduler_ =
        std::make_shared<gcs::GcsResourceScheduler>(*gcs_resource_manager_);
  }

  void TearDown() override {
    gcs_resource_scheduler_.reset();
    gcs_resource_manager_.reset();
  }

  void AddClusterResources(const NodeID &node_id, const std::string &resource_name,
                           double resource_value) {
    std::unordered_map<std::string, double> resource_map;
    resource_map[resource_name] = resource_value;
    gcs_resource_manager_->UpdateResourceCapacity(node_id, resource_map);
  }

  void CheckClusterAvailableResources(const NodeID &node_id,
                                      const std::string &resource_name,
                                      double resource_value) {
    const auto &cluster_resource = gcs_resource_manager_->GetClusterResources();
    auto iter = cluster_resource.find(node_id);
    ASSERT_TRUE(iter != cluster_resource.end());
    ASSERT_EQ(iter->second.GetAvailableResources().GetResource(resource_name).Double(),
              resource_value);
  }

  void TestResourceLeaks(const gcs::SchedulingType &scheduling_type) {
    // Add node resources.
    const auto &node_id = NodeID::FromRandom();
    const std::string cpu_resource = "CPU";
    const double node_cpu_num = 6.0;
    AddClusterResources(node_id, cpu_resource, node_cpu_num);

    // Scheduling succeeded and node resources are used up.
    std::vector<ResourceSet> required_resources_list;
    std::unordered_map<std::string, double> resource_map;
    for (int bundle_cpu_num = 1; bundle_cpu_num <= 3; ++bundle_cpu_num) {
      resource_map[cpu_resource] = bundle_cpu_num;
      required_resources_list.emplace_back(resource_map);
    }
    const auto &result1 =
        gcs_resource_scheduler_->Schedule(required_resources_list, scheduling_type);
    ASSERT_EQ(result1.size(), 3);

    // Check for resource leaks.
    CheckClusterAvailableResources(node_id, cpu_resource, node_cpu_num);

    // Scheduling failure.
    resource_map[cpu_resource] = 5;
    required_resources_list.emplace_back(resource_map);
    const auto &result2 =
        gcs_resource_scheduler_->Schedule(required_resources_list, scheduling_type);
    ASSERT_EQ(result2.size(), 0);

    // Check for resource leaks.
    CheckClusterAvailableResources(node_id, cpu_resource, node_cpu_num);
  }

  std::shared_ptr<gcs::GcsResourceManager> gcs_resource_manager_;
  std::shared_ptr<gcs::GcsResourceScheduler> gcs_resource_scheduler_;

 private:
  instrumented_io_context io_service_;
};

TEST_F(GcsResourceSchedulerTest, TestPackScheduleResourceLeaks) {
  TestResourceLeaks(gcs::SchedulingType::PACK);
}

TEST_F(GcsResourceSchedulerTest, TestSpreadScheduleResourceLeaks) {
  TestResourceLeaks(gcs::SchedulingType::SPREAD);
}

TEST_F(GcsResourceSchedulerTest, TestNodeFilter) {
  // Add node resources.
  const auto &node_id = NodeID::FromRandom();
  const std::string cpu_resource = "CPU";
  const double node_cpu_num = 10.0;
  AddClusterResources(node_id, cpu_resource, node_cpu_num);

  // Scheduling failure.
  std::vector<ResourceSet> required_resources_list;
  std::unordered_map<std::string, double> resource_map;
  resource_map[cpu_resource] = 1;
  required_resources_list.emplace_back(resource_map);
  const auto &result1 = gcs_resource_scheduler_->Schedule(
      required_resources_list, gcs::SchedulingType::STRICT_SPREAD,
      [](const NodeID &) { return false; });
  ASSERT_EQ(result1.size(), 0);

  // Scheduling succeeded.
  const auto &result2 = gcs_resource_scheduler_->Schedule(
      required_resources_list, gcs::SchedulingType::STRICT_SPREAD,
      [](const NodeID &) { return true; });
  ASSERT_EQ(result2.size(), 1);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
