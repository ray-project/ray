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
#include "ray/raylet/scheduling/cluster_resource_scheduler.h"
#include "ray/raylet/scheduling/policy/scheduling_context.h"
#include "ray/raylet/scheduling/policy/scheduling_options.h"

namespace ray {

using raylet_scheduling_policy::BundleSchedulingContext;
using raylet_scheduling_policy::SchedulingType;

class GcsResourceSchedulerTest : public ::testing::Test {
 public:
  void SetUp() override {
    cluster_resource_scheduler_ = std::make_shared<ClusterResourceScheduler>(
        io_context_,
        scheduling::NodeID(NodeID::FromRandom().Binary()),
        NodeResources(),
        /*is_node_available_fn=*/
        [](auto) { return true; },
        /*is_local_node_with_raylet=*/false);
  }

  void TearDown() override { cluster_resource_scheduler_.reset(); }

  void AddNode(const rpc::GcsNodeInfo &node) {
    scheduling::NodeID node_id(node.node_id());
    auto &cluster_resource_manager =
        cluster_resource_scheduler_->GetClusterResourceManager();
    for (const auto &entry : node.resources_total()) {
      cluster_resource_manager.UpdateResourceCapacity(
          node_id, scheduling::ResourceID(entry.first), entry.second);
    }
  }

  void AddClusterResources(const NodeID &node_id,
                           const std::string &resource_name,
                           double resource_value) {
    auto node = std::make_shared<rpc::GcsNodeInfo>();
    node->set_node_id(node_id.Binary());
    (*node->mutable_resources_total())[resource_name] = resource_value;
    AddNode(*node);
  }

  void AddClusterResources(const NodeID &node_id,
                           const std::vector<std::pair<std::string, double>> &resource) {
    auto node = std::make_shared<rpc::GcsNodeInfo>();
    node->set_node_id(node_id.Binary());
    for (auto r : resource) {
      (*node->mutable_resources_total())[r.first] = r.second;
    }
    AddNode(*node);
  }

  void CheckClusterAvailableResources(const NodeID &node_id,
                                      const std::string &resource_name,
                                      double resource_value) {
    const auto &cluster_resource_manager =
        cluster_resource_scheduler_->GetClusterResourceManager();
    const auto &node_resources =
        cluster_resource_manager.GetNodeResources(scheduling::NodeID(node_id.Binary()));
    auto resource_id = scheduling::ResourceID(resource_name);

    ASSERT_TRUE(node_resources.available.Has(resource_id));
    ASSERT_EQ(node_resources.available.Get(resource_id).Double(), resource_value);
  }

  void TestResourceLeaks(SchedulingOptions scheduling_options) {
    // Add node resources.
    auto node_id = NodeID::FromRandom();
    const std::string cpu_resource = "CPU";
    const double node_cpu_num = 6.0;
    AddClusterResources(node_id, cpu_resource, node_cpu_num);

    // Scheduling succeeded and node resources are used up.
    std::vector<ResourceRequest> requests;
    absl::flat_hash_map<std::string, double> resource_map;
    for (int bundle_cpu_num = 1; bundle_cpu_num <= 3; ++bundle_cpu_num) {
      resource_map[cpu_resource] = bundle_cpu_num;
      requests.emplace_back(ResourceMapToResourceRequest(
          resource_map, /*requires_object_store_memory=*/false));
    }

    std::vector<const ResourceRequest *> resource_request_list;
    for (auto &request : requests) {
      resource_request_list.emplace_back(&request);
    }
    const auto &result1 =
        cluster_resource_scheduler_->Schedule(resource_request_list, scheduling_options);
    ASSERT_TRUE(result1.status.IsSuccess());
    ASSERT_EQ(result1.selected_nodes.size(), 3);

    // Check for resource leaks.
    CheckClusterAvailableResources(node_id, cpu_resource, node_cpu_num);

    // Scheduling failure.
    resource_map[cpu_resource] = 5;
    requests.emplace_back(
        ResourceMapToResourceRequest(resource_map,
                                     /*requires_object_store_memory=*/false));

    resource_request_list.clear();
    for (auto &request : requests) {
      resource_request_list.emplace_back(&request);
    }
    const auto &result2 =
        cluster_resource_scheduler_->Schedule(resource_request_list, scheduling_options);
    ASSERT_TRUE(result2.status.IsFailed());
    ASSERT_EQ(result2.selected_nodes.size(), 0);

    // Check for resource leaks.
    CheckClusterAvailableResources(node_id, cpu_resource, node_cpu_num);
  }

  void TestBinPackingByPriority(SchedulingOptions scheduling_options) {
    // Add node resources.
    std::string cpu_resource = "CPU";
    std::string gpu_resource = "GPU";
    std::string mem_resource = "memory";
    std::string custom_resource = "custom";

    std::vector<std::vector<std::pair<std::string, double>>> resources_list;

    resources_list.emplace_back(std::vector({std::make_pair(cpu_resource, 1.0)}));
    resources_list.emplace_back(std::vector({std::make_pair(cpu_resource, 2.0)}));
    resources_list.emplace_back(std::vector({std::make_pair(cpu_resource, 3.0)}));
    resources_list.emplace_back(std::vector(
        {std::make_pair(cpu_resource, 1.0), std::make_pair(mem_resource, 1.0)}));
    resources_list.emplace_back(std::vector(
        {std::make_pair(cpu_resource, 1.0), std::make_pair(mem_resource, 2.0)}));
    resources_list.emplace_back(std::vector(
        {std::make_pair(cpu_resource, 1.0), std::make_pair(gpu_resource, 1.0)}));
    resources_list.emplace_back(std::vector(
        {std::make_pair(cpu_resource, 1.0), std::make_pair(gpu_resource, 2.0)}));
    resources_list.emplace_back(std::vector(
        {std::make_pair(cpu_resource, 1.0), std::make_pair(custom_resource, 1.0)}));
    resources_list.emplace_back(std::vector(
        {std::make_pair(cpu_resource, 1.0), std::make_pair(custom_resource, 2.0)}));

    std::vector<NodeID> node_ids;
    for (auto r : resources_list) {
      node_ids.emplace_back(NodeID::FromRandom());
      AddClusterResources(node_ids.back(), r);
    }

    // Scheduling succeeded and node resources are used up.
    std::vector<ResourceRequest> requests;
    for (auto resources : resources_list) {
      absl::flat_hash_map<std::string, double> resource_map;
      for (auto r : resources) {
        resource_map[r.first] = r.second;
      }
      requests.emplace_back(ResourceMapToResourceRequest(
          resource_map, /*requires_object_store_memory=*/false));
    }

    std::vector<const ResourceRequest *> resource_request_list;
    for (auto &request : requests) {
      resource_request_list.emplace_back(&request);
    }
    auto result =
        cluster_resource_scheduler_->Schedule(resource_request_list, scheduling_options);
    ASSERT_TRUE(result.status.IsSuccess());
    ASSERT_EQ(result.selected_nodes.size(), resources_list.size());
  }
  instrumented_io_context io_context_;
  std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler_;
};

TEST_F(GcsResourceSchedulerTest, TestPackBinPackingByPriority) {
  TestBinPackingByPriority(SchedulingOptions::BundlePack());
}

TEST_F(GcsResourceSchedulerTest, TestStrictSpreadBinPackingByPriority) {
  TestBinPackingByPriority(SchedulingOptions::BundleStrictSpread());
}

TEST_F(GcsResourceSchedulerTest, TestSpreadBinPackingByPriority) {
  TestBinPackingByPriority(SchedulingOptions::BundleSpread());
}

TEST_F(GcsResourceSchedulerTest, TestPackScheduleResourceLeaks) {
  TestResourceLeaks(SchedulingOptions::BundlePack());
}

TEST_F(GcsResourceSchedulerTest, TestSpreadScheduleResourceLeaks) {
  TestResourceLeaks(SchedulingOptions::BundleSpread());
}

TEST_F(GcsResourceSchedulerTest, TestNodeFilter) {
  // Add node resources.
  const auto &node_id = NodeID::FromRandom();
  const std::string cpu_resource = "CPU";
  const double node_cpu_num = 10.0;
  AddClusterResources(node_id, cpu_resource, node_cpu_num);

  // Scheduling failure.
  absl::flat_hash_map<std::string, double> resource_map{{cpu_resource, 1}};
  std::vector<ResourceRequest> requests;
  requests.emplace_back(
      ResourceMapToResourceRequest(resource_map, /*requires_object_store_memory=*/false));

  std::vector<const ResourceRequest *> resource_request_list;
  for (auto &request : requests) {
    resource_request_list.emplace_back(&request);
  }

  auto bundle_locations = std::make_shared<BundleLocations>();
  BundleID bundle_id{PlacementGroupID::Of(JobID::FromInt(1)), 0};
  bundle_locations->emplace(bundle_id, std::make_pair(node_id, nullptr));
  auto result1 = cluster_resource_scheduler_->Schedule(
      resource_request_list,
      SchedulingOptions::BundleStrictSpread(
          /*max_cpu_fraction_per_node*/ 1.0,
          std::make_unique<BundleSchedulingContext>(bundle_locations)));
  ASSERT_TRUE(result1.status.IsInfeasible());
  ASSERT_EQ(result1.selected_nodes.size(), 0);

  // Scheduling succeeded.
  auto result2 = cluster_resource_scheduler_->Schedule(
      resource_request_list,
      SchedulingOptions::BundleStrictSpread(
          /*max_cpu_fraction_per_node*/ 1.0,
          std::make_unique<BundleSchedulingContext>(nullptr)));
  ASSERT_TRUE(result2.status.IsSuccess());
  ASSERT_EQ(result2.selected_nodes.size(), 1);
}

TEST_F(GcsResourceSchedulerTest, TestSchedulingResultStatusForStrictStrategy) {
  // Init resources with two node.
  const auto &node_one_id = NodeID::FromRandom();
  const auto &node_tow_id = NodeID::FromRandom();
  const std::string cpu_resource = "CPU";
  const double node_cpu_num = 10.0;
  AddClusterResources(node_one_id, cpu_resource, node_cpu_num);
  AddClusterResources(node_tow_id, cpu_resource, node_cpu_num);

  // Mock a request that has three required resources.
  std::vector<ResourceRequest> requests;
  absl::flat_hash_map<std::string, double> resource_map{{cpu_resource, 1}};
  for (int node_number = 0; node_number < 3; node_number++) {
    requests.emplace_back(ResourceMapToResourceRequest(
        resource_map, /*requires_object_store_memory=*/false));
  }

  std::vector<const ResourceRequest *> resource_request_list;
  for (auto &request : requests) {
    resource_request_list.emplace_back(&request);
  }
  auto result1 = cluster_resource_scheduler_->Schedule(
      resource_request_list, SchedulingOptions::BundleStrictSpread());
  ASSERT_TRUE(result1.status.IsInfeasible());
  ASSERT_EQ(result1.selected_nodes.size(), 0);

  // Check for resource leaks.
  CheckClusterAvailableResources(node_one_id, cpu_resource, node_cpu_num);
  CheckClusterAvailableResources(node_tow_id, cpu_resource, node_cpu_num);

  // Mock a request that only has one required resource but bigger than the maximum
  // resource.
  requests.clear();
  resource_map.clear();
  resource_map[cpu_resource] = 50;
  requests.emplace_back(
      ResourceMapToResourceRequest(resource_map, /*requires_object_store_memory=*/false));

  resource_request_list.clear();
  for (auto &request : requests) {
    resource_request_list.emplace_back(&request);
  }
  const auto &result2 = cluster_resource_scheduler_->Schedule(
      resource_request_list, SchedulingOptions::BundleStrictPack());
  ASSERT_TRUE(result2.status.IsInfeasible());
  ASSERT_EQ(result2.selected_nodes.size(), 0);

  // Check for resource leaks.
  CheckClusterAvailableResources(node_one_id, cpu_resource, node_cpu_num);
  CheckClusterAvailableResources(node_tow_id, cpu_resource, node_cpu_num);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
