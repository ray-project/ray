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

// clang-format off
#include "ray/raylet/scheduling/cluster_resource_scheduler.h"

#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/ray_config.h"
#include "ray/common/task/scheduling_resources.h"
#include "ray/raylet/scheduling/scheduling_ids.h"
#include "mock/ray/gcs/gcs_client/gcs_client.h"
#ifdef UNORDERED_VS_ABSL_MAPS_EVALUATION
#include <chrono>

#include "absl/container/flat_hash_map.h"
#endif  // UNORDERED_VS_ABSL_MAPS_EVALUATION
// clang-format on
using namespace std;

#define ASSERT_RESOURCES_EQ(data, expected_available, expected_total) \
  {                                                                   \
    auto available = data->resources_available();                     \
    ASSERT_EQ(available[kCPU_ResourceLabel], expected_available);     \
    auto total = data->resources_total();                             \
    ASSERT_EQ(total[kCPU_ResourceLabel], expected_total);             \
  }

#define ASSERT_RESOURCES_EMPTY(data)                   \
  {                                                    \
    ASSERT_FALSE(data->resources_available_changed()); \
    ASSERT_TRUE(data->resources_available().empty());  \
    ASSERT_TRUE(data->resources_total().empty());      \
  }

namespace ray {

namespace scheduling {

/// Helper function to set which resource IDs are condiered unit-instance resources.
/// Note, this function must have the same namespace as "ResourceID".
void SetUnitInstanceResourceIds(absl::flat_hash_set<ResourceID> ids) {
  auto &set = ResourceID::UnitInstanceResources();
  set.clear();
  for (auto id : ids) {
    set.insert(id.ToInt());
  }
}

}  // namespace scheduling

using scheduling::SetUnitInstanceResourceIds;

ResourceRequest CreateResourceRequest(
    const absl::flat_hash_map<ResourceID, double> &resource_map) {
  ResourceRequest request;
  for (auto &entry : resource_map) {
    request.Set(entry.first, entry.second);
  }
  return request;
}

NodeResources CreateNodeResources(
    const absl::flat_hash_map<ResourceID, double> &resource_map) {
  return NodeResources(CreateResourceRequest(resource_map));
}

ResourceRequest RandomResourceRequest() {
  auto ids = {ResourceID::CPU(),
              ResourceID::Memory(),
              ResourceID::GPU(),
              ResourceID("custom1"),
              ResourceID("custom2")};
  ResourceRequest resource_request;
  for (auto &id : ids) {
    if (rand() % 3 != 0) {
      resource_request.Set(id, rand() % 10);
    }
  }
  return resource_request;
}

NodeResources RandomNodeResources() { return NodeResources(RandomResourceRequest()); }

class ClusterResourceSchedulerTest : public ::testing::Test {
 public:
  void SetUp() {
    // The legacy scheduling policy is easier to reason about for testing purposes. See
    // `scheduling_policy_test.cc` for comprehensive testing of the hybrid scheduling
    // policy.
    gcs_client_ = std::make_unique<gcs::MockGcsClient>();
    is_node_available_fn_ = [this](scheduling::NodeID node_id) {
      return gcs_client_->Nodes().Get(NodeID::FromBinary(node_id.Binary())) != nullptr;
    };
    node_name = NodeID::FromRandom().Binary();
    node_info.set_node_id(node_name);
    ON_CALL(*gcs_client_->mock_node_accessor, Get(::testing::_, ::testing::_))
        .WillByDefault(::testing::Return(&node_info));
  }

  void Shutdown() {}

  void initCluster(ClusterResourceScheduler &resource_scheduler, int n) {
    for (int i = 0; i < n; i++) {
      NodeResources node_resources = RandomNodeResources();
      resource_scheduler.GetClusterResourceManager().AddOrUpdateNode(
          scheduling::NodeID(i), node_resources);
    }
  }

  void AssertPredefinedNodeResources() {
    ASSERT_EQ(ray::kCPU_ResourceLabel, scheduling::ResourceID(CPU).Binary());
    ASSERT_EQ(ray::kGPU_ResourceLabel, scheduling::ResourceID(GPU).Binary());
    ASSERT_EQ(ray::kObjectStoreMemory_ResourceLabel,
              scheduling::ResourceID(OBJECT_STORE_MEM).Binary());
    ASSERT_EQ(ray::kMemory_ResourceLabel, scheduling::ResourceID(MEM).Binary());
  }
  std::unique_ptr<gcs::MockGcsClient> gcs_client_;
  std::function<bool(scheduling::NodeID)> is_node_available_fn_;
  std::string node_name;
  rpc::GcsNodeInfo node_info;
};

TEST_F(ClusterResourceSchedulerTest, SchedulingFixedPointTest) {
  {
    FixedPoint fp(1.);
    FixedPoint fp1(1.);
    FixedPoint fp2(2.);

    ASSERT_TRUE(fp1 < fp2);
    ASSERT_TRUE(fp2 > fp1);
    ASSERT_TRUE(fp1 <= fp2);
    ASSERT_TRUE(fp2 >= fp1);
    ASSERT_TRUE(fp1 != fp2);

    ASSERT_TRUE(fp1 == fp);
    ASSERT_TRUE(fp1 >= fp);
    ASSERT_TRUE(fp1 <= fp);
  }

  {
    FixedPoint fp1(1.);
    FixedPoint fp2(2.);

    ASSERT_TRUE(fp1 < 2);
    ASSERT_TRUE(fp2 > 1.);
    ASSERT_TRUE(fp1 <= 2.);
    ASSERT_TRUE(fp2 >= 1.);
    ASSERT_TRUE(fp1 != 2.);

    ASSERT_TRUE(fp1 == 1.);
    ASSERT_TRUE(fp1 >= 1.);
    ASSERT_TRUE(fp1 <= 1.);

    ASSERT_TRUE(fp1 + fp2 == 3.);
    ASSERT_TRUE(fp2 - fp1 == 1.);

    ASSERT_TRUE((fp1 += 1.) == 2.);
    ASSERT_TRUE((fp2 -= 1.) == 1.);
  }

  {
    FixedPoint fp1(1.);

    ASSERT_TRUE(fp1.Double() == 1.);
  }

  {
    FixedPoint fp1(1.);
    auto fp2 = fp1 - 1.0;
    ASSERT_TRUE(fp2 == 0);

    auto fp3 = fp1 + 1.0;
    ASSERT_TRUE(fp3 == 2);

    auto fp4 = -fp1;
    ASSERT_TRUE(fp4 == -1.0);

    fp1 = 2.0;
    ASSERT_TRUE(fp1 == 2.0);

    FixedPoint fp5(-1.0);
    ASSERT_TRUE(fp5 == -1);

    FixedPoint f6(-1);
    FixedPoint f7(int64_t(-1));
    ASSERT_TRUE(f6 == f7);

    ASSERT_TRUE(f6 == -1.0);
  }
}

TEST_F(ClusterResourceSchedulerTest, SchedulingIdTest) {
  StringIdMap ids;
  hash<string> hasher;
  const size_t num = 10;  // should be greater than 10.

  for (size_t i = 0; i < num; i++) {
    ids.Insert(to_string(i));
  }
  ASSERT_EQ(ids.Count(), num);

  ASSERT_EQ(ids.Get(to_string(3)), static_cast<int64_t>(hasher(to_string(3))));

  ASSERT_TRUE(ids.Get(to_string(100)) == -1);

  // Test for handling collision.
  StringIdMap short_ids;
  uint8_t max_id = 8;
  for (size_t i = 0; i < max_id; i++) {
    int64_t id = short_ids.Insert(to_string(i), max_id);
    ASSERT_TRUE(id < max_id);
  }
  ASSERT_EQ(short_ids.Count(), max_id);
}

TEST_F(ClusterResourceSchedulerTest, SchedulingIdInsertOrDieTest) {
  StringIdMap ids;
  ids.InsertOrDie("123", 2);
  ids.InsertOrDie("1234", 3);

  ASSERT_EQ(ids.Count(), 2);
  ASSERT_TRUE(ids.Get(to_string(100)) == -1);
  ASSERT_EQ(ids.Get("123"), 2);
  ASSERT_EQ(ids.Get(2), std::string("123"));
  ASSERT_EQ(ids.Get("1234"), 3);
  ASSERT_EQ(ids.Get(3), std::string("1234"));
}

TEST_F(ClusterResourceSchedulerTest, SchedulingInitClusterTest) {
  int num_nodes = 10;
  ClusterResourceScheduler resource_scheduler(
      scheduling::NodeID(num_nodes + 1), NodeResources(), [](auto) { return true; });
  AssertPredefinedNodeResources();

  initCluster(resource_scheduler, num_nodes);

  ASSERT_EQ(resource_scheduler.GetClusterResourceManager().NumNodes(), num_nodes + 1);
}

TEST_F(ClusterResourceSchedulerTest, SchedulingDeleteClusterNodeTest) {
  int num_nodes = 4;
  int64_t remove_id = 2;

  ClusterResourceScheduler resource_scheduler(
      scheduling::NodeID(num_nodes + 1), NodeResources(), [](auto) { return true; });

  initCluster(resource_scheduler, num_nodes);
  resource_scheduler.GetClusterResourceManager().RemoveNode(
      scheduling::NodeID(remove_id));

  ASSERT_TRUE(num_nodes == resource_scheduler.GetClusterResourceManager().NumNodes());
}

TEST_F(ClusterResourceSchedulerTest, SchedulingModifyClusterNodeTest) {
  int num_nodes = 4;
  int64_t update_id = 2;
  ClusterResourceScheduler resource_scheduler(
      scheduling::NodeID(num_nodes + 1), NodeResources(), [](auto) { return true; });

  initCluster(resource_scheduler, num_nodes);

  NodeResources node_resources = RandomNodeResources();
  resource_scheduler.GetClusterResourceManager().AddOrUpdateNode(
      scheduling::NodeID(update_id), node_resources);
  ASSERT_TRUE(num_nodes + 1 == resource_scheduler.GetClusterResourceManager().NumNodes());
}

TEST_F(ClusterResourceSchedulerTest, NodeAffinitySchedulingStrategyTest) {
  absl::flat_hash_map<std::string, double> resource_total({{"CPU", 10}});
  auto local_node_id = scheduling::NodeID(NodeID::FromRandom().Binary());
  ClusterResourceScheduler resource_scheduler(
      local_node_id, resource_total, is_node_available_fn_);
  AssertPredefinedNodeResources();
  auto remote_node_id = scheduling::NodeID(NodeID::FromRandom().Binary());
  resource_scheduler.GetClusterResourceManager().AddOrUpdateNode(
      remote_node_id, resource_total, resource_total);

  absl::flat_hash_map<std::string, double> resource_request({{"CPU", 1}});
  int64_t violations;
  bool is_infeasible;
  rpc::SchedulingStrategy scheduling_strategy;
  scheduling_strategy.mutable_node_affinity_scheduling_strategy()->set_node_id(
      remote_node_id.Binary());
  scheduling_strategy.mutable_node_affinity_scheduling_strategy()->set_soft(false);
  auto node_id_1 = resource_scheduler.GetBestSchedulableNode(resource_request,
                                                             scheduling_strategy,
                                                             false,
                                                             false,
                                                             false,
                                                             &violations,
                                                             &is_infeasible);
  ASSERT_EQ(node_id_1, remote_node_id);

  scheduling_strategy.mutable_node_affinity_scheduling_strategy()->set_node_id(
      local_node_id.Binary());
  scheduling_strategy.mutable_node_affinity_scheduling_strategy()->set_soft(false);
  auto node_id_2 = resource_scheduler.GetBestSchedulableNode(resource_request,
                                                             scheduling_strategy,
                                                             false,
                                                             false,
                                                             false,
                                                             &violations,
                                                             &is_infeasible);
  ASSERT_EQ(node_id_2, local_node_id);

  scheduling_strategy.mutable_node_affinity_scheduling_strategy()->set_node_id(
      NodeID::FromRandom().Binary());
  scheduling_strategy.mutable_node_affinity_scheduling_strategy()->set_soft(false);
  auto node_id_3 = resource_scheduler.GetBestSchedulableNode(resource_request,
                                                             scheduling_strategy,
                                                             false,
                                                             false,
                                                             false,
                                                             &violations,
                                                             &is_infeasible);
  ASSERT_TRUE(node_id_3.IsNil());

  scheduling_strategy.mutable_node_affinity_scheduling_strategy()->set_node_id(
      NodeID::FromRandom().Binary());
  scheduling_strategy.mutable_node_affinity_scheduling_strategy()->set_soft(true);
  auto node_id_4 = resource_scheduler.GetBestSchedulableNode(resource_request,
                                                             scheduling_strategy,
                                                             false,
                                                             false,
                                                             false,
                                                             &violations,
                                                             &is_infeasible);
  ASSERT_EQ(node_id_4, local_node_id);
}

TEST_F(ClusterResourceSchedulerTest, SpreadSchedulingStrategyTest) {
  absl::flat_hash_map<std::string, double> resource_total({{"CPU", 10}});
  auto local_node_id = scheduling::NodeID(NodeID::FromRandom().Binary());
  ClusterResourceScheduler resource_scheduler(
      local_node_id, resource_total, is_node_available_fn_);
  AssertPredefinedNodeResources();
  auto remote_node_id = scheduling::NodeID(NodeID::FromRandom().Binary());
  resource_scheduler.GetClusterResourceManager().AddOrUpdateNode(
      remote_node_id, resource_total, resource_total);

  absl::flat_hash_map<std::string, double> resource_request({{"CPU", 1}});
  int64_t violations;
  bool is_infeasible;
  rpc::SchedulingStrategy scheduling_strategy;
  scheduling_strategy.mutable_spread_scheduling_strategy();
  auto node_id_1 = resource_scheduler.GetBestSchedulableNode(resource_request,
                                                             scheduling_strategy,
                                                             false,
                                                             false,
                                                             false,
                                                             &violations,
                                                             &is_infeasible);
  absl::flat_hash_map<std::string, double> resource_available({{"CPU", 9}});
  resource_scheduler.GetClusterResourceManager().AddOrUpdateNode(
      node_id_1, resource_total, resource_available);
  auto node_id_2 = resource_scheduler.GetBestSchedulableNode(resource_request,
                                                             scheduling_strategy,
                                                             false,
                                                             false,
                                                             false,
                                                             &violations,
                                                             &is_infeasible);
  ASSERT_EQ((std::set<scheduling::NodeID>{node_id_1, node_id_2}),
            (std::set<scheduling::NodeID>{local_node_id, remote_node_id}));
}

TEST_F(ClusterResourceSchedulerTest, SchedulingUpdateAvailableResourcesTest) {
  // Create cluster resources.
  NodeResources node_resources = CreateNodeResources({{ResourceID::CPU(), 10},
                                                      {ResourceID::Memory(), 5},
                                                      {ResourceID::GPU(), 3},
                                                      {ResourceID("custom1"), 5},
                                                      {ResourceID("custom2"), 5}});
  ClusterResourceScheduler resource_scheduler(
      scheduling::NodeID(1), node_resources, is_node_available_fn_);
  AssertPredefinedNodeResources();

  {
    ResourceRequest resource_request =
        CreateResourceRequest({{ResourceID::CPU(), 7},
                               {ResourceID::Memory(), 5},
                               {ResourceID("custom1"), 3},
                               {ResourceID("custom2"), 5}});
    int64_t violations;
    bool is_infeasible;
    rpc::SchedulingStrategy scheduling_strategy;
    scheduling_strategy.mutable_default_scheduling_strategy();
    auto node_id = resource_scheduler.GetBestSchedulableNode(
        resource_request, scheduling_strategy, false, false, &violations, &is_infeasible);
    ASSERT_EQ(node_id.ToInt(), 1);
    ASSERT_TRUE(violations == 0);

    NodeResources nr1, nr2;
    ASSERT_TRUE(
        resource_scheduler.GetClusterResourceManager().GetNodeResources(node_id, &nr1));
    ASSERT_TRUE(
        resource_scheduler.GetClusterResourceManager().GetNodeResources(node_id, &nr1));
    auto task_allocation = std::make_shared<TaskResourceInstances>();
    ASSERT_TRUE(resource_scheduler.GetLocalResourceManager().AllocateLocalTaskResources(
        resource_request, task_allocation));
    ASSERT_TRUE(
        resource_scheduler.GetClusterResourceManager().GetNodeResources(node_id, &nr2));

    for (auto &resource_id : nr1.total.ResourceIds()) {
      auto t = nr1.available.Get(resource_id) - resource_request.Get(resource_id);
      if (t < 0) t = 0;
      ASSERT_EQ(nr2.available.Get(resource_id), t);
    }
  }
}

TEST_F(ClusterResourceSchedulerTest, SchedulingUpdateTotalResourcesTest) {
  absl::flat_hash_map<std::string, double> initial_resources = {
      {ray::kCPU_ResourceLabel, 1}, {"custom1", 1}};
  std::string name = NodeID::FromRandom().Binary();
  ClusterResourceScheduler resource_scheduler(scheduling::NodeID(name),
                                              initial_resources,
                                              is_node_available_fn_,
                                              nullptr,
                                              nullptr);

  resource_scheduler.GetLocalResourceManager().AddLocalResourceInstances(
      scheduling::ResourceID(ray::kCPU_ResourceLabel), {0, 1, 1});
  resource_scheduler.GetLocalResourceManager().AddLocalResourceInstances(
      scheduling::ResourceID("custom1"), {0, 1, 1});

  const auto &cpu_total = resource_scheduler.GetClusterResourceManager()
                              .GetNodeResources(scheduling::NodeID(name))
                              .total.Get(ResourceID::CPU())
                              .Double();
  ASSERT_EQ(cpu_total, 3);

  const auto &custom_total = resource_scheduler.GetClusterResourceManager()
                                 .GetNodeResources(scheduling::NodeID(name))
                                 .total.Get(ResourceID("custom1"))
                                 .Double();
  ASSERT_EQ(custom_total, 3);
}

TEST_F(ClusterResourceSchedulerTest, SchedulingAddOrUpdateNodeTest) {
  ClusterResourceScheduler resource_scheduler(
      scheduling::NodeID(0), NodeResources(), [](auto) { return true; });
  NodeResources nr, nr_out;
  int64_t node_id = 1;

  // Add node.
  {
    NodeResources node_resources = CreateNodeResources({{ResourceID::CPU(), 10},
                                                        {ResourceID::Memory(), 5},
                                                        {ResourceID::GPU(), 3},
                                                        {ResourceID("custom1"), 5},
                                                        {ResourceID("custom2"), 5}});
    resource_scheduler.GetClusterResourceManager().AddOrUpdateNode(
        scheduling::NodeID(node_id), node_resources);
    nr = node_resources;
  }

  // Check whether node resources were correctly added.
  if (resource_scheduler.GetClusterResourceManager().GetNodeResources(
          scheduling::NodeID(node_id), &nr_out)) {
    ASSERT_TRUE(nr == nr_out);
  } else {
    ASSERT_TRUE(false);
  }

  // Update node.
  {
    NodeResources node_resources = CreateNodeResources({{ResourceID::CPU(), 10},
                                                        {ResourceID::Memory(), 10},
                                                        {ResourceID("custom1"), 6},
                                                        {ResourceID("custom2"), 6}});
    resource_scheduler.GetClusterResourceManager().AddOrUpdateNode(
        scheduling::NodeID(node_id), node_resources);
    nr = node_resources;
  }
  if (resource_scheduler.GetClusterResourceManager().GetNodeResources(
          scheduling::NodeID(node_id), &nr_out)) {
    ASSERT_TRUE(nr == nr_out);
  } else {
    ASSERT_TRUE(false);
  }
}

TEST_F(ClusterResourceSchedulerTest, SchedulingResourceRequestTest) {
  // Create cluster resources containing local node.
  NodeResources node_resources = CreateNodeResources(
      {{ResourceID::CPU(), 5}, {ResourceID::Memory(), 5}, {ResourceID("custom1"), 10}});
  ClusterResourceScheduler resource_scheduler(
      scheduling::NodeID(0), node_resources, is_node_available_fn_);
  auto node_id = NodeID::FromRandom();
  rpc::SchedulingStrategy scheduling_strategy;
  scheduling_strategy.mutable_default_scheduling_strategy();
  {
    NodeResources node_resources = CreateNodeResources({{ResourceID::CPU(), 10},
                                                        {ResourceID::Memory(), 2},
                                                        {ResourceID::GPU(), 3},
                                                        {ResourceID("custom1"), 5},
                                                        {ResourceID("custom2"), 5}});
    resource_scheduler.GetClusterResourceManager().AddOrUpdateNode(
        scheduling::NodeID(node_id.Binary()), node_resources);
  }
  // Predefined resources, hard constraint violation
  {
    ResourceRequest resource_request = CreateResourceRequest({{ResourceID::CPU(), 11}});
    int64_t violations;
    bool is_infeasible;
    auto node_id = resource_scheduler.GetBestSchedulableNode(
        resource_request, scheduling_strategy, false, false, &violations, &is_infeasible);
    ASSERT_TRUE(node_id.IsNil());
  }

  // Predefined resources, no constraint violation.
  {
    ResourceRequest resource_request = CreateResourceRequest({{ResourceID::CPU(), 5}});
    int64_t violations;
    bool is_infeasible;
    auto node_id = resource_scheduler.GetBestSchedulableNode(
        resource_request, scheduling_strategy, false, false, &violations, &is_infeasible);
    ASSERT_TRUE(!node_id.IsNil());
    ASSERT_TRUE(violations == 0);
  }
  // Custom resources, hard constraint violation.
  {
    ResourceRequest resource_request = CreateResourceRequest(
        {{ResourceID::CPU(), 5}, {ResourceID::Memory(), 2}, {ResourceID("custom1"), 11}});
    int64_t violations;
    bool is_infeasible;
    auto node_id = resource_scheduler.GetBestSchedulableNode(
        resource_request, scheduling_strategy, false, false, &violations, &is_infeasible);
    ASSERT_TRUE(node_id.IsNil());
  }
  // Custom resources, no constraint violation.
  {
    ResourceRequest resource_request = CreateResourceRequest(
        {{ResourceID::CPU(), 5}, {ResourceID::Memory(), 2}, {ResourceID("custom1"), 5}});
    int64_t violations;
    bool is_infeasible;
    auto node_id = resource_scheduler.GetBestSchedulableNode(
        resource_request, scheduling_strategy, false, false, &violations, &is_infeasible);
    ASSERT_TRUE(!node_id.IsNil());
    ASSERT_TRUE(violations == 0);
  }
  // Custom resource missing, hard constraint violation.
  {
    ResourceRequest resource_request =
        CreateResourceRequest({{ResourceID::CPU(), 5},
                               {ResourceID::Memory(), 2},
                               {ResourceID("custom100"), 5}});
    int64_t violations;
    bool is_infeasible;
    auto node_id = resource_scheduler.GetBestSchedulableNode(
        resource_request, scheduling_strategy, false, false, &violations, &is_infeasible);
    ASSERT_TRUE(node_id.IsNil());
  }
  // Placement hints, no constraint violation.
  {
    ResourceRequest resource_request = CreateResourceRequest(
        {{ResourceID::CPU(), 5}, {ResourceID::Memory(), 2}, {ResourceID("custom1"), 5}});
    int64_t violations;
    bool is_infeasible;
    auto node_id = resource_scheduler.GetBestSchedulableNode(
        resource_request, scheduling_strategy, false, false, &violations, &is_infeasible);
    ASSERT_TRUE(!node_id.IsNil());
    ASSERT_TRUE(violations == 0);
  }
}

TEST_F(ClusterResourceSchedulerTest, GetLocalAvailableResourcesWithCpuUnitTest) {
  SetUnitInstanceResourceIds({ResourceID::CPU(), ResourceID::GPU()});
  // Create cluster resources containing local node.
  NodeResources node_resources = CreateNodeResources({{ResourceID::CPU(), 3},
                                                      {ResourceID::Memory(), 4},
                                                      {ResourceID::GPU(), 5},
                                                      {ResourceID("custom1"), 8}});
  ClusterResourceScheduler resource_scheduler(
      scheduling::NodeID(0), node_resources, is_node_available_fn_);

  TaskResourceInstances available_cluster_resources =
      resource_scheduler.GetLocalResourceManager()
          .GetLocalResources()
          .GetAvailableResourceInstances();

  TaskResourceInstances expected_cluster_resources;
  expected_cluster_resources.Set(ResourceID::CPU(), {1., 1., 1.});
  expected_cluster_resources.Set(ResourceID::Memory(), {4.});
  expected_cluster_resources.Set(ResourceID::GPU(), {1., 1., 1., 1., 1.});

  ASSERT_EQ(expected_cluster_resources == available_cluster_resources, false);

  expected_cluster_resources.Set(ResourceID("custom1"), {8.});

  ASSERT_EQ(expected_cluster_resources == available_cluster_resources, true);
}

TEST_F(ClusterResourceSchedulerTest, GetLocalAvailableResourcesTest) {
  SetUnitInstanceResourceIds({ResourceID::GPU()});
  // Create cluster resources containing local node.
  NodeResources node_resources = CreateNodeResources({{ResourceID::CPU(), 3},
                                                      {ResourceID::Memory(), 4},
                                                      {ResourceID::GPU(), 5},
                                                      {ResourceID("custom1"), 8}});
  ClusterResourceScheduler resource_scheduler(
      scheduling::NodeID(0), node_resources, is_node_available_fn_);

  TaskResourceInstances available_cluster_resources =
      resource_scheduler.GetLocalResourceManager()
          .GetLocalResources()
          .GetAvailableResourceInstances();

  TaskResourceInstances expected_cluster_resources;
  expected_cluster_resources.Set(ResourceID::CPU(), {3.});
  expected_cluster_resources.Set(ResourceID::Memory(), {4.});
  expected_cluster_resources.Set(ResourceID::GPU(), {1., 1., 1., 1., 1.});

  ASSERT_EQ(expected_cluster_resources == available_cluster_resources, false);

  expected_cluster_resources.Set(ResourceID("custom1"), {8.});

  ASSERT_EQ(expected_cluster_resources == available_cluster_resources, true);
}

TEST_F(ClusterResourceSchedulerTest, GetCPUInstancesDoubleTest) {
  TaskResourceInstances task_resources;
  task_resources.Set(ResourceID::CPU(), {1., 1., 1.});
  task_resources.Set(ResourceID::Memory(), {4.});
  task_resources.Set(ResourceID::GPU(), {1., 1., 1., 1., 1.});

  std::vector<FixedPoint> cpu_instances = task_resources.Get(ResourceID::CPU());
  std::vector<FixedPoint> expected_cpu_instances{1., 1., 1.};

  ASSERT_EQ(cpu_instances, expected_cpu_instances);
}

TEST_F(ClusterResourceSchedulerTest, AvailableResourceInstancesOpsTest) {
  NodeResources node_resources = CreateNodeResources({{ResourceID::CPU(), 3}});
  ClusterResourceScheduler cluster(
      scheduling::NodeID(0), node_resources, is_node_available_fn_);

  std::vector<FixedPoint> total = {6., 6., 6.};
  std::vector<FixedPoint> available = {3., 2., 5.};
  auto old_total = total;
  auto old_available = available;

  std::vector<FixedPoint> a{1., 1., 1.};
  cluster.GetLocalResourceManager().AddAvailableResourceInstances(a, total, available);
  cluster.GetLocalResourceManager().SubtractAvailableResourceInstances(a, available);

  ASSERT_EQ(available, old_available);

  a = {10., 1., 1.};
  cluster.GetLocalResourceManager().AddAvailableResourceInstances(a, total, available);
  std::vector<FixedPoint> expected_available{6., 3., 6.};

  ASSERT_EQ(available, expected_available);

  a = {10., 1., 1.};
  cluster.GetLocalResourceManager().SubtractAvailableResourceInstances(a, available);
  expected_available = {0., 2., 5.};
  ASSERT_EQ(available, expected_available);
}

TEST_F(ClusterResourceSchedulerTest, TaskResourceInstancesTest) {
  // Allocate resources for a task request specifying only predefined resources.
  {
    NodeResources node_resources = CreateNodeResources(
        {{ResourceID::CPU(), 3}, {ResourceID::Memory(), 4}, {ResourceID::GPU(), 5}});
    ClusterResourceScheduler resource_scheduler(
        scheduling::NodeID(0), node_resources, is_node_available_fn_);

    ResourceRequest resource_request = CreateResourceRequest(
        {{ResourceID::CPU(), 3}, {ResourceID::Memory(), 2}, {ResourceID::GPU(), 1.5}});

    NodeResourceInstances old_local_resources =
        resource_scheduler.GetLocalResourceManager().GetLocalResources();

    std::shared_ptr<TaskResourceInstances> task_allocation =
        std::make_shared<TaskResourceInstances>();
    bool success =
        resource_scheduler.GetLocalResourceManager().AllocateTaskResourceInstances(
            resource_request, task_allocation);

    ASSERT_EQ(success, true);

    resource_scheduler.GetLocalResourceManager().FreeTaskResourceInstances(
        task_allocation);

    ASSERT_EQ((resource_scheduler.GetLocalResourceManager().GetLocalResources() ==
               old_local_resources),
              true);
  }
  // Try to allocate resources for a task request that overallocates a hard constrained
  // resource.
  {
    NodeResources node_resources = CreateNodeResources(
        {{ResourceID::CPU(), 3}, {ResourceID::Memory(), 4}, {ResourceID::GPU(), 5}});
    ClusterResourceScheduler resource_scheduler(
        scheduling::NodeID(0), node_resources, is_node_available_fn_);

    ResourceRequest resource_request = CreateResourceRequest(
        {{ResourceID::CPU(), 4}, {ResourceID::Memory(), 2}, {ResourceID::GPU(), 1.5}});

    NodeResourceInstances old_local_resources =
        resource_scheduler.GetLocalResourceManager().GetLocalResources();
    std::shared_ptr<TaskResourceInstances> task_allocation =
        std::make_shared<TaskResourceInstances>();
    bool success =
        resource_scheduler.GetLocalResourceManager().AllocateTaskResourceInstances(
            resource_request, task_allocation);

    ASSERT_EQ(success, false);
    ASSERT_EQ((resource_scheduler.GetLocalResourceManager().GetLocalResources() ==
               old_local_resources),
              true);
  }
  // Allocate resources for a task request specifying both predefined and custom
  // resources.
  {
    NodeResources node_resources = CreateNodeResources({{ResourceID::CPU(), 3},
                                                        {ResourceID::Memory(), 4},
                                                        {ResourceID::GPU(), 5},
                                                        {ResourceID("custom1"), 4},
                                                        {ResourceID("custom2"), 4}});
    ClusterResourceScheduler resource_scheduler(
        scheduling::NodeID(0), node_resources, is_node_available_fn_);

    ResourceRequest resource_request =
        CreateResourceRequest({{ResourceID::CPU(), 3},
                               {ResourceID::Memory(), 2},
                               {ResourceID::GPU(), 1.5},
                               {ResourceID("custom1"), 2}});

    NodeResourceInstances old_local_resources =
        resource_scheduler.GetLocalResourceManager().GetLocalResources();
    std::shared_ptr<TaskResourceInstances> task_allocation =
        std::make_shared<TaskResourceInstances>();
    bool success =
        resource_scheduler.GetLocalResourceManager().AllocateTaskResourceInstances(
            resource_request, task_allocation);

    ASSERT_EQ(success, true);

    resource_scheduler.GetLocalResourceManager().FreeTaskResourceInstances(
        task_allocation);

    ASSERT_EQ((resource_scheduler.GetLocalResourceManager().GetLocalResources() ==
               old_local_resources),
              true);
  }
  // Allocate resources for a task request specifying both predefined and custom
  // resources, but overallocates a hard-constrained custom resource.
  {
    NodeResources node_resources = CreateNodeResources({{ResourceID::CPU(), 3},
                                                        {ResourceID::Memory(), 4},
                                                        {ResourceID::GPU(), 5},
                                                        {ResourceID("custom1"), 4},
                                                        {ResourceID("custom2"), 4}});
    ClusterResourceScheduler resource_scheduler(
        scheduling::NodeID(0), node_resources, is_node_available_fn_);

    ResourceRequest resource_request =
        CreateResourceRequest({{ResourceID::CPU(), 3},
                               {ResourceID::Memory(), 2},
                               {ResourceID::GPU(), 1.5},
                               {ResourceID("custom1"), 10}});

    NodeResourceInstances old_local_resources =
        resource_scheduler.GetLocalResourceManager().GetLocalResources();
    std::shared_ptr<TaskResourceInstances> task_allocation =
        std::make_shared<TaskResourceInstances>();
    bool success =
        resource_scheduler.GetLocalResourceManager().AllocateTaskResourceInstances(
            resource_request, task_allocation);

    ASSERT_EQ(success, false);
    ASSERT_EQ((resource_scheduler.GetLocalResourceManager().GetLocalResources() ==
               old_local_resources),
              true);
  }
}

TEST_F(ClusterResourceSchedulerTest, TaskResourceInstancesAllocationFailureTest) {
  /// Make sure there's no leak when the resource allocation failed in the middle.
  NodeResources node_resources = CreateNodeResources({{ResourceID::CPU(), 1},
                                                      {ResourceID::Memory(), 1},
                                                      {ResourceID::GPU(), 1},
                                                      {ResourceID("custom1"), 4},
                                                      {ResourceID("custom2"), 4},
                                                      {ResourceID("custom3"), 4}});
  ClusterResourceScheduler resource_scheduler(
      scheduling::NodeID(0), node_resources, is_node_available_fn_);

  ResourceRequest resource_request = CreateResourceRequest({{ResourceID("custom1"), 3},
                                                            {ResourceID("custom3"), 3},
                                                            {ResourceID("custom5"), 4}});

  NodeResourceInstances old_local_resources =
      resource_scheduler.GetLocalResourceManager().GetLocalResources();
  std::shared_ptr<TaskResourceInstances> task_allocation =
      std::make_shared<TaskResourceInstances>();
  bool success =
      resource_scheduler.GetLocalResourceManager().AllocateTaskResourceInstances(
          resource_request, task_allocation);

  ASSERT_EQ(success, false);
  // resource_scheduler.FreeTaskResourceInstances(task_allocation);
  ASSERT_EQ((resource_scheduler.GetLocalResourceManager().GetLocalResources() ==
             old_local_resources),
            true);
}

TEST_F(ClusterResourceSchedulerTest, TaskResourceInstancesTest2) {
  {
    NodeResources node_resources = CreateNodeResources({{ResourceID::CPU(), 4},
                                                        {ResourceID::Memory(), 4},
                                                        {ResourceID::GPU(), 5},
                                                        {ResourceID("custom1"), 4},
                                                        {ResourceID("custom2"), 4}});
    ClusterResourceScheduler resource_scheduler(
        scheduling::NodeID(0), node_resources, is_node_available_fn_);

    ResourceRequest resource_request =
        CreateResourceRequest({{ResourceID::CPU(), 2},
                               {ResourceID::Memory(), 2},
                               {ResourceID::GPU(), 1.5},
                               {ResourceID("custom1"), 3},
                               {ResourceID("custom2"), 2}});

    std::shared_ptr<TaskResourceInstances> task_allocation =
        std::make_shared<TaskResourceInstances>();
    bool success =
        resource_scheduler.GetLocalResourceManager().AllocateTaskResourceInstances(
            resource_request, task_allocation);

    NodeResourceInstances old_local_resources =
        resource_scheduler.GetLocalResourceManager().GetLocalResources();
    ASSERT_EQ(success, true);
    std::vector<double> cpu_instances = task_allocation->GetDouble(ResourceID::CPU());
    resource_scheduler.GetLocalResourceManager().AddResourceInstances(ResourceID::CPU(),
                                                                      cpu_instances);
    resource_scheduler.GetLocalResourceManager().SubtractResourceInstances(
        ResourceID::CPU(), cpu_instances);

    ASSERT_EQ((resource_scheduler.GetLocalResourceManager().GetLocalResources() ==
               old_local_resources),
              true);
  }
}

TEST_F(ClusterResourceSchedulerTest, DeadNodeTest) {
  ClusterResourceScheduler resource_scheduler(scheduling::NodeID("local"),
                                              absl::flat_hash_map<std::string, double>{},
                                              is_node_available_fn_);
  absl::flat_hash_map<std::string, double> resource;
  resource["CPU"] = 10000.0;
  auto node_id = NodeID::FromRandom();
  resource_scheduler.GetClusterResourceManager().AddOrUpdateNode(
      scheduling::NodeID(node_id.Binary()), resource, resource);
  int64_t violations = 0;
  bool is_infeasible = false;
  rpc::SchedulingStrategy scheduling_strategy;
  scheduling_strategy.mutable_default_scheduling_strategy();
  ASSERT_EQ(scheduling::NodeID(node_id.Binary()),
            resource_scheduler.GetBestSchedulableNode(resource,
                                                      scheduling_strategy,
                                                      false,
                                                      false,
                                                      false,
                                                      &violations,
                                                      &is_infeasible));
  EXPECT_CALL(*gcs_client_->mock_node_accessor, Get(node_id, ::testing::_))
      .WillOnce(::testing::Return(nullptr))
      .WillOnce(::testing::Return(nullptr));
  ASSERT_TRUE(resource_scheduler
                  .GetBestSchedulableNode(resource,
                                          scheduling_strategy,
                                          false,
                                          false,
                                          false,
                                          &violations,
                                          &is_infeasible)
                  .IsNil());
}

TEST_F(ClusterResourceSchedulerTest, TaskGPUResourceInstancesTest) {
  {
    NodeResources node_resources = CreateNodeResources({{ResourceID::CPU(), 1},
                                                        {ResourceID::Memory(), 1},
                                                        {ResourceID::GPU(), 4},
                                                        {ResourceID("custom1"), 8}});
    ClusterResourceScheduler resource_scheduler(
        scheduling::NodeID(0), node_resources, is_node_available_fn_);

    std::vector<double> allocate_gpu_instances{0.5, 0.5, 0.5, 0.5};
    resource_scheduler.GetLocalResourceManager().SubtractResourceInstances(
        ResourceID::GPU(), allocate_gpu_instances);
    std::vector<double> available_gpu_instances =
        resource_scheduler.GetLocalResourceManager()
            .GetLocalResources()
            .GetAvailableResourceInstances()
            .GetDouble(ResourceID::GPU());
    std::vector<double> expected_available_gpu_instances{0.5, 0.5, 0.5, 0.5};
    ASSERT_TRUE(std::equal(available_gpu_instances.begin(),
                           available_gpu_instances.end(),
                           expected_available_gpu_instances.begin()));

    resource_scheduler.GetLocalResourceManager().AddResourceInstances(
        ResourceID::GPU(), allocate_gpu_instances);
    available_gpu_instances = resource_scheduler.GetLocalResourceManager()
                                  .GetLocalResources()
                                  .GetAvailableResourceInstances()
                                  .GetDouble(ResourceID::GPU());
    expected_available_gpu_instances = {1., 1., 1., 1.};
    ASSERT_TRUE(std::equal(available_gpu_instances.begin(),
                           available_gpu_instances.end(),
                           expected_available_gpu_instances.begin()));

    allocate_gpu_instances = {1.5, 1.5, .5, 1.5};
    std::vector<double> underflow =
        resource_scheduler.GetLocalResourceManager().SubtractResourceInstances(
            ResourceID::GPU(), allocate_gpu_instances);
    std::vector<double> expected_underflow{.5, .5, 0., .5};
    ASSERT_TRUE(
        std::equal(underflow.begin(), underflow.end(), expected_underflow.begin()));
    available_gpu_instances = resource_scheduler.GetLocalResourceManager()
                                  .GetLocalResources()
                                  .GetAvailableResourceInstances()
                                  .GetDouble(ResourceID::GPU());
    expected_available_gpu_instances = {0., 0., 0.5, 0.};
    ASSERT_TRUE(std::equal(available_gpu_instances.begin(),
                           available_gpu_instances.end(),
                           expected_available_gpu_instances.begin()));

    allocate_gpu_instances = {1.0, .5, 1., .5};
    std::vector<double> overflow =
        resource_scheduler.GetLocalResourceManager().AddResourceInstances(
            ResourceID::GPU(), allocate_gpu_instances);
    std::vector<double> expected_overflow{.0, .0, .5, 0.};
    ASSERT_TRUE(std::equal(overflow.begin(), overflow.end(), expected_overflow.begin()));
    available_gpu_instances = resource_scheduler.GetLocalResourceManager()
                                  .GetLocalResources()
                                  .GetAvailableResourceInstances()
                                  .GetDouble(ResourceID::GPU());
    expected_available_gpu_instances = {1., .5, 1., .5};
    ASSERT_TRUE(std::equal(available_gpu_instances.begin(),
                           available_gpu_instances.end(),
                           expected_available_gpu_instances.begin()));
  }
}

TEST_F(ClusterResourceSchedulerTest,
       UpdateLocalAvailableResourcesFromResourceInstancesTest) {
  {
    NodeResources node_resources = CreateNodeResources({{ResourceID::CPU(), 1},
                                                        {ResourceID::Memory(), 1},
                                                        {ResourceID::GPU(), 4},
                                                        {ResourceID("custom1"), 8}});
    ClusterResourceScheduler resource_scheduler(
        scheduling::NodeID(0), node_resources, is_node_available_fn_);

    {
      std::vector<double> allocate_gpu_instances{0.5, 0.5, 2, 0.5};
      // SubtractGPUResourceInstances() calls
      // UpdateLocalAvailableResourcesFromResourceInstances() under the hood.
      resource_scheduler.GetLocalResourceManager().SubtractResourceInstances(
          ResourceID::GPU(), allocate_gpu_instances);
      std::vector<double> available_gpu_instances =
          resource_scheduler.GetLocalResourceManager()
              .GetLocalResources()
              .GetAvailableResourceInstances()
              .GetDouble(ResourceID::GPU());
      std::vector<double> expected_available_gpu_instances{0.5, 0.5, 0., 0.5};
      ASSERT_TRUE(std::equal(available_gpu_instances.begin(),
                             available_gpu_instances.end(),
                             expected_available_gpu_instances.begin()));

      NodeResources nr;
      resource_scheduler.GetClusterResourceManager().GetNodeResources(
          scheduling::NodeID(0), &nr);
      ASSERT_TRUE(nr.available.Get(ResourceID::GPU()) == 1.5);
    }

    {
      std::vector<double> allocate_gpu_instances{1.5, 0.5, 2, 0.3};
      // SubtractGPUResourceInstances() calls
      // UpdateLocalAvailableResourcesFromResourceInstances() under the hood.
      resource_scheduler.GetLocalResourceManager().AddResourceInstances(
          ResourceID::GPU(), allocate_gpu_instances);
      std::vector<double> available_gpu_instances =
          resource_scheduler.GetLocalResourceManager()
              .GetLocalResources()
              .GetAvailableResourceInstances()
              .GetDouble(ResourceID::GPU());
      std::vector<double> expected_available_gpu_instances{1., 1., 1., 0.8};
      ASSERT_TRUE(std::equal(available_gpu_instances.begin(),
                             available_gpu_instances.end(),
                             expected_available_gpu_instances.begin()));

      NodeResources nr;
      resource_scheduler.GetClusterResourceManager().GetNodeResources(
          scheduling::NodeID(0), &nr);
      ASSERT_TRUE(nr.available.Get(ResourceID::GPU()) == 3.8);
    }
  }
}

TEST_F(ClusterResourceSchedulerTest, TaskResourceInstanceWithHardRequestTest) {
  NodeResources node_resources = CreateNodeResources(
      {{ResourceID::CPU(), 4}, {ResourceID::Memory(), 2}, {ResourceID::GPU(), 4}});
  ClusterResourceScheduler resource_scheduler(
      scheduling::NodeID(0), node_resources, is_node_available_fn_);

  ResourceRequest resource_request = CreateResourceRequest(
      {{ResourceID::CPU(), 2}, {ResourceID::Memory(), 2}, {ResourceID::GPU(), 1.5}});

  std::shared_ptr<TaskResourceInstances> task_allocation =
      std::make_shared<TaskResourceInstances>();
  bool success =
      resource_scheduler.GetLocalResourceManager().AllocateTaskResourceInstances(
          resource_request, task_allocation);

  ASSERT_EQ(success, true);

  vector<FixedPoint> gpu_instances = task_allocation->Get(ResourceID::GPU());
  vector<FixedPoint> expect_gpu_instance{1., 0.5, 0., 0.};

  ASSERT_EQ(gpu_instances, expect_gpu_instance);
}

TEST_F(ClusterResourceSchedulerTest, TaskResourceInstanceWithoutCpuUnitTest) {
  NodeResources node_resources = CreateNodeResources(
      {{ResourceID::CPU(), 4}, {ResourceID::Memory(), 2}, {ResourceID::GPU(), 4}});
  ClusterResourceScheduler resource_scheduler(
      scheduling::NodeID(0), node_resources, is_node_available_fn_);

  ResourceRequest resource_request = CreateResourceRequest(
      {{ResourceID::CPU(), 2}, {ResourceID::Memory(), 2}, {ResourceID::GPU(), 1.5}});

  std::shared_ptr<TaskResourceInstances> task_allocation =
      std::make_shared<TaskResourceInstances>();
  bool success =
      resource_scheduler.GetLocalResourceManager().AllocateTaskResourceInstances(
          resource_request, task_allocation);

  ASSERT_EQ(success, true);

  vector<FixedPoint> cpu_instances = task_allocation->Get(ResourceID::CPU());
  vector<FixedPoint> expect_cpu_instance{2};

  ASSERT_EQ(cpu_instances, expect_cpu_instance);
}

TEST_F(ClusterResourceSchedulerTest, TestAlwaysSpillInfeasibleTask) {
  absl::flat_hash_map<std::string, double> resource_spec({{"CPU", 1}});
  ClusterResourceScheduler resource_scheduler(scheduling::NodeID("local"),
                                              absl::flat_hash_map<std::string, double>{},
                                              is_node_available_fn_);
  for (int i = 0; i < 100; i++) {
    resource_scheduler.GetClusterResourceManager().AddOrUpdateNode(
        scheduling::NodeID(NodeID::FromRandom().Binary()), {}, {});
  }

  // No feasible nodes.
  int64_t total_violations;
  bool is_infeasible;
  rpc::SchedulingStrategy scheduling_strategy;
  scheduling_strategy.mutable_default_scheduling_strategy();
  ASSERT_TRUE(resource_scheduler
                  .GetBestSchedulableNode(resource_spec,
                                          scheduling_strategy,
                                          false,
                                          false,
                                          false,
                                          &total_violations,
                                          &is_infeasible)
                  .IsNil());

  // Feasible remote node, but doesn't currently have resources available. We
  // should spill there.
  auto remote_feasible = scheduling::NodeID(NodeID::FromRandom().Binary());
  resource_scheduler.GetClusterResourceManager().AddOrUpdateNode(
      remote_feasible, resource_spec, {{"CPU", 0.}});
  ASSERT_EQ(remote_feasible,
            resource_scheduler.GetBestSchedulableNode(resource_spec,
                                                      scheduling_strategy,
                                                      false,
                                                      false,
                                                      false,
                                                      &total_violations,
                                                      &is_infeasible));

  // Feasible remote node, and it currently has resources available. We should
  // prefer to spill there.
  auto remote_available = scheduling::NodeID(NodeID::FromRandom().Binary());
  resource_scheduler.GetClusterResourceManager().AddOrUpdateNode(
      remote_available, resource_spec, resource_spec);
  ASSERT_EQ(remote_available,
            resource_scheduler.GetBestSchedulableNode(resource_spec,
                                                      scheduling_strategy,
                                                      false,
                                                      false,
                                                      false,
                                                      &total_violations,
                                                      &is_infeasible));
}

TEST_F(ClusterResourceSchedulerTest, ResourceUsageReportTest) {
  vector<int64_t> cust_ids{1, 2, 3, 4, 5};

  NodeResources node_resources;

  absl::flat_hash_map<std::string, double> initial_resources(
      {{"CPU", 1}, {"GPU", 2}, {"memory", 3}, {"1", 1}, {"2", 2}, {"3", 3}});
  ClusterResourceScheduler resource_scheduler(
      scheduling::NodeID("0"), initial_resources, is_node_available_fn_);
  NodeResources other_node_resources = CreateNodeResources({{ResourceID::CPU(), 1},
                                                            {ResourceID::Memory(), 1},
                                                            {ResourceID::GPU(), 1},
                                                            {ResourceID("custom1"), 5},
                                                            {ResourceID("custom2"), 4},
                                                            {ResourceID("custom3"), 3},
                                                            {ResourceID("custom4"), 2},
                                                            {ResourceID("custom5"), 1}});
  resource_scheduler.GetClusterResourceManager().AddOrUpdateNode(
      scheduling::NodeID(12345), other_node_resources);

  {  // Cluster is idle.
    rpc::ResourcesData data;
    resource_scheduler.GetLocalResourceManager().FillResourceUsage(data);

    auto available = data.resources_available();
    auto total = data.resources_total();

    ASSERT_EQ(available[kCPU_ResourceLabel], 1);
    ASSERT_EQ(available[kGPU_ResourceLabel], 2);
    ASSERT_EQ(available[kMemory_ResourceLabel], 3);
    ASSERT_EQ(available["1"], 1);
    ASSERT_EQ(available["2"], 2);
    ASSERT_EQ(available["3"], 3);

    ASSERT_EQ(total[kCPU_ResourceLabel], 1);
    ASSERT_EQ(total[kGPU_ResourceLabel], 2);
    ASSERT_EQ(total[kMemory_ResourceLabel], 3);
    ASSERT_EQ(total["1"], 1);
    ASSERT_EQ(total["2"], 2);
    ASSERT_EQ(total["3"], 3);

    // GCS doesn't like entries which are 0.
    ASSERT_EQ(available.size(), 6);
    ASSERT_EQ(total.size(), 6);
  }
  {  // Task running on node with {"CPU": 0.1, "1": 0.1}
    std::shared_ptr<TaskResourceInstances> allocations =
        std::make_shared<TaskResourceInstances>();
    allocations->Set(ResourceID::CPU(), {0.1});
    allocations->Set(ResourceID("1"), {0.1});
    absl::flat_hash_map<std::string, double> allocation_map({
        {"CPU", 0.1},
        {"1", 0.1},
    });
    resource_scheduler.GetLocalResourceManager().AllocateLocalTaskResources(
        allocation_map, allocations);
    rpc::ResourcesData data;
    resource_scheduler.GetLocalResourceManager().ResetLastReportResourceUsage(
        NodeResources{});
    resource_scheduler.GetLocalResourceManager().FillResourceUsage(data);

    auto available = data.resources_available();
    auto total = data.resources_total();

    ASSERT_EQ(available[kCPU_ResourceLabel], 0.9);
    ASSERT_EQ(available[kGPU_ResourceLabel], 2);
    ASSERT_EQ(available[kMemory_ResourceLabel], 3);
    ASSERT_EQ(available["1"], 0.9);
    ASSERT_EQ(available["2"], 2);
    ASSERT_EQ(available["3"], 3);

    ASSERT_EQ(total[kCPU_ResourceLabel], 1);
    ASSERT_EQ(total[kGPU_ResourceLabel], 2);
    ASSERT_EQ(total[kMemory_ResourceLabel], 3);
    ASSERT_EQ(total["1"], 1);
    ASSERT_EQ(total["2"], 2);
    ASSERT_EQ(total["3"], 3);
  }
}

TEST_F(ClusterResourceSchedulerTest, ObjectStoreMemoryUsageTest) {
  vector<int64_t> cust_ids{1};
  NodeResources node_resources;
  absl::flat_hash_map<std::string, double> initial_resources(
      {{"CPU", 1},
       {"GPU", 2},
       {"memory", 3},
       {"object_store_memory", 1000 * 1024 * 1024}});
  int64_t used_object_store_memory = 250 * 1024 * 1024;
  int64_t *ptr = &used_object_store_memory;
  ClusterResourceScheduler resource_scheduler(
      scheduling::NodeID("0"), initial_resources, is_node_available_fn_, [&] {
        return *ptr;
      });
  NodeResources other_node_resources = CreateNodeResources({{ResourceID::CPU(), 1},
                                                            {ResourceID::Memory(), 1},
                                                            {ResourceID::GPU(), 1},
                                                            {ResourceID("custom1"), 10}});
  resource_scheduler.GetClusterResourceManager().AddOrUpdateNode(
      scheduling::NodeID(12345), other_node_resources);

  {
    rpc::ResourcesData data;
    resource_scheduler.GetLocalResourceManager().FillResourceUsage(data);
    auto available = data.resources_available();
    auto total = data.resources_total();
    ASSERT_EQ(available["object_store_memory"], 750 * 1024 * 1024);
    ASSERT_EQ(total["object_store_memory"], 1000 * 1024 * 1024);
    ASSERT_EQ(resource_scheduler.GetLocalResourceManager()
                  .GetLocalResources()
                  .available.Sum(ResourceID::ObjectStoreMemory())
                  .Double(),
              750 * 1024 * 1024);
    ASSERT_EQ(resource_scheduler.GetLocalResourceManager()
                  .GetLocalResources()
                  .total.Sum(ResourceID::ObjectStoreMemory())
                  .Double(),
              1000 * 1024 * 1024);
  }

  used_object_store_memory = 450 * 1024 * 1024;
  {
    rpc::ResourcesData data;
    resource_scheduler.GetLocalResourceManager().FillResourceUsage(data);
    auto available = data.resources_available();
    auto total = data.resources_total();
    ASSERT_EQ(available["object_store_memory"], 550 * 1024 * 1024);
    ASSERT_EQ(resource_scheduler.GetLocalResourceManager()
                  .GetLocalResources()
                  .available.Sum(ResourceID::ObjectStoreMemory())
                  .Double(),
              550 * 1024 * 1024);
    ASSERT_EQ(resource_scheduler.GetLocalResourceManager()
                  .GetLocalResources()
                  .total.Sum(ResourceID::ObjectStoreMemory())
                  .Double(),
              1000 * 1024 * 1024);
  }

  used_object_store_memory = 0;
  {
    rpc::ResourcesData data;
    resource_scheduler.GetLocalResourceManager().FillResourceUsage(data);
    auto available = data.resources_available();
    auto total = data.resources_total();
    ASSERT_EQ(available["object_store_memory"], 1000 * 1024 * 1024);
    ASSERT_EQ(resource_scheduler.GetLocalResourceManager()
                  .GetLocalResources()
                  .available.Sum(ResourceID::ObjectStoreMemory())
                  .Double(),
              1000 * 1024 * 1024);
    ASSERT_EQ(resource_scheduler.GetLocalResourceManager()
                  .GetLocalResources()
                  .total.Sum(ResourceID::ObjectStoreMemory())
                  .Double(),
              1000 * 1024 * 1024);
  }

  used_object_store_memory = 9999999999;
  {
    rpc::ResourcesData data;
    resource_scheduler.GetLocalResourceManager().FillResourceUsage(data);
    auto available = data.resources_available();
    auto total = data.resources_total();
    ASSERT_EQ(available["object_store_memory"], 0);
    ASSERT_EQ(resource_scheduler.GetLocalResourceManager()
                  .GetLocalResources()
                  .available.Sum(ResourceID::ObjectStoreMemory())
                  .Double(),
              0);
    ASSERT_EQ(resource_scheduler.GetLocalResourceManager()
                  .GetLocalResources()
                  .total.Sum(ResourceID::ObjectStoreMemory())
                  .Double(),
              1000 * 1024 * 1024);
  }
}

TEST_F(ClusterResourceSchedulerTest, DirtyLocalViewTest) {
  absl::flat_hash_map<std::string, double> initial_resources({{"CPU", 1}});
  ClusterResourceScheduler resource_scheduler(
      scheduling::NodeID("local"), initial_resources, is_node_available_fn_);
  auto remote = scheduling::NodeID(NodeID::FromRandom().Binary());
  resource_scheduler.GetClusterResourceManager().AddOrUpdateNode(
      remote, {{"CPU", 2.}}, {{"CPU", 2.}});
  const absl::flat_hash_map<std::string, double> task_spec = {{"CPU", 1.}};

  // Allocate local resources to force tasks onto the remote node when
  // resources are available.
  std::shared_ptr<TaskResourceInstances> task_allocation =
      std::make_shared<TaskResourceInstances>();
  ASSERT_TRUE(resource_scheduler.GetLocalResourceManager().AllocateLocalTaskResources(
      task_spec, task_allocation));
  task_allocation = std::make_shared<TaskResourceInstances>();
  ASSERT_FALSE(resource_scheduler.GetLocalResourceManager().AllocateLocalTaskResources(
      task_spec, task_allocation));
  // View of local resources is not affected by resource usage report.
  rpc::ResourcesData data;
  resource_scheduler.GetLocalResourceManager().FillResourceUsage(data);
  ASSERT_FALSE(resource_scheduler.GetLocalResourceManager().AllocateLocalTaskResources(
      task_spec, task_allocation));

  for (int num_slots_available = 0; num_slots_available <= 2; num_slots_available++) {
    rpc::ResourcesData data;
    int64_t t;
    bool is_infeasible;
    rpc::SchedulingStrategy scheduling_strategy;
    scheduling_strategy.mutable_default_scheduling_strategy();
    for (int i = 0; i < 3; i++) {
      // Remote node reports update local view.
      resource_scheduler.GetClusterResourceManager().AddOrUpdateNode(
          remote, {{"CPU", 2.}}, {{"CPU", num_slots_available}});
      for (int j = 0; j < num_slots_available; j++) {
        ASSERT_EQ(
            remote,
            resource_scheduler.GetBestSchedulableNode(
                task_spec, scheduling_strategy, false, false, true, &t, &is_infeasible));
        // Allocate remote resources.
        ASSERT_TRUE(resource_scheduler.AllocateRemoteTaskResources(remote, task_spec));
      }
      // Our local view says there are not enough resources on the remote node to
      // schedule another task.
      ASSERT_EQ(
          resource_scheduler.GetBestSchedulableNode(
              task_spec, scheduling_strategy, false, false, true, &t, &is_infeasible),
          scheduling::NodeID::Nil());
      ASSERT_FALSE(
          resource_scheduler.GetLocalResourceManager().AllocateLocalTaskResources(
              task_spec, task_allocation));
      ASSERT_FALSE(resource_scheduler.AllocateRemoteTaskResources(remote, task_spec));
    }
  }
}

TEST_F(ClusterResourceSchedulerTest, DynamicResourceTest) {
  ClusterResourceScheduler resource_scheduler(
      scheduling::NodeID("local"), {{"CPU", 2}}, is_node_available_fn_);

  absl::flat_hash_map<std::string, double> resource_request = {{"CPU", 1},
                                                               {"custom123", 2}};
  int64_t t;
  bool is_infeasible;
  rpc::SchedulingStrategy scheduling_strategy;
  scheduling_strategy.mutable_default_scheduling_strategy();

  auto result = resource_scheduler.GetBestSchedulableNode(
      resource_request, scheduling_strategy, false, false, false, &t, &is_infeasible);
  ASSERT_TRUE(result.IsNil());

  resource_scheduler.GetLocalResourceManager().AddLocalResourceInstances(
      scheduling::ResourceID("custom123"), {0., 1.0, 1.0});

  result = resource_scheduler.GetBestSchedulableNode(
      resource_request, scheduling_strategy, false, false, false, &t, &is_infeasible);
  ASSERT_FALSE(result.IsNil()) << resource_scheduler.DebugString();

  resource_request["custom123"] = 3;
  result = resource_scheduler.GetBestSchedulableNode(
      resource_request, scheduling_strategy, false, false, false, &t, &is_infeasible);
  ASSERT_TRUE(result.IsNil());

  resource_scheduler.GetLocalResourceManager().AddLocalResourceInstances(
      scheduling::ResourceID("custom123"), {1.0});
  result = resource_scheduler.GetBestSchedulableNode(
      resource_request, scheduling_strategy, false, false, false, &t, &is_infeasible);
  ASSERT_FALSE(result.IsNil());

  resource_scheduler.GetLocalResourceManager().DeleteLocalResource(
      scheduling::ResourceID("custom123"));
  result = resource_scheduler.GetBestSchedulableNode(
      resource_request, scheduling_strategy, false, false, false, &t, &is_infeasible);
  ASSERT_TRUE(result.IsNil());
}

TEST_F(ClusterResourceSchedulerTest, AvailableResourceEmptyTest) {
  ClusterResourceScheduler resource_scheduler(
      scheduling::NodeID("local"), {{"custom123", 5}}, is_node_available_fn_);
  std::shared_ptr<TaskResourceInstances> resource_instances =
      std::make_shared<TaskResourceInstances>();
  absl::flat_hash_map<std::string, double> resource_request = {{"custom123", 5}};
  bool allocated =
      resource_scheduler.GetLocalResourceManager().AllocateLocalTaskResources(
          resource_request, resource_instances);
  ASSERT_TRUE(allocated);
  ASSERT_TRUE(resource_scheduler.GetLocalResourceManager().IsAvailableResourceEmpty(
      scheduling::ResourceID("custom123")));
}

TEST_F(ClusterResourceSchedulerTest, TestForceSpillback) {
  absl::flat_hash_map<std::string, double> resource_spec({{"CPU", 1}});
  ClusterResourceScheduler resource_scheduler(
      scheduling::NodeID("local"), resource_spec, is_node_available_fn_);
  std::vector<scheduling::NodeID> node_ids;
  for (int i = 0; i < 100; i++) {
    node_ids.emplace_back(NodeID::FromRandom().Binary());
    resource_scheduler.GetClusterResourceManager().AddOrUpdateNode(
        node_ids.back(), {}, {});
  }

  // No feasible nodes.
  int64_t total_violations;
  bool is_infeasible;
  rpc::SchedulingStrategy scheduling_strategy;
  scheduling_strategy.mutable_default_scheduling_strategy();
  // Normally we prefer local.
  ASSERT_EQ(resource_scheduler.GetBestSchedulableNode(resource_spec,
                                                      scheduling_strategy,
                                                      false,
                                                      false,
                                                      /*force_spillback=*/false,
                                                      &total_violations,
                                                      &is_infeasible),
            scheduling::NodeID("local"));
  // If spillback is forced, we try to spill to remote, but only if there is a
  // schedulable node.
  ASSERT_EQ(resource_scheduler.GetBestSchedulableNode(resource_spec,
                                                      scheduling_strategy,
                                                      false,
                                                      false,
                                                      /*force_spillback=*/true,
                                                      &total_violations,
                                                      &is_infeasible),
            scheduling::NodeID::Nil());
  // Choose a remote node that has the resources available.
  resource_scheduler.GetClusterResourceManager().AddOrUpdateNode(
      node_ids[50], resource_spec, {});
  ASSERT_EQ(resource_scheduler.GetBestSchedulableNode(resource_spec,
                                                      scheduling_strategy,
                                                      false,
                                                      false,
                                                      /*force_spillback=*/true,
                                                      &total_violations,
                                                      &is_infeasible),
            scheduling::NodeID::Nil());
  resource_scheduler.GetClusterResourceManager().AddOrUpdateNode(
      node_ids[51], resource_spec, resource_spec);
  ASSERT_EQ(resource_scheduler.GetBestSchedulableNode(resource_spec,
                                                      scheduling_strategy,
                                                      false,
                                                      false,
                                                      /*force_spillback=*/true,
                                                      &total_violations,
                                                      &is_infeasible),
            node_ids[51]);
}

TEST_F(ClusterResourceSchedulerTest, CustomResourceInstanceTest) {
  SetUnitInstanceResourceIds({ResourceID("FPGA")});
  ClusterResourceScheduler resource_scheduler(
      scheduling::NodeID("local"), {{"CPU", 4}, {"FPGA", 2}}, is_node_available_fn_);

  auto fpga_resource_id = ResourceID("FPGA");

  ResourceRequest resource_request =
      CreateResourceRequest({{ResourceID::CPU(), 1}, {fpga_resource_id, 0.7}});

  std::shared_ptr<TaskResourceInstances> task_allocation =
      std::make_shared<TaskResourceInstances>();
  bool success =
      resource_scheduler.GetLocalResourceManager().AllocateTaskResourceInstances(
          resource_request, task_allocation);
  ASSERT_TRUE(success) << resource_scheduler.DebugString();

  success = resource_scheduler.GetLocalResourceManager().AllocateTaskResourceInstances(
      resource_request, task_allocation);
  ASSERT_TRUE(success) << resource_scheduler.DebugString();

  ResourceRequest fail_resource_request =
      CreateResourceRequest({{ResourceID::CPU(), 1}, {fpga_resource_id, 0.5}});
  success = resource_scheduler.GetLocalResourceManager().AllocateTaskResourceInstances(
      fail_resource_request, task_allocation);
  ASSERT_FALSE(success) << resource_scheduler.DebugString();
}

TEST_F(ClusterResourceSchedulerTest, TaskResourceInstancesSerializedStringTest) {
  SetUnitInstanceResourceIds({ResourceID("GPU")});
  ClusterResourceScheduler resource_scheduler(scheduling::NodeID("local"),
                                              {{"CPU", 4}, {"memory", 4}, {"GPU", 2}},
                                              is_node_available_fn_);
  std::shared_ptr<TaskResourceInstances> cluster_resources =
      std::make_shared<TaskResourceInstances>();
  cluster_resources->Set(ResourceID::CPU(), {2.});
  cluster_resources->Set(ResourceID::Memory(), {4.});
  cluster_resources->Set(ResourceID::GPU(), {1., 1.});
  std::string serialized_string = cluster_resources->SerializeAsJson();
  std::string expected_serialized_string =
      R"({"CPU":20000,"memory":40000,"GPU":[10000, 10000]})";
  ASSERT_EQ(serialized_string, expected_serialized_string);

  SetUnitInstanceResourceIds({ResourceID::CPU(), ResourceID::GPU()});
  std::shared_ptr<TaskResourceInstances> cluster_instance_resources =
      std::make_shared<TaskResourceInstances>();
  cluster_instance_resources->Set(ResourceID::CPU(), {1., 1.});
  cluster_instance_resources->Set(ResourceID::Memory(), {4.});
  cluster_instance_resources->Set(ResourceID::GPU(), {1., 1.});
  ClusterResourceScheduler resource_scheduler_cpu_instance(
      scheduling::NodeID("local"),
      {{"CPU", 4}, {"memory", 4}, {"GPU", 2}},
      is_node_available_fn_);
  std::string instance_serialized_string = cluster_instance_resources->SerializeAsJson();
  std::string expected_instance_serialized_string =
      R"({"CPU":[10000, 10000],"memory":40000,"GPU":[10000, 10000]})";
  ASSERT_EQ(instance_serialized_string, expected_instance_serialized_string);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
