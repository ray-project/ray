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
// Used to path empty vector arguments.
vector<int64_t> EmptyIntVector;
vector<bool> EmptyBoolVector;
vector<FixedPoint> EmptyFixedPointVector;

void initResourceRequest(ResourceRequest &res_request, vector<FixedPoint> pred_demands,
                         vector<int64_t> cust_ids, vector<FixedPoint> cust_demands) {
  res_request.predefined_resources.resize(PredefinedResources_MAX + pred_demands.size());
  for (size_t i = 0; i < pred_demands.size(); i++) {
    res_request.predefined_resources[i] = pred_demands[i];
  }

  for (size_t i = pred_demands.size(); i < PredefinedResources_MAX; i++) {
    res_request.predefined_resources.push_back(0);
  }

  for (size_t i = 0; i < cust_ids.size(); i++) {
    res_request.custom_resources[cust_ids[i]] = cust_demands[i];
  }
};

void addTaskResourceInstances(bool predefined, vector<double> allocation, uint64_t idx,
                              TaskResourceInstances *task_allocation) {
  std::vector<FixedPoint> allocation_fp = VectorDoubleToVectorFixedPoint(allocation);

  if (task_allocation->predefined_resources.size() < PredefinedResources_MAX) {
    task_allocation->predefined_resources.resize(PredefinedResources_MAX);
  }
  if (predefined) {
    task_allocation->predefined_resources[idx] = allocation_fp;
  } else {
    task_allocation->custom_resources.insert(
        std::pair<int64_t, vector<FixedPoint>>(idx, allocation_fp));
  }
};

void initNodeResources(NodeResources &node, vector<FixedPoint> &pred_capacities,
                       vector<int64_t> &cust_ids, vector<FixedPoint> &cust_capacities) {
  for (size_t i = 0; i < pred_capacities.size(); i++) {
    ResourceCapacity rc;
    rc.total = rc.available = pred_capacities[i];
    node.predefined_resources.push_back(rc);
  }

  if (pred_capacities.size() < PredefinedResources_MAX) {
    for (int i = pred_capacities.size(); i < PredefinedResources_MAX; i++) {
      ResourceCapacity rc;
      rc.total = rc.available = 0;
      node.predefined_resources.push_back(rc);
    }
  }

  ResourceCapacity rc;
  for (size_t i = 0; i < cust_capacities.size(); i++) {
    rc.total = rc.available = cust_capacities[i];
    node.custom_resources.insert(pair<int64_t, ResourceCapacity>(cust_ids[i], rc));
  }
}

bool nodeResourcesEqual(const NodeResources &nr1, const NodeResources &nr2) {
  if (nr1.predefined_resources.size() != nr2.predefined_resources.size()) {
    cout << nr1.predefined_resources.size() << " " << nr2.predefined_resources.size()
         << endl;
    return false;
  }

  for (size_t i = 0; i < nr1.predefined_resources.size(); i++) {
    if (nr1.predefined_resources[i].available != nr2.predefined_resources[i].available) {
      return false;
    }
    if (nr1.predefined_resources[i].total != nr2.predefined_resources[i].total) {
      return false;
    }
  }

  if (nr1.custom_resources.size() != nr2.custom_resources.size()) {
    return false;
  }

  auto cr1 = nr1.custom_resources;
  auto cr2 = nr2.custom_resources;
  for (auto it1 = cr1.begin(); it1 != cr1.end(); ++it1) {
    auto it2 = cr2.find(it1->first);
    if (it2 == cr2.end()) {
      return false;
    }
    if (it1->second.total != it2->second.total) {
      return false;
    }
    if (it1->second.available != it2->second.available) {
      return false;
    }
  }
  return true;
}

class ClusterResourceSchedulerTest : public ::testing::Test {
 public:
  void SetUp() {
    // The legacy scheduling policy is easier to reason about for testing purposes. See
    // `scheduling_policy_test.cc` for comprehensive testing of the hybrid scheduling
    // policy.
    gcs_client_ = std::make_unique<gcs::MockGcsClient>();
    node_info.set_node_id(NodeID::FromRandom().Binary());
    ON_CALL(*gcs_client_->mock_node_accessor, Get(::testing::_, ::testing::_))
        .WillByDefault(::testing::Return(&node_info));
  }

  void Shutdown() {}

  void initCluster(ClusterResourceScheduler &resource_scheduler, int n) {
    vector<FixedPoint> pred_capacities;
    vector<int64_t> cust_ids;
    vector<FixedPoint> cust_capacities;
    int i, k;

    for (i = 0; i < n; i++) {
      NodeResources node_resources;

      for (k = 0; k < PredefinedResources_MAX; k++) {
        if (rand() % 3 == 0) {
          pred_capacities.push_back(0);
        } else {
          pred_capacities.push_back(rand() % 10);
        }
      }

      int m = min(rand() % PredefinedResources_MAX, n);

      int start = rand() % n;
      for (k = 0; k < m; k++) {
        cust_ids.push_back((start + k) % n);
        cust_capacities.push_back(rand() % 10);
      }

      initNodeResources(node_resources, pred_capacities, cust_ids, cust_capacities);

      resource_scheduler.AddOrUpdateNode(i, node_resources);

      node_resources.custom_resources.clear();
    }
  }
  std::unique_ptr<gcs::MockGcsClient> gcs_client_;
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

TEST_F(ClusterResourceSchedulerTest, SchedulingInitClusterTest) {
  int num_nodes = 10;
  ClusterResourceScheduler resource_scheduler;

  initCluster(resource_scheduler, num_nodes);

  ASSERT_EQ(resource_scheduler.NumNodes(), num_nodes);
}

TEST_F(ClusterResourceSchedulerTest, SchedulingDeleteClusterNodeTest) {
  int num_nodes = 4;
  int64_t remove_id = 2;

  ClusterResourceScheduler resource_scheduler;

  initCluster(resource_scheduler, num_nodes);
  resource_scheduler.RemoveNode(remove_id);

  ASSERT_TRUE(num_nodes - 1 == resource_scheduler.NumNodes());
}

TEST_F(ClusterResourceSchedulerTest, SchedulingModifyClusterNodeTest) {
  int num_nodes = 4;
  int64_t update_id = 2;
  ClusterResourceScheduler resource_scheduler;

  initCluster(resource_scheduler, num_nodes);

  NodeResources node_resources;
  vector<FixedPoint> pred_capacities;
  vector<int64_t> cust_ids;
  vector<FixedPoint> cust_capacities;
  int k;

  for (k = 0; k < PredefinedResources_MAX; k++) {
    if (rand() % 3 == 0) {
      pred_capacities.push_back(0);
    } else {
      pred_capacities.push_back(rand() % 10);
    }
  }

  int m = min(rand() % PredefinedResources_MAX, num_nodes);

  int start = rand() % num_nodes;
  for (k = 0; k < m; k++) {
    cust_ids.push_back((start + k) % num_nodes);
    cust_capacities.push_back(rand() % 10);

    initNodeResources(node_resources, pred_capacities, cust_ids, cust_capacities);
    resource_scheduler.AddOrUpdateNode(update_id, node_resources);
  }
  ASSERT_TRUE(num_nodes == resource_scheduler.NumNodes());
}

TEST_F(ClusterResourceSchedulerTest, SpreadSchedulingStrategyTest) {
  absl::flat_hash_map<std::string, double> resource_total({{"CPU", 10}});
  auto local_node_id = NodeID::FromRandom().Binary();
  ClusterResourceScheduler resource_scheduler(local_node_id, resource_total,
                                              *gcs_client_);
  auto remote_node_id = NodeID::FromRandom().Binary();
  resource_scheduler.AddOrUpdateNode(remote_node_id, resource_total, resource_total);

  absl::flat_hash_map<std::string, double> resource_request({{"CPU", 1}});
  int64_t violations;
  bool is_infeasible;
  rpc::SchedulingStrategy scheduling_strategy;
  scheduling_strategy.mutable_spread_scheduling_strategy();
  std::string node_id = resource_scheduler.GetBestSchedulableNode(
      resource_request, scheduling_strategy, false, false, false, &violations,
      &is_infeasible);
  ASSERT_EQ(node_id, local_node_id);
  absl::flat_hash_map<std::string, double> resource_available({{"CPU", 9}});
  resource_scheduler.AddOrUpdateNode(local_node_id, resource_total, resource_available);
  node_id = resource_scheduler.GetBestSchedulableNode(resource_request,
                                                      scheduling_strategy, false, false,
                                                      false, &violations, &is_infeasible);
  ASSERT_EQ(node_id, remote_node_id);
}

TEST_F(ClusterResourceSchedulerTest, SchedulingUpdateAvailableResourcesTest) {
  // Create cluster resources.
  NodeResources node_resources;
  vector<FixedPoint> pred_capacities{10, 5, 3};
  vector<int64_t> cust_ids{1, 2};
  vector<FixedPoint> cust_capacities{5, 5};
  initNodeResources(node_resources, pred_capacities, cust_ids, cust_capacities);
  ClusterResourceScheduler resource_scheduler(1, node_resources, *gcs_client_);

  {
    ResourceRequest resource_request;
#define PRED_CUSTOM_LEN 2
    vector<FixedPoint> pred_demands{7, 5};
    vector<int64_t> cust_ids{1, 2};
    vector<FixedPoint> cust_demands{3, 5};
    initResourceRequest(resource_request, pred_demands, cust_ids, cust_demands);
    int64_t violations;
    bool is_infeasible;
    rpc::SchedulingStrategy scheduling_strategy;
    scheduling_strategy.mutable_default_scheduling_strategy();
    int64_t node_id = resource_scheduler.GetBestSchedulableNode(
        resource_request, scheduling_strategy, false, false, &violations, &is_infeasible);
    ASSERT_EQ(node_id, 1);
    ASSERT_TRUE(violations == 0);

    NodeResources nr1, nr2;
    ASSERT_TRUE(resource_scheduler.GetNodeResources(node_id, &nr1));
    auto task_allocation = std::make_shared<TaskResourceInstances>();
    ASSERT_TRUE(
        resource_scheduler.AllocateLocalTaskResources(resource_request, task_allocation));
    ASSERT_TRUE(resource_scheduler.GetNodeResources(node_id, &nr2));

    for (size_t i = 0; i < PRED_CUSTOM_LEN; i++) {
      auto t = nr1.predefined_resources[i].available -
               resource_request.predefined_resources[i];
      if (t < 0) t = 0;
      ASSERT_EQ(nr2.predefined_resources[i].available, t);
    }

    for (size_t i = 1; i <= PRED_CUSTOM_LEN; i++) {
      auto it1 = nr1.custom_resources.find(i);
      if (it1 != nr1.custom_resources.end()) {
        auto it2 = nr2.custom_resources.find(i);
        if (it2 != nr2.custom_resources.end()) {
          auto t = it1->second.available - resource_request.custom_resources[i];
          if (t < 0) t = 0;
          ASSERT_EQ(it2->second.available, t);
        }
      }
    }
  }
}

TEST_F(ClusterResourceSchedulerTest, SchedulingUpdateTotalResourcesTest) {
  absl::flat_hash_map<std::string, double> initial_resources = {
      {ray::kCPU_ResourceLabel, 1}, {"custom", 1}};
  ClusterResourceScheduler resource_scheduler(
      NodeID::FromRandom().Binary(), initial_resources, *gcs_client_, nullptr, nullptr);

  resource_scheduler.AddLocalResourceInstances(ray::kCPU_ResourceLabel, {0, 1, 1});
  resource_scheduler.AddLocalResourceInstances("custom", {0, 1, 1});

  const auto &predefined_resources =
      resource_scheduler.GetLocalNodeResources().predefined_resources;
  ASSERT_EQ(predefined_resources[CPU].total.Double(), 3);

  const auto &custom_resources =
      resource_scheduler.GetLocalNodeResources().custom_resources;
  auto resource_id = resource_scheduler.string_to_int_map_.Get("custom");
  ASSERT_EQ(custom_resources.find(resource_id)->second.total.Double(), 3);
}

TEST_F(ClusterResourceSchedulerTest, SchedulingAddOrUpdateNodeTest) {
  ClusterResourceScheduler resource_scheduler;
  NodeResources nr, nr_out;
  int64_t node_id = 1;

  // Add node.
  {
    NodeResources node_resources;
    vector<FixedPoint> pred_capacities{10, 5, 3};
    vector<int64_t> cust_ids{1, 2};
    vector<FixedPoint> cust_capacities{5, 5};
    initNodeResources(node_resources, pred_capacities, cust_ids, cust_capacities);
    resource_scheduler.AddOrUpdateNode(node_id, node_resources);
    nr = node_resources;
  }

  // Check whether node resources were correctly added.
  if (resource_scheduler.GetNodeResources(node_id, &nr_out)) {
    ASSERT_TRUE(nodeResourcesEqual(nr, nr_out));
  } else {
    ASSERT_TRUE(false);
  }

  // Update node.
  {
    NodeResources node_resources;
    vector<FixedPoint> pred_capacities{10, 10};
    vector<int64_t> cust_ids{2, 3};
    vector<FixedPoint> cust_capacities{6, 6};
    initNodeResources(node_resources, pred_capacities, cust_ids, cust_capacities);
    resource_scheduler.AddOrUpdateNode(node_id, node_resources);
    nr = node_resources;
  }
  if (resource_scheduler.GetNodeResources(node_id, &nr_out)) {
    ASSERT_TRUE(nodeResourcesEqual(nr, nr_out));
  } else {
    ASSERT_TRUE(false);
  }
}

TEST_F(ClusterResourceSchedulerTest, SchedulingResourceRequestTest) {
  // Create cluster resources containing local node.
  NodeResources node_resources;
  vector<FixedPoint> pred_capacities{5, 5};
  vector<int64_t> cust_ids{1};
  vector<FixedPoint> cust_capacities{10};
  initNodeResources(node_resources, pred_capacities, cust_ids, cust_capacities);
  ClusterResourceScheduler resource_scheduler(0, node_resources, *gcs_client_);
  auto node_id = NodeID::FromRandom();
  auto node_internal_id = resource_scheduler.string_to_int_map_.Insert(node_id.Binary());
  rpc::SchedulingStrategy scheduling_strategy;
  scheduling_strategy.mutable_default_scheduling_strategy();
  {
    NodeResources node_resources;
    vector<FixedPoint> pred_capacities{10, 2, 3};
    vector<int64_t> cust_ids{1, 2};
    vector<FixedPoint> cust_capacities{5, 5};
    initNodeResources(node_resources, pred_capacities, cust_ids, cust_capacities);
    resource_scheduler.AddOrUpdateNode(node_internal_id, node_resources);
  }
  // Predefined resources, hard constraint violation
  {
    ResourceRequest resource_request;
    vector<FixedPoint> pred_demands = {11};
    initResourceRequest(resource_request, pred_demands, EmptyIntVector,
                        EmptyFixedPointVector);
    int64_t violations;
    bool is_infeasible;
    int64_t node_id = resource_scheduler.GetBestSchedulableNode(
        resource_request, scheduling_strategy, false, false, &violations, &is_infeasible);
    ASSERT_EQ(node_id, -1);
  }

  // Predefined resources, no constraint violation.
  {
    ResourceRequest resource_request;
    vector<FixedPoint> pred_demands = {5};
    initResourceRequest(resource_request, pred_demands, EmptyIntVector,
                        EmptyFixedPointVector);
    int64_t violations;
    bool is_infeasible;
    int64_t node_id = resource_scheduler.GetBestSchedulableNode(
        resource_request, scheduling_strategy, false, false, &violations, &is_infeasible);
    ASSERT_TRUE(node_id != -1);
    ASSERT_TRUE(violations == 0);
  }
  // Custom resources, hard constraint violation.
  {
    ResourceRequest resource_request;
    vector<FixedPoint> pred_demands{5, 2};
    vector<int64_t> cust_ids{1};
    vector<FixedPoint> cust_demands{11};
    initResourceRequest(resource_request, pred_demands, cust_ids, cust_demands);
    int64_t violations;
    bool is_infeasible;
    int64_t node_id = resource_scheduler.GetBestSchedulableNode(
        resource_request, scheduling_strategy, false, false, &violations, &is_infeasible);
    ASSERT_TRUE(node_id == -1);
  }
  // Custom resources, no constraint violation.
  {
    ResourceRequest resource_request;
    vector<FixedPoint> pred_demands{5, 2};
    vector<int64_t> cust_ids{1};
    vector<FixedPoint> cust_demands{5};
    initResourceRequest(resource_request, pred_demands, cust_ids, cust_demands);
    int64_t violations;
    bool is_infeasible;
    int64_t node_id = resource_scheduler.GetBestSchedulableNode(
        resource_request, scheduling_strategy, false, false, &violations, &is_infeasible);
    ASSERT_TRUE(node_id != -1);
    ASSERT_TRUE(violations == 0);
  }
  // Custom resource missing, hard constraint violation.
  {
    ResourceRequest resource_request;
    vector<FixedPoint> pred_demands{5, 2};
    vector<int64_t> cust_ids{100};
    vector<FixedPoint> cust_demands{5};
    initResourceRequest(resource_request, pred_demands, cust_ids, cust_demands);
    int64_t violations;
    bool is_infeasible;
    int64_t node_id = resource_scheduler.GetBestSchedulableNode(
        resource_request, scheduling_strategy, false, false, &violations, &is_infeasible);
    ASSERT_TRUE(node_id == -1);
  }
  // Placement hints, no constraint violation.
  {
    ResourceRequest resource_request;
    vector<FixedPoint> pred_demands{5, 2};
    vector<int64_t> cust_ids{1};
    vector<FixedPoint> cust_demands{5};
    initResourceRequest(resource_request, pred_demands, cust_ids, cust_demands);
    int64_t violations;
    bool is_infeasible;
    int64_t node_id = resource_scheduler.GetBestSchedulableNode(
        resource_request, scheduling_strategy, false, false, &violations, &is_infeasible);
    ASSERT_TRUE(node_id != -1);
    ASSERT_TRUE(violations == 0);
  }
}

TEST_F(ClusterResourceSchedulerTest, GetLocalAvailableResourcesWithCpuUnitTest) {
  RayConfig::instance().initialize(
      R"(
{
  "predefined_unit_instance_resources": "CPU,GPU"
}
  )");
  // Create cluster resources containing local node.
  NodeResources node_resources;
  vector<FixedPoint> pred_capacities{3 /* CPU */, 4 /* MEM */, 5 /* GPU */};
  vector<int64_t> cust_ids{1};
  vector<FixedPoint> cust_capacities{8};
  initNodeResources(node_resources, pred_capacities, cust_ids, cust_capacities);
  ClusterResourceScheduler resource_scheduler(0, node_resources, *gcs_client_);

  TaskResourceInstances available_cluster_resources =
      resource_scheduler.GetLocalResources().GetAvailableResourceInstances();

  TaskResourceInstances expected_cluster_resources;
  addTaskResourceInstances(true, {1., 1., 1.}, 0, &expected_cluster_resources);
  addTaskResourceInstances(true, {4.}, 1, &expected_cluster_resources);
  addTaskResourceInstances(true, {1., 1., 1., 1., 1.}, 2, &expected_cluster_resources);

  ASSERT_EQ(expected_cluster_resources == available_cluster_resources, false);

  addTaskResourceInstances(false, {8.}, 1, &expected_cluster_resources);

  ASSERT_EQ(expected_cluster_resources == available_cluster_resources, true);
}

TEST_F(ClusterResourceSchedulerTest, GetLocalAvailableResourcesTest) {
  RayConfig::instance().initialize(
      R"(
{
  "predefined_unit_instance_resources": "GPU"
}
  )");
  // Create cluster resources containing local node.
  NodeResources node_resources;
  vector<FixedPoint> pred_capacities{3 /* CPU */, 4 /* MEM */, 5 /* GPU */};
  vector<int64_t> cust_ids{1};
  vector<FixedPoint> cust_capacities{8};
  initNodeResources(node_resources, pred_capacities, cust_ids, cust_capacities);
  ClusterResourceScheduler resource_scheduler(0, node_resources, *gcs_client_);

  TaskResourceInstances available_cluster_resources =
      resource_scheduler.GetLocalResources().GetAvailableResourceInstances();

  TaskResourceInstances expected_cluster_resources;
  addTaskResourceInstances(true, {3.}, 0, &expected_cluster_resources);
  addTaskResourceInstances(true, {4.}, 1, &expected_cluster_resources);
  addTaskResourceInstances(true, {1., 1., 1., 1., 1.}, 2, &expected_cluster_resources);

  ASSERT_EQ(expected_cluster_resources == available_cluster_resources, false);

  addTaskResourceInstances(false, {8.}, 1, &expected_cluster_resources);

  ASSERT_EQ(expected_cluster_resources == available_cluster_resources, true);
}

TEST_F(ClusterResourceSchedulerTest, GetCPUInstancesDoubleTest) {
  TaskResourceInstances task_resources;
  addTaskResourceInstances(true, {1., 1., 1.}, CPU, &task_resources);
  addTaskResourceInstances(true, {4.}, MEM, &task_resources);
  addTaskResourceInstances(true, {1., 1., 1., 1., 1.}, GPU, &task_resources);

  std::vector<FixedPoint> cpu_instances = task_resources.GetCPUInstances();
  std::vector<FixedPoint> expected_cpu_instances{1., 1., 1.};

  ASSERT_EQ(EqualVectors(cpu_instances, expected_cpu_instances), true);
}

TEST_F(ClusterResourceSchedulerTest, AvailableResourceInstancesOpsTest) {
  NodeResources node_resources;
  vector<FixedPoint> pred_capacities{3 /* CPU */};
  initNodeResources(node_resources, pred_capacities, EmptyIntVector,
                    EmptyFixedPointVector);
  ClusterResourceScheduler cluster(0, node_resources, *gcs_client_);

  ResourceInstanceCapacities instances;

  instances.total = {6., 6., 6.};
  instances.available = {3., 2., 5.};
  ResourceInstanceCapacities old_instances = instances;

  std::vector<FixedPoint> a{1., 1., 1.};
  cluster.AddAvailableResourceInstances(a, &instances);
  cluster.SubtractAvailableResourceInstances(a, &instances);

  ASSERT_EQ(EqualVectors(instances.available, old_instances.available), true);

  a = {10., 1., 1.};
  cluster.AddAvailableResourceInstances(a, &instances);
  std::vector<FixedPoint> expected_available{6., 3., 6.};

  ASSERT_EQ(EqualVectors(instances.available, expected_available), true);

  a = {10., 1., 1.};
  cluster.SubtractAvailableResourceInstances(a, &instances);
  expected_available = {0., 2., 5.};
  ASSERT_EQ(EqualVectors(instances.available, expected_available), true);
}

TEST_F(ClusterResourceSchedulerTest, TaskResourceInstancesTest) {
  // Allocate resources for a task request specifying only predefined resources.
  {
    NodeResources node_resources;
    vector<FixedPoint> pred_capacities{3. /* CPU */, 4. /* MEM */, 5. /* GPU */};
    initNodeResources(node_resources, pred_capacities, EmptyIntVector,
                      EmptyFixedPointVector);
    ClusterResourceScheduler resource_scheduler(0, node_resources, *gcs_client_);

    ResourceRequest resource_request;
    vector<FixedPoint> pred_demands = {3. /* CPU */, 2. /* MEM */, 1.5 /* GPU */};
    initResourceRequest(resource_request, pred_demands, EmptyIntVector,
                        EmptyFixedPointVector);

    NodeResourceInstances old_local_resources = resource_scheduler.GetLocalResources();

    std::shared_ptr<TaskResourceInstances> task_allocation =
        std::make_shared<TaskResourceInstances>();
    bool success = resource_scheduler.AllocateTaskResourceInstances(resource_request,
                                                                    task_allocation);

    ASSERT_EQ(success, true);

    resource_scheduler.FreeTaskResourceInstances(task_allocation);

    ASSERT_EQ((resource_scheduler.GetLocalResources() == old_local_resources), true);
  }
  // Try to allocate resources for a task request that overallocates a hard constrained
  // resource.
  {
    NodeResources node_resources;
    vector<FixedPoint> pred_capacities{3 /* CPU */, 4 /* MEM */, 5 /* GPU */};
    initNodeResources(node_resources, pred_capacities, EmptyIntVector,
                      EmptyFixedPointVector);
    ClusterResourceScheduler resource_scheduler(0, node_resources, *gcs_client_);

    ResourceRequest resource_request;
    vector<FixedPoint> pred_demands = {4. /* CPU */, 2. /* MEM */, 1.5 /* GPU */};
    initResourceRequest(resource_request, pred_demands, EmptyIntVector,
                        EmptyFixedPointVector);

    NodeResourceInstances old_local_resources = resource_scheduler.GetLocalResources();
    std::shared_ptr<TaskResourceInstances> task_allocation =
        std::make_shared<TaskResourceInstances>();
    bool success = resource_scheduler.AllocateTaskResourceInstances(resource_request,
                                                                    task_allocation);

    ASSERT_EQ(success, false);
    ASSERT_EQ((resource_scheduler.GetLocalResources() == old_local_resources), true);
  }
  // Allocate resources for a task request specifying both predefined and custom
  // resources.
  {
    NodeResources node_resources;
    vector<FixedPoint> pred_capacities{3 /* CPU */, 4 /* MEM */, 5 /* GPU */};
    vector<int64_t> cust_ids{1, 2};
    vector<FixedPoint> cust_capacities{4, 4};
    initNodeResources(node_resources, pred_capacities, cust_ids, cust_capacities);
    ClusterResourceScheduler resource_scheduler(0, node_resources, *gcs_client_);

    ResourceRequest resource_request;
    vector<FixedPoint> pred_demands = {3. /* CPU */, 2. /* MEM */, 1.5 /* GPU */};
    vector<FixedPoint> cust_demands{3, 2};
    initResourceRequest(resource_request, pred_demands, cust_ids, cust_demands);

    NodeResourceInstances old_local_resources = resource_scheduler.GetLocalResources();
    std::shared_ptr<TaskResourceInstances> task_allocation =
        std::make_shared<TaskResourceInstances>();
    bool success = resource_scheduler.AllocateTaskResourceInstances(resource_request,
                                                                    task_allocation);

    ASSERT_EQ(success, true);

    resource_scheduler.FreeTaskResourceInstances(task_allocation);

    ASSERT_EQ((resource_scheduler.GetLocalResources() == old_local_resources), true);
  }
  // Allocate resources for a task request specifying both predefined and custom
  // resources, but overallocates a hard-constrained custom resource.
  {
    NodeResources node_resources;
    vector<FixedPoint> pred_capacities{3 /* CPU */, 4 /* MEM */, 5 /* GPU */};
    vector<int64_t> cust_ids{1, 2};
    vector<FixedPoint> cust_capacities{4, 4};
    initNodeResources(node_resources, pred_capacities, cust_ids, cust_capacities);
    ClusterResourceScheduler resource_scheduler(0, node_resources, *gcs_client_);

    ResourceRequest resource_request;
    vector<FixedPoint> pred_demands = {3. /* CPU */, 2. /* MEM */, 1.5 /* GPU */};
    vector<FixedPoint> cust_demands{3, 10};
    initResourceRequest(resource_request, pred_demands, cust_ids, cust_demands);

    NodeResourceInstances old_local_resources = resource_scheduler.GetLocalResources();
    std::shared_ptr<TaskResourceInstances> task_allocation =
        std::make_shared<TaskResourceInstances>();
    bool success = resource_scheduler.AllocateTaskResourceInstances(resource_request,
                                                                    task_allocation);

    ASSERT_EQ(success, false);
    ASSERT_EQ((resource_scheduler.GetLocalResources() == old_local_resources), true);
  }
}

TEST_F(ClusterResourceSchedulerTest, TaskResourceInstancesAllocationFailureTest) {
  /// Make sure there's no leak when the resource allocation failed in the middle.
  NodeResources node_resources;
  vector<FixedPoint> pred_capacities{1 /* CPU */, 1 /* MEM */, 1 /* GPU */};
  vector<int64_t> cust_ids{1, 2, 3};
  vector<FixedPoint> cust_capacities{4, 4, 4};
  initNodeResources(node_resources, pred_capacities, cust_ids, cust_capacities);
  ClusterResourceScheduler resource_scheduler(0, node_resources, *gcs_client_);

  ResourceRequest resource_request;
  vector<FixedPoint> pred_demands = {0. /* CPU */, 0. /* MEM */, 0. /* GPU */};
  vector<int64_t> req_cust_ids{1, 3, 5};
  vector<FixedPoint> cust_demands{3, 3, 4};
  initResourceRequest(resource_request, pred_demands, req_cust_ids, cust_demands);

  NodeResourceInstances old_local_resources = resource_scheduler.GetLocalResources();
  std::shared_ptr<TaskResourceInstances> task_allocation =
      std::make_shared<TaskResourceInstances>();
  bool success =
      resource_scheduler.AllocateTaskResourceInstances(resource_request, task_allocation);

  ASSERT_EQ(success, false);
  // resource_scheduler.FreeTaskResourceInstances(task_allocation);
  ASSERT_EQ((resource_scheduler.GetLocalResources() == old_local_resources), true);
}

TEST_F(ClusterResourceSchedulerTest, TaskResourceInstancesTest2) {
  {
    NodeResources node_resources;
    vector<FixedPoint> pred_capacities{4. /* CPU */, 4. /* MEM */, 5. /* GPU */};
    vector<int64_t> cust_ids{1, 2};
    vector<FixedPoint> cust_capacities{4., 4.};
    initNodeResources(node_resources, pred_capacities, cust_ids, cust_capacities);
    ClusterResourceScheduler resource_scheduler(0, node_resources, *gcs_client_);

    ResourceRequest resource_request;
    vector<FixedPoint> pred_demands = {2. /* CPU */, 2. /* MEM */, 1.5 /* GPU */};
    vector<FixedPoint> cust_demands{3., 2.};
    initResourceRequest(resource_request, pred_demands, cust_ids, cust_demands);

    std::shared_ptr<TaskResourceInstances> task_allocation =
        std::make_shared<TaskResourceInstances>();
    bool success = resource_scheduler.AllocateTaskResourceInstances(resource_request,
                                                                    task_allocation);

    NodeResourceInstances old_local_resources = resource_scheduler.GetLocalResources();
    ASSERT_EQ(success, true);
    std::vector<double> cpu_instances = task_allocation->GetCPUInstancesDouble();
    resource_scheduler.AddCPUResourceInstances(cpu_instances);
    resource_scheduler.SubtractCPUResourceInstances(cpu_instances);

    ASSERT_EQ((resource_scheduler.GetLocalResources() == old_local_resources), true);
  }
}

TEST_F(ClusterResourceSchedulerTest, DeadNodeTest) {
  ClusterResourceScheduler resource_scheduler("local", {}, *gcs_client_);
  absl::flat_hash_map<std::string, double> resource;
  resource["CPU"] = 10000.0;
  auto node_id = NodeID::FromRandom();
  resource_scheduler.AddOrUpdateNode(node_id.Binary(), resource, resource);
  int64_t violations = 0;
  bool is_infeasible = false;
  rpc::SchedulingStrategy scheduling_strategy;
  scheduling_strategy.mutable_default_scheduling_strategy();
  ASSERT_EQ(node_id.Binary(), resource_scheduler.GetBestSchedulableNode(
                                  resource, scheduling_strategy, false, false, false,
                                  &violations, &is_infeasible));
  EXPECT_CALL(*gcs_client_->mock_node_accessor, Get(node_id, ::testing::_))
      .WillOnce(::testing::Return(nullptr))
      .WillOnce(::testing::Return(nullptr));
  ASSERT_EQ("", resource_scheduler.GetBestSchedulableNode(resource, scheduling_strategy,
                                                          false, false, false,
                                                          &violations, &is_infeasible));
}

TEST_F(ClusterResourceSchedulerTest, TaskGPUResourceInstancesTest) {
  {
    NodeResources node_resources;
    vector<FixedPoint> pred_capacities{1 /* CPU */, 1 /* MEM */, 4 /* GPU */};
    vector<int64_t> cust_ids{1};
    vector<FixedPoint> cust_capacities{8};
    initNodeResources(node_resources, pred_capacities, cust_ids, cust_capacities);
    ClusterResourceScheduler resource_scheduler(0, node_resources, *gcs_client_);

    std::vector<double> allocate_gpu_instances{0.5, 0.5, 0.5, 0.5};
    resource_scheduler.SubtractGPUResourceInstances(allocate_gpu_instances);
    std::vector<double> available_gpu_instances = resource_scheduler.GetLocalResources()
                                                      .GetAvailableResourceInstances()
                                                      .GetGPUInstancesDouble();
    std::vector<double> expected_available_gpu_instances{0.5, 0.5, 0.5, 0.5};
    ASSERT_TRUE(std::equal(available_gpu_instances.begin(), available_gpu_instances.end(),
                           expected_available_gpu_instances.begin()));

    resource_scheduler.AddGPUResourceInstances(allocate_gpu_instances);
    available_gpu_instances = resource_scheduler.GetLocalResources()
                                  .GetAvailableResourceInstances()
                                  .GetGPUInstancesDouble();
    expected_available_gpu_instances = {1., 1., 1., 1.};
    ASSERT_TRUE(std::equal(available_gpu_instances.begin(), available_gpu_instances.end(),
                           expected_available_gpu_instances.begin()));

    allocate_gpu_instances = {1.5, 1.5, .5, 1.5};
    std::vector<double> underflow =
        resource_scheduler.SubtractGPUResourceInstances(allocate_gpu_instances);
    std::vector<double> expected_underflow{.5, .5, 0., .5};
    ASSERT_TRUE(
        std::equal(underflow.begin(), underflow.end(), expected_underflow.begin()));
    available_gpu_instances = resource_scheduler.GetLocalResources()
                                  .GetAvailableResourceInstances()
                                  .GetGPUInstancesDouble();
    expected_available_gpu_instances = {0., 0., 0.5, 0.};
    ASSERT_TRUE(std::equal(available_gpu_instances.begin(), available_gpu_instances.end(),
                           expected_available_gpu_instances.begin()));

    allocate_gpu_instances = {1.0, .5, 1., .5};
    std::vector<double> overflow =
        resource_scheduler.AddGPUResourceInstances(allocate_gpu_instances);
    std::vector<double> expected_overflow{.0, .0, .5, 0.};
    ASSERT_TRUE(std::equal(overflow.begin(), overflow.end(), expected_overflow.begin()));
    available_gpu_instances = resource_scheduler.GetLocalResources()
                                  .GetAvailableResourceInstances()
                                  .GetGPUInstancesDouble();
    expected_available_gpu_instances = {1., .5, 1., .5};
    ASSERT_TRUE(std::equal(available_gpu_instances.begin(), available_gpu_instances.end(),
                           expected_available_gpu_instances.begin()));
  }
}

TEST_F(ClusterResourceSchedulerTest,
       UpdateLocalAvailableResourcesFromResourceInstancesTest) {
  {
    NodeResources node_resources;
    vector<FixedPoint> pred_capacities{1 /* CPU */, 1 /* MEM */, 4 /* GPU */};
    vector<int64_t> cust_ids{1};
    vector<FixedPoint> cust_capacities{8};
    initNodeResources(node_resources, pred_capacities, cust_ids, cust_capacities);
    ClusterResourceScheduler resource_scheduler(0, node_resources, *gcs_client_);

    {
      std::vector<double> allocate_gpu_instances{0.5, 0.5, 2, 0.5};
      // SubtractGPUResourceInstances() calls
      // UpdateLocalAvailableResourcesFromResourceInstances() under the hood.
      resource_scheduler.SubtractGPUResourceInstances(allocate_gpu_instances);
      std::vector<double> available_gpu_instances = resource_scheduler.GetLocalResources()
                                                        .GetAvailableResourceInstances()
                                                        .GetGPUInstancesDouble();
      std::vector<double> expected_available_gpu_instances{0.5, 0.5, 0., 0.5};
      ASSERT_TRUE(std::equal(available_gpu_instances.begin(),
                             available_gpu_instances.end(),
                             expected_available_gpu_instances.begin()));

      NodeResources nr;
      resource_scheduler.GetNodeResources(0, &nr);
      ASSERT_TRUE(nr.predefined_resources[GPU].available == 1.5);
    }

    {
      std::vector<double> allocate_gpu_instances{1.5, 0.5, 2, 0.3};
      // SubtractGPUResourceInstances() calls
      // UpdateLocalAvailableResourcesFromResourceInstances() under the hood.
      resource_scheduler.AddGPUResourceInstances(allocate_gpu_instances);
      std::vector<double> available_gpu_instances = resource_scheduler.GetLocalResources()
                                                        .GetAvailableResourceInstances()
                                                        .GetGPUInstancesDouble();
      std::vector<double> expected_available_gpu_instances{1., 1., 1., 0.8};
      ASSERT_TRUE(std::equal(available_gpu_instances.begin(),
                             available_gpu_instances.end(),
                             expected_available_gpu_instances.begin()));

      NodeResources nr;
      resource_scheduler.GetNodeResources(0, &nr);
      ASSERT_TRUE(nr.predefined_resources[GPU].available == 3.8);
    }
  }
}

TEST_F(ClusterResourceSchedulerTest, TaskResourceInstanceWithHardRequestTest) {
  NodeResources node_resources;
  vector<FixedPoint> pred_capacities{4. /* CPU */, 2. /* MEM */, 4. /* GPU */};
  initNodeResources(node_resources, pred_capacities, EmptyIntVector,
                    EmptyFixedPointVector);
  ClusterResourceScheduler resource_scheduler(0, node_resources, *gcs_client_);

  ResourceRequest resource_request;
  vector<FixedPoint> pred_demands = {2. /* CPU */, 2. /* MEM */, 1.5 /* GPU */};
  initResourceRequest(resource_request, pred_demands, EmptyIntVector,
                      EmptyFixedPointVector);

  std::shared_ptr<TaskResourceInstances> task_allocation =
      std::make_shared<TaskResourceInstances>();
  bool success =
      resource_scheduler.AllocateTaskResourceInstances(resource_request, task_allocation);

  ASSERT_EQ(success, true);

  vector<FixedPoint> gpu_instances = task_allocation->GetGPUInstances();
  vector<FixedPoint> expect_gpu_instance{1., 0.5, 0., 0.};

  ASSERT_TRUE(EqualVectors(gpu_instances, expect_gpu_instance));
}

TEST_F(ClusterResourceSchedulerTest, TaskResourceInstanceWithoutCpuUnitTest) {
  NodeResources node_resources;
  vector<FixedPoint> pred_capacities{4. /* CPU */, 2. /* MEM */, 4. /* GPU */};
  initNodeResources(node_resources, pred_capacities, EmptyIntVector,
                    EmptyFixedPointVector);
  ClusterResourceScheduler resource_scheduler(0, node_resources, *gcs_client_);

  ResourceRequest resource_request;
  vector<FixedPoint> pred_demands = {2. /* CPU */, 2. /* MEM */, 1.5 /* GPU */};
  initResourceRequest(resource_request, pred_demands, EmptyIntVector,
                      EmptyFixedPointVector);

  std::shared_ptr<TaskResourceInstances> task_allocation =
      std::make_shared<TaskResourceInstances>();
  bool success =
      resource_scheduler.AllocateTaskResourceInstances(resource_request, task_allocation);

  ASSERT_EQ(success, true);

  vector<FixedPoint> cpu_instances = task_allocation->GetCPUInstances();
  vector<FixedPoint> expect_cpu_instance{2};

  ASSERT_TRUE(EqualVectors(cpu_instances, expect_cpu_instance));
}

TEST_F(ClusterResourceSchedulerTest, TestAlwaysSpillInfeasibleTask) {
  absl::flat_hash_map<std::string, double> resource_spec({{"CPU", 1}});
  ClusterResourceScheduler resource_scheduler("local", {}, *gcs_client_);
  for (int i = 0; i < 100; i++) {
    resource_scheduler.AddOrUpdateNode(NodeID::FromRandom().Binary(), {}, {});
  }

  // No feasible nodes.
  int64_t total_violations;
  bool is_infeasible;
  rpc::SchedulingStrategy scheduling_strategy;
  scheduling_strategy.mutable_default_scheduling_strategy();
  ASSERT_EQ(resource_scheduler.GetBestSchedulableNode(resource_spec, scheduling_strategy,
                                                      false, false, false,
                                                      &total_violations, &is_infeasible),
            "");

  // Feasible remote node, but doesn't currently have resources available. We
  // should spill there.
  auto remote_feasible = NodeID::FromRandom().Binary();
  resource_scheduler.AddOrUpdateNode(remote_feasible, resource_spec, {{"CPU", 0.}});
  ASSERT_EQ(remote_feasible, resource_scheduler.GetBestSchedulableNode(
                                 resource_spec, scheduling_strategy, false, false, false,
                                 &total_violations, &is_infeasible));

  // Feasible remote node, and it currently has resources available. We should
  // prefer to spill there.
  auto remote_available = NodeID::FromRandom().Binary();
  resource_scheduler.AddOrUpdateNode(remote_available, resource_spec, resource_spec);
  ASSERT_EQ(remote_available, resource_scheduler.GetBestSchedulableNode(
                                  resource_spec, scheduling_strategy, false, false, false,
                                  &total_violations, &is_infeasible));
}

TEST_F(ClusterResourceSchedulerTest, ResourceUsageReportTest) {
  vector<int64_t> cust_ids{1, 2, 3, 4, 5};

  NodeResources node_resources;

  absl::flat_hash_map<std::string, double> initial_resources(
      {{"CPU", 1}, {"GPU", 2}, {"memory", 3}, {"1", 1}, {"2", 2}, {"3", 3}});
  ClusterResourceScheduler resource_scheduler("0", initial_resources, *gcs_client_);
  NodeResources other_node_resources;
  vector<FixedPoint> other_pred_capacities{1. /* CPU */, 1. /* MEM */, 1. /* GPU */};
  vector<FixedPoint> other_cust_capacities{5., 4., 3., 2., 1.};
  initNodeResources(other_node_resources, other_pred_capacities, cust_ids,
                    other_cust_capacities);
  resource_scheduler.AddOrUpdateNode(12345, other_node_resources);

  {  // Cluster is idle.
    rpc::ResourcesData data;
    resource_scheduler.FillResourceUsage(data);

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
    allocations->predefined_resources = {
        {0.1},  // CPU
    };
    allocations->custom_resources = {
        {1, {0.1}},  // "1"
    };
    absl::flat_hash_map<std::string, double> allocation_map({
        {"CPU", 0.1},
        {"1", 0.1},
    });
    resource_scheduler.AllocateLocalTaskResources(allocation_map, allocations);
    rpc::ResourcesData data;
    resource_scheduler.UpdateLastResourceUsage(std::make_shared<SchedulingResources>());
    resource_scheduler.FillResourceUsage(data);

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
  ClusterResourceScheduler resource_scheduler("0", initial_resources, *gcs_client_,
                                              [&] { return *ptr; });
  NodeResources other_node_resources;
  vector<FixedPoint> other_pred_capacities{1. /* CPU */, 1. /* MEM */, 1. /* GPU */};
  vector<FixedPoint> other_cust_capacities{10.};
  initNodeResources(other_node_resources, other_pred_capacities, cust_ids,
                    other_cust_capacities);
  resource_scheduler.AddOrUpdateNode(12345, other_node_resources);

  {
    rpc::ResourcesData data;
    resource_scheduler.FillResourceUsage(data);
    auto available = data.resources_available();
    auto total = data.resources_total();
    ASSERT_EQ(available["object_store_memory"], 750 * 1024 * 1024);
    ASSERT_EQ(total["object_store_memory"], 1000 * 1024 * 1024);
  }

  used_object_store_memory = 450 * 1024 * 1024;
  {
    rpc::ResourcesData data;
    resource_scheduler.FillResourceUsage(data);
    auto available = data.resources_available();
    auto total = data.resources_total();
    ASSERT_EQ(available["object_store_memory"], 550 * 1024 * 1024);
  }

  used_object_store_memory = 0;
  {
    rpc::ResourcesData data;
    resource_scheduler.FillResourceUsage(data);
    auto available = data.resources_available();
    auto total = data.resources_total();
    ASSERT_EQ(available["object_store_memory"], 1000 * 1024 * 1024);
  }

  used_object_store_memory = 9999999999;
  {
    rpc::ResourcesData data;
    resource_scheduler.FillResourceUsage(data);
    auto available = data.resources_available();
    auto total = data.resources_total();
    ASSERT_EQ(available["object_store_memory"], 0);
  }
}

TEST_F(ClusterResourceSchedulerTest, DirtyLocalViewTest) {
  absl::flat_hash_map<std::string, double> initial_resources({{"CPU", 1}});
  ClusterResourceScheduler resource_scheduler("local", initial_resources, *gcs_client_);
  auto remote = NodeID::FromRandom().Binary();
  resource_scheduler.AddOrUpdateNode(remote, {{"CPU", 2.}}, {{"CPU", 2.}});
  const absl::flat_hash_map<std::string, double> task_spec = {{"CPU", 1.}};

  // Allocate local resources to force tasks onto the remote node when
  // resources are available.
  std::shared_ptr<TaskResourceInstances> task_allocation =
      std::make_shared<TaskResourceInstances>();
  ASSERT_TRUE(resource_scheduler.AllocateLocalTaskResources(task_spec, task_allocation));
  task_allocation = std::make_shared<TaskResourceInstances>();
  ASSERT_FALSE(resource_scheduler.AllocateLocalTaskResources(task_spec, task_allocation));
  // View of local resources is not affected by resource usage report.
  rpc::ResourcesData data;
  resource_scheduler.FillResourceUsage(data);
  ASSERT_FALSE(resource_scheduler.AllocateLocalTaskResources(task_spec, task_allocation));

  for (int num_slots_available = 0; num_slots_available <= 2; num_slots_available++) {
    rpc::ResourcesData data;
    int64_t t;
    bool is_infeasible;
    rpc::SchedulingStrategy scheduling_strategy;
    scheduling_strategy.mutable_default_scheduling_strategy();
    for (int i = 0; i < 3; i++) {
      // Remote node reports update local view.
      resource_scheduler.AddOrUpdateNode(remote, {{"CPU", 2.}},
                                         {{"CPU", num_slots_available}});
      for (int j = 0; j < num_slots_available; j++) {
        ASSERT_EQ(remote, resource_scheduler.GetBestSchedulableNode(
                              task_spec, scheduling_strategy, false, false, true, &t,
                              &is_infeasible));
        // Allocate remote resources.
        ASSERT_TRUE(resource_scheduler.AllocateRemoteTaskResources(remote, task_spec));
      }
      // Our local view says there are not enough resources on the remote node to
      // schedule another task.
      ASSERT_EQ("", resource_scheduler.GetBestSchedulableNode(
                        task_spec, scheduling_strategy, false, false, true, &t,
                        &is_infeasible));
      ASSERT_FALSE(
          resource_scheduler.AllocateLocalTaskResources(task_spec, task_allocation));
      ASSERT_FALSE(resource_scheduler.AllocateRemoteTaskResources(remote, task_spec));
    }
  }
}

TEST_F(ClusterResourceSchedulerTest, DynamicResourceTest) {
  ClusterResourceScheduler resource_scheduler("local", {{"CPU", 2}}, *gcs_client_);

  absl::flat_hash_map<std::string, double> resource_request = {{"CPU", 1},
                                                               {"custom123", 2}};
  int64_t t;
  bool is_infeasible;
  rpc::SchedulingStrategy scheduling_strategy;
  scheduling_strategy.mutable_default_scheduling_strategy();

  std::string result = resource_scheduler.GetBestSchedulableNode(
      resource_request, scheduling_strategy, false, false, false, &t, &is_infeasible);
  ASSERT_TRUE(result.empty());

  resource_scheduler.AddLocalResourceInstances("custom123", {0., 1.0, 1.0});

  result = resource_scheduler.GetBestSchedulableNode(
      resource_request, scheduling_strategy, false, false, false, &t, &is_infeasible);
  ASSERT_FALSE(result.empty()) << resource_scheduler.DebugString();

  resource_request["custom123"] = 3;
  result = resource_scheduler.GetBestSchedulableNode(
      resource_request, scheduling_strategy, false, false, false, &t, &is_infeasible);
  ASSERT_TRUE(result.empty());

  resource_scheduler.AddLocalResourceInstances("custom123", {1.0});
  result = resource_scheduler.GetBestSchedulableNode(
      resource_request, scheduling_strategy, false, false, false, &t, &is_infeasible);
  ASSERT_FALSE(result.empty());

  resource_scheduler.DeleteLocalResource("custom123");
  result = resource_scheduler.GetBestSchedulableNode(
      resource_request, scheduling_strategy, false, false, false, &t, &is_infeasible);
  ASSERT_TRUE(result.empty());
}

TEST_F(ClusterResourceSchedulerTest, AvailableResourceEmptyTest) {
  ClusterResourceScheduler resource_scheduler("local", {{"custom123", 5}}, *gcs_client_);
  std::shared_ptr<TaskResourceInstances> resource_instances =
      std::make_shared<TaskResourceInstances>();
  absl::flat_hash_map<std::string, double> resource_request = {{"custom123", 5}};
  bool allocated =
      resource_scheduler.AllocateLocalTaskResources(resource_request, resource_instances);
  ASSERT_TRUE(allocated);
  ASSERT_TRUE(resource_scheduler.IsAvailableResourceEmpty("custom123"));
}

TEST_F(ClusterResourceSchedulerTest, TestForceSpillback) {
  absl::flat_hash_map<std::string, double> resource_spec({{"CPU", 1}});
  ClusterResourceScheduler resource_scheduler("local", resource_spec, *gcs_client_);
  std::vector<string> node_ids;
  for (int i = 0; i < 100; i++) {
    node_ids.push_back(NodeID::FromRandom().Binary());
    resource_scheduler.AddOrUpdateNode(node_ids.back(), {}, {});
  }

  // No feasible nodes.
  int64_t total_violations;
  bool is_infeasible;
  rpc::SchedulingStrategy scheduling_strategy;
  scheduling_strategy.mutable_default_scheduling_strategy();
  // Normally we prefer local.
  ASSERT_EQ(resource_scheduler.GetBestSchedulableNode(
                resource_spec, scheduling_strategy, false, false,
                /*force_spillback=*/false, &total_violations, &is_infeasible),
            "local");
  // If spillback is forced, we try to spill to remote, but only if there is a
  // schedulable node.
  ASSERT_EQ(resource_scheduler.GetBestSchedulableNode(
                resource_spec, scheduling_strategy, false, false,
                /*force_spillback=*/true, &total_violations, &is_infeasible),
            "");
  // Choose a remote node that has the resources available.
  resource_scheduler.AddOrUpdateNode(node_ids[50], resource_spec, {});
  ASSERT_EQ(resource_scheduler.GetBestSchedulableNode(
                resource_spec, scheduling_strategy, false, false,
                /*force_spillback=*/true, &total_violations, &is_infeasible),
            "");
  resource_scheduler.AddOrUpdateNode(node_ids[51], resource_spec, resource_spec);
  ASSERT_EQ(resource_scheduler.GetBestSchedulableNode(
                resource_spec, scheduling_strategy, false, false,
                /*force_spillback=*/true, &total_violations, &is_infeasible),
            node_ids[51]);
}

TEST_F(ClusterResourceSchedulerTest, CustomResourceInstanceTest) {
  RayConfig::instance().initialize(
      R"(
{
  "custom_unit_instance_resources": "FPGA"
}
  )");
  ClusterResourceScheduler resource_scheduler("local", {{"CPU", 4}, {"FPGA", 2}},
                                              *gcs_client_);

  StringIdMap mock_string_to_int_map;
  int64_t fpga_resource_id = mock_string_to_int_map.Insert("FPGA");

  ResourceRequest resource_request;
  vector<FixedPoint> pred_demands = {1. /* CPU */};
  vector<FixedPoint> cust_demands{0.7};
  vector<int64_t> cust_ids{fpga_resource_id};
  initResourceRequest(resource_request, pred_demands, cust_ids, cust_demands);

  std::shared_ptr<TaskResourceInstances> task_allocation =
      std::make_shared<TaskResourceInstances>();
  bool success =
      resource_scheduler.AllocateTaskResourceInstances(resource_request, task_allocation);
  ASSERT_TRUE(success) << resource_scheduler.DebugString();

  success =
      resource_scheduler.AllocateTaskResourceInstances(resource_request, task_allocation);
  ASSERT_TRUE(success) << resource_scheduler.DebugString();

  ResourceRequest fail_resource_request;
  vector<FixedPoint> fail_cust_demands{0.5};
  initResourceRequest(fail_resource_request, pred_demands, cust_ids, fail_cust_demands);
  success = resource_scheduler.AllocateTaskResourceInstances(fail_resource_request,
                                                             task_allocation);
  ASSERT_FALSE(success) << resource_scheduler.DebugString();
}

TEST_F(ClusterResourceSchedulerTest, TaskResourceInstancesSerializedStringTest) {
  ClusterResourceScheduler resource_scheduler(
      "local", {{"CPU", 4}, {"memory", 4}, {"GPU", 2}}, *gcs_client_);
  std::shared_ptr<TaskResourceInstances> cluster_resources =
      std::make_shared<TaskResourceInstances>();
  addTaskResourceInstances(true, {2.}, 0, cluster_resources.get());
  addTaskResourceInstances(true, {4.}, 1, cluster_resources.get());
  addTaskResourceInstances(true, {1., 1.}, 2, cluster_resources.get());
  std::string serialized_string =
      resource_scheduler.SerializedTaskResourceInstances(cluster_resources);
  std::string expected_serialized_string =
      R"({"CPU":20000,"memory":40000,"GPU":[10000, 10000]})";
  ASSERT_EQ(serialized_string == expected_serialized_string, true);

  RayConfig::instance().initialize(
      R"(
{
  "predefined_unit_instance_resources": "CPU,GPU"
}
  )");
  std::shared_ptr<TaskResourceInstances> cluster_instance_resources =
      std::make_shared<TaskResourceInstances>();
  addTaskResourceInstances(true, {1., 1.}, 0, cluster_instance_resources.get());
  addTaskResourceInstances(true, {4.}, 1, cluster_instance_resources.get());
  addTaskResourceInstances(true, {1., 1.}, 2, cluster_instance_resources.get());
  ClusterResourceScheduler resource_scheduler_cpu_instance(
      "local", {{"CPU", 4}, {"memory", 4}, {"GPU", 2}}, *gcs_client_);
  std::string instance_serialized_string =
      resource_scheduler_cpu_instance.SerializedTaskResourceInstances(
          cluster_instance_resources);
  std::string expected_instance_serialized_string =
      R"({"CPU":[10000, 10000],"memory":40000,"GPU":[10000, 10000]})";
  ASSERT_EQ(instance_serialized_string == expected_instance_serialized_string, true);

  // reset global config
  RayConfig::instance().initialize(
      R"(
{
  "predefined_unit_instance_resources": "GPU"
}
  )");
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
