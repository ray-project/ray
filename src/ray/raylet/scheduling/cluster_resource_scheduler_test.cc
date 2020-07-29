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

#include "ray/raylet/scheduling/cluster_resource_scheduler.h"

#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/raylet/scheduling/scheduling_ids.h"

#ifdef UNORDERED_VS_ABSL_MAPS_EVALUATION
#include <chrono>

#include "absl/container/flat_hash_map.h"
#endif  // UNORDERED_VS_ABSL_MAPS_EVALUATION

using namespace std;

// Used to path empty vector argiuments.
vector<int64_t> EmptyIntVector;
vector<bool> EmptyBoolVector;
vector<FixedPoint> EmptyFixedPointVector;

void initTaskRequest(TaskRequest &tr, vector<FixedPoint> &pred_demands,
                     vector<bool> &pred_soft, vector<int64_t> &cust_ids,
                     vector<FixedPoint> &cust_demands, vector<bool> &cust_soft,
                     vector<int64_t> &placement_hints) {
  for (size_t i = 0; i < pred_demands.size(); i++) {
    ResourceRequest rq;
    rq.demand = pred_demands[i];
    rq.soft = pred_soft[i];
    tr.predefined_resources.push_back(rq);
  }

  for (size_t i = pred_demands.size(); i < PredefinedResources_MAX; i++) {
    ResourceRequest rq;
    rq.demand = 0;
    rq.soft = 0;
    tr.predefined_resources.push_back(rq);
  }

  for (size_t i = 0; i < cust_ids.size(); i++) {
    ResourceRequestWithId rq;
    rq.id = cust_ids[i];
    rq.demand = cust_demands[i];
    rq.soft = cust_soft[i];
    tr.custom_resources.push_back(rq);
  }

  for (size_t i = 0; i < placement_hints.size(); i++) {
    tr.placement_hints.insert(placement_hints[i]);
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

void initCluster(ClusterResourceScheduler &cluster_resources, int n) {
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

    cluster_resources.AddOrUpdateNode(i, node_resources);

    node_resources.custom_resources.clear();
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

namespace ray {

class ClusterResourceSchedulerTest : public ::testing::Test {
 public:
  void SetUp() {}

  void Shutdown() {}
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
  size_t num = 10;  // should be greater than 10.

  for (size_t i = 0; i < num; i++) {
    ids.Insert(to_string(i));
  }
  ASSERT_EQ(ids.Count(), num);

  ids.Remove(to_string(1));
  ASSERT_EQ(ids.Count(), num - 1);

  ids.Remove(hasher(to_string(2)));
  ASSERT_EQ(ids.Count(), num - 2);

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
  ClusterResourceScheduler cluster_resources;

  initCluster(cluster_resources, num_nodes);

  ASSERT_EQ(cluster_resources.NumNodes(), num_nodes);
}

TEST_F(ClusterResourceSchedulerTest, SchedulingDeleteClusterNodeTest) {
  int num_nodes = 4;
  int64_t remove_id = 2;

  ClusterResourceScheduler cluster_resources;

  initCluster(cluster_resources, num_nodes);
  cluster_resources.RemoveNode(remove_id);

  ASSERT_TRUE(num_nodes - 1 == cluster_resources.NumNodes());
}

TEST_F(ClusterResourceSchedulerTest, SchedulingModifyClusterNodeTest) {
  int num_nodes = 4;
  int64_t update_id = 2;
  ClusterResourceScheduler cluster_resources;

  initCluster(cluster_resources, num_nodes);

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
    cluster_resources.AddOrUpdateNode(update_id, node_resources);
  }
  ASSERT_TRUE(num_nodes == cluster_resources.NumNodes());
}

TEST_F(ClusterResourceSchedulerTest, SchedulingUpdateAvailableResourcesTest) {
  // Create cluster resources.
  NodeResources node_resources;
  vector<FixedPoint> pred_capacities{10, 5, 3};
  vector<int64_t> cust_ids{1, 2};
  vector<FixedPoint> cust_capacities{5, 5};
  initNodeResources(node_resources, pred_capacities, cust_ids, cust_capacities);
  ClusterResourceScheduler cluster_resources(1, node_resources);

  {
    TaskRequest task_req;
#define PRED_CUSTOM_LEN 2
    vector<FixedPoint> pred_demands{7, 7};
    vector<bool> pred_soft{false, true};
    vector<int64_t> cust_ids{1, 2};
    vector<FixedPoint> cust_demands{3, 10};
    vector<bool> cust_soft{false, true};
    initTaskRequest(task_req, pred_demands, pred_soft, cust_ids, cust_demands, cust_soft,
                    EmptyIntVector);
    int64_t violations;
    int64_t node_id = cluster_resources.GetBestSchedulableNode(task_req, &violations);
    ASSERT_TRUE(node_id != -1);
    ASSERT_TRUE(violations > 0);

    NodeResources nr1, nr2;
    ASSERT_TRUE(cluster_resources.GetNodeResources(node_id, &nr1));
    cluster_resources.SubtractNodeAvailableResources(node_id, task_req);
    ASSERT_TRUE(cluster_resources.GetNodeResources(node_id, &nr2));

    for (size_t i = 0; i < PRED_CUSTOM_LEN; i++) {
      auto t =
          nr1.predefined_resources[i].available - task_req.predefined_resources[i].demand;
      if (t < 0) t = 0;
      ASSERT_EQ(nr2.predefined_resources[i].available, t);
    }

    for (size_t i = 0; i < PRED_CUSTOM_LEN; i++) {
      auto it1 = nr1.custom_resources.find(task_req.custom_resources[i].id);
      if (it1 != nr1.custom_resources.end()) {
        auto it2 = nr2.custom_resources.find(task_req.custom_resources[i].id);
        if (it2 != nr2.custom_resources.end()) {
          auto t = it1->second.available - task_req.custom_resources[i].demand;
          if (t < 0) t = 0;
          ASSERT_EQ(it2->second.available, t);
        }
      }
    }
  }
}

TEST_F(ClusterResourceSchedulerTest, SchedulingAddOrUpdateNodeTest) {
  ClusterResourceScheduler cluster_resources;
  NodeResources nr, nr_out;
  int64_t node_id = 1;

  // Add node.
  {
    NodeResources node_resources;
    vector<FixedPoint> pred_capacities{10, 5, 3};
    vector<int64_t> cust_ids{1, 2};
    vector<FixedPoint> cust_capacities{5, 5};
    initNodeResources(node_resources, pred_capacities, cust_ids, cust_capacities);
    cluster_resources.AddOrUpdateNode(node_id, node_resources);
    nr = node_resources;
  }

  // Check whether node resources were correctly added.
  if (cluster_resources.GetNodeResources(node_id, &nr_out)) {
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
    cluster_resources.AddOrUpdateNode(node_id, node_resources);
    nr = node_resources;
  }
  if (cluster_resources.GetNodeResources(node_id, &nr_out)) {
    ASSERT_TRUE(nodeResourcesEqual(nr, nr_out));
  } else {
    ASSERT_TRUE(false);
  }
}

TEST_F(ClusterResourceSchedulerTest, SchedulingTaskRequestTest) {
  // Create cluster resources containing local node.
  NodeResources node_resources;
  vector<FixedPoint> pred_capacities{5, 5};
  vector<int64_t> cust_ids{1};
  vector<FixedPoint> cust_capacities{10};
  initNodeResources(node_resources, pred_capacities, cust_ids, cust_capacities);
  ClusterResourceScheduler cluster_resources(0, node_resources);

  std::cerr << "XXXXXXXXXXX" << std::endl;

  {
    NodeResources node_resources;
    vector<FixedPoint> pred_capacities{10, 2, 3};
    vector<int64_t> cust_ids{1, 2};
    vector<FixedPoint> cust_capacities{5, 5};
    initNodeResources(node_resources, pred_capacities, cust_ids, cust_capacities);
    cluster_resources.AddOrUpdateNode(1, node_resources);
  }
  // Predefined resources, hard constraint violation
  {
    TaskRequest task_req;
    vector<FixedPoint> pred_demands = {11};
    vector<bool> pred_soft = {false};
    initTaskRequest(task_req, pred_demands, pred_soft, EmptyIntVector,
                    EmptyFixedPointVector, EmptyBoolVector, EmptyIntVector);
    int64_t violations;
    int64_t node_id = cluster_resources.GetBestSchedulableNode(task_req, &violations);
    ASSERT_EQ(node_id, -1);
  }
  // Predefined resources, soft constraint violation
  {
    TaskRequest task_req;
    vector<FixedPoint> pred_demands = {11};
    vector<bool> pred_soft = {true};
    initTaskRequest(task_req, pred_demands, pred_soft, EmptyIntVector,
                    EmptyFixedPointVector, EmptyBoolVector, EmptyIntVector);
    int64_t violations;
    int64_t node_id = cluster_resources.GetBestSchedulableNode(task_req, &violations);
    ASSERT_TRUE(node_id != -1);
    ASSERT_TRUE(violations > 0);
  }

  // Predefined resources, no constraint violation.
  {
    TaskRequest task_req;
    vector<FixedPoint> pred_demands = {5};
    vector<bool> pred_soft = {false};
    initTaskRequest(task_req, pred_demands, pred_soft, EmptyIntVector,
                    EmptyFixedPointVector, EmptyBoolVector, EmptyIntVector);
    int64_t violations;
    int64_t node_id = cluster_resources.GetBestSchedulableNode(task_req, &violations);
    ASSERT_TRUE(node_id != -1);
    ASSERT_TRUE(violations == 0);
  }
  // Custom resources, hard constraint violation.
  {
    TaskRequest task_req;
    vector<FixedPoint> pred_demands{5, 2};
    vector<bool> pred_soft{false, true};
    vector<int64_t> cust_ids{1};
    vector<FixedPoint> cust_demands{11};
    vector<bool> cust_soft{false};
    initTaskRequest(task_req, pred_demands, pred_soft, cust_ids, cust_demands, cust_soft,
                    EmptyIntVector);
    int64_t violations;
    int64_t node_id = cluster_resources.GetBestSchedulableNode(task_req, &violations);
    ASSERT_TRUE(node_id == -1);
  }
  // Custom resources, soft constraint violation.
  {
    TaskRequest task_req;
    vector<FixedPoint> pred_demands{5, 2};
    vector<bool> pred_soft{false, true};
    vector<int64_t> cust_ids{1};
    vector<FixedPoint> cust_demands{11};
    vector<bool> cust_soft{true};
    initTaskRequest(task_req, pred_demands, pred_soft, cust_ids, cust_demands, cust_soft,
                    EmptyIntVector);
    int64_t violations;
    int64_t node_id = cluster_resources.GetBestSchedulableNode(task_req, &violations);
    ASSERT_TRUE(node_id != -1);
    ASSERT_TRUE(violations > 0);
  }
  // Custom resources, no constraint violation.
  {
    TaskRequest task_req;
    vector<FixedPoint> pred_demands{5, 2};
    vector<bool> pred_soft{false, true};
    vector<int64_t> cust_ids{1};
    vector<FixedPoint> cust_demands{5};
    vector<bool> cust_soft{false};
    initTaskRequest(task_req, pred_demands, pred_soft, cust_ids, cust_demands, cust_soft,
                    EmptyIntVector);
    int64_t violations;
    int64_t node_id = cluster_resources.GetBestSchedulableNode(task_req, &violations);
    ASSERT_TRUE(node_id != -1);
    ASSERT_TRUE(violations == 0);
  }
  // Custom resource missing, hard constraint violation.
  {
    TaskRequest task_req;
    vector<FixedPoint> pred_demands{5, 2};
    vector<bool> pred_soft{false, true};
    vector<int64_t> cust_ids{100};
    vector<FixedPoint> cust_demands{5};
    vector<bool> cust_soft{false};
    initTaskRequest(task_req, pred_demands, pred_soft, cust_ids, cust_demands, cust_soft,
                    EmptyIntVector);
    int64_t violations;
    int64_t node_id = cluster_resources.GetBestSchedulableNode(task_req, &violations);
    ASSERT_TRUE(node_id == -1);
  }
  // Custom resource missing, soft constraint violation.
  {
    TaskRequest task_req;
    vector<FixedPoint> pred_demands{5, 2};
    vector<bool> pred_soft{false, true};
    vector<int64_t> cust_ids{100};
    vector<FixedPoint> cust_demands{5};
    vector<bool> cust_soft{true};
    initTaskRequest(task_req, pred_demands, pred_soft, cust_ids, cust_demands, cust_soft,
                    EmptyIntVector);
    int64_t violations;
    int64_t node_id = cluster_resources.GetBestSchedulableNode(task_req, &violations);
    ASSERT_TRUE(node_id != -1);
    ASSERT_TRUE(violations > 0);
  }
  // Placement_hints, soft constraint violation.
  {
    TaskRequest task_req;
    vector<FixedPoint> pred_demands{5, 2};
    vector<bool> pred_soft{false, true};
    vector<int64_t> cust_ids{1};
    vector<FixedPoint> cust_demands{5};
    vector<bool> cust_soft{true};
    vector<int64_t> placement_hints{2, 3};
    initTaskRequest(task_req, pred_demands, pred_soft, cust_ids, cust_demands, cust_soft,
                    placement_hints);
    int64_t violations;
    int64_t node_id = cluster_resources.GetBestSchedulableNode(task_req, &violations);
    ASSERT_TRUE(node_id != -1);
    ASSERT_TRUE(violations > 0);
  }
  // Placement hints, no constraint violation.
  {
    TaskRequest task_req;
    vector<FixedPoint> pred_demands{5, 2};
    vector<bool> pred_soft{false, true};
    vector<int64_t> cust_ids{1};
    vector<FixedPoint> cust_demands{5};
    vector<bool> cust_soft{true};
    vector<int64_t> placement_hints{1, 2, 3};
    initTaskRequest(task_req, pred_demands, pred_soft, cust_ids, cust_demands, cust_soft,
                    placement_hints);
    int64_t violations;
    int64_t node_id = cluster_resources.GetBestSchedulableNode(task_req, &violations);
    ASSERT_TRUE(node_id != -1);
    ASSERT_TRUE(violations == 0);
  }
}

TEST_F(ClusterResourceSchedulerTest, GetLocalAvailableResourcesTest) {
  // Create cluster resources containing local node.
  NodeResources node_resources;
  vector<FixedPoint> pred_capacities{3 /* CPU */, 4 /* MEM */, 5 /* GPU */};
  vector<int64_t> cust_ids{1};
  vector<FixedPoint> cust_capacities{8};
  initNodeResources(node_resources, pred_capacities, cust_ids, cust_capacities);
  ClusterResourceScheduler cluster_resources(0, node_resources);

  TaskResourceInstances available_cluster_resources =
      cluster_resources.GetLocalResources().GetAvailableResourceInstances();

  TaskResourceInstances expected_cluster_resources;
  addTaskResourceInstances(true, {1., 1., 1.}, 0, &expected_cluster_resources);
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
  ClusterResourceScheduler cluster(0, node_resources);

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
    ClusterResourceScheduler cluster_resources(0, node_resources);

    TaskRequest task_req;
    vector<FixedPoint> pred_demands = {3. /* CPU */, 2. /* MEM */, 1.5 /* GPU */};
    vector<bool> pred_soft = {false, false, false};
    initTaskRequest(task_req, pred_demands, pred_soft, EmptyIntVector,
                    EmptyFixedPointVector, EmptyBoolVector, EmptyIntVector);

    NodeResourceInstances old_local_resources = cluster_resources.GetLocalResources();

    std::shared_ptr<TaskResourceInstances> task_allocation =
        std::make_shared<TaskResourceInstances>();
    bool success =
        cluster_resources.AllocateTaskResourceInstances(task_req, task_allocation);

    ASSERT_EQ(success, true);

    cluster_resources.FreeTaskResourceInstances(task_allocation);

    ASSERT_EQ((cluster_resources.GetLocalResources() == old_local_resources), true);
  }
  // Try to allocate resources for a task request that overallocates a hard constrained
  // resource.
  {
    NodeResources node_resources;
    vector<FixedPoint> pred_capacities{3 /* CPU */, 4 /* MEM */, 5 /* GPU */};
    initNodeResources(node_resources, pred_capacities, EmptyIntVector,
                      EmptyFixedPointVector);
    ClusterResourceScheduler cluster_resources(0, node_resources);

    TaskRequest task_req;
    vector<FixedPoint> pred_demands = {4. /* CPU */, 2. /* MEM */, 1.5 /* GPU */};
    vector<bool> pred_soft = {false};  // Hard constrained resource.
    initTaskRequest(task_req, pred_demands, pred_soft, EmptyIntVector,
                    EmptyFixedPointVector, EmptyBoolVector, EmptyIntVector);

    NodeResourceInstances old_local_resources = cluster_resources.GetLocalResources();
    std::shared_ptr<TaskResourceInstances> task_allocation =
        std::make_shared<TaskResourceInstances>();
    bool success =
        cluster_resources.AllocateTaskResourceInstances(task_req, task_allocation);

    ASSERT_EQ(success, false);
    ASSERT_EQ((cluster_resources.GetLocalResources() == old_local_resources), true);
  }
  // Allocate resources for a task request that overallocates a soft constrained resource.
  {
    NodeResources node_resources;
    vector<FixedPoint> pred_capacities{3 /* CPU */, 4 /* MEM */, 5 /* GPU */};
    initNodeResources(node_resources, pred_capacities, EmptyIntVector,
                      EmptyFixedPointVector);
    ClusterResourceScheduler cluster_resources(0, node_resources);

    TaskRequest task_req;
    vector<FixedPoint> pred_demands = {4. /* CPU */, 2. /* MEM */, 1.5 /* GPU */};
    vector<bool> pred_soft = {true};  // Soft constrained resource.
    initTaskRequest(task_req, pred_demands, pred_soft, EmptyIntVector,
                    EmptyFixedPointVector, EmptyBoolVector, EmptyIntVector);

    NodeResourceInstances old_local_resources = cluster_resources.GetLocalResources();
    std::shared_ptr<TaskResourceInstances> task_allocation =
        std::make_shared<TaskResourceInstances>();
    bool success =
        cluster_resources.AllocateTaskResourceInstances(task_req, task_allocation);

    ASSERT_EQ(success, true);

    TaskResourceInstances expected_task_allocation;
    addTaskResourceInstances(true, {0., 0., 0.}, CPU, &expected_task_allocation);
    addTaskResourceInstances(true, {2.}, MEM, &expected_task_allocation);
    addTaskResourceInstances(true, {0., 0.5, 1., 1., 1.}, GPU, &expected_task_allocation);

    TaskResourceInstances local_available_resources =
        cluster_resources.GetLocalResources().GetAvailableResourceInstances();

    ASSERT_EQ((local_available_resources == expected_task_allocation), true);
  }
  // Allocate resources for a task request specifying both predefined and custom
  // resources.
  {
    NodeResources node_resources;
    vector<FixedPoint> pred_capacities{3 /* CPU */, 4 /* MEM */, 5 /* GPU */};
    vector<int64_t> cust_ids{1, 2};
    vector<FixedPoint> cust_capacities{4, 4};
    initNodeResources(node_resources, pred_capacities, cust_ids, cust_capacities);
    ClusterResourceScheduler cluster_resources(0, node_resources);

    TaskRequest task_req;
    vector<FixedPoint> pred_demands = {3. /* CPU */, 2. /* MEM */, 1.5 /* GPU */};
    vector<bool> pred_soft = {false, false, false};
    vector<FixedPoint> cust_demands{3, 2};
    vector<bool> cust_soft{false, false};
    initTaskRequest(task_req, pred_demands, pred_soft, cust_ids, cust_demands, cust_soft,
                    EmptyIntVector);

    NodeResourceInstances old_local_resources = cluster_resources.GetLocalResources();
    std::shared_ptr<TaskResourceInstances> task_allocation =
        std::make_shared<TaskResourceInstances>();
    bool success =
        cluster_resources.AllocateTaskResourceInstances(task_req, task_allocation);

    ASSERT_EQ(success, true);

    cluster_resources.FreeTaskResourceInstances(task_allocation);

    ASSERT_EQ((cluster_resources.GetLocalResources() == old_local_resources), true);
  }
  // Allocate resources for a task request specifying both predefined and custom
  // resources, but overallocates a hard-constrained custom resource.
  {
    NodeResources node_resources;
    vector<FixedPoint> pred_capacities{3 /* CPU */, 4 /* MEM */, 5 /* GPU */};
    vector<int64_t> cust_ids{1, 2};
    vector<FixedPoint> cust_capacities{4, 4};
    initNodeResources(node_resources, pred_capacities, cust_ids, cust_capacities);
    ClusterResourceScheduler cluster_resources(0, node_resources);

    TaskRequest task_req;
    vector<FixedPoint> pred_demands = {3. /* CPU */, 2. /* MEM */, 1.5 /* GPU */};
    vector<bool> pred_soft = {false, false, false};
    vector<FixedPoint> cust_demands{3, 10};
    vector<bool> cust_soft{false, false};
    initTaskRequest(task_req, pred_demands, pred_soft, cust_ids, cust_demands, cust_soft,
                    EmptyIntVector);

    NodeResourceInstances old_local_resources = cluster_resources.GetLocalResources();
    std::shared_ptr<TaskResourceInstances> task_allocation =
        std::make_shared<TaskResourceInstances>();
    bool success =
        cluster_resources.AllocateTaskResourceInstances(task_req, task_allocation);

    ASSERT_EQ(success, false);
    ASSERT_EQ((cluster_resources.GetLocalResources() == old_local_resources), true);
  }
  // Allocate resources for a task request specifying both predefined and custom
  // resources, but overallocates a soft-constrained custom resource.
  {
    NodeResources node_resources;
    vector<FixedPoint> pred_capacities{3 /* CPU */, 4 /* MEM */, 5 /* GPU */};
    vector<int64_t> cust_ids{1, 2};
    vector<FixedPoint> cust_capacities{4, 4};
    initNodeResources(node_resources, pred_capacities, cust_ids, cust_capacities);
    ClusterResourceScheduler cluster_resources(0, node_resources);

    TaskRequest task_req;
    vector<FixedPoint> pred_demands = {3. /* CPU */, 2. /* MEM */, 1.5 /* GPU */};
    vector<bool> pred_soft = {false, false, false};
    vector<FixedPoint> cust_demands{3, 10};
    vector<bool> cust_soft{false, true};
    initTaskRequest(task_req, pred_demands, pred_soft, cust_ids, cust_demands, cust_soft,
                    EmptyIntVector);

    NodeResourceInstances old_local_resources = cluster_resources.GetLocalResources();
    std::shared_ptr<TaskResourceInstances> task_allocation =
        std::make_shared<TaskResourceInstances>();
    bool success =
        cluster_resources.AllocateTaskResourceInstances(task_req, task_allocation);

    ASSERT_EQ(success, true);

    TaskResourceInstances expected_task_allocation;
    addTaskResourceInstances(true, {0., 0., 0.}, CPU, &expected_task_allocation);
    addTaskResourceInstances(true, {2.}, MEM, &expected_task_allocation);
    addTaskResourceInstances(true, {0., 0.5, 1., 1., 1.}, GPU, &expected_task_allocation);
    addTaskResourceInstances(false, {1.}, 1, &expected_task_allocation);
    addTaskResourceInstances(false, {0.}, 2, &expected_task_allocation);

    TaskResourceInstances local_available_resources =
        cluster_resources.GetLocalResources().GetAvailableResourceInstances();

    ASSERT_EQ((local_available_resources == expected_task_allocation), true);
  }
}

TEST_F(ClusterResourceSchedulerTest, TaskResourceInstancesTest2) {
  {
    NodeResources node_resources;
    vector<FixedPoint> pred_capacities{4. /* CPU */, 4. /* MEM */, 5. /* GPU */};
    vector<int64_t> cust_ids{1, 2};
    vector<FixedPoint> cust_capacities{4., 4.};
    initNodeResources(node_resources, pred_capacities, cust_ids, cust_capacities);
    ClusterResourceScheduler cluster_resources(0, node_resources);

    TaskRequest task_req;
    vector<FixedPoint> pred_demands = {2. /* CPU */, 2. /* MEM */, 1.5 /* GPU */};
    vector<bool> pred_soft = {false};
    vector<FixedPoint> cust_demands{3., 2.};
    vector<bool> cust_soft{false, false};
    initTaskRequest(task_req, pred_demands, pred_soft, cust_ids, cust_demands, cust_soft,
                    EmptyIntVector);

    std::shared_ptr<TaskResourceInstances> task_allocation =
        std::make_shared<TaskResourceInstances>();
    bool success =
        cluster_resources.AllocateTaskResourceInstances(task_req, task_allocation);

    NodeResourceInstances old_local_resources = cluster_resources.GetLocalResources();
    ASSERT_EQ(success, true);
    std::vector<double> cpu_instances = task_allocation->GetCPUInstancesDouble();
    cluster_resources.AddCPUResourceInstances(cpu_instances);
    cluster_resources.SubtractCPUResourceInstances(cpu_instances);

    ASSERT_EQ((cluster_resources.GetLocalResources() == old_local_resources), true);
  }
}

TEST_F(ClusterResourceSchedulerTest, TaskGPUResourceInstancesTest) {
  {
    NodeResources node_resources;
    vector<FixedPoint> pred_capacities{1 /* CPU */, 1 /* MEM */, 4 /* GPU */};
    vector<int64_t> cust_ids{1};
    vector<FixedPoint> cust_capacities{8};
    initNodeResources(node_resources, pred_capacities, cust_ids, cust_capacities);
    ClusterResourceScheduler cluster_resources(0, node_resources);

    std::vector<double> allocate_gpu_instances{0.5, 0.5, 0.5, 0.5};
    cluster_resources.SubtractGPUResourceInstances(allocate_gpu_instances);
    std::vector<double> available_gpu_instances = cluster_resources.GetLocalResources()
                                                      .GetAvailableResourceInstances()
                                                      .GetGPUInstancesDouble();
    std::vector<double> expected_available_gpu_instances{0.5, 0.5, 0.5, 0.5};
    ASSERT_TRUE(std::equal(available_gpu_instances.begin(), available_gpu_instances.end(),
                           expected_available_gpu_instances.begin()));

    cluster_resources.AddGPUResourceInstances(allocate_gpu_instances);
    available_gpu_instances = cluster_resources.GetLocalResources()
                                  .GetAvailableResourceInstances()
                                  .GetGPUInstancesDouble();
    expected_available_gpu_instances = {1., 1., 1., 1.};
    ASSERT_TRUE(std::equal(available_gpu_instances.begin(), available_gpu_instances.end(),
                           expected_available_gpu_instances.begin()));

    allocate_gpu_instances = {1.5, 1.5, .5, 1.5};
    std::vector<double> underflow =
        cluster_resources.SubtractGPUResourceInstances(allocate_gpu_instances);
    std::vector<double> expected_underflow{.5, .5, 0., .5};
    ASSERT_TRUE(
        std::equal(underflow.begin(), underflow.end(), expected_underflow.begin()));
    available_gpu_instances = cluster_resources.GetLocalResources()
                                  .GetAvailableResourceInstances()
                                  .GetGPUInstancesDouble();
    expected_available_gpu_instances = {0., 0., 0.5, 0.};
    ASSERT_TRUE(std::equal(available_gpu_instances.begin(), available_gpu_instances.end(),
                           expected_available_gpu_instances.begin()));

    allocate_gpu_instances = {1.0, .5, 1., .5};
    std::vector<double> overflow =
        cluster_resources.AddGPUResourceInstances(allocate_gpu_instances);
    std::vector<double> expected_overflow{.0, .0, .5, 0.};
    ASSERT_TRUE(std::equal(overflow.begin(), overflow.end(), expected_overflow.begin()));
    available_gpu_instances = cluster_resources.GetLocalResources()
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
    ClusterResourceScheduler cluster_resources(0, node_resources);

    {
      std::vector<double> allocate_gpu_instances{0.5, 0.5, 2, 0.5};
      // SubtractGPUResourceInstances() calls
      // UpdateLocalAvailableResourcesFromResourceInstances() under the hood.
      cluster_resources.SubtractGPUResourceInstances(allocate_gpu_instances);
      std::vector<double> available_gpu_instances = cluster_resources.GetLocalResources()
                                                        .GetAvailableResourceInstances()
                                                        .GetGPUInstancesDouble();
      std::vector<double> expected_available_gpu_instances{0.5, 0.5, 0., 0.5};
      ASSERT_TRUE(std::equal(available_gpu_instances.begin(),
                             available_gpu_instances.end(),
                             expected_available_gpu_instances.begin()));

      NodeResources nr;
      cluster_resources.GetNodeResources(0, &nr);
      ASSERT_TRUE(nr.predefined_resources[GPU].available == 1.5);
    }

    {
      std::vector<double> allocate_gpu_instances{1.5, 0.5, 2, 0.3};
      // SubtractGPUResourceInstances() calls
      // UpdateLocalAvailableResourcesFromResourceInstances() under the hood.
      cluster_resources.AddGPUResourceInstances(allocate_gpu_instances);
      std::vector<double> available_gpu_instances = cluster_resources.GetLocalResources()
                                                        .GetAvailableResourceInstances()
                                                        .GetGPUInstancesDouble();
      std::vector<double> expected_available_gpu_instances{1., 1., 1., 0.8};
      ASSERT_TRUE(std::equal(available_gpu_instances.begin(),
                             available_gpu_instances.end(),
                             expected_available_gpu_instances.begin()));

      NodeResources nr;
      cluster_resources.GetNodeResources(0, &nr);
      ASSERT_TRUE(nr.predefined_resources[GPU].available == 3.8);
    }
  }
}

TEST_F(ClusterResourceSchedulerTest, TaskResourceInstanceWithHardRequestTest) {
  NodeResources node_resources;
  vector<FixedPoint> pred_capacities{4. /* CPU */, 2. /* MEM */, 4. /* GPU */};
  initNodeResources(node_resources, pred_capacities, EmptyIntVector,
                    EmptyFixedPointVector);
  ClusterResourceScheduler cluster_resources(0, node_resources);

  TaskRequest task_req;
  vector<FixedPoint> pred_demands = {2. /* CPU */, 2. /* MEM */, 1.5 /* GPU */};
  vector<bool> pred_soft = {false, false, true};
  initTaskRequest(task_req, pred_demands, pred_soft, EmptyIntVector,
                  EmptyFixedPointVector, EmptyBoolVector, EmptyIntVector);

  std::shared_ptr<TaskResourceInstances> task_allocation =
      std::make_shared<TaskResourceInstances>();
  bool success =
      cluster_resources.AllocateTaskResourceInstances(task_req, task_allocation);

  ASSERT_EQ(success, true);

  vector<FixedPoint> cpu_instances = task_allocation->GetGPUInstances();
  vector<FixedPoint> expect_cpu_instance{1., 0.5, 0., 0.};

  ASSERT_TRUE(EqualVectors(cpu_instances, expect_cpu_instance));
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
