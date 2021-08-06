// Copyright 2021 The Ray Authors.
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

#include "ray/raylet/scheduling/scheduling_policy.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace ray {

namespace raylet {

using ::testing::_;

NodeResources CreateNodeResources(double available_cpu, double total_cpu,
                                  double available_memory, double total_memory,
                                  double available_gpu, double total_gpu) {
  NodeResources resources;
  resources.predefined_resources = {{available_cpu, total_cpu},
                                    {available_memory, total_memory},
                                    {available_gpu, total_gpu}};
  return resources;
}

class SchedulingPolicyTest : public ::testing::Test {};

TEST_F(SchedulingPolicyTest, FeasibleDefinitionTest) {
  StringIdMap map;
  auto task_req1 =
      ResourceMapToResourceRequest(map, {{"CPU", 1}, {"object_store_memory", 1}}, false);
  auto task_req2 = ResourceMapToResourceRequest(map, {{"CPU", 1}}, false);
  {
    // Don't break with a non-resized predefined resources array.
    NodeResources resources;
    resources.predefined_resources = {{0, 2.0}};
    ASSERT_FALSE(resources.IsFeasible(task_req1));
    ASSERT_TRUE(resources.IsFeasible(task_req2));
  }

  {
    // After resizing, make sure it doesn't break under with resources with 0 total.
    NodeResources resources;
    resources.predefined_resources = {{0, 2.0}};
    resources.predefined_resources.resize(PredefinedResources_MAX);
    ASSERT_FALSE(resources.IsFeasible(task_req1));
    ASSERT_TRUE(resources.IsFeasible(task_req2));
  }
}

TEST_F(SchedulingPolicyTest, AvailableDefinitionTest) {
  StringIdMap map;
  auto task_req1 =
      ResourceMapToResourceRequest(map, {{"CPU", 1}, {"object_store_memory", 1}}, false);
  auto task_req2 = ResourceMapToResourceRequest(map, {{"CPU", 1}}, false);
  {
    // Don't break with a non-resized predefined resources array.
    NodeResources resources;
    resources.predefined_resources = {{2, 2.0}};
    ASSERT_FALSE(resources.IsAvailable(task_req1));
    ASSERT_TRUE(resources.IsAvailable(task_req2));
  }

  {
    // After resizing, make sure it doesn't break under with resources with 0 total.
    NodeResources resources;
    resources.predefined_resources = {{2, 2.0}};
    resources.predefined_resources.resize(PredefinedResources_MAX);
    ASSERT_FALSE(resources.IsAvailable(task_req1));
    ASSERT_TRUE(resources.IsAvailable(task_req2));
  }
}

TEST_F(SchedulingPolicyTest, CriticalResourceUtilizationDefinitionTest) {
  {
    // Don't break with a non-resized predefined resources array.
    NodeResources resources;
    resources.predefined_resources = {{1.0, 2.0}};
    ASSERT_EQ(resources.CalculateCriticalResourceUtilization(), 0.5);
  }

  {
    // After resizing, make sure it doesn't break under with resources with 0 total.
    NodeResources resources;
    resources.predefined_resources = {{1.0, 2.0}};
    resources.predefined_resources.resize(PredefinedResources_MAX);
    ASSERT_EQ(resources.CalculateCriticalResourceUtilization(), 0.5);
  }

  {
    // Basic test of max
    NodeResources resources;
    resources.predefined_resources = {/* CPU */ {1.0, 2.0},
                                      /* MEM  */ {0.25, 1},
                                      /* GPU (skipped) */ {1, 2},
                                      /* OBJECT_STORE_MEM*/ {50, 100}};
    resources.predefined_resources.resize(PredefinedResources_MAX);
    ASSERT_EQ(resources.CalculateCriticalResourceUtilization(), 0.75);
  }

  {
    // Skip GPU
    NodeResources resources;
    resources.predefined_resources = {/* CPU */ {1.0, 2.0},
                                      /* MEM  */ {0.25, 1},
                                      /* GPU (skipped) */ {0, 2},
                                      /* OBJECT_STORE_MEM*/ {50, 100}};
    resources.predefined_resources.resize(PredefinedResources_MAX);
    ASSERT_EQ(resources.CalculateCriticalResourceUtilization(), 0.75);
  }
}

TEST_F(SchedulingPolicyTest, AvailableTruncationTest) {
  // In this test, the local node and a remote node are both  available. The remote node
  // has a lower critical resource utilization, but they're both truncated to 0, so we
  // should still pick the local node (due to traversal order).
  StringIdMap map;
  ResourceRequest req = ResourceMapToResourceRequest(map, {{"CPU", 1}}, false);
  int64_t local_node = 0;
  int64_t remote_node = 1;

  absl::flat_hash_map<int64_t, Node> nodes;
  nodes.emplace(local_node, CreateNodeResources(1, 2, 0, 0, 0, 0));
  nodes.emplace(remote_node, CreateNodeResources(0.75, 2, 0, 0, 0, 0));

  int to_schedule =
      raylet_scheduling_policy::HybridPolicy(req, local_node, nodes, 0.51, false, false);
  ASSERT_EQ(to_schedule, local_node);
}

TEST_F(SchedulingPolicyTest, AvailableTieBreakTest) {
  // In this test, the local node and a remote node are both available. The remote node
  // has a lower critical resource utilization so we schedule on it.
  StringIdMap map;
  ResourceRequest req = ResourceMapToResourceRequest(map, {{"CPU", 1}}, false);
  int64_t local_node = 0;
  int64_t remote_node = 1;

  absl::flat_hash_map<int64_t, Node> nodes;
  nodes.emplace(local_node, CreateNodeResources(1, 2, 0, 0, 0, 0));
  nodes.emplace(remote_node, CreateNodeResources(1.5, 2, 0, 0, 0, 0));

  int to_schedule =
      raylet_scheduling_policy::HybridPolicy(req, local_node, nodes, 0.50, false, false);
  ASSERT_EQ(to_schedule, remote_node);
}

TEST_F(SchedulingPolicyTest, AvailableOverFeasibleTest) {
  // In this test, the local node is feasible and has a lower critical resource
  // utilization, but the remote node can run the task immediately, so we pick the remote
  // node.
  StringIdMap map;
  ResourceRequest req =
      ResourceMapToResourceRequest(map, {{"CPU", 1}, {"GPU", 1}}, false);
  int64_t local_node = 0;
  int64_t remote_node = 1;

  absl::flat_hash_map<int64_t, Node> nodes;
  nodes.emplace(local_node, CreateNodeResources(10, 10, 0, 0, 0, 1));
  nodes.emplace(remote_node, CreateNodeResources(1, 10, 0, 0, 1, 1));

  int to_schedule =
      raylet_scheduling_policy::HybridPolicy(req, local_node, nodes, 0.50, false, false);
  ASSERT_EQ(to_schedule, remote_node);
}

TEST_F(SchedulingPolicyTest, InfeasibleTest) {
  // All the nodes are infeasible, so we return -1.
  StringIdMap map;
  ResourceRequest req =
      ResourceMapToResourceRequest(map, {{"CPU", 1}, {"GPU", 1}}, false);
  int64_t local_node = 0;
  int64_t remote_node = 1;

  absl::flat_hash_map<int64_t, Node> nodes;
  nodes.emplace(local_node, CreateNodeResources(10, 10, 0, 0, 0, 0));
  nodes.emplace(remote_node, CreateNodeResources(1, 10, 0, 0, 0, 0));

  int to_schedule =
      raylet_scheduling_policy::HybridPolicy(req, local_node, nodes, 0.50, false, false);
  ASSERT_EQ(to_schedule, -1);
}

TEST_F(SchedulingPolicyTest, BarelyFeasibleTest) {
  // Test the edge case where a task requires all of a node's resources, and the node is
  // fully utilized.
  StringIdMap map;
  ResourceRequest req =
      ResourceMapToResourceRequest(map, {{"CPU", 1}, {"GPU", 1}}, false);
  int64_t local_node = 0;

  absl::flat_hash_map<int64_t, Node> nodes;
  nodes.emplace(local_node, CreateNodeResources(0, 1, 0, 0, 0, 1));

  int to_schedule =
      raylet_scheduling_policy::HybridPolicy(req, local_node, nodes, 0.50, false, false);
  ASSERT_EQ(to_schedule, local_node);
}

TEST_F(SchedulingPolicyTest, TruncationAcrossFeasibleNodesTest) {
  // Same as AvailableTruncationTest except now none of the nodes are available, but the
  // tie break logic should apply to feasible nodes too.
  StringIdMap map;
  ResourceRequest req =
      ResourceMapToResourceRequest(map, {{"CPU", 1}, {"GPU", 1}}, false);
  int64_t local_node = 0;
  int64_t remote_node = 1;

  absl::flat_hash_map<int64_t, Node> nodes;
  nodes.emplace(local_node, CreateNodeResources(1, 2, 0, 0, 0, 1));
  nodes.emplace(remote_node, CreateNodeResources(0.75, 2, 0, 0, 0, 1));

  int to_schedule =
      raylet_scheduling_policy::HybridPolicy(req, local_node, nodes, 0.51, false, false);
  ASSERT_EQ(to_schedule, local_node);
}

TEST_F(SchedulingPolicyTest, ForceSpillbackIfAvailableTest) {
  // The local node is better, but we force spillback, so we'll schedule on a non-local
  // node anyways.
  StringIdMap map;
  ResourceRequest req =
      ResourceMapToResourceRequest(map, {{"CPU", 1}, {"GPU", 1}}, false);
  int64_t local_node = 0;
  int64_t remote_node = 1;

  absl::flat_hash_map<int64_t, Node> nodes;
  nodes.emplace(local_node, CreateNodeResources(2, 2, 0, 0, 1, 1));
  nodes.emplace(remote_node, CreateNodeResources(1, 10, 0, 0, 1, 10));

  int to_schedule =
      raylet_scheduling_policy::HybridPolicy(req, local_node, nodes, 0.51, true, true);
  ASSERT_EQ(to_schedule, remote_node);
}

TEST_F(SchedulingPolicyTest, ForceSpillbackTest) {
  // The local node is available but disqualified.
  StringIdMap map;
  ResourceRequest req =
      ResourceMapToResourceRequest(map, {{"CPU", 1}, {"GPU", 1}}, false);
  int64_t local_node = 0;
  int64_t remote_node = 1;

  absl::flat_hash_map<int64_t, Node> nodes;
  nodes.emplace(local_node, CreateNodeResources(2, 2, 0, 0, 1, 1));
  nodes.emplace(remote_node, CreateNodeResources(0, 2, 0, 0, 0, 1));

  int to_schedule =
      raylet_scheduling_policy::HybridPolicy(req, local_node, nodes, 0.51, true, false);
  ASSERT_EQ(to_schedule, remote_node);
}

TEST_F(SchedulingPolicyTest, ForceSpillbackOnlyFeasibleLocallyTest) {
  // The local node is better, but we force spillback, so we'll schedule on a non-local
  // node anyways.
  StringIdMap map;
  ResourceRequest req =
      ResourceMapToResourceRequest(map, {{"CPU", 1}, {"GPU", 1}}, false);
  int64_t local_node = 0;
  int64_t remote_node = 1;

  absl::flat_hash_map<int64_t, Node> nodes;
  nodes.emplace(local_node, CreateNodeResources(2, 2, 0, 0, 1, 1));
  nodes.emplace(remote_node, CreateNodeResources(0, 2, 0, 0, 0, 0));

  int to_schedule =
      raylet_scheduling_policy::HybridPolicy(req, local_node, nodes, 0.51, true, false);
  ASSERT_EQ(to_schedule, -1);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

}  // namespace raylet

}  // namespace ray
