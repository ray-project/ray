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
  TaskRequest req = ResourceMapToTaskRequest(map, {{"CPU", 1}});
  int64_t local_node = 0;
  int64_t remote_node = 1;

  absl::flat_hash_map<int64_t, Node> nodes;
  nodes.emplace(local_node, CreateNodeResources(1, 2, 0, 0, 0, 0));
  nodes.emplace(remote_node, CreateNodeResources(0.75, 2, 0, 0, 0, 0));

  int to_schedule = raylet_scheduling_policy::HybridPolicy(req, local_node, nodes, 0.51);
  ASSERT_EQ(to_schedule, local_node);
}

TEST_F(SchedulingPolicyTest, AvailableTieBreakTest) {
  // In this test, the local node and a remote node are both available. The remote node
  // has a lower critical resource utilization so we schedule on it.
  StringIdMap map;
  TaskRequest req = ResourceMapToTaskRequest(map, {{"CPU", 1}});
  int64_t local_node = 0;
  int64_t remote_node = 1;

  absl::flat_hash_map<int64_t, Node> nodes;
  nodes.emplace(local_node, CreateNodeResources(1, 2, 0, 0, 0, 0));
  nodes.emplace(remote_node, CreateNodeResources(1.5, 2, 0, 0, 0, 0));

  int to_schedule = raylet_scheduling_policy::HybridPolicy(req, local_node, nodes, 0.50);
  ASSERT_EQ(to_schedule, remote_node);
}

TEST_F(SchedulingPolicyTest, AvailableOverFeasibleTest) {
  // In this test, the local node is feasible and has a lower critical resource
  // utilization, but the remote node can run the task immediately, so we pick the remote
  // node.
  StringIdMap map;
  TaskRequest req = ResourceMapToTaskRequest(map, {{"CPU", 1}, {"GPU", 1}});
  int64_t local_node = 0;
  int64_t remote_node = 1;

  absl::flat_hash_map<int64_t, Node> nodes;
  nodes.emplace(local_node, CreateNodeResources(10, 10, 0, 0, 0, 1));
  nodes.emplace(remote_node, CreateNodeResources(1, 10, 0, 0, 1, 1));

  int to_schedule = raylet_scheduling_policy::HybridPolicy(req, local_node, nodes, 0.50);
  ASSERT_EQ(to_schedule, remote_node);
}

TEST_F(SchedulingPolicyTest, InfeasibleTest) {
  // All the nodes are infeasible, so we return -1.
  StringIdMap map;
  TaskRequest req = ResourceMapToTaskRequest(map, {{"CPU", 1}, {"GPU", 1}});
  int64_t local_node = 0;
  int64_t remote_node = 1;

  absl::flat_hash_map<int64_t, Node> nodes;
  nodes.emplace(local_node, CreateNodeResources(10, 10, 0, 0, 0, 0));
  nodes.emplace(remote_node, CreateNodeResources(1, 10, 0, 0, 0, 0));

  int to_schedule = raylet_scheduling_policy::HybridPolicy(req, local_node, nodes, 0.50);
  ASSERT_EQ(to_schedule, -1);
}

TEST_F(SchedulingPolicyTest, TruncationAcrossFeasibleNodesTest) {
  // Same as AvailableTruncationTest except now none of the nodes are available, but the
  // tie break logic should apply to feasible nodes too.
  StringIdMap map;
  TaskRequest req = ResourceMapToTaskRequest(map, {{"CPU", 1}, {"GPU", 1}});
  int64_t local_node = 0;
  int64_t remote_node = 1;

  absl::flat_hash_map<int64_t, Node> nodes;
  nodes.emplace(local_node, CreateNodeResources(1, 2, 0, 0, 0, 1));
  nodes.emplace(remote_node, CreateNodeResources(0.75, 2, 0, 0, 0, 1));

  int to_schedule = raylet_scheduling_policy::HybridPolicy(req, local_node, nodes, 0.51);
  ASSERT_EQ(to_schedule, local_node);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

}  // namespace raylet

}  // namespace ray
