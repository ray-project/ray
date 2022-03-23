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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/raylet/scheduling/policy/composite_scheduling_policy.h"

namespace ray {

namespace raylet {

using ::testing::_;
using namespace ray::raylet_scheduling_policy;

NodeResources CreateNodeResources(double available_cpu,
                                  double total_cpu,
                                  double available_memory,
                                  double total_memory,
                                  double available_gpu,
                                  double total_gpu) {
  NodeResources resources;
  resources.predefined_resources = {{available_cpu, total_cpu},
                                    {available_memory, total_memory},
                                    {available_gpu, total_gpu}};
  return resources;
}

class SchedulingPolicyTest : public ::testing::Test {
 public:
  scheduling::NodeID local_node = scheduling::NodeID(0);
  scheduling::NodeID remote_node = scheduling::NodeID(1);
  scheduling::NodeID remote_node_2 = scheduling::NodeID(2);
  scheduling::NodeID remote_node_3 = scheduling::NodeID(3);
  absl::flat_hash_map<scheduling::NodeID, Node> nodes;

  SchedulingOptions HybridOptions(
      float spread,
      bool avoid_local_node,
      bool require_node_available,
      bool avoid_gpu_nodes = RayConfig::instance().scheduler_avoid_gpu_nodes()) {
    return SchedulingOptions(SchedulingType::HYBRID,
                             RayConfig::instance().scheduler_spread_threshold(),
                             avoid_local_node,
                             require_node_available,
                             avoid_gpu_nodes);
  }
};

TEST_F(SchedulingPolicyTest, SpreadPolicyTest) {
  ResourceRequest req = ResourceMapToResourceRequest({{"CPU", 1}}, false);

  nodes.emplace(local_node, CreateNodeResources(20, 20, 0, 0, 0, 0));
  // Unavailable node
  nodes.emplace(remote_node, CreateNodeResources(0, 20, 0, 0, 0, 0));
  // Infeasible node
  nodes.emplace(remote_node_2, CreateNodeResources(0, 0, 0, 0, 0, 0));
  nodes.emplace(remote_node_3, CreateNodeResources(20, 20, 0, 0, 0, 0));

  raylet_scheduling_policy::CompositeSchedulingPolicy scheduling_policy(
      local_node, nodes, [](auto) { return true; });

  auto to_schedule =
      scheduling_policy.Schedule(req, SchedulingOptions::Spread(false, false));
  ASSERT_EQ(to_schedule, local_node);

  to_schedule = scheduling_policy.Schedule(req, SchedulingOptions::Spread(false, false));
  ASSERT_EQ(to_schedule, remote_node_3);

  to_schedule = scheduling_policy.Schedule(
      req, SchedulingOptions::Spread(/*force_spillback=*/true, false));
  ASSERT_EQ(to_schedule, remote_node_3);
}

TEST_F(SchedulingPolicyTest, RandomPolicyTest) {
  ResourceRequest req = ResourceMapToResourceRequest({{"CPU", 1}}, false);

  nodes.emplace(local_node, CreateNodeResources(20, 20, 0, 0, 0, 0));
  nodes.emplace(remote_node, CreateNodeResources(20, 20, 0, 0, 0, 0));
  // Unavailable node
  nodes.emplace(remote_node_2, CreateNodeResources(0, 20, 0, 0, 0, 0));
  // Infeasible node
  nodes.emplace(remote_node_3, CreateNodeResources(0, 0, 0, 0, 0, 0));

  raylet_scheduling_policy::CompositeSchedulingPolicy scheduling_policy(
      local_node, nodes, [](auto) { return true; });

  std::map<scheduling::NodeID, size_t> decisions;
  size_t num_node_0_picks = 0;
  size_t num_node_1_picks = 0;
  for (int i = 0; i < 1000; i++) {
    auto to_schedule = scheduling_policy.Schedule(req, SchedulingOptions::Random());
    ASSERT_TRUE(to_schedule.ToInt() >= 0);
    ASSERT_TRUE(to_schedule.ToInt() <= 1);
    if (to_schedule.ToInt() == 0) {
      num_node_0_picks++;
    } else {
      num_node_1_picks++;
    }
  }
  // It's extremely unlikely the only node 0 or node 1 is picked for 1000 runs.
  ASSERT_TRUE(num_node_0_picks > 0);
  ASSERT_TRUE(num_node_1_picks > 0);
}

TEST_F(SchedulingPolicyTest, FeasibleDefinitionTest) {
  auto task_req1 =
      ResourceMapToResourceRequest({{"CPU", 1}, {"object_store_memory", 1}}, false);
  auto task_req2 = ResourceMapToResourceRequest({{"CPU", 1}}, false);
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
  auto task_req1 =
      ResourceMapToResourceRequest({{"CPU", 1}, {"object_store_memory", 1}}, false);
  auto task_req2 = ResourceMapToResourceRequest({{"CPU", 1}}, false);
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
  ResourceRequest req = ResourceMapToResourceRequest({{"CPU", 1}}, false);

  nodes.emplace(local_node, CreateNodeResources(1, 2, 0, 0, 0, 0));
  nodes.emplace(remote_node, CreateNodeResources(0.75, 2, 0, 0, 0, 0));

  auto to_schedule =
      raylet_scheduling_policy::CompositeSchedulingPolicy(local_node, nodes, [](auto) {
        return true;
      }).Schedule(req, HybridOptions(0.51, false, false));
  ASSERT_EQ(to_schedule, local_node);
}

TEST_F(SchedulingPolicyTest, AvailableTieBreakTest) {
  // In this test, the local node and a remote node are both available. The remote node
  // has a lower critical resource utilization so we schedule on it.
  ResourceRequest req = ResourceMapToResourceRequest({{"CPU", 1}}, false);

  nodes.emplace(local_node, CreateNodeResources(1, 2, 0, 0, 0, 0));
  nodes.emplace(remote_node, CreateNodeResources(1.5, 2, 0, 0, 0, 0));

  auto to_schedule =
      raylet_scheduling_policy::CompositeSchedulingPolicy(local_node, nodes, [](auto) {
        return true;
      }).Schedule(req, HybridOptions(0.50, false, false));
  ASSERT_EQ(to_schedule, remote_node);
}

TEST_F(SchedulingPolicyTest, AvailableOverFeasibleTest) {
  // In this test, the local node is feasible and has a lower critical resource
  // utilization, but the remote node can run the task immediately, so we pick the remote
  // node.
  ResourceRequest req = ResourceMapToResourceRequest({{"CPU", 1}, {"GPU", 1}}, false);
  nodes.emplace(local_node, CreateNodeResources(10, 10, 0, 0, 0, 1));
  nodes.emplace(remote_node, CreateNodeResources(1, 10, 0, 0, 1, 1));

  auto to_schedule =
      raylet_scheduling_policy::CompositeSchedulingPolicy(local_node, nodes, [](auto) {
        return true;
      }).Schedule(req, HybridOptions(0.50, false, false));
  ASSERT_EQ(to_schedule, remote_node);
}

TEST_F(SchedulingPolicyTest, InfeasibleTest) {
  // All the nodes are infeasible, so we return -1.
  ResourceRequest req = ResourceMapToResourceRequest({{"CPU", 1}, {"GPU", 1}}, false);
  nodes.emplace(local_node, CreateNodeResources(10, 10, 0, 0, 0, 0));
  nodes.emplace(remote_node, CreateNodeResources(1, 10, 0, 0, 0, 0));

  auto to_schedule =
      raylet_scheduling_policy::CompositeSchedulingPolicy(local_node, nodes, [](auto) {
        return true;
      }).Schedule(req, HybridOptions(0.50, false, false));
  ASSERT_TRUE(to_schedule.IsNil());
}

TEST_F(SchedulingPolicyTest, BarelyFeasibleTest) {
  // Test the edge case where a task requires all of a node's resources, and the node is
  // fully utilized.
  ResourceRequest req = ResourceMapToResourceRequest({{"CPU", 1}, {"GPU", 1}}, false);

  nodes.emplace(local_node, CreateNodeResources(0, 1, 0, 0, 0, 1));

  auto to_schedule =
      raylet_scheduling_policy::CompositeSchedulingPolicy(local_node, nodes, [](auto) {
        return true;
      }).Schedule(req, HybridOptions(0.50, false, false));
  ASSERT_EQ(to_schedule, local_node);
}

TEST_F(SchedulingPolicyTest, TruncationAcrossFeasibleNodesTest) {
  // Same as AvailableTruncationTest except now none of the nodes are available, but the
  // tie break logic should apply to feasible nodes too.
  ResourceRequest req = ResourceMapToResourceRequest({{"CPU", 1}, {"GPU", 1}}, false);
  nodes.emplace(local_node, CreateNodeResources(1, 2, 0, 0, 0, 1));
  nodes.emplace(remote_node, CreateNodeResources(0.75, 2, 0, 0, 0, 1));

  auto to_schedule =
      raylet_scheduling_policy::CompositeSchedulingPolicy(local_node, nodes, [](auto) {
        return true;
      }).Schedule(req, HybridOptions(0.51, false, false));
  ASSERT_EQ(to_schedule, local_node);
}

TEST_F(SchedulingPolicyTest, ForceSpillbackIfAvailableTest) {
  // The local node is better, but we force spillback, so we'll schedule on a non-local
  // node anyways.
  ResourceRequest req = ResourceMapToResourceRequest({{"CPU", 1}, {"GPU", 1}}, false);
  nodes.emplace(local_node, CreateNodeResources(2, 2, 0, 0, 1, 1));
  nodes.emplace(remote_node, CreateNodeResources(1, 10, 0, 0, 1, 10));

  auto to_schedule =
      raylet_scheduling_policy::CompositeSchedulingPolicy(local_node, nodes, [](auto) {
        return true;
      }).Schedule(req, HybridOptions(0.51, true, true));
  ASSERT_EQ(to_schedule, remote_node);
}

TEST_F(SchedulingPolicyTest, AvoidSchedulingCPURequestsOnGPUNodes) {
  nodes.emplace(local_node, CreateNodeResources(10, 10, 0, 0, 1, 1));
  nodes.emplace(remote_node, CreateNodeResources(1, 2, 0, 0, 0, 0));

  {
    // The local node is better, but it has GPUs, the request is
    // non GPU, and the remote node does not have GPUs, thus
    // we should schedule on remote node.
    const ResourceRequest req = ResourceMapToResourceRequest({{"CPU", 1}}, false);
    const auto to_schedule =
        raylet_scheduling_policy::CompositeSchedulingPolicy(
            local_node, nodes, [](auto) { return true; })
            .Schedule(ResourceMapToResourceRequest({{"CPU", 1}}, false),
                      HybridOptions(0.51, false, true, true));
    ASSERT_EQ(to_schedule, remote_node);
  }
  {
    // A GPU request should be scheduled on a GPU node.
    const ResourceRequest req = ResourceMapToResourceRequest({{"GPU", 1}}, false);
    const auto to_schedule =
        raylet_scheduling_policy::CompositeSchedulingPolicy(local_node, nodes, [](auto) {
          return true;
        }).Schedule(req, HybridOptions(0.51, false, true, true));
    ASSERT_EQ(to_schedule, local_node);
  }
  {
    // A CPU request can be be scheduled on a CPU node.
    const ResourceRequest req = ResourceMapToResourceRequest({{"CPU", 1}}, false);
    const auto to_schedule =
        raylet_scheduling_policy::CompositeSchedulingPolicy(local_node, nodes, [](auto) {
          return true;
        }).Schedule(req, HybridOptions(0.51, false, true, true));
    ASSERT_EQ(to_schedule, remote_node);
  }
  {
    // A mixed CPU/GPU request should be scheduled on a GPU node.
    const ResourceRequest req =
        ResourceMapToResourceRequest({{"CPU", 1}, {"GPU", 1}}, false);
    const auto to_schedule =
        raylet_scheduling_policy::CompositeSchedulingPolicy(local_node, nodes, [](auto) {
          return true;
        }).Schedule(req, HybridOptions(0.51, false, true, true));
    ASSERT_EQ(to_schedule, local_node);
  }
}

TEST_F(SchedulingPolicyTest, SchedulenCPURequestsOnGPUNodeAsALastResort) {
  // Schedule on remote node, even though the request is CPU only, because
  // we can not schedule on CPU nodes.
  ResourceRequest req = ResourceMapToResourceRequest({{"CPU", 1}}, false);
  nodes.emplace(local_node, CreateNodeResources(0, 10, 0, 0, 0, 0));
  nodes.emplace(remote_node, CreateNodeResources(1, 1, 0, 0, 1, 1));

  const auto to_schedule =
      raylet_scheduling_policy::CompositeSchedulingPolicy(local_node, nodes, [](auto) {
        return true;
      }).Schedule(req, HybridOptions(0.51, false, true, true));
  ASSERT_EQ(to_schedule, remote_node);
}

TEST_F(SchedulingPolicyTest, ForceSpillbackTest) {
  // The local node is available but disqualified.
  ResourceRequest req = ResourceMapToResourceRequest({{"CPU", 1}, {"GPU", 1}}, false);

  nodes.emplace(local_node, CreateNodeResources(2, 2, 0, 0, 1, 1));
  nodes.emplace(remote_node, CreateNodeResources(0, 2, 0, 0, 0, 1));

  auto to_schedule =
      raylet_scheduling_policy::CompositeSchedulingPolicy(local_node, nodes, [](auto) {
        return true;
      }).Schedule(req, HybridOptions(0.51, true, false));
  ASSERT_EQ(to_schedule, remote_node);
}

TEST_F(SchedulingPolicyTest, ForceSpillbackOnlyFeasibleLocallyTest) {
  // The local node is better, but we force spillback, so we'll schedule on a non-local
  // node anyways.
  ResourceRequest req = ResourceMapToResourceRequest({{"CPU", 1}, {"GPU", 1}}, false);

  nodes.emplace(local_node, CreateNodeResources(2, 2, 0, 0, 1, 1));
  nodes.emplace(remote_node, CreateNodeResources(0, 2, 0, 0, 0, 0));

  auto to_schedule =
      raylet_scheduling_policy::CompositeSchedulingPolicy(local_node, nodes, [](auto) {
        return true;
      }).Schedule(req, HybridOptions(0.51, true, false));
  ASSERT_TRUE(to_schedule.IsNil());
}

TEST_F(SchedulingPolicyTest, NonGpuNodePreferredSchedulingTest) {
  // Prefer to schedule on CPU nodes first.
  // GPU nodes should be preferred as a last resort.

  // local {CPU:2, GPU:1}
  // Remote {CPU: 2}
  nodes.emplace(local_node, CreateNodeResources(2, 2, 0, 0, 1, 1));
  nodes.emplace(remote_node, CreateNodeResources(2, 2, 0, 0, 0, 0));
  nodes.emplace(remote_node_2, CreateNodeResources(3, 3, 0, 0, 0, 0));

  ResourceRequest req = ResourceMapToResourceRequest({{"CPU", 1}}, false);
  auto to_schedule = raylet_scheduling_policy::CompositeSchedulingPolicy(
                         local_node, nodes, [](auto) { return true; })
                         .Schedule(req,
                                   HybridOptions(0.51,
                                                 false,
                                                 true,
                                                 /*gpu_avoid_scheduling*/ true));
  ASSERT_EQ(to_schedule, remote_node);

  req = ResourceMapToResourceRequest({{"CPU", 3}}, false);
  to_schedule = raylet_scheduling_policy::CompositeSchedulingPolicy(
                    local_node, nodes, [](auto) { return true; })
                    .Schedule(req,
                              HybridOptions(0.51,
                                            false,
                                            true,
                                            /*gpu_avoid_scheduling*/ true));
  ASSERT_EQ(to_schedule, remote_node_2);

  req = ResourceMapToResourceRequest({{"CPU", 1}, {"GPU", 1}}, false);
  to_schedule = raylet_scheduling_policy::CompositeSchedulingPolicy(
                    local_node, nodes, [](auto) { return true; })
                    .Schedule(req,
                              HybridOptions(0.51,
                                            false,
                                            true,
                                            /*gpu_avoid_scheduling*/ true));
  ASSERT_EQ(to_schedule, local_node);

  req = ResourceMapToResourceRequest({{"CPU", 2}}, false);
  to_schedule = raylet_scheduling_policy::CompositeSchedulingPolicy(
                    local_node, nodes, [](auto) { return true; })
                    .Schedule(req,
                              HybridOptions(0.51,
                                            false,
                                            true,
                                            /*gpu_avoid_scheduling*/ true));
  ASSERT_EQ(to_schedule, remote_node);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

}  // namespace raylet

}  // namespace ray
