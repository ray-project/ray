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
  resources.available.Set(ResourceID::CPU(), available_cpu)
      .Set(ResourceID::Memory(), available_memory)
      .Set(ResourceID::GPU(), available_gpu);
  resources.total.Set(ResourceID::CPU(), total_cpu)
      .Set(ResourceID::Memory(), total_memory)
      .Set(ResourceID::GPU(), total_gpu);
  return resources;
}

class HybridSchedulingPolicyTest : public ::testing::Test {
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
      bool avoid_gpu_nodes = RayConfig::instance().scheduler_avoid_gpu_nodes(),
      int schedule_top_k_absolute = 1,
      float scheduler_top_k_fraction = 0.1,
      int64_t max_pending_lease_requests_per_scheduling_category = 5) {
    return SchedulingOptions(SchedulingType::HYBRID,
                             RayConfig::instance().scheduler_spread_threshold(),
                             avoid_local_node,
                             require_node_available,
                             avoid_gpu_nodes,
                             /*max_cpu_fraction_per_node*/ 1.0,
                             /*scheduling_context*/ nullptr,
                             schedule_top_k_absolute,
                             scheduler_top_k_fraction,
                             max_pending_lease_requests_per_scheduling_category);
  }

  ClusterResourceManager MockClusterResourceManager(
      const absl::flat_hash_map<scheduling::NodeID, Node> &nodes) {
    ClusterResourceManager cluster_resource_manager;
    cluster_resource_manager.nodes_ = nodes;
    return cluster_resource_manager;
  }
};

TEST_F(HybridSchedulingPolicyTest, AvailableTieBreakTest) {
  // In this test, the local node and a remote node are both available. The remote node
  // has a lower critical resource utilization so we schedule on it.
  ResourceRequest req = ResourceMapToResourceRequest({{"CPU", 1}}, false);

  nodes.emplace(local_node, CreateNodeResources(1, 2, 0, 0, 0, 0));
  nodes.emplace(remote_node, CreateNodeResources(1.5, 2, 0, 0, 0, 0));

  auto cluster_resource_manager = MockClusterResourceManager(nodes);
  auto to_schedule = raylet_scheduling_policy::CompositeSchedulingPolicy(
                         local_node, cluster_resource_manager, [](auto) { return true; })
                         .Schedule(req, HybridOptions(0.50, false, false));
  ASSERT_EQ(to_schedule, remote_node);
}

TEST_F(HybridSchedulingPolicyTest, AvailableOverFeasibleTest) {
  // In this test, the local node is feasible and has a lower critical resource
  // utilization, but the remote node can run the task immediately, so we pick the remote
  // node.
  ResourceRequest req = ResourceMapToResourceRequest({{"CPU", 1}, {"GPU", 1}}, false);
  nodes.emplace(local_node, CreateNodeResources(10, 10, 0, 0, 0, 1));
  nodes.emplace(remote_node, CreateNodeResources(1, 10, 0, 0, 1, 1));

  auto cluster_resource_manager = MockClusterResourceManager(nodes);
  auto to_schedule = raylet_scheduling_policy::CompositeSchedulingPolicy(
                         local_node, cluster_resource_manager, [](auto) { return true; })
                         .Schedule(req, HybridOptions(0.50, false, false));
  ASSERT_EQ(to_schedule, remote_node);
}

TEST_F(HybridSchedulingPolicyTest, InfeasibleTest) {
  // All the nodes are infeasible, so we return -1.
  ResourceRequest req = ResourceMapToResourceRequest({{"CPU", 1}, {"GPU", 1}}, false);
  nodes.emplace(local_node, CreateNodeResources(10, 10, 0, 0, 0, 0));
  nodes.emplace(remote_node, CreateNodeResources(1, 10, 0, 0, 0, 0));

  auto cluster_resource_manager = MockClusterResourceManager(nodes);
  auto to_schedule = raylet_scheduling_policy::CompositeSchedulingPolicy(
                         local_node, cluster_resource_manager, [](auto) { return true; })
                         .Schedule(req, HybridOptions(0.50, false, false));
  ASSERT_TRUE(to_schedule.IsNil());
}

TEST_F(HybridSchedulingPolicyTest, BarelyFeasibleTest) {
  // Test the edge case where a task requires all of a node's resources, and the node is
  // fully utilized.
  ResourceRequest req = ResourceMapToResourceRequest({{"CPU", 1}, {"GPU", 1}}, false);

  nodes.emplace(local_node, CreateNodeResources(0, 1, 0, 0, 0, 1));

  auto cluster_resource_manager = MockClusterResourceManager(nodes);
  auto to_schedule = raylet_scheduling_policy::CompositeSchedulingPolicy(
                         local_node, cluster_resource_manager, [](auto) { return true; })
                         .Schedule(req, HybridOptions(0.50, false, false));
  ASSERT_EQ(to_schedule, local_node);
}

TEST_F(HybridSchedulingPolicyTest, TruncationAcrossFeasibleNodesTest) {
  // Same as AvailableTruncationTest except now none of the nodes are available, but the
  // tie break logic should apply to feasible nodes too.
  ResourceRequest req = ResourceMapToResourceRequest({{"CPU", 1}, {"GPU", 1}}, false);
  nodes.emplace(local_node, CreateNodeResources(1, 2, 0, 0, 0, 1));
  nodes.emplace(remote_node, CreateNodeResources(0.75, 2, 0, 0, 0, 1));

  auto cluster_resource_manager = MockClusterResourceManager(nodes);
  auto to_schedule = raylet_scheduling_policy::CompositeSchedulingPolicy(
                         local_node, cluster_resource_manager, [](auto) { return true; })
                         .Schedule(req, HybridOptions(0.51, false, false));
  ASSERT_EQ(to_schedule, local_node);
}

TEST_F(HybridSchedulingPolicyTest, ForceSpillbackIfAvailableTest) {
  // The local node is better, but we force spillback, so we'll schedule on a non-local
  // node anyways.
  ResourceRequest req = ResourceMapToResourceRequest({{"CPU", 1}, {"GPU", 1}}, false);
  nodes.emplace(local_node, CreateNodeResources(2, 2, 0, 0, 1, 1));
  nodes.emplace(remote_node, CreateNodeResources(1, 10, 0, 0, 1, 10));

  auto cluster_resource_manager = MockClusterResourceManager(nodes);
  auto to_schedule = raylet_scheduling_policy::CompositeSchedulingPolicy(
                         local_node, cluster_resource_manager, [](auto) { return true; })
                         .Schedule(req, HybridOptions(0.51, true, true));
  ASSERT_EQ(to_schedule, remote_node);
}

TEST_F(HybridSchedulingPolicyTest, AvoidSchedulingCPURequestsOnGPUNodes) {
  nodes.emplace(local_node, CreateNodeResources(10, 10, 0, 0, 1, 1));
  nodes.emplace(remote_node, CreateNodeResources(1, 2, 0, 0, 0, 0));
  auto cluster_resource_manager = MockClusterResourceManager(nodes);

  {
    // The local node is better, but it has GPUs, the request is
    // non GPU, and the remote node does not have GPUs, thus
    // we should schedule on remote node.
    const ResourceRequest req = ResourceMapToResourceRequest({{"CPU", 1}}, false);
    const auto to_schedule =
        raylet_scheduling_policy::CompositeSchedulingPolicy(
            local_node, cluster_resource_manager, [](auto) { return true; })
            .Schedule(ResourceMapToResourceRequest({{"CPU", 1}}, false),
                      HybridOptions(0.51, false, true, true));
    ASSERT_EQ(to_schedule, remote_node);
  }
  {
    // A GPU request should be scheduled on a GPU node.
    const ResourceRequest req = ResourceMapToResourceRequest({{"GPU", 1}}, false);
    const auto to_schedule =
        raylet_scheduling_policy::CompositeSchedulingPolicy(
            local_node, cluster_resource_manager, [](auto) { return true; })
            .Schedule(req, HybridOptions(0.51, false, true, true));
    ASSERT_EQ(to_schedule, local_node);
  }
  {
    // A CPU request can be be scheduled on a CPU node.
    const ResourceRequest req = ResourceMapToResourceRequest({{"CPU", 1}}, false);
    const auto to_schedule =
        raylet_scheduling_policy::CompositeSchedulingPolicy(
            local_node, cluster_resource_manager, [](auto) { return true; })
            .Schedule(req, HybridOptions(0.51, false, true, true));
    ASSERT_EQ(to_schedule, remote_node);
  }
  {
    // A mixed CPU/GPU request should be scheduled on a GPU node.
    const ResourceRequest req =
        ResourceMapToResourceRequest({{"CPU", 1}, {"GPU", 1}}, false);
    const auto to_schedule =
        raylet_scheduling_policy::CompositeSchedulingPolicy(
            local_node, cluster_resource_manager, [](auto) { return true; })
            .Schedule(req, HybridOptions(0.51, false, true, true));
    ASSERT_EQ(to_schedule, local_node);
  }
}

TEST_F(HybridSchedulingPolicyTest, SchedulenCPURequestsOnGPUNodeAsALastResort) {
  // Schedule on remote node, even though the request is CPU only, because
  // we can not schedule on CPU nodes.
  ResourceRequest req = ResourceMapToResourceRequest({{"CPU", 1}}, false);
  nodes.emplace(local_node, CreateNodeResources(0, 10, 0, 0, 0, 0));
  nodes.emplace(remote_node, CreateNodeResources(1, 1, 0, 0, 1, 1));

  auto cluster_resource_manager = MockClusterResourceManager(nodes);
  const auto to_schedule =
      raylet_scheduling_policy::CompositeSchedulingPolicy(
          local_node, cluster_resource_manager, [](auto) { return true; })
          .Schedule(req, HybridOptions(0.51, false, true, true));
  ASSERT_EQ(to_schedule, remote_node);
}

TEST_F(HybridSchedulingPolicyTest, ForceSpillbackTest) {
  // The local node is available but disqualified.
  ResourceRequest req = ResourceMapToResourceRequest({{"CPU", 1}, {"GPU", 1}}, false);

  nodes.emplace(local_node, CreateNodeResources(2, 2, 0, 0, 1, 1));
  nodes.emplace(remote_node, CreateNodeResources(0, 2, 0, 0, 0, 1));

  auto cluster_resource_manager = MockClusterResourceManager(nodes);
  auto to_schedule = raylet_scheduling_policy::CompositeSchedulingPolicy(
                         local_node, cluster_resource_manager, [](auto) { return true; })
                         .Schedule(req, HybridOptions(0.51, true, false));
  ASSERT_EQ(to_schedule, remote_node);
}

TEST_F(HybridSchedulingPolicyTest, ForceSpillbackOnlyFeasibleLocallyTest) {
  // The local node is better, but we force spillback, so we'll schedule on a non-local
  // node anyways.
  ResourceRequest req = ResourceMapToResourceRequest({{"CPU", 1}, {"GPU", 1}}, false);

  nodes.emplace(local_node, CreateNodeResources(2, 2, 0, 0, 1, 1));
  nodes.emplace(remote_node, CreateNodeResources(0, 2, 0, 0, 0, 0));

  auto cluster_resource_manager = MockClusterResourceManager(nodes);
  auto to_schedule = raylet_scheduling_policy::CompositeSchedulingPolicy(
                         local_node, cluster_resource_manager, [](auto) { return true; })
                         .Schedule(req, HybridOptions(0.51, true, false));
  ASSERT_TRUE(to_schedule.IsNil());
}

TEST_F(HybridSchedulingPolicyTest, NonGpuNodePreferredSchedulingTest) {
  // Prefer to schedule on CPU nodes first.
  // GPU nodes should be preferred as a last resort.

  // local {CPU:2, GPU:1}
  // Remote {CPU: 2}
  nodes.emplace(local_node, CreateNodeResources(2, 2, 0, 0, 1, 1));
  nodes.emplace(remote_node, CreateNodeResources(2, 2, 0, 0, 0, 0));
  nodes.emplace(remote_node_2, CreateNodeResources(3, 3, 0, 0, 0, 0));
  auto cluster_resource_manager = MockClusterResourceManager(nodes);

  ResourceRequest req = ResourceMapToResourceRequest({{"CPU", 1}}, false);
  auto to_schedule = raylet_scheduling_policy::CompositeSchedulingPolicy(
                         local_node, cluster_resource_manager, [](auto) { return true; })
                         .Schedule(req,
                                   HybridOptions(0.51,
                                                 false,
                                                 true,
                                                 /*gpu_avoid_scheduling*/ true));
  ASSERT_EQ(to_schedule, remote_node);

  req = ResourceMapToResourceRequest({{"CPU", 3}}, false);
  to_schedule = raylet_scheduling_policy::CompositeSchedulingPolicy(
                    local_node, cluster_resource_manager, [](auto) { return true; })
                    .Schedule(req,
                              HybridOptions(0.51,
                                            false,
                                            true,
                                            /*gpu_avoid_scheduling*/ true));
  ASSERT_EQ(to_schedule, remote_node_2);

  req = ResourceMapToResourceRequest({{"CPU", 1}, {"GPU", 1}}, false);
  to_schedule = raylet_scheduling_policy::CompositeSchedulingPolicy(
                    local_node, cluster_resource_manager, [](auto) { return true; })
                    .Schedule(req,
                              HybridOptions(0.51,
                                            false,
                                            true,
                                            /*gpu_avoid_scheduling*/ true));
  ASSERT_EQ(to_schedule, local_node);

  req = ResourceMapToResourceRequest({{"CPU", 2}}, false);
  to_schedule = raylet_scheduling_policy::CompositeSchedulingPolicy(
                    local_node, cluster_resource_manager, [](auto) { return true; })
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
