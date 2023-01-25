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

#include "absl/random/mock_distributions.h"
#include "absl/random/mocking_bit_gen.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/raylet/scheduling/policy/composite_scheduling_policy.h"

namespace ray {

namespace raylet_scheduling_policy {

using namespace ::testing;
using namespace ray::raylet;

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
  scheduling::NodeID n1 = scheduling::NodeID(1);
  scheduling::NodeID n2 = scheduling::NodeID(2);
  scheduling::NodeID n3 = scheduling::NodeID(3);
  scheduling::NodeID n4 = scheduling::NodeID(4);
  absl::flat_hash_map<scheduling::NodeID, Node> nodes;

  SchedulingOptions HybridOptions(
      float spread,
      bool avoid_local_node,
      bool require_node_available,
      bool avoid_gpu_nodes = RayConfig::instance().scheduler_avoid_gpu_nodes(),
      int schedule_top_k_absolute = 1,
      float scheduler_top_k_fraction = 0.1) {
    return SchedulingOptions(SchedulingType::HYBRID,
                             RayConfig::instance().scheduler_spread_threshold(),
                             avoid_local_node,
                             require_node_available,
                             avoid_gpu_nodes,
                             /*max_cpu_fraction_per_node*/ 1.0,
                             /*scheduling_context*/ nullptr,
                             /*preferred_node*/"",
                             schedule_top_k_absolute,
                             scheduler_top_k_fraction);
  }

  ClusterResourceManager MockClusterResourceManager(
      const absl::flat_hash_map<scheduling::NodeID, Node> &nodes) {
    ClusterResourceManager cluster_resource_manager;
    cluster_resource_manager.nodes_ = nodes;
    return cluster_resource_manager;
  }
};

TEST_F(HybridSchedulingPolicyTest, GetBestNode) {
  std::vector<std::pair<scheduling::NodeID, float>> node_scores{
      {n3, 0.6},
      {n4, 0.7},
      {n1, 0},
      {n2, 0},
  };

  // Test return 1 node always return the first node.
  {
    HybridSchedulingPolicy policy{local_node, {}, [](auto) { return true; }};
    EXPECT_EQ(n1,
              policy.GetBestNode(node_scores,
                                 /*num_candidate_nodes*/ 1,
                                 /*prioritize_local_node*/ false,
                                 /*local_node_score*/ 1));
  }

  // Test return 3 node calls to the random generator.
  {
    absl::MockingBitGen mock;
    EXPECT_CALL(absl::MockUniform<size_t>(), Call(mock, 0u, 3u))
        .WillOnce(Return(1))
        .WillOnce(Return(2))
        .WillOnce(Return(0));
    HybridSchedulingPolicy policy{local_node, {}, [](auto) { return true; }};
    policy.bitgenref_ = absl::BitGenRef{mock};
    EXPECT_EQ(n2,
              policy.GetBestNode(node_scores,
                                 /*num_candidate_nodes*/ 3,
                                 /*prioritize_local_node*/ false,
                                 /*local_node_score*/ 1));
    EXPECT_EQ(n3,
              policy.GetBestNode(node_scores,
                                 /*num_candidate_nodes*/ 3,
                                 /*prioritize_local_node*/ false,
                                 /*local_node_score*/ 1));
    EXPECT_EQ(n1,
              policy.GetBestNode(node_scores,
                                 /*num_candidate_nodes*/ 3,
                                 /*prioritize_local_node*/ false,
                                 /*local_node_score*/ 1));
  }
}

TEST_F(HybridSchedulingPolicyTest, GetBestNodePrioritizeLocalNode) {
  {
    std::vector<std::pair<scheduling::NodeID, float>> node_scores{
        {n3, 0.6},
        {n4, 0.7},
        {n1, 0},
        {n2, 0},
    };

    HybridSchedulingPolicy policy{local_node, {}, [](auto) { return true; }};
    // local node score is greater than the smallest one
    EXPECT_EQ(n1,
              policy.GetBestNode(node_scores,
                                 /*num_candidate_nodes*/ 1,
                                 /*prioritize_local_node*/ true,
                                 /*local_node_score*/ 0.1));

    // local node score is equal to the smallest one.
    EXPECT_EQ(local_node,
              policy.GetBestNode(node_scores,
                                 /*num_candidate_nodes*/ 1,
                                 /*prioritize_local_node*/ true,
                                 /*local_node_score*/ 0));
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

}  // namespace raylet_scheduling_policy

}  // namespace ray
