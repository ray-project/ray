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

#include <cstdint>
#include <limits>
#include <utility>
#include <vector>

#include "absl/random/bit_gen_ref.h"
#include "gtest/gtest.h"
#include "ray/raylet/scheduling/policy/composite_scheduling_policy.h"

namespace ray {

namespace raylet_scheduling_policy {

using namespace ray::raylet;

// Deterministic fake uniform-random-bit generator. It advertises a full 64-bit
// range so that absl::Uniform consumes exactly one value per draw and returns it
// unmodified as the source bits. For absl::Uniform<size_t>(gen, 0, 3) the returned
// index is floor(bits * 3 / 2^64), so the values below map to fixed indices:
//   0x8000000000000000 -> 1, 0xF000000000000000 -> 2, 1 -> 0.
class FakeBitGen {
 public:
  using result_type = uint64_t;
  static constexpr result_type(min)() { return 0; }
  static constexpr result_type(max)() {
    return (std::numeric_limits<result_type>::max)();
  }
  explicit FakeBitGen(std::vector<uint64_t> values) : values_(std::move(values)) {}
  result_type operator()() {
    result_type value = values_[index_ % values_.size()];
    ++index_;
    return value;
  }

 private:
  std::vector<uint64_t> values_;
  size_t index_ = 0;
};

NodeResources CreateNodeResources(double available_cpu,
                                  double total_cpu,
                                  double available_memory,
                                  double total_memory,
                                  double available_gpu,
                                  double total_gpu) {
  NodeResources resources;
  resources.SetAvailableResource(ResourceID::CPU(), available_cpu);
  resources.SetAvailableResource(ResourceID::Memory(), available_memory);
  resources.SetAvailableResource(ResourceID::GPU(), available_gpu);
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
                             /*target_topology_assignment*/ std::nullopt,
                             /*scheduling_context*/ nullptr,
                             /*preferred_node*/ "",
                             schedule_top_k_absolute,
                             scheduler_top_k_fraction);
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
                                 /*preferred_node*/ {},
                                 /*preferred_node_score*/ 1));
  }

  // Test return 3 node calls to the random generator. The fake generator is set up
  // so that the three draws yield indices 1, 2, and 0 respectively.
  {
    FakeBitGen fake({0x8000000000000000ULL, 0xF000000000000000ULL, 1ULL});
    HybridSchedulingPolicy policy{local_node, {}, [](auto) { return true; }};
    policy.bitgenref_ = absl::BitGenRef{fake};
    EXPECT_EQ(n2,
              policy.GetBestNode(node_scores,
                                 /*num_candidate_nodes*/ 3,
                                 /*preferred_node_id*/ {},
                                 /*preferred_node_score*/ 1));
    EXPECT_EQ(n3,
              policy.GetBestNode(node_scores,
                                 /*num_candidate_nodes*/ 3,
                                 /*preferred_node_id*/ {},
                                 /*preferred_node_score*/ 1));
    EXPECT_EQ(n1,
              policy.GetBestNode(node_scores,
                                 /*num_candidate_nodes*/ 3,
                                 /*preferred_node_id*/ {},
                                 /*preferred_node_score*/ 1));
  }
}

TEST_F(HybridSchedulingPolicyTest, GetBestNodePrioritizePreferredNode) {
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
                                 /*preferred_node_id*/ {local_node},
                                 /*preferred_node_score*/ 0.5));

    // local node score is equal to the smallest one.
    EXPECT_EQ(local_node,
              policy.GetBestNode(node_scores,
                                 /*num_candidate_nodes*/ 1,
                                 /*preferred_node_id*/ {local_node},
                                 /*preferred_node_score*/ 0));
    // preferred node score is equal to the smallest one.
    EXPECT_EQ(n2,
              policy.GetBestNode(node_scores,
                                 /*num_candidate_nodes*/ 1,
                                 /*preferred_node_id*/ {n2},
                                 /*preferred_node_score*/ 0));
  }
}

}  // namespace raylet_scheduling_policy

}  // namespace ray
