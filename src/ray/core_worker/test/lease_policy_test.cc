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

#include "ray/core_worker/lease_policy.h"

#include "gtest/gtest.h"
#include "ray/common/task/task_spec.h"

namespace ray {
namespace core {

TaskSpecification CreateFakeTask(std::vector<ObjectID> deps) {
  TaskSpecification spec;
  spec.GetMutableMessage().set_task_id(TaskID::FromRandom(JobID::FromInt(1)).Binary());
  for (auto &dep : deps) {
    spec.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(
        dep.Binary());
  }
  spec.GetMutableMessage()
      .mutable_scheduling_strategy()
      ->mutable_default_scheduling_strategy();
  return spec;
}

class MockLocalityDataProvider : public LocalityDataProviderInterface {
 public:
  MockLocalityDataProvider() {}

  MockLocalityDataProvider(absl::flat_hash_map<ObjectID, LocalityData> locality_data)
      : locality_data_(locality_data) {}

  absl::optional<LocalityData> GetLocalityData(const ObjectID &object_id) const {
    num_locality_data_fetches++;
    return locality_data_[object_id];
  };

  ~MockLocalityDataProvider() {}

  mutable int num_locality_data_fetches = 0;
  mutable absl::flat_hash_map<ObjectID, LocalityData> locality_data_;
};

absl::optional<rpc::Address> MockNodeAddrFactory(const NodeID &node_id) {
  rpc::Address mock_rpc_address;
  mock_rpc_address.set_raylet_id(node_id.Binary());
  absl::optional<rpc::Address> opt_mock_rpc_address = mock_rpc_address;
  return opt_mock_rpc_address;
}

absl::optional<rpc::Address> MockNodeAddrFactoryAlwaysNull(const NodeID &node_id) {
  return absl::nullopt;
}

TEST(LocalLeasePolicyTest, TestReturnFallback) {
  NodeID fallback_node = NodeID::FromRandom();
  rpc::Address fallback_rpc_address = MockNodeAddrFactory(fallback_node).value();
  LocalLeasePolicy local_lease_policy(fallback_rpc_address);
  ObjectID obj1 = ObjectID::FromRandom();
  ObjectID obj2 = ObjectID::FromRandom();
  std::vector<ObjectID> deps{obj1, obj2};
  auto task_spec = CreateFakeTask(deps);
  auto [best_node_address, is_selected_based_on_locality] =
      local_lease_policy.GetBestNodeForTask(task_spec);
  // Test that fallback node was chosen.
  ASSERT_EQ(NodeID::FromBinary(best_node_address.raylet_id()), fallback_node);
  ASSERT_FALSE(is_selected_based_on_locality);
}

TEST(LocalityAwareLeasePolicyTest, TestBestLocalityFallbackSpreadSchedulingStrategy) {
  absl::flat_hash_map<ObjectID, LocalityData> locality_data;
  NodeID fallback_node = NodeID::FromRandom();
  rpc::Address fallback_rpc_address = MockNodeAddrFactory(fallback_node).value();
  NodeID best_node = NodeID::FromRandom();
  ObjectID obj1 = ObjectID::FromRandom();
  ObjectID obj2 = ObjectID::FromRandom();
  // Both objects are local on best_node.
  locality_data.emplace(obj1, LocalityData{8, {best_node}});
  locality_data.emplace(obj2, LocalityData{16, {best_node}});
  auto mock_locality_data_provider =
      std::make_shared<MockLocalityDataProvider>(locality_data);
  LocalityAwareLeasePolicy locality_lease_policy(
      mock_locality_data_provider, MockNodeAddrFactory, fallback_rpc_address);
  std::vector<ObjectID> deps{obj1, obj2};
  auto task_spec = CreateFakeTask(deps);
  task_spec.GetMutableMessage()
      .mutable_scheduling_strategy()
      ->mutable_spread_scheduling_strategy();
  auto [best_node_address, is_selected_based_on_locality] =
      locality_lease_policy.GetBestNodeForTask(task_spec);
  // Locality logic is not run since it's a spread scheduling strategy.
  ASSERT_EQ(mock_locality_data_provider->num_locality_data_fetches, 0);
  // Test that fallback node was chosen.
  ASSERT_EQ(NodeID::FromBinary(best_node_address.raylet_id()), fallback_node);
  ASSERT_FALSE(is_selected_based_on_locality);
}

TEST(LocalityAwareLeasePolicyTest,
     TestBestLocalityFallbackNodeAffinitySchedulingStrategy) {
  absl::flat_hash_map<ObjectID, LocalityData> locality_data;
  NodeID fallback_node = NodeID::FromRandom();
  rpc::Address fallback_rpc_address = MockNodeAddrFactory(fallback_node).value();
  NodeID best_node = NodeID::FromRandom();
  ObjectID obj1 = ObjectID::FromRandom();
  ObjectID obj2 = ObjectID::FromRandom();
  // Both objects are local on best_node.
  locality_data.emplace(obj1, LocalityData{8, {best_node}});
  locality_data.emplace(obj2, LocalityData{16, {best_node}});
  auto mock_locality_data_provider =
      std::make_shared<MockLocalityDataProvider>(locality_data);
  LocalityAwareLeasePolicy locality_lease_policy(
      mock_locality_data_provider, MockNodeAddrFactory, fallback_rpc_address);
  std::vector<ObjectID> deps{obj1, obj2};
  auto task_spec = CreateFakeTask(deps);
  NodeID node_affinity_node = NodeID::FromRandom();
  task_spec.GetMutableMessage()
      .mutable_scheduling_strategy()
      ->mutable_node_affinity_scheduling_strategy()
      ->set_node_id(node_affinity_node.Binary());
  auto [best_node_address, is_selected_based_on_locality] =
      locality_lease_policy.GetBestNodeForTask(task_spec);
  // Locality logic is not run since it's a node affinity scheduling strategy.
  ASSERT_EQ(mock_locality_data_provider->num_locality_data_fetches, 0);
  // Test that node affinity node was chosen.
  ASSERT_EQ(NodeID::FromBinary(best_node_address.raylet_id()), node_affinity_node);
  ASSERT_FALSE(is_selected_based_on_locality);
}

TEST(LocalityAwareLeasePolicyTest, TestBestLocalityDominatingNode) {
  absl::flat_hash_map<ObjectID, LocalityData> locality_data;
  NodeID fallback_node = NodeID::FromRandom();
  rpc::Address fallback_rpc_address = MockNodeAddrFactory(fallback_node).value();
  NodeID best_node = NodeID::FromRandom();
  ObjectID obj1 = ObjectID::FromRandom();
  ObjectID obj2 = ObjectID::FromRandom();
  // Both objects are local on best_node.
  locality_data.emplace(obj1, LocalityData{8, {best_node}});
  locality_data.emplace(obj2, LocalityData{16, {best_node}});
  auto mock_locality_data_provider =
      std::make_shared<MockLocalityDataProvider>(locality_data);
  LocalityAwareLeasePolicy locality_lease_policy(
      mock_locality_data_provider, MockNodeAddrFactory, fallback_rpc_address);
  std::vector<ObjectID> deps{obj1, obj2};
  auto task_spec = CreateFakeTask(deps);
  auto [best_node_address, is_selected_based_on_locality] =
      locality_lease_policy.GetBestNodeForTask(task_spec);
  // Locality data provider should be called once for each dependency.
  ASSERT_EQ(mock_locality_data_provider->num_locality_data_fetches, deps.size());
  // Test that best node was chosen.
  ASSERT_EQ(NodeID::FromBinary(best_node_address.raylet_id()), best_node);
  ASSERT_TRUE(is_selected_based_on_locality);
}

TEST(LocalityAwareLeasePolicyTest, TestBestLocalityBiggerObject) {
  absl::flat_hash_map<ObjectID, LocalityData> locality_data;
  NodeID fallback_node = NodeID::FromRandom();
  rpc::Address fallback_rpc_address = MockNodeAddrFactory(fallback_node).value();
  NodeID best_node = NodeID::FromRandom();
  NodeID bad_node = NodeID::FromRandom();
  ObjectID obj1 = ObjectID::FromRandom();
  ObjectID obj2 = ObjectID::FromRandom();
  // Larger object is local on best_node.
  locality_data.emplace(obj1, LocalityData{8, {bad_node}});
  locality_data.emplace(obj2, LocalityData{16, {best_node}});
  auto mock_locality_data_provider =
      std::make_shared<MockLocalityDataProvider>(locality_data);
  LocalityAwareLeasePolicy locality_lease_policy(
      mock_locality_data_provider, MockNodeAddrFactory, fallback_rpc_address);
  std::vector<ObjectID> deps{obj1, obj2};
  auto task_spec = CreateFakeTask(deps);
  auto [best_node_address, is_selected_based_on_locality] =
      locality_lease_policy.GetBestNodeForTask(task_spec);
  // Locality data provider should be called once for each dependency.
  ASSERT_EQ(mock_locality_data_provider->num_locality_data_fetches, deps.size());
  // Test that best node was chosen.
  ASSERT_EQ(NodeID::FromBinary(best_node_address.raylet_id()), best_node);
  ASSERT_TRUE(is_selected_based_on_locality);
}

TEST(LocalityAwareLeasePolicyTest, TestBestLocalityBetterNode) {
  absl::flat_hash_map<ObjectID, LocalityData> locality_data;
  NodeID fallback_node = NodeID::FromRandom();
  rpc::Address fallback_rpc_address = MockNodeAddrFactory(fallback_node).value();
  NodeID best_node = NodeID::FromRandom();
  NodeID bad_node = NodeID::FromRandom();
  ObjectID obj1 = ObjectID::FromRandom();
  ObjectID obj2 = ObjectID::FromRandom();
  ObjectID obj3 = ObjectID::FromRandom();
  // fallback_node:  8 bytes local
  // bad_node:      24 bytes local
  // best_node:     28 bytes local
  locality_data.emplace(obj1, LocalityData{8, {fallback_node, bad_node}});
  locality_data.emplace(obj2, LocalityData{16, {best_node, bad_node}});
  locality_data.emplace(obj3, LocalityData{12, {best_node}});
  auto mock_locality_data_provider =
      std::make_shared<MockLocalityDataProvider>(locality_data);
  LocalityAwareLeasePolicy locality_lease_policy(
      mock_locality_data_provider, MockNodeAddrFactory, fallback_rpc_address);
  std::vector<ObjectID> deps{obj1, obj2, obj3};
  auto task_spec = CreateFakeTask(deps);
  auto [best_node_address, is_selected_based_on_locality] =
      locality_lease_policy.GetBestNodeForTask(task_spec);
  // Locality data provider should be called once for each dependency.
  ASSERT_EQ(mock_locality_data_provider->num_locality_data_fetches, deps.size());
  // Test that best node was chosen.
  ASSERT_EQ(NodeID::FromBinary(best_node_address.raylet_id()), best_node);
  ASSERT_TRUE(is_selected_based_on_locality);
}

TEST(LocalityAwareLeasePolicyTest, TestBestLocalityFallbackNoLocations) {
  absl::flat_hash_map<ObjectID, LocalityData> locality_data;
  NodeID fallback_node = NodeID::FromRandom();
  rpc::Address fallback_rpc_address = MockNodeAddrFactory(fallback_node).value();
  ObjectID obj1 = ObjectID::FromRandom();
  ObjectID obj2 = ObjectID::FromRandom();
  // No known object locations.
  locality_data.emplace(obj1, LocalityData{8, {}});
  locality_data.emplace(obj2, LocalityData{16, {}});
  auto mock_locality_data_provider =
      std::make_shared<MockLocalityDataProvider>(locality_data);
  LocalityAwareLeasePolicy locality_lease_policy(
      mock_locality_data_provider, MockNodeAddrFactory, fallback_rpc_address);
  std::vector<ObjectID> deps{obj1, obj2};
  auto task_spec = CreateFakeTask(deps);
  auto [best_node_address, is_selected_based_on_locality] =
      locality_lease_policy.GetBestNodeForTask(task_spec);
  // Locality data provider should be called once for each dependency.
  ASSERT_EQ(mock_locality_data_provider->num_locality_data_fetches, deps.size());
  // Test that fallback node was chosen.
  ASSERT_EQ(NodeID::FromBinary(best_node_address.raylet_id()), fallback_node);
  ASSERT_FALSE(is_selected_based_on_locality);
}

TEST(LocalityAwareLeasePolicyTest, TestBestLocalityFallbackNoDeps) {
  absl::flat_hash_map<ObjectID, LocalityData> locality_data;
  NodeID fallback_node = NodeID::FromRandom();
  rpc::Address fallback_rpc_address = MockNodeAddrFactory(fallback_node).value();
  auto mock_locality_data_provider = std::make_shared<MockLocalityDataProvider>();
  LocalityAwareLeasePolicy locality_lease_policy(
      mock_locality_data_provider, MockNodeAddrFactory, fallback_rpc_address);
  // No task dependencies.
  std::vector<ObjectID> deps;
  auto task_spec = CreateFakeTask(deps);
  auto [best_node_address, is_selected_based_on_locality] =
      locality_lease_policy.GetBestNodeForTask(task_spec);
  // Locality data provider should be called once for each dependency.
  ASSERT_EQ(mock_locality_data_provider->num_locality_data_fetches, deps.size());
  // Test that fallback node was chosen.
  ASSERT_EQ(NodeID::FromBinary(best_node_address.raylet_id()), fallback_node);
  ASSERT_FALSE(is_selected_based_on_locality);
}

TEST(LocalityAwareLeasePolicyTest, TestBestLocalityFallbackAddrFetchFail) {
  absl::flat_hash_map<ObjectID, LocalityData> locality_data;
  NodeID fallback_node = NodeID::FromRandom();
  rpc::Address fallback_rpc_address = MockNodeAddrFactory(fallback_node).value();
  NodeID best_node = NodeID::FromRandom();
  ObjectID obj1 = ObjectID::FromRandom();
  ObjectID obj2 = ObjectID::FromRandom();
  locality_data.emplace(obj1, LocalityData{8, {best_node}});
  locality_data.emplace(obj2, LocalityData{16, {best_node}});
  auto mock_locality_data_provider =
      std::make_shared<MockLocalityDataProvider>(locality_data);
  // Provided node address factory always returns absl::nullopt.
  LocalityAwareLeasePolicy locality_lease_policy(
      mock_locality_data_provider, MockNodeAddrFactoryAlwaysNull, fallback_rpc_address);
  std::vector<ObjectID> deps{obj1, obj2};
  auto task_spec = CreateFakeTask(deps);
  auto [best_node_address, is_selected_based_on_locality] =
      locality_lease_policy.GetBestNodeForTask(task_spec);
  // Locality data provider should be called once for each dependency.
  ASSERT_EQ(mock_locality_data_provider->num_locality_data_fetches, deps.size());
  // Test that fallback node was chosen.
  ASSERT_EQ(NodeID::FromBinary(best_node_address.raylet_id()), fallback_node);
  ASSERT_FALSE(is_selected_based_on_locality);
}

}  // namespace core
}  // namespace ray
