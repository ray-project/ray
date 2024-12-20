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
// limitations under the License

#include "ray/raylet/virtual_cluster_manager.h"

#include "absl/container/flat_hash_set.h"
#include "gtest/gtest.h"

namespace ray {

namespace raylet {

class VirtualClusterManagerTest : public ::testing::Test {};

TEST_F(VirtualClusterManagerTest, UpdateVirtualCluster) {
  VirtualClusterManager virtual_cluster_manager;

  std::string virtual_cluster_id_0 = "virtual_cluster_id_0";
  ASSERT_FALSE(virtual_cluster_manager.ContainsVirtualCluster("virtual_cluster_id"));

  rpc::VirtualClusterTableData virtual_cluster_data;
  virtual_cluster_data.set_id(virtual_cluster_id_0);
  virtual_cluster_data.set_mode(rpc::AllocationMode::EXCLUSIVE);
  virtual_cluster_data.set_revision(100);
  for (size_t i = 0; i < 100; ++i) {
    auto node_id = NodeID::FromRandom();
    virtual_cluster_data.mutable_node_instances()->insert(
        {node_id.Hex(), ray::rpc::NodeInstance()});
  }
  ASSERT_FALSE(virtual_cluster_manager.UpdateVirtualCluster(virtual_cluster_data));
  ASSERT_FALSE(virtual_cluster_manager.ContainsVirtualCluster(virtual_cluster_id_0));

  virtual_cluster_data.set_mode(rpc::AllocationMode::MIXED);
  ASSERT_TRUE(virtual_cluster_manager.UpdateVirtualCluster(virtual_cluster_data));
  ASSERT_TRUE(virtual_cluster_manager.ContainsVirtualCluster(virtual_cluster_id_0));

  virtual_cluster_data.set_revision(50);
  ASSERT_FALSE(virtual_cluster_manager.UpdateVirtualCluster(virtual_cluster_data));

  virtual_cluster_data.set_revision(150);
  ASSERT_TRUE(virtual_cluster_manager.UpdateVirtualCluster(virtual_cluster_data));

  virtual_cluster_data.set_is_removed(true);
  ASSERT_TRUE(virtual_cluster_manager.UpdateVirtualCluster(virtual_cluster_data));
  ASSERT_FALSE(virtual_cluster_manager.ContainsVirtualCluster(virtual_cluster_id_0));
}

TEST_F(VirtualClusterManagerTest, TestContainsNodeInstance) {
  VirtualClusterManager virtual_cluster_manager;
  std::string virtual_cluster_id_0 = "virtual_cluster_id_0";

  rpc::VirtualClusterTableData virtual_cluster_data;
  virtual_cluster_data.set_id(virtual_cluster_id_0);
  virtual_cluster_data.set_mode(rpc::AllocationMode::MIXED);
  virtual_cluster_data.set_revision(100);
  absl::flat_hash_set<NodeID> node_ids;
  for (size_t i = 0; i < 100; ++i) {
    auto node_id = NodeID::FromRandom();
    node_ids.emplace(node_id);

    virtual_cluster_data.mutable_node_instances()->insert(
        {node_id.Hex(), ray::rpc::NodeInstance()});
  }
  ASSERT_TRUE(virtual_cluster_manager.UpdateVirtualCluster(virtual_cluster_data));
  ASSERT_TRUE(virtual_cluster_manager.ContainsVirtualCluster(virtual_cluster_id_0));

  for (const auto &node_id : node_ids) {
    ASSERT_TRUE(
        virtual_cluster_manager.ContainsNodeInstance(virtual_cluster_id_0, node_id));
  }
}

}  // namespace raylet
}  // namespace ray
