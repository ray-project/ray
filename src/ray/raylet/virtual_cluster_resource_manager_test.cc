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
#include "ray/raylet/virtual_cluster_resource_manager.h"

#include <memory>

#include "gtest/gtest.h"
#include "ray/common/id.h"
#include "ray/common/scheduling/resource_set.h"
#include "ray/common/virtual_cluster_node_spec.h"
#include "ray/gcs/test/gcs_test_util.h"
#include "mock/ray/gcs/gcs_client/gcs_client.h"
// clang-format on

namespace ray {

namespace {

// Adds all fixedNodes' nodes to the nodes_spec.
VirtualClusterNodesSpec NodesSpecForAllFixedNodes(const VirtualClusterSpecification &vc) {
  VirtualClusterNodesSpec nodes_spec;
  auto vc_id = vc.VirtualClusterId();
  nodes_spec.vc_id = vc_id;
  for (const auto &nodes : vc.GetMessage().fixed_size_nodes()) {
    for (const auto &node : nodes.nodes()) {
      nodes_spec.fixed_size_nodes.emplace_back(node, vc_id);
    }
  }
  return nodes_spec;
}
}  // namespace

class VirtualClusterResourceManagerTest : public ::testing::Test {
 public:
  instrumented_io_context io_context;
  std::unique_ptr<raylet::VirtualClusterResourceManager>
      virtual_cluster_resource_manager_;
  std::shared_ptr<ClusterResourceScheduler> vc_resource_scheduler_;
  std::unique_ptr<gcs::MockGcsClient> gcs_client_;
  std::function<bool(scheduling::NodeID)> is_node_available_fn_;
  rpc::GcsNodeInfo node_info_;
  void SetUp() {
    gcs_client_ = std::make_unique<gcs::MockGcsClient>();
    is_node_available_fn_ = [this](scheduling::NodeID node_id) {
      return gcs_client_->Nodes().Get(NodeID::FromBinary(node_id.Binary())) != nullptr;
    };
    EXPECT_CALL(*gcs_client_->mock_node_accessor, Get(::testing::_, ::testing::_))
        .WillRepeatedly(::testing::Return(&node_info_));
  }
  void InitLocalAvailableResource(
      absl::flat_hash_map<std::string, double> &unit_resource) {
    vc_resource_scheduler_ = std::make_shared<ClusterResourceScheduler>(
        io_context, scheduling::NodeID("local"), unit_resource, is_node_available_fn_);
    virtual_cluster_resource_manager_ =
        std::make_unique<raylet::VirtualClusterResourceManager>(vc_resource_scheduler_);
  }

  void CheckAvailableResourceEmpty(const std::string &resource) {
    ASSERT_TRUE(
        vc_resource_scheduler_->GetLocalResourceManager().IsAvailableResourceEmpty(
            scheduling::ResourceID(resource)));
  }

  void CheckRemainingResourceCorrect(NodeResources &node_resources) {
    auto local_node_resource =
        vc_resource_scheduler_->GetClusterResourceManager().GetNodeResources(
            scheduling::NodeID("local"));
    ASSERT_EQ(local_node_resource, node_resources)
        << "expected " << node_resources.DebugString() << ", got "
        << local_node_resource.DebugString();
  }
};

TEST_F(VirtualClusterResourceManagerTest, TestParse) {
  ASSERT_FALSE(
      VirtualClusterResourceLabel::Parse("CPU_vc_0_4482dec0faaf5ead891ff1659a9501000000")
          .has_value());
  ASSERT_FALSE(VirtualClusterResourceLabel::Parse("CPU").has_value());
  ASSERT_EQ(
      VirtualClusterResourceLabel::Parse("CPU_vc_4482dec0faaf5ead891ff1659a9501000000")
          ->original_resource,
      "CPU");
  ASSERT_EQ(
      VirtualClusterResourceLabel::Parse("GPU_vc_4482dec0faaf5ead891ff1659a9501000000")
          ->original_resource,
      "GPU");
  ASSERT_FALSE(
      VirtualClusterResourceLabel::Parse("GPU_vc_0_4482dec0faaf5ead891ff1659a9501000000")
          .has_value());
}

TEST_F(VirtualClusterResourceManagerTest, TestParseVirtualClusterResourceLabel) {
  // Test indexed resources
  ASSERT_FALSE(
      VirtualClusterResourceLabel::Parse("CPU_vc_0_4482dec0faaf5ead891ff1659a9501000000")
          .has_value());

  // Parse incorrect resource parsing.
  ASSERT_FALSE(VirtualClusterResourceLabel::Parse("CPU").has_value());

  // Parse wildcard resources.
  ASSERT_EQ(
      VirtualClusterResourceLabel::Parse("CPU_vc_4482dec0faaf5ead891ff1659a9501000000")
          ->original_resource,
      "CPU");
  ASSERT_EQ(
      VirtualClusterResourceLabel::Parse("custom_vc_4482dec0faaf5ead891ff1659a9501000000")
          ->original_resource,
      "custom");
  ASSERT_EQ(
      VirtualClusterResourceLabel::Parse("GPU_vc_4482dec0faaf5ead891ff1659a9501000000")
          ->original_resource,
      "GPU");
}

TEST_F(VirtualClusterResourceManagerTest, TestPrepareBundleResource) {
  // 1. create bundle spec.
  auto vc_id = VirtualClusterID::FromRandom();
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto vc_spec = Mocker::GenVirtualCluster(vc_id, unit_resource);
  /// 2. init local available resource.
  InitLocalAvailableResource(unit_resource);
  /// 3. prepare bundle resource.
  ASSERT_TRUE(virtual_cluster_resource_manager_->PrepareBundle(
      NodesSpecForAllFixedNodes(vc_spec)));
  /// 4. check remaining resources is correct.
  CheckAvailableResourceEmpty("CPU");
}

TEST_F(VirtualClusterResourceManagerTest, TestPrepareBundleWithInsufficientResource) {
  // 1. create bundle spec.
  auto vc_id = VirtualClusterID::FromRandom();
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 2.0});
  auto vc_spec = Mocker::GenVirtualCluster(vc_id, unit_resource);
  /// 2. init local available resource.
  absl::flat_hash_map<std::string, double> init_unit_resource;
  init_unit_resource.insert({"CPU", 1.0});
  InitLocalAvailableResource(init_unit_resource);
  /// 3. prepare bundle resource.
  ASSERT_FALSE(virtual_cluster_resource_manager_->PrepareBundle(
      NodesSpecForAllFixedNodes(vc_spec)));
}

TEST_F(VirtualClusterResourceManagerTest, TestPrepareBundleDuringDraining) {
  // 1. create bundle spec.
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto vc1_id = VirtualClusterID::FromRandom();
  auto bundle1_spec = Mocker::GenVirtualCluster(vc1_id, unit_resource);

  auto vc2_id = VirtualClusterID::FromRandom();
  auto bundle2_spec = Mocker::GenVirtualCluster(vc2_id, unit_resource);

  /// 2. init local available resource.
  absl::flat_hash_map<std::string, double> init_unit_resource;
  init_unit_resource.insert({"CPU", 2.0});
  InitLocalAvailableResource(init_unit_resource);

  ASSERT_TRUE(virtual_cluster_resource_manager_->PrepareBundle(
      NodesSpecForAllFixedNodes(bundle1_spec)));
  // Drain the node,  bundle prepare will fail.
  vc_resource_scheduler_->GetLocalResourceManager().SetLocalNodeDraining();
  ASSERT_FALSE(virtual_cluster_resource_manager_->PrepareBundle(
      NodesSpecForAllFixedNodes(bundle2_spec)));
  // Prepared bundle can still be committed.
  virtual_cluster_resource_manager_->CommitBundle(bundle1_spec.VirtualClusterId());
  absl::flat_hash_map<std::string, double> remaining_resources = {
      {"CPU_vc_" + vc1_id.Hex(), 1.0},
      {"CPU", 2.0},
      {"vcbundle_vc_" + vc1_id.Hex(), 1000}};
  auto remaining_resource_scheduler =
      std::make_shared<ClusterResourceScheduler>(io_context,
                                                 scheduling::NodeID("remaining"),
                                                 remaining_resources,
                                                 is_node_available_fn_);
  std::shared_ptr<TaskResourceInstances> resource_instances =
      std::make_shared<TaskResourceInstances>();
  ASSERT_TRUE(
      remaining_resource_scheduler->GetLocalResourceManager().AllocateLocalTaskResources(
          unit_resource, resource_instances));
  auto remaining_resource_instance =
      remaining_resource_scheduler->GetClusterResourceManager().GetNodeResources(
          scheduling::NodeID("remaining"));
  CheckRemainingResourceCorrect(remaining_resource_instance);
}

TEST_F(VirtualClusterResourceManagerTest, TestCommitBundleResource) {
  // 1. create bundle spec.
  auto vc_id = VirtualClusterID::FromRandom();
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto vc_spec = Mocker::GenVirtualCluster(vc_id, unit_resource);
  /// 2. init local available resource.
  InitLocalAvailableResource(unit_resource);
  /// 3. prepare and commit bundle resource.
  ASSERT_TRUE(virtual_cluster_resource_manager_->PrepareBundle(
      NodesSpecForAllFixedNodes(vc_spec)));
  virtual_cluster_resource_manager_->CommitBundle(vc_spec.VirtualClusterId());
  /// 4. check remaining resources is correct.
  absl::flat_hash_map<std::string, double> remaining_resources = {
      {"CPU_vc_" + vc_id.Hex(), 1.0}, {"CPU", 1.0}, {"vcbundle_vc_" + vc_id.Hex(), 1000}};
  auto remaining_resource_scheduler =
      std::make_shared<ClusterResourceScheduler>(io_context,
                                                 scheduling::NodeID("remaining"),
                                                 remaining_resources,
                                                 is_node_available_fn_);
  std::shared_ptr<TaskResourceInstances> resource_instances =
      std::make_shared<TaskResourceInstances>();
  ASSERT_TRUE(
      remaining_resource_scheduler->GetLocalResourceManager().AllocateLocalTaskResources(
          unit_resource, resource_instances));
  auto remaining_resource_instance =
      remaining_resource_scheduler->GetClusterResourceManager().GetNodeResources(
          scheduling::NodeID("remaining"));
  CheckRemainingResourceCorrect(remaining_resource_instance);
}

TEST_F(VirtualClusterResourceManagerTest, TestReturnBundleResource) {
  // 1. create bundle spec.
  auto vc_id = VirtualClusterID::FromRandom();
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto vc_spec = Mocker::GenVirtualCluster(vc_id, unit_resource);
  /// 2. init local available resource.
  InitLocalAvailableResource(unit_resource);
  /// 3. prepare and commit bundle resource.
  ASSERT_TRUE(virtual_cluster_resource_manager_->PrepareBundle(
      NodesSpecForAllFixedNodes(vc_spec)));
  virtual_cluster_resource_manager_->CommitBundle(vc_spec.VirtualClusterId());
  /// 4. return bundle resource.
  virtual_cluster_resource_manager_->ReturnBundle(vc_spec.VirtualClusterId());
  /// 5. check remaining resources is correct.
  auto remaining_resource_scheduler = std::make_shared<ClusterResourceScheduler>(
      io_context, scheduling::NodeID("remaining"), unit_resource, is_node_available_fn_);
  auto remaining_resource_instance =
      remaining_resource_scheduler->GetClusterResourceManager().GetNodeResources(
          scheduling::NodeID("remaining"));
  CheckRemainingResourceCorrect(remaining_resource_instance);
}

TEST_F(VirtualClusterResourceManagerTest, TestIdempotencyWithMultiPrepare) {
  // 1. create one bundle spec.

  auto vc_id = VirtualClusterID::FromRandom();
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto vc_spec = Mocker::GenVirtualCluster(vc_id, unit_resource);
  /// 2. init local available resource.
  absl::flat_hash_map<std::string, double> available_resource = {
      std::make_pair("CPU", 3.0)};
  InitLocalAvailableResource(available_resource);
  /// 3. prepare bundle resource 10 times.
  for (int i = 0; i < 10; i++) {
    ASSERT_TRUE(virtual_cluster_resource_manager_->PrepareBundle(
        NodesSpecForAllFixedNodes(vc_spec)));
  }
  /// 4. check remaining resources is correct.
  absl::flat_hash_map<std::string, double> remaining_resources = {{"CPU", 3.0}};
  auto remaining_resource_scheduler =
      std::make_shared<ClusterResourceScheduler>(io_context,
                                                 scheduling::NodeID("remaining"),
                                                 remaining_resources,
                                                 is_node_available_fn_);
  std::shared_ptr<TaskResourceInstances> resource_instances =
      std::make_shared<TaskResourceInstances>();
  ASSERT_TRUE(
      remaining_resource_scheduler->GetLocalResourceManager().AllocateLocalTaskResources(
          unit_resource, resource_instances));
  auto remaining_resource_instance =
      remaining_resource_scheduler->GetClusterResourceManager().GetNodeResources(
          scheduling::NodeID("remaining"));
  CheckRemainingResourceCorrect(remaining_resource_instance);
}

TEST_F(VirtualClusterResourceManagerTest, TestIdempotencyWithRandomOrder) {
  // 1. create one bundle spec.
  auto vc_id = VirtualClusterID::FromRandom();
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto vc_spec = Mocker::GenVirtualCluster(vc_id, unit_resource);
  /// 2. init local available resource.
  absl::flat_hash_map<std::string, double> available_resource = {
      std::make_pair("CPU", 3.0)};
  InitLocalAvailableResource(available_resource);
  /// 3. prepare bundle -> commit bundle -> prepare bundle.
  ASSERT_TRUE(virtual_cluster_resource_manager_->PrepareBundle(
      NodesSpecForAllFixedNodes(vc_spec)));
  virtual_cluster_resource_manager_->CommitBundle(vc_spec.VirtualClusterId());
  ASSERT_TRUE(virtual_cluster_resource_manager_->PrepareBundle(
      NodesSpecForAllFixedNodes(vc_spec)));
  /// 4. check remaining resources is correct.
  absl::flat_hash_map<std::string, double> remaining_resources = {
      {"CPU_vc_" + vc_id.Hex(), 1.0}, {"CPU", 3.0}, {"vcbundle_vc_" + vc_id.Hex(), 1000}};
  auto remaining_resource_scheduler =
      std::make_shared<ClusterResourceScheduler>(io_context,
                                                 scheduling::NodeID("remaining"),
                                                 remaining_resources,
                                                 is_node_available_fn_);
  std::shared_ptr<TaskResourceInstances> resource_instances =
      std::make_shared<TaskResourceInstances>();
  ASSERT_TRUE(
      remaining_resource_scheduler->GetLocalResourceManager().AllocateLocalTaskResources(
          unit_resource, resource_instances));
  auto remaining_resource_instance =
      remaining_resource_scheduler->GetClusterResourceManager().GetNodeResources(
          scheduling::NodeID("remaining"));
  CheckRemainingResourceCorrect(remaining_resource_instance);
  virtual_cluster_resource_manager_->ReturnBundle(vc_spec.VirtualClusterId());
  // 5. prepare bundle -> commit bundle -> commit bundle.
  ASSERT_TRUE(virtual_cluster_resource_manager_->PrepareBundle(
      NodesSpecForAllFixedNodes(vc_spec)));
  virtual_cluster_resource_manager_->CommitBundle(vc_spec.VirtualClusterId());
  virtual_cluster_resource_manager_->CommitBundle(vc_spec.VirtualClusterId());
  // 6. check remaining resources is correct.
  CheckRemainingResourceCorrect(remaining_resource_instance);
  virtual_cluster_resource_manager_->ReturnBundle(vc_spec.VirtualClusterId());
  // 7. prepare bundle -> return bundle -> commit bundle.
  ASSERT_TRUE(virtual_cluster_resource_manager_->PrepareBundle(
      NodesSpecForAllFixedNodes(vc_spec)));
  virtual_cluster_resource_manager_->ReturnBundle(vc_spec.VirtualClusterId());
  virtual_cluster_resource_manager_->CommitBundle(vc_spec.VirtualClusterId());
  // 8. check remaining resources is correct.
  remaining_resource_scheduler =
      std::make_shared<ClusterResourceScheduler>(io_context,
                                                 scheduling::NodeID("remaining"),
                                                 available_resource,
                                                 is_node_available_fn_);
  remaining_resource_instance =
      remaining_resource_scheduler->GetClusterResourceManager().GetNodeResources(
          scheduling::NodeID("remaining"));
  CheckRemainingResourceCorrect(remaining_resource_instance);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
