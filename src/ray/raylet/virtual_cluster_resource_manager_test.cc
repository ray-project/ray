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
#include "ray/common/bundle_spec.h"
#include "ray/common/id.h"
#include "ray/common/scheduling/resource_set.h"
#include "ray/gcs/test/gcs_test_util.h"
#include "mock/ray/gcs/gcs_client/gcs_client.h"
// clang-format on

namespace ray {

class VirtualClusterResourceManagerTest : public ::testing::Test {
 public:
  instrumented_io_context io_context;
  std::unique_ptr<raylet::VirtualClusterResourceManager>
      virtual_cluster_resource_manager_;
  std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler_;
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
    cluster_resource_scheduler_ = std::make_shared<ClusterResourceScheduler>(
        io_context, scheduling::NodeID("local"), unit_resource, is_node_available_fn_);
    virtual_cluster_resource_manager_ =
        std::make_unique<raylet::VirtualClusterResourceManager>(
            cluster_resource_scheduler_);
  }

  void CheckAvailableResourceEmpty(const std::string &resource) {
    ASSERT_TRUE(
        cluster_resource_scheduler_->GetLocalResourceManager().IsAvailableResourceEmpty(
            scheduling::ResourceID(resource)));
  }

  void CheckRemainingResourceCorrect(NodeResources &node_resources) {
    auto local_node_resource =
        cluster_resource_scheduler_->GetClusterResourceManager().GetNodeResources(
            scheduling::NodeID("local"));
    ASSERT_EQ(local_node_resource, node_resources)
        << "expected " << node_resources.DebugString() << ", got "
        << local_node_resource.DebugString();
  }

  // TODO(@clay4444): Remove this once we did the batch rpc request refactor!
  std::vector<std::shared_ptr<const VirtualClusterBundleSpec>>
  ConvertSingleSpecToVectorPtrs(const VirtualClusterBundleSpec &bundle_spec) {
    std::vector<std::shared_ptr<const VirtualClusterBundleSpec>> bundle_specs;
    // Copies!
    bundle_specs.push_back(std::make_shared<const VirtualClusterBundleSpec>(bundle_spec));
    return bundle_specs;
  }
};

TEST_F(VirtualClusterResourceManagerTest, TestParseFromWildcard) {
  ASSERT_FALSE(VirtualClusterBundleResourceLabel::ParseFromWildcard(
                   "CPU_vc_0_4482dec0faaf5ead891ff1659a9501000000")
                   .has_value());
  ASSERT_FALSE(VirtualClusterBundleResourceLabel::ParseFromWildcard("CPU").has_value());
  ASSERT_EQ(VirtualClusterBundleResourceLabel::ParseFromWildcard(
                "CPU_vc_4482dec0faaf5ead891ff1659a9501000000")
                ->original_resource,
            "CPU");
  ASSERT_EQ(VirtualClusterBundleResourceLabel::ParseFromWildcard(
                "GPU_vc_4482dec0faaf5ead891ff1659a9501000000")
                ->original_resource,
            "GPU");
  ASSERT_FALSE(VirtualClusterBundleResourceLabel::ParseFromWildcard(
                   "GPU_vc_0_4482dec0faaf5ead891ff1659a9501000000")
                   .has_value());
}

TEST_F(VirtualClusterResourceManagerTest, TestParseVirtualClusterBundleResourceLabel) {
  // Test indexed resources
  ASSERT_EQ(VirtualClusterBundleResourceLabel::ParseFromIndexed(
                "CPU_vc_0_4482dec0faaf5ead891ff1659a9501000000")
                ->original_resource,
            "CPU");
  ASSERT_EQ(VirtualClusterBundleResourceLabel::ParseFromIndexed(
                "custom_vc_0_4482dec0faaf5ead891ff1659a9501000000")
                ->original_resource,
            "custom");
  ASSERT_EQ(VirtualClusterBundleResourceLabel::ParseFromIndexed(
                "GPU_vc_0_4482dec0faaf5ead891ff1659a9501000000")
                ->original_resource,
            "GPU");
  ASSERT_EQ(VirtualClusterBundleResourceLabel::ParseFromIndexed(
                "CPU_vc_0_4482dec0faaf5ead891ff1659a9501000000")
                ->bundle_index.value(),
            0);
  ASSERT_TRUE(!VirtualClusterBundleResourceLabel::ParseFromWildcard(
      "CPU_vc_0_4482dec0faaf5ead891ff1659a9501000000"));

  // Parse incorrect resource parsing.
  ASSERT_TRUE(!VirtualClusterBundleResourceLabel::ParseFromEither("CPU"));
  ASSERT_TRUE(!VirtualClusterBundleResourceLabel::ParseFromIndexed("CPU"));
  ASSERT_TRUE(!VirtualClusterBundleResourceLabel::ParseFromWildcard("CPU"));

  // Parse wildcard resources.
  ASSERT_EQ(VirtualClusterBundleResourceLabel::ParseFromWildcard(
                "CPU_vc_4482dec0faaf5ead891ff1659a9501000000")
                ->original_resource,
            "CPU");
  ASSERT_EQ(VirtualClusterBundleResourceLabel::ParseFromWildcard(
                "custom_vc_4482dec0faaf5ead891ff1659a9501000000")
                ->original_resource,
            "custom");
  ASSERT_EQ(VirtualClusterBundleResourceLabel::ParseFromWildcard(
                "GPU_vc_4482dec0faaf5ead891ff1659a9501000000")
                ->original_resource,
            "GPU");
  ASSERT_EQ(VirtualClusterBundleResourceLabel::ParseFromWildcard(
                "CPU_vc_4482dec0faaf5ead891ff1659a9501000000")
                ->bundle_index,
            std::nullopt);
  ASSERT_TRUE(!VirtualClusterBundleResourceLabel::ParseFromIndexed(
      "CPU_vc_4482dec0faaf5ead891ff1659a9501000000"));
}

TEST_F(VirtualClusterResourceManagerTest, TestPrepareBundleResource) {
  // 1. create bundle spec.
  auto cluster_id = VirtualClusterID::Of(JobID::FromInt(1));
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto bundle_specs = Mocker::GenVirtualClusterBundleSpecs(cluster_id, unit_resource, 1);
  /// 2. init local available resource.
  InitLocalAvailableResource(unit_resource);
  /// 3. prepare bundle resource.
  ASSERT_TRUE(virtual_cluster_resource_manager_->PrepareBundles(bundle_specs));
  /// 4. check remaining resources is correct.
  CheckAvailableResourceEmpty("CPU");
}

TEST_F(VirtualClusterResourceManagerTest, TestPrepareBundleWithInsufficientResource) {
  // 1. create bundle spec.
  auto cluster_id = VirtualClusterID::Of(JobID::FromInt(1));
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 2.0});
  auto bundle_specs = Mocker::GenVirtualClusterBundleSpecs(cluster_id, unit_resource, 1);
  /// 2. init local available resource.
  absl::flat_hash_map<std::string, double> init_unit_resource;
  init_unit_resource.insert({"CPU", 1.0});
  InitLocalAvailableResource(init_unit_resource);
  /// 3. prepare bundle resource.
  ASSERT_FALSE(virtual_cluster_resource_manager_->PrepareBundles(bundle_specs));
}

TEST_F(VirtualClusterResourceManagerTest, TestPrepareBundleDuringDraining) {
  // 1. create bundle spec.
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto cluster1_id = VirtualClusterID::Of(JobID::FromInt(1));
  auto bundle1_specs =
      Mocker::GenVirtualClusterBundleSpecs(cluster1_id, unit_resource, 1);
  auto cluster2_id = VirtualClusterID::Of(JobID::FromInt(2));
  auto bundle2_specs =
      Mocker::GenVirtualClusterBundleSpecs(cluster2_id, unit_resource, 1);
  /// 2. init local available resource.
  absl::flat_hash_map<std::string, double> init_unit_resource;
  init_unit_resource.insert({"CPU", 2.0});
  InitLocalAvailableResource(init_unit_resource);

  ASSERT_TRUE(virtual_cluster_resource_manager_->PrepareBundles(bundle1_specs));
  // Drain the node,  bundle prepare will fail.
  cluster_resource_scheduler_->GetLocalResourceManager().SetLocalNodeDraining();
  ASSERT_FALSE(virtual_cluster_resource_manager_->PrepareBundles(bundle2_specs));
  // Prepared bundles can still be committed.
  virtual_cluster_resource_manager_->CommitBundles(bundle1_specs);
  absl::flat_hash_map<std::string, double> remaining_resources = {
      {"CPU_vc_" + cluster1_id.Hex(), 1.0},
      {"CPU_vc_1_" + cluster1_id.Hex(), 1.0},
      {"CPU", 2.0},
      {"vcbundle_vc_1_" + cluster1_id.Hex(), 1000},
      {"vcbundle_vc_" + cluster1_id.Hex(), 1000}};
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
  auto cluster_id = VirtualClusterID::Of(JobID::FromInt(1));
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto bundle_specs = Mocker::GenVirtualClusterBundleSpecs(cluster_id, unit_resource, 1);
  /// 2. init local available resource.
  InitLocalAvailableResource(unit_resource);
  /// 3. prepare and commit bundle resource.
  ASSERT_TRUE(virtual_cluster_resource_manager_->PrepareBundles(bundle_specs));
  virtual_cluster_resource_manager_->CommitBundles(bundle_specs);
  /// 4. check remaining resources is correct.
  absl::flat_hash_map<std::string, double> remaining_resources = {
      {"CPU_vc_" + cluster_id.Hex(), 1.0},
      {"CPU_vc_1_" + cluster_id.Hex(), 1.0},
      {"CPU", 1.0},
      {"vcbundle_vc_1_" + cluster_id.Hex(), 1000},
      {"vcbundle_vc_" + cluster_id.Hex(), 1000}};
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
  auto cluster_id = VirtualClusterID::Of(JobID::FromInt(1));
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto bundle_spec = Mocker::GenVirtualClusterBundle(cluster_id, 1, unit_resource);
  /// 2. init local available resource.
  InitLocalAvailableResource(unit_resource);
  /// 3. prepare and commit bundle resource.
  ASSERT_TRUE(virtual_cluster_resource_manager_->PrepareBundles(
      ConvertSingleSpecToVectorPtrs(bundle_spec)));
  virtual_cluster_resource_manager_->CommitBundles(
      ConvertSingleSpecToVectorPtrs(bundle_spec));
  /// 4. return bundle resource.
  virtual_cluster_resource_manager_->ReturnBundle(bundle_spec);
  /// 5. check remaining resources is correct.
  auto remaining_resource_scheduler = std::make_shared<ClusterResourceScheduler>(
      io_context, scheduling::NodeID("remaining"), unit_resource, is_node_available_fn_);
  auto remaining_resource_instance =
      remaining_resource_scheduler->GetClusterResourceManager().GetNodeResources(
          scheduling::NodeID("remaining"));
  CheckRemainingResourceCorrect(remaining_resource_instance);
}

TEST_F(VirtualClusterResourceManagerTest, TestMultipleBundlesCommitAndReturn) {
  // 1. create two bundles spec.
  auto cluster_id = VirtualClusterID::Of(JobID::FromInt(1));
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto first_bundle_spec = Mocker::GenVirtualClusterBundle(cluster_id, 1, unit_resource);
  auto second_bundle_spec = Mocker::GenVirtualClusterBundle(cluster_id, 2, unit_resource);
  /// 2. init local available resource.
  absl::flat_hash_map<std::string, double> init_unit_resource;
  init_unit_resource.insert({"CPU", 2.0});
  InitLocalAvailableResource(init_unit_resource);
  /// 3. prepare and commit two bundle resource.
  ASSERT_TRUE(virtual_cluster_resource_manager_->PrepareBundles(
      ConvertSingleSpecToVectorPtrs(first_bundle_spec)));
  ASSERT_TRUE(virtual_cluster_resource_manager_->PrepareBundles(
      ConvertSingleSpecToVectorPtrs(second_bundle_spec)));
  virtual_cluster_resource_manager_->CommitBundles(
      ConvertSingleSpecToVectorPtrs(first_bundle_spec));
  virtual_cluster_resource_manager_->CommitBundles(
      ConvertSingleSpecToVectorPtrs(second_bundle_spec));
  /// 4. check remaining resources is correct after commit phase.
  absl::flat_hash_map<std::string, double> remaining_resources = {
      {"CPU_vc_" + cluster_id.Hex(), 2.0},
      {"CPU_vc_1_" + cluster_id.Hex(), 1.0},
      {"CPU_vc_2_" + cluster_id.Hex(), 1.0},
      {"CPU", 2.0},
      {"vcbundle_vc_1_" + cluster_id.Hex(), 1000},
      {"vcbundle_vc_2_" + cluster_id.Hex(), 1000},
      {"vcbundle_vc_" + cluster_id.Hex(), 2000}};
  auto remaining_resource_scheduler =
      std::make_shared<ClusterResourceScheduler>(io_context,
                                                 scheduling::NodeID("remaining"),
                                                 remaining_resources,
                                                 is_node_available_fn_);
  std::shared_ptr<TaskResourceInstances> resource_instances =
      std::make_shared<TaskResourceInstances>();
  ASSERT_TRUE(
      remaining_resource_scheduler->GetLocalResourceManager().AllocateLocalTaskResources(
          init_unit_resource, resource_instances));
  auto remaining_resource_instance =
      remaining_resource_scheduler->GetClusterResourceManager().GetNodeResources(
          scheduling::NodeID("remaining"));

  CheckRemainingResourceCorrect(remaining_resource_instance);
  /// 5. return second bundle.
  virtual_cluster_resource_manager_->ReturnBundle(second_bundle_spec);
  /// 6. check remaining resources is correct after return second bundle.
  remaining_resources = {{"CPU_vc_" + cluster_id.Hex(), 2.0},
                         {"CPU_vc_1_" + cluster_id.Hex(), 1.0},
                         {"CPU", 2.0},
                         {"vcbundle_vc_1_" + cluster_id.Hex(), 1000},
                         {"vcbundle_vc_" + cluster_id.Hex(), 2000}};
  remaining_resource_scheduler =
      std::make_shared<ClusterResourceScheduler>(io_context,
                                                 scheduling::NodeID("remaining"),
                                                 remaining_resources,
                                                 is_node_available_fn_);
  ASSERT_TRUE(
      remaining_resource_scheduler->GetLocalResourceManager().AllocateLocalTaskResources(
          {{"CPU_vc_" + cluster_id.Hex(), 1.0},
           {"CPU", 1.0},
           {"vcbundle_vc_" + cluster_id.Hex(), 1000}},
          resource_instances));
  remaining_resource_instance =
      remaining_resource_scheduler->GetClusterResourceManager().GetNodeResources(
          scheduling::NodeID("remaining"));
  CheckRemainingResourceCorrect(remaining_resource_instance);
  /// 7. return first bundle.
  virtual_cluster_resource_manager_->ReturnBundle(first_bundle_spec);
  /// 8. check remaining resources is correct after all bundle returned.
  remaining_resources = {{"CPU", 2.0}};
  remaining_resource_scheduler =
      std::make_shared<ClusterResourceScheduler>(io_context,
                                                 scheduling::NodeID("remaining"),
                                                 remaining_resources,
                                                 is_node_available_fn_);
  remaining_resource_instance =
      remaining_resource_scheduler->GetClusterResourceManager().GetNodeResources(
          scheduling::NodeID("remaining"));
  CheckRemainingResourceCorrect(remaining_resource_instance);
}

TEST_F(VirtualClusterResourceManagerTest, TestIdempotencyWithMultiPrepare) {
  // 1. create one bundle spec.
  auto cluster_id = VirtualClusterID::Of(JobID::FromInt(1));
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto bundle_specs = Mocker::GenVirtualClusterBundleSpecs(cluster_id, unit_resource, 1);
  /// 2. init local available resource.
  absl::flat_hash_map<std::string, double> available_resource = {
      std::make_pair("CPU", 3.0)};
  InitLocalAvailableResource(available_resource);
  /// 3. prepare bundle resource 10 times.
  for (int i = 0; i < 10; i++) {
    ASSERT_TRUE(virtual_cluster_resource_manager_->PrepareBundles(bundle_specs));
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
  auto cluster_id = VirtualClusterID::Of(JobID::FromInt(1));
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto bundle_spec = Mocker::GenVirtualClusterBundle(cluster_id, 1, unit_resource);
  /// 2. init local available resource.
  absl::flat_hash_map<std::string, double> available_resource = {
      std::make_pair("CPU", 3.0)};
  InitLocalAvailableResource(available_resource);
  /// 3. prepare bundle -> commit bundle -> prepare bundle.
  ASSERT_TRUE(virtual_cluster_resource_manager_->PrepareBundles(
      ConvertSingleSpecToVectorPtrs(bundle_spec)));
  virtual_cluster_resource_manager_->CommitBundles(
      ConvertSingleSpecToVectorPtrs(bundle_spec));
  ASSERT_TRUE(virtual_cluster_resource_manager_->PrepareBundles(
      ConvertSingleSpecToVectorPtrs(bundle_spec)));
  /// 4. check remaining resources is correct.
  absl::flat_hash_map<std::string, double> remaining_resources = {
      {"CPU_vc_" + cluster_id.Hex(), 1.0},
      {"CPU_vc_1_" + cluster_id.Hex(), 1.0},
      {"CPU", 3.0},
      {"vcbundle_vc_1_" + cluster_id.Hex(), 1000},
      {"vcbundle_vc_" + cluster_id.Hex(), 1000}};
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
  virtual_cluster_resource_manager_->ReturnBundle(bundle_spec);
  // 5. prepare bundle -> commit bundle -> commit bundle.
  ASSERT_TRUE(virtual_cluster_resource_manager_->PrepareBundles(
      ConvertSingleSpecToVectorPtrs(bundle_spec)));
  virtual_cluster_resource_manager_->CommitBundles(
      ConvertSingleSpecToVectorPtrs(bundle_spec));
  virtual_cluster_resource_manager_->CommitBundles(
      ConvertSingleSpecToVectorPtrs(bundle_spec));
  // 6. check remaining resources is correct.
  CheckRemainingResourceCorrect(remaining_resource_instance);
  virtual_cluster_resource_manager_->ReturnBundle(bundle_spec);
  // 7. prepare bundle -> return bundle -> commit bundle.
  ASSERT_TRUE(virtual_cluster_resource_manager_->PrepareBundles(
      ConvertSingleSpecToVectorPtrs(bundle_spec)));
  virtual_cluster_resource_manager_->ReturnBundle(bundle_spec);
  virtual_cluster_resource_manager_->CommitBundles(
      ConvertSingleSpecToVectorPtrs(bundle_spec));
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

TEST_F(VirtualClusterResourceManagerTest, TestPreparedResourceBatched) {
  // 1. create a virtual cluster spec with 4 bundles and each required 1 CPU.
  auto cluster_id = VirtualClusterID::Of(JobID::FromInt(1));
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto bundle_specs = Mocker::GenVirtualClusterBundleSpecs(cluster_id, unit_resource, 4);
  // 2. init local available resource with 3 CPUs.
  absl::flat_hash_map<std::string, double> available_resource = {
      std::make_pair("CPU", 3.0)};
  InitLocalAvailableResource(available_resource);
  // 3. prepare resources for the four bundles.
  ASSERT_FALSE(virtual_cluster_resource_manager_->PrepareBundles(bundle_specs));
  // make sure it keeps Idempotency.
  ASSERT_FALSE(virtual_cluster_resource_manager_->PrepareBundles(bundle_specs));
  // 4. check remaining resources is correct.
  absl::flat_hash_map<std::string, double> remaining_resources = {{"CPU", 3.0}};
  auto remaining_resource_scheduler =
      std::make_shared<ClusterResourceScheduler>(io_context,
                                                 scheduling::NodeID("remaining"),
                                                 remaining_resources,
                                                 is_node_available_fn_);
  auto remaining_resource_instance =
      remaining_resource_scheduler->GetClusterResourceManager().GetNodeResources(
          scheduling::NodeID("remaining"));
  CheckRemainingResourceCorrect(remaining_resource_instance);
  // 5. re-init the local available resource with 4 CPUs.
  available_resource = {std::make_pair("CPU", 4.0)};
  InitLocalAvailableResource(available_resource);
  // 6. re-prepare resources for the four bundles, but this time it should be
  // successfully.
  ASSERT_TRUE(virtual_cluster_resource_manager_->PrepareBundles(bundle_specs));
  ASSERT_TRUE(virtual_cluster_resource_manager_->PrepareBundles(bundle_specs));
  virtual_cluster_resource_manager_->CommitBundles(bundle_specs);
  // 7. re-check remaining resources is correct.
  remaining_resources = {{"CPU_vc_" + cluster_id.Hex(), 4.0},
                         {"CPU_vc_1_" + cluster_id.Hex(), 1.0},
                         {"CPU_vc_2_" + cluster_id.Hex(), 1.0},
                         {"CPU_vc_3_" + cluster_id.Hex(), 1.0},
                         {"CPU_vc_4_" + cluster_id.Hex(), 1.0},
                         {"CPU", 4.0},
                         {"vcbundle_vc_1_" + cluster_id.Hex(), 1000},
                         {"vcbundle_vc_2_" + cluster_id.Hex(), 1000},
                         {"vcbundle_vc_3_" + cluster_id.Hex(), 1000},
                         {"vcbundle_vc_4_" + cluster_id.Hex(), 1000},
                         {"vcbundle_vc_" + cluster_id.Hex(), 4000}};
  remaining_resource_scheduler =
      std::make_shared<ClusterResourceScheduler>(io_context,
                                                 scheduling::NodeID("remaining"),
                                                 remaining_resources,
                                                 is_node_available_fn_);
  std::shared_ptr<TaskResourceInstances> resource_instances =
      std::make_shared<TaskResourceInstances>();
  absl::flat_hash_map<std::string, double> allocating_resource;
  allocating_resource.insert({"CPU", 4.0});
  ASSERT_TRUE(
      remaining_resource_scheduler->GetLocalResourceManager().AllocateLocalTaskResources(
          allocating_resource, resource_instances));
  remaining_resource_instance =
      remaining_resource_scheduler->GetClusterResourceManager().GetNodeResources(
          scheduling::NodeID("remaining"));
  RAY_LOG(INFO) << "The current local resource view: "
                << cluster_resource_scheduler_->DebugString();
  CheckRemainingResourceCorrect(remaining_resource_instance);
}

TEST_F(VirtualClusterResourceManagerTest, TestCommiteResourceBatched) {
  // 1. create a virtual cluster spec with 4 bundles and each required 1 CPU.
  auto cluster_id = VirtualClusterID::Of(JobID::FromInt(1));
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto bundle_specs = Mocker::GenVirtualClusterBundleSpecs(cluster_id, unit_resource, 4);
  // 2. init local available resource with 4 CPUs.
  absl::flat_hash_map<std::string, double> available_resource = {
      std::make_pair("CPU", 4.0)};
  InitLocalAvailableResource(available_resource);
  // 3. prepare resources for the four bundles and make sure it succeeds.
  ASSERT_TRUE(virtual_cluster_resource_manager_->PrepareBundles(bundle_specs));
  // 4. prepare resources for the four bundles.
  virtual_cluster_resource_manager_->CommitBundles(bundle_specs);
  // make sure it keeps Idempotency.
  virtual_cluster_resource_manager_->CommitBundles(bundle_specs);
  // 5. check remaining resources is correct.
  absl::flat_hash_map<std::string, double> remaining_resources = {
      {"CPU_vc_" + cluster_id.Hex(), 4.0},
      {"CPU_vc_1_" + cluster_id.Hex(), 1.0},
      {"CPU_vc_2_" + cluster_id.Hex(), 1.0},
      {"CPU_vc_3_" + cluster_id.Hex(), 1.0},
      {"CPU_vc_4_" + cluster_id.Hex(), 1.0},
      {"CPU", 4.0},
      {"vcbundle_vc_1_" + cluster_id.Hex(), 1000},
      {"vcbundle_vc_2_" + cluster_id.Hex(), 1000},
      {"vcbundle_vc_3_" + cluster_id.Hex(), 1000},
      {"vcbundle_vc_4_" + cluster_id.Hex(), 1000},
      {"vcbundle_vc_" + cluster_id.Hex(), 4000}};
  auto remaining_resource_scheduler =
      std::make_shared<ClusterResourceScheduler>(io_context,
                                                 scheduling::NodeID("remaining"),
                                                 remaining_resources,
                                                 is_node_available_fn_);
  std::shared_ptr<TaskResourceInstances> resource_instances =
      std::make_shared<TaskResourceInstances>();
  absl::flat_hash_map<std::string, double> allocating_resource;
  allocating_resource.insert({"CPU", 4.0});
  ASSERT_TRUE(
      remaining_resource_scheduler->GetLocalResourceManager().AllocateLocalTaskResources(
          allocating_resource, resource_instances));
  auto remaining_resource_instance =
      remaining_resource_scheduler->GetClusterResourceManager().GetNodeResources(
          scheduling::NodeID("remaining"));
  RAY_LOG(INFO) << "The current local resource view: "
                << cluster_resource_scheduler_->DebugString();
  CheckRemainingResourceCorrect(remaining_resource_instance);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
