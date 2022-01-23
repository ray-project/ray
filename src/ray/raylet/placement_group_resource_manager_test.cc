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
#include "ray/raylet/placement_group_resource_manager.h"

#include <memory>

#include "gtest/gtest.h"
#include "ray/common/bundle_spec.h"
#include "ray/common/id.h"
#include "ray/common/task/scheduling_resources.h"
#include "ray/gcs/test/gcs_test_util.h"
#include "mock/ray/gcs/gcs_client/gcs_client.h"
// clang-format on

namespace ray {

class NewPlacementGroupResourceManagerTest : public ::testing::Test {
 public:
  std::unique_ptr<raylet::NewPlacementGroupResourceManager>
      new_placement_group_resource_manager_;
  std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler_;
  std::unique_ptr<gcs::MockGcsClient> gcs_client_;
  rpc::GcsNodeInfo node_info_;
  void SetUp() {
    gcs_client_ = std::make_unique<gcs::MockGcsClient>();
    EXPECT_CALL(*gcs_client_->mock_node_accessor, Get(::testing::_, ::testing::_))
        .WillRepeatedly(::testing::Return(&node_info_));
  }
  void InitLocalAvailableResource(
      absl::flat_hash_map<std::string, double> &unit_resource) {
    cluster_resource_scheduler_ =
        std::make_shared<ClusterResourceScheduler>("local", unit_resource, *gcs_client_);
    new_placement_group_resource_manager_.reset(
        new raylet::NewPlacementGroupResourceManager(
            cluster_resource_scheduler_,
            [this](const ray::gcs::NodeResourceInfoAccessor::ResourceMap &resources) {
              update_called_ = true;
            },
            [this](const std::vector<std::string> &resource_names) {
              delete_called_ = true;
            }));
  }

  void CheckAvailableResoueceEmpty(const std::string &resource) {
    ASSERT_TRUE(cluster_resource_scheduler_->IsAvailableResourceEmpty(resource));
  }

  void CheckRemainingResourceCorrect(NodeResources &node_resources) {
    auto local_node_resource = cluster_resource_scheduler_->GetLocalNodeResources();
    ASSERT_TRUE(local_node_resource == node_resources);
  }

  // TODO(@clay4444): Remove this once we did the batch rpc request refactor!
  std::vector<std::shared_ptr<const BundleSpecification>> ConvertSingleSpecToVectorPtrs(
      BundleSpecification bundle_spec) {
    std::vector<std::shared_ptr<const BundleSpecification>> bundle_specs;
    bundle_specs.push_back(
        std::make_shared<const BundleSpecification>(std::move(bundle_spec)));
    return bundle_specs;
  }

  bool update_called_ = false;
  bool delete_called_ = false;
};

TEST_F(NewPlacementGroupResourceManagerTest, TestNewPrepareBundleResource) {
  // 1. create bundle spec.
  auto group_id = PlacementGroupID::FromRandom();
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto bundle_spec = Mocker::GenBundleCreation(group_id, 1, unit_resource);
  /// 2. init local available resource.
  InitLocalAvailableResource(unit_resource);
  /// 3. prepare bundle resource.
  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundles(
      ConvertSingleSpecToVectorPtrs(bundle_spec)));
  /// 4. check remaining resources is correct.
  CheckAvailableResoueceEmpty("CPU");
}

TEST_F(NewPlacementGroupResourceManagerTest,
       TestNewPrepareBundleWithInsufficientResource) {
  // 1. create bundle spec.
  auto group_id = PlacementGroupID::FromRandom();
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 2.0});
  auto bundle_spec = Mocker::GenBundleCreation(group_id, 1, unit_resource);
  /// 2. init local available resource.
  absl::flat_hash_map<std::string, double> init_unit_resource;
  init_unit_resource.insert({"CPU", 1.0});
  InitLocalAvailableResource(init_unit_resource);
  /// 3. prepare bundle resource.
  ASSERT_FALSE(new_placement_group_resource_manager_->PrepareBundles(
      ConvertSingleSpecToVectorPtrs(bundle_spec)));
}

TEST_F(NewPlacementGroupResourceManagerTest, TestNewCommitBundleResource) {
  // 1. create bundle spec.
  auto group_id = PlacementGroupID::FromRandom();
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto bundle_spec = Mocker::GenBundleCreation(group_id, 1, unit_resource);
  /// 2. init local available resource.
  InitLocalAvailableResource(unit_resource);
  /// 3. prepare and commit bundle resource.
  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundles(
      ConvertSingleSpecToVectorPtrs(bundle_spec)));
  ASSERT_FALSE(update_called_);
  new_placement_group_resource_manager_->CommitBundle(bundle_spec);
  ASSERT_TRUE(update_called_);
  /// 4. check remaining resources is correct.
  absl::flat_hash_map<std::string, double> remaining_resources = {
      {"CPU_group_" + group_id.Hex(), 1.0},
      {"CPU_group_1_" + group_id.Hex(), 1.0},
      {"CPU", 1.0},
      {"bundle_group_1_" + group_id.Hex(), 1000},
      {"bundle_group_" + group_id.Hex(), 1000}};
  auto remaining_resource_scheduler = std::make_shared<ClusterResourceScheduler>(
      "remaining", remaining_resources, *gcs_client_);
  std::shared_ptr<TaskResourceInstances> resource_instances =
      std::make_shared<TaskResourceInstances>();
  ASSERT_TRUE(remaining_resource_scheduler->AllocateLocalTaskResources(
      unit_resource, resource_instances));
  auto remaining_resource_instance =
      remaining_resource_scheduler->GetLocalNodeResources();
  CheckRemainingResourceCorrect(remaining_resource_instance);
}

TEST_F(NewPlacementGroupResourceManagerTest, TestNewReturnBundleResource) {
  // 1. create bundle spec.
  auto group_id = PlacementGroupID::FromRandom();
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto bundle_spec = Mocker::GenBundleCreation(group_id, 1, unit_resource);
  /// 2. init local available resource.
  InitLocalAvailableResource(unit_resource);
  /// 3. prepare and commit bundle resource.
  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundles(
      ConvertSingleSpecToVectorPtrs(bundle_spec)));
  ASSERT_FALSE(update_called_);
  new_placement_group_resource_manager_->CommitBundle(bundle_spec);
  ASSERT_TRUE(update_called_);
  /// 4. return bundle resource.
  ASSERT_FALSE(delete_called_);
  new_placement_group_resource_manager_->ReturnBundle(bundle_spec);
  ASSERT_TRUE(delete_called_);
  /// 5. check remaining resources is correct.
  auto remaining_resource_scheduler = std::make_shared<ClusterResourceScheduler>(
      "remaining", unit_resource, *gcs_client_);
  auto remaining_resource_instance =
      remaining_resource_scheduler->GetLocalNodeResources();
  CheckRemainingResourceCorrect(remaining_resource_instance);
}

TEST_F(NewPlacementGroupResourceManagerTest, TestNewMultipleBundlesCommitAndReturn) {
  // 1. create two bundles spec.
  auto group_id = PlacementGroupID::FromRandom();
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto first_bundle_spec = Mocker::GenBundleCreation(group_id, 1, unit_resource);
  auto second_bundle_spec = Mocker::GenBundleCreation(group_id, 2, unit_resource);
  /// 2. init local available resource.
  absl::flat_hash_map<std::string, double> init_unit_resource;
  init_unit_resource.insert({"CPU", 2.0});
  InitLocalAvailableResource(init_unit_resource);
  /// 3. prepare and commit two bundle resource.
  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundles(
      ConvertSingleSpecToVectorPtrs(first_bundle_spec)));
  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundles(
      ConvertSingleSpecToVectorPtrs(second_bundle_spec)));
  ASSERT_FALSE(update_called_);
  ASSERT_FALSE(delete_called_);
  new_placement_group_resource_manager_->CommitBundle(first_bundle_spec);
  new_placement_group_resource_manager_->CommitBundle(second_bundle_spec);
  ASSERT_TRUE(update_called_);
  ASSERT_FALSE(delete_called_);
  /// 4. check remaining resources is correct after commit phase.
  absl::flat_hash_map<std::string, double> remaining_resources = {
      {"CPU_group_" + group_id.Hex(), 2.0},
      {"CPU_group_1_" + group_id.Hex(), 1.0},
      {"CPU_group_2_" + group_id.Hex(), 1.0},
      {"CPU", 2.0},
      {"bundle_group_1_" + group_id.Hex(), 1000},
      {"bundle_group_2_" + group_id.Hex(), 1000},
      {"bundle_group_" + group_id.Hex(), 2000}};
  auto remaining_resource_scheduler = std::make_shared<ClusterResourceScheduler>(
      "remaining", remaining_resources, *gcs_client_);
  std::shared_ptr<TaskResourceInstances> resource_instances =
      std::make_shared<TaskResourceInstances>();
  ASSERT_TRUE(remaining_resource_scheduler->AllocateLocalTaskResources(
      init_unit_resource, resource_instances));
  auto remaining_resource_instance =
      remaining_resource_scheduler->GetLocalNodeResources();

  CheckRemainingResourceCorrect(remaining_resource_instance);
  /// 5. return second bundle.
  ASSERT_TRUE(update_called_);
  ASSERT_FALSE(delete_called_);
  new_placement_group_resource_manager_->ReturnBundle(second_bundle_spec);
  ASSERT_TRUE(update_called_);
  ASSERT_TRUE(delete_called_);
  /// 6. check remaining resources is correct after return second bundle.
  remaining_resources = {{"CPU_group_" + group_id.Hex(), 2.0},
                         {"CPU_group_1_" + group_id.Hex(), 1.0},
                         {"CPU", 2.0},
                         {"bundle_group_1_" + group_id.Hex(), 1000},
                         {"bundle_group_" + group_id.Hex(), 2000}};
  remaining_resource_scheduler = std::make_shared<ClusterResourceScheduler>(
      "remaining", remaining_resources, *gcs_client_);
  ASSERT_TRUE(remaining_resource_scheduler->AllocateLocalTaskResources(
      {{"CPU_group_" + group_id.Hex(), 1.0},
       {"CPU", 1.0},
       {"bundle_group_" + group_id.Hex(), 1000}},
      resource_instances));
  remaining_resource_instance = remaining_resource_scheduler->GetLocalNodeResources();
  CheckRemainingResourceCorrect(remaining_resource_instance);
  /// 7. return first bundle.
  new_placement_group_resource_manager_->ReturnBundle(first_bundle_spec);
  /// 8. check remaining resources is correct after all bundle returned.
  remaining_resources = {{"CPU", 2.0}};
  remaining_resource_scheduler = std::make_shared<ClusterResourceScheduler>(
      "remaining", remaining_resources, *gcs_client_);
  remaining_resource_instance = remaining_resource_scheduler->GetLocalNodeResources();
  ASSERT_TRUE(update_called_);
  ASSERT_TRUE(delete_called_);
  CheckRemainingResourceCorrect(remaining_resource_instance);
}

TEST_F(NewPlacementGroupResourceManagerTest, TestNewIdempotencyWithMultiPrepare) {
  // 1. create one bundle spec.
  auto group_id = PlacementGroupID::FromRandom();
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto bundle_spec = Mocker::GenBundleCreation(group_id, 1, unit_resource);
  /// 2. init local available resource.
  absl::flat_hash_map<std::string, double> available_resource = {
      std::make_pair("CPU", 3.0)};
  InitLocalAvailableResource(available_resource);
  /// 3. prepare bundle resource 10 times.
  for (int i = 0; i < 10; i++) {
    new_placement_group_resource_manager_->PrepareBundles(
        ConvertSingleSpecToVectorPtrs(bundle_spec));
  }
  /// 4. check remaining resources is correct.
  absl::flat_hash_map<std::string, double> remaining_resources = {{"CPU", 3.0}};
  auto remaining_resource_scheduler = std::make_shared<ClusterResourceScheduler>(
      "remaining", remaining_resources, *gcs_client_);
  std::shared_ptr<TaskResourceInstances> resource_instances =
      std::make_shared<TaskResourceInstances>();
  ASSERT_TRUE(remaining_resource_scheduler->AllocateLocalTaskResources(
      unit_resource, resource_instances));
  auto remaining_resource_instance =
      remaining_resource_scheduler->GetLocalNodeResources();
  CheckRemainingResourceCorrect(remaining_resource_instance);
}

TEST_F(NewPlacementGroupResourceManagerTest, TestNewIdempotencyWithRandomOrder) {
  // 1. create one bundle spec.
  auto group_id = PlacementGroupID::FromRandom();
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto bundle_spec = Mocker::GenBundleCreation(group_id, 1, unit_resource);
  /// 2. init local available resource.
  absl::flat_hash_map<std::string, double> available_resource = {
      std::make_pair("CPU", 3.0)};
  InitLocalAvailableResource(available_resource);
  /// 3. prepare bundle -> commit bundle -> prepare bundle.
  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundles(
      ConvertSingleSpecToVectorPtrs(bundle_spec)));
  new_placement_group_resource_manager_->CommitBundle(bundle_spec);
  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundles(
      ConvertSingleSpecToVectorPtrs(bundle_spec)));
  /// 4. check remaining resources is correct.
  absl::flat_hash_map<std::string, double> remaining_resources = {
      {"CPU_group_" + group_id.Hex(), 1.0},
      {"CPU_group_1_" + group_id.Hex(), 1.0},
      {"CPU", 3.0},
      {"bundle_group_1_" + group_id.Hex(), 1000},
      {"bundle_group_" + group_id.Hex(), 1000}};
  auto remaining_resource_scheduler = std::make_shared<ClusterResourceScheduler>(
      "remaining", remaining_resources, *gcs_client_);
  std::shared_ptr<TaskResourceInstances> resource_instances =
      std::make_shared<TaskResourceInstances>();
  ASSERT_TRUE(remaining_resource_scheduler->AllocateLocalTaskResources(
      unit_resource, resource_instances));
  auto remaining_resource_instance =
      remaining_resource_scheduler->GetLocalNodeResources();
  CheckRemainingResourceCorrect(remaining_resource_instance);
  new_placement_group_resource_manager_->ReturnBundle(bundle_spec);
  // 5. prepare bundle -> commit bundle -> commit bundle.
  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundles(
      ConvertSingleSpecToVectorPtrs(bundle_spec)));
  new_placement_group_resource_manager_->CommitBundle(bundle_spec);
  new_placement_group_resource_manager_->CommitBundle(bundle_spec);
  // 6. check remaining resources is correct.
  CheckRemainingResourceCorrect(remaining_resource_instance);
  new_placement_group_resource_manager_->ReturnBundle(bundle_spec);
  // 7. prepare bundle -> return bundle -> commit bundle.
  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundles(
      ConvertSingleSpecToVectorPtrs(bundle_spec)));
  new_placement_group_resource_manager_->ReturnBundle(bundle_spec);
  new_placement_group_resource_manager_->CommitBundle(bundle_spec);
  // 8. check remaining resources is correct.
  remaining_resource_scheduler = std::make_shared<ClusterResourceScheduler>(
      "remaining", available_resource, *gcs_client_);
  remaining_resource_instance = remaining_resource_scheduler->GetLocalNodeResources();
  CheckRemainingResourceCorrect(remaining_resource_instance);
}

TEST_F(NewPlacementGroupResourceManagerTest, TestPreparedResourceBatched) {
  // 1. create a placement group spec with 4 bundles and each required 1 CPU.
  auto group_id = PlacementGroupID::FromRandom();
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto bundle_specs = Mocker::GenBundleSpecifications(group_id, unit_resource, 4);
  // 2. init local available resource with 3 CPUs.
  absl::flat_hash_map<std::string, double> available_resource = {
      std::make_pair("CPU", 3.0)};
  InitLocalAvailableResource(available_resource);
  // 3. prepare resources for the four bundles.
  ASSERT_FALSE(new_placement_group_resource_manager_->PrepareBundles(bundle_specs));
  // make sure it keeps Idempotency.
  ASSERT_FALSE(new_placement_group_resource_manager_->PrepareBundles(bundle_specs));
  // 4. check remaining resources is correct.
  absl::flat_hash_map<std::string, double> remaining_resources = {{"CPU", 3.0}};
  auto remaining_resource_scheduler = std::make_shared<ClusterResourceScheduler>(
      "remaining", remaining_resources, *gcs_client_);
  auto remaining_resource_instance =
      remaining_resource_scheduler->GetLocalNodeResources();
  CheckRemainingResourceCorrect(remaining_resource_instance);
  // 5. re-init the local available resource with 4 CPUs.
  available_resource = {std::make_pair("CPU", 4.0)};
  InitLocalAvailableResource(available_resource);
  // 6. re-prepare resources for the four bundles, but this time it should be
  // successfully.
  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundles(bundle_specs));
  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundles(bundle_specs));
  for (const auto &bundle_spec : bundle_specs) {
    new_placement_group_resource_manager_->CommitBundle(*bundle_spec);
  }
  // 7. re-check remaining resources is correct.
  remaining_resources = {{"CPU_group_" + group_id.Hex(), 4.0},
                         {"CPU_group_1_" + group_id.Hex(), 1.0},
                         {"CPU_group_2_" + group_id.Hex(), 1.0},
                         {"CPU_group_3_" + group_id.Hex(), 1.0},
                         {"CPU_group_4_" + group_id.Hex(), 1.0},
                         {"CPU", 4.0},
                         {"bundle_group_1_" + group_id.Hex(), 1000},
                         {"bundle_group_2_" + group_id.Hex(), 1000},
                         {"bundle_group_3_" + group_id.Hex(), 1000},
                         {"bundle_group_4_" + group_id.Hex(), 1000},
                         {"bundle_group_" + group_id.Hex(), 4000}};
  remaining_resource_scheduler = std::make_shared<ClusterResourceScheduler>(
      "remaining", remaining_resources, *gcs_client_);
  std::shared_ptr<TaskResourceInstances> resource_instances =
      std::make_shared<TaskResourceInstances>();
  absl::flat_hash_map<std::string, double> allocating_resource;
  allocating_resource.insert({"CPU", 4.0});
  ASSERT_TRUE(remaining_resource_scheduler->AllocateLocalTaskResources(
      allocating_resource, resource_instances));
  remaining_resource_instance = remaining_resource_scheduler->GetLocalNodeResources();
  RAY_LOG(INFO) << "The current local resource view: "
                << cluster_resource_scheduler_->DebugString();
  CheckRemainingResourceCorrect(remaining_resource_instance);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
