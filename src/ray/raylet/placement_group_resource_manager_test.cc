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

#include "ray/raylet/placement_group_resource_manager.h"
#include "ray/common/bundle_spec.h"
#include "ray/common/id.h"
#include "ray/common/task/scheduling_resources.h"
#include "ray/gcs/test/gcs_test_util.h"

#include <memory>

#include "gtest/gtest.h"

namespace ray {

class OldPlacementGroupResourceManagerTest : public ::testing::Test {
 public:
  OldPlacementGroupResourceManagerTest() {
    old_placement_group_resource_manager_.reset(
        new raylet::OldPlacementGroupResourceManager(
            local_available_resources_, cluster_resource_map_, self_node_id_));
  }

  std::unique_ptr<raylet::OldPlacementGroupResourceManager>
      old_placement_group_resource_manager_;

  void InitLocalAvailableResource(
      std::unordered_map<std::string, double> &unit_resource) {
    ResourceSet init_resourece(unit_resource);
    cluster_resource_map_[self_node_id_] = SchedulingResources(init_resourece);
    local_available_resources_ = ResourceIdSet(init_resourece);
  }

  void CheckRemainingResourceCorrect(ResourceSet &result_resource) {
    auto &remaining_resource =
        old_placement_group_resource_manager_->GetAllResourceSetWithoutId();
    ASSERT_EQ(1, remaining_resource.GetAvailableResources().IsEqual(result_resource))
        << remaining_resource.GetAvailableResources().ToString() << " vs "
        << result_resource.ToString();
    ASSERT_EQ(1, local_available_resources_.ToResourceSet().IsEqual(result_resource))
        << local_available_resources_.ToResourceSet().ToString() << " vs "
        << result_resource.ToString();
  }

 protected:
  ResourceIdSet local_available_resources_;
  std::unordered_map<NodeID, SchedulingResources> cluster_resource_map_;
  NodeID self_node_id_ = NodeID::FromRandom();
};

TEST_F(OldPlacementGroupResourceManagerTest, TestPrepareBundleResource) {
  // 1. create bundle spec.
  auto group_id = PlacementGroupID::FromRandom();
  std::unordered_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto bundle_spec = Mocker::GenBundleCreation(group_id, 1, unit_resource);
  /// 2. init local available resource.
  InitLocalAvailableResource(unit_resource);
  /// 3. prepare bundle resource.
  old_placement_group_resource_manager_->PrepareBundle(bundle_spec);
  /// 4. check remaining resources is correct.
  auto &remaining_resource =
      old_placement_group_resource_manager_->GetAllResourceSetWithoutId();
  ResourceSet result_resource;
  ASSERT_EQ(0, local_available_resources_.AvailableResources().size());
  ASSERT_EQ(1, remaining_resource.GetAvailableResources().IsEqual(result_resource))
      << remaining_resource.GetAvailableResources().ToString() << " vs "
      << result_resource.ToString();
  ASSERT_EQ(1, local_available_resources_.ToResourceSet().IsEqual(result_resource))
      << local_available_resources_.ToResourceSet().ToString() << " vs "
      << result_resource.ToString();
}

TEST_F(OldPlacementGroupResourceManagerTest, TestPrepareBundleWithInsufficientResource) {
  // 1. create bundle spec.
  auto group_id = PlacementGroupID::FromRandom();
  std::unordered_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 2.0});
  auto bundle_spec = Mocker::GenBundleCreation(group_id, 1, unit_resource);
  /// 2. init local available resource.
  std::unordered_map<std::string, double> init_unit_resource;
  init_unit_resource.insert({"CPU", 1.0});
  InitLocalAvailableResource(init_unit_resource);
  /// 3. prepare bundle resource.
  ASSERT_FALSE(old_placement_group_resource_manager_->PrepareBundle(bundle_spec));
}

TEST_F(OldPlacementGroupResourceManagerTest, TestCommitBundleResource) {
  // 1. create bundle spec.
  auto group_id = PlacementGroupID::FromRandom();
  std::unordered_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto bundle_spec = Mocker::GenBundleCreation(group_id, 1, unit_resource);
  /// 2. init local available resource.
  InitLocalAvailableResource(unit_resource);
  /// 3. prepare and commit bundle resource.
  old_placement_group_resource_manager_->PrepareBundle(bundle_spec);
  old_placement_group_resource_manager_->CommitBundle(bundle_spec);
  /// 4. check remaining resources is correct.
  auto &remaining_resource =
      old_placement_group_resource_manager_->GetAllResourceSetWithoutId();
  std::vector<std::string> resource_labels = {"CPU_group_" + group_id.Hex(),
                                              "CPU_group_1_" + group_id.Hex()};
  std::vector<double> resource_capacity = {1.0, 1.0};
  ResourceSet result_resource(resource_labels, resource_capacity);
  ASSERT_EQ(2, local_available_resources_.AvailableResources().size());
  ASSERT_EQ(1, remaining_resource.GetAvailableResources().IsEqual(result_resource))
      << remaining_resource.GetAvailableResources().ToString() << " vs "
      << result_resource.ToString();
  ASSERT_EQ(1, local_available_resources_.ToResourceSet().IsEqual(result_resource))
      << local_available_resources_.ToResourceSet().ToString() << " vs "
      << result_resource.ToString();
}

TEST_F(OldPlacementGroupResourceManagerTest, TestReturnBundleResource) {
  // 1. create bundle spec.
  auto group_id = PlacementGroupID::FromRandom();
  std::unordered_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto bundle_spec = Mocker::GenBundleCreation(group_id, 1, unit_resource);
  /// 2. init local available resource.
  InitLocalAvailableResource(unit_resource);
  /// 3. prepare and commit bundle resource.
  old_placement_group_resource_manager_->PrepareBundle(bundle_spec);
  old_placement_group_resource_manager_->CommitBundle(bundle_spec);
  /// 4. return bundle resource.
  old_placement_group_resource_manager_->ReturnBundle(bundle_spec);
  /// 5. check remaining resources is correct.
  auto &remaining_resource =
      old_placement_group_resource_manager_->GetAllResourceSetWithoutId();
  ResourceSet result_resource(unit_resource);
  ASSERT_EQ(1, local_available_resources_.AvailableResources().size());
  ASSERT_EQ(1, remaining_resource.GetAvailableResources().IsEqual(result_resource))
      << remaining_resource.GetAvailableResources().ToString() << " vs "
      << result_resource.ToString();
  ASSERT_EQ(1, local_available_resources_.ToResourceSet().IsEqual(result_resource))
      << local_available_resources_.ToResourceSet().ToString() << " vs "
      << result_resource.ToString();
}

TEST_F(OldPlacementGroupResourceManagerTest, TestMultipleBundlesCommitAndReturn) {
  // 1. create two bundles spec.
  auto group_id = PlacementGroupID::FromRandom();
  std::unordered_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto first_bundle_spec = Mocker::GenBundleCreation(group_id, 1, unit_resource);
  auto second_bundle_spec = Mocker::GenBundleCreation(group_id, 2, unit_resource);
  /// 2. init local available resource.
  std::unordered_map<std::string, double> init_unit_resource;
  init_unit_resource.insert({"CPU", 2.0});
  InitLocalAvailableResource(init_unit_resource);
  /// 3. prepare and commit two bundle resource.
  old_placement_group_resource_manager_->PrepareBundle(first_bundle_spec);
  old_placement_group_resource_manager_->PrepareBundle(second_bundle_spec);
  old_placement_group_resource_manager_->CommitBundle(first_bundle_spec);
  old_placement_group_resource_manager_->CommitBundle(second_bundle_spec);
  /// 4. check remaining resources is correct after commit phase.
  auto &remaining_resource =
      old_placement_group_resource_manager_->GetAllResourceSetWithoutId();
  std::vector<std::string> resource_labels = {"CPU_group_" + group_id.Hex(),
                                              "CPU_group_1_" + group_id.Hex(),
                                              "CPU_group_2_" + group_id.Hex()};
  std::vector<double> resource_capacity = {2.0, 1.0, 1.0};
  ResourceSet result_resource(resource_labels, resource_capacity);
  ASSERT_EQ(3, local_available_resources_.AvailableResources().size());
  ASSERT_EQ(1, remaining_resource.GetAvailableResources().IsEqual(result_resource))
      << remaining_resource.GetAvailableResources().ToString() << " vs "
      << result_resource.ToString();
  ASSERT_EQ(1, local_available_resources_.ToResourceSet().IsEqual(result_resource))
      << local_available_resources_.ToResourceSet().ToString() << " vs "
      << result_resource.ToString();
  /// 5. return second bundle.
  old_placement_group_resource_manager_->ReturnBundle(second_bundle_spec);
  /// 6. check remaining resources is correct after return second bundle.
  resource_labels = {"CPU", "CPU_group_" + group_id.Hex(),
                     "CPU_group_1_" + group_id.Hex()};
  resource_capacity = {1.0, 1.0, 1.0};
  result_resource = ResourceSet(resource_labels, resource_capacity);
  ASSERT_EQ(1, remaining_resource.GetAvailableResources().IsEqual(result_resource))
      << remaining_resource.GetAvailableResources().ToString() << " vs "
      << result_resource.ToString();
  ASSERT_EQ(1, local_available_resources_.ToResourceSet().IsEqual(result_resource))
      << local_available_resources_.ToResourceSet().ToString() << " vs "
      << result_resource.ToString();
  /// 7. return first bundel.
  old_placement_group_resource_manager_->ReturnBundle(first_bundle_spec);
  /// 8. check remaining resources is correct after all bundle returned.
  result_resource = ResourceSet(init_unit_resource);
  ASSERT_EQ(1, remaining_resource.GetAvailableResources().IsEqual(result_resource))
      << remaining_resource.GetAvailableResources().ToString() << " vs "
      << result_resource.ToString();
  ASSERT_EQ(1, local_available_resources_.ToResourceSet().IsEqual(result_resource))
      << local_available_resources_.ToResourceSet().ToString() << " vs "
      << result_resource.ToString();
}

TEST_F(OldPlacementGroupResourceManagerTest, TestIdempotencyWithMultiPrepare) {
  // 1. create one bundle spec.
  auto group_id = PlacementGroupID::FromRandom();
  std::unordered_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto bundle_spec = Mocker::GenBundleCreation(group_id, 1, unit_resource);
  /// 2. init local available resource.
  std::unordered_map<std::string, double> available_resource = {
      std::make_pair("CPU", 3.0)};
  InitLocalAvailableResource(available_resource);
  /// 3. prepare bundle resource 10 times.
  for (int i = 0; i < 10; i++) {
    old_placement_group_resource_manager_->PrepareBundle(bundle_spec);
  }
  /// 4. check remaining resources is correct.
  std::unordered_map<std::string, double> result_resource_map = {
      std::make_pair("CPU", 2.0)};
  ResourceSet result_resource(result_resource_map);
  CheckRemainingResourceCorrect(result_resource);
}

TEST_F(OldPlacementGroupResourceManagerTest, TestIdempotencyWithRandomOrder) {
  // 1. create one bundle spec.
  auto group_id = PlacementGroupID::FromRandom();
  std::unordered_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto bundle_spec = Mocker::GenBundleCreation(group_id, 1, unit_resource);
  /// 2. init local available resource.
  std::unordered_map<std::string, double> available_resource = {
      std::make_pair("CPU", 3.0)};
  InitLocalAvailableResource(available_resource);
  /// 3. prepare bundle -> commit bundle -> prepare bundle.
  old_placement_group_resource_manager_->PrepareBundle(bundle_spec);
  old_placement_group_resource_manager_->CommitBundle(bundle_spec);
  old_placement_group_resource_manager_->PrepareBundle(bundle_spec);
  /// 4. check remaining resources is correct.
  std::vector<std::string> resource_labels = {"CPU_group_" + group_id.Hex(),
                                              "CPU_group_1_" + group_id.Hex(), "CPU"};
  std::vector<double> resource_capacity = {1.0, 1.0, 2.0};
  ResourceSet result_resource(resource_labels, resource_capacity);
  CheckRemainingResourceCorrect(result_resource);
  old_placement_group_resource_manager_->ReturnBundle(bundle_spec);
  // 5. prepare bundle -> commit bundle -> commit bundle.
  old_placement_group_resource_manager_->PrepareBundle(bundle_spec);
  old_placement_group_resource_manager_->CommitBundle(bundle_spec);
  old_placement_group_resource_manager_->CommitBundle(bundle_spec);
  // 6. check remaining resources is correct.
  CheckRemainingResourceCorrect(result_resource);
  old_placement_group_resource_manager_->ReturnBundle(bundle_spec);
  // 7. prepare bundle -> return bundle -> commit bundle.
  old_placement_group_resource_manager_->PrepareBundle(bundle_spec);
  old_placement_group_resource_manager_->ReturnBundle(bundle_spec);
  old_placement_group_resource_manager_->CommitBundle(bundle_spec);
  result_resource = ResourceSet(available_resource);
  CheckRemainingResourceCorrect(result_resource);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
