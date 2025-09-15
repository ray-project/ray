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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "mock/ray/gcs_client/gcs_client.h"
#include "ray/common/bundle_spec.h"
#include "ray/common/id.h"
#include "ray/common/scheduling/placement_group_util.h"
#include "ray/common/scheduling/resource_set.h"

namespace ray {

namespace {

BundleSpecification GenBundleCreation(
    const PlacementGroupID &placement_group_id,
    const int bundle_index,
    const absl::flat_hash_map<std::string, double> &unit_resource) {
  rpc::Bundle bundle;
  auto mutable_bundle_id = bundle.mutable_bundle_id();
  mutable_bundle_id->set_bundle_index(bundle_index);
  mutable_bundle_id->set_placement_group_id(placement_group_id.Binary());
  auto mutable_unit_resources = bundle.mutable_unit_resources();
  for (auto &resource : unit_resource) {
    mutable_unit_resources->insert({resource.first, resource.second});
  }
  return BundleSpecification(bundle);
}

std::vector<std::shared_ptr<const BundleSpecification>> GenBundleSpecifications(
    const PlacementGroupID &placement_group_id,
    const absl::flat_hash_map<std::string, double> &unit_resource,
    int bundles_size = 1) {
  std::vector<std::shared_ptr<const BundleSpecification>> bundle_specs;
  for (int i = 0; i < bundles_size; i++) {
    rpc::Bundle bundle;
    auto mutable_bundle_id = bundle.mutable_bundle_id();
    // The bundle index is start from 1.
    mutable_bundle_id->set_bundle_index(i + 1);
    mutable_bundle_id->set_placement_group_id(placement_group_id.Binary());
    auto mutable_unit_resources = bundle.mutable_unit_resources();
    for (auto &resource : unit_resource) {
      mutable_unit_resources->insert({resource.first, resource.second});
    }
    bundle_specs.emplace_back(std::make_shared<BundleSpecification>(bundle));
  }
  return bundle_specs;
}

}  // namespace

class NewPlacementGroupResourceManagerTest : public ::testing::Test {
 public:
  instrumented_io_context io_context;
  std::unique_ptr<raylet::NewPlacementGroupResourceManager>
      new_placement_group_resource_manager_;
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
    new_placement_group_resource_manager_ =
        std::make_unique<raylet::NewPlacementGroupResourceManager>(
            *cluster_resource_scheduler_);
  }

  void CheckAvailableResoueceEmpty(const std::string &resource) {
    ASSERT_TRUE(
        cluster_resource_scheduler_->GetLocalResourceManager().IsAvailableResourceEmpty(
            scheduling::ResourceID(resource)));
  }

  void CheckRemainingResourceCorrect(NodeResources &node_resources) {
    auto local_node_resource =
        cluster_resource_scheduler_->GetClusterResourceManager().GetNodeResources(
            scheduling::NodeID("local"));
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
};

TEST_F(NewPlacementGroupResourceManagerTest,
       TestGetOriginalResourceNameFromWildcardResource) {
  scheduling::ResourceID resourceId = scheduling::ResourceID("GPU");
  ASSERT_EQ(resourceId, ResourceID::GPU());
  ASSERT_EQ(GetOriginalResourceNameFromWildcardResource(
                "CPU_group_0_4482dec0faaf5ead891ff1659a9501000000"),
            "");
  ASSERT_EQ(GetOriginalResourceNameFromWildcardResource("CPU"), "");
  ASSERT_EQ(GetOriginalResourceNameFromWildcardResource(
                "CPU_group_4482dec0faaf5ead891ff1659a9501000000"),
            "CPU");
  ASSERT_EQ(GetOriginalResourceNameFromWildcardResource(
                "GPU_group_4482dec0faaf5ead891ff1659a9501000000"),
            "GPU");
  ASSERT_EQ(GetOriginalResourceNameFromWildcardResource(
                "GPU_group_0_4482dec0faaf5ead891ff1659a9501000000"),
            "");
}

TEST_F(NewPlacementGroupResourceManagerTest, TestParsePgResources) {
  /// Test indexed resources
  ASSERT_EQ(ParsePgFormattedResource(
                "CPU_group_0_4482dec0faaf5ead891ff1659a9501000000", false, true)
                ->original_resource,
            "CPU");
  ASSERT_EQ(ParsePgFormattedResource(
                "custom_group_0_4482dec0faaf5ead891ff1659a9501000000", false, true)
                ->original_resource,
            "custom");
  ASSERT_EQ(ParsePgFormattedResource(
                "GPU_group_0_4482dec0faaf5ead891ff1659a9501000000", false, true)
                ->original_resource,
            "GPU");
  ASSERT_EQ(ParsePgFormattedResource(
                "CPU_group_0_4482dec0faaf5ead891ff1659a9501000000", false, true)
                ->bundle_index,
            0);
  ASSERT_TRUE(!ParsePgFormattedResource(
      "CPU_group_0_4482dec0faaf5ead891ff1659a9501000000", true, false));

  /// Parse incorrect resource parsing.
  ASSERT_TRUE(!ParsePgFormattedResource("CPU", true, true));
  ASSERT_TRUE(!ParsePgFormattedResource("CPU", false, true));
  ASSERT_TRUE(!ParsePgFormattedResource("CPU", true, false));

  /// Parse wildcard resources.
  ASSERT_EQ(ParsePgFormattedResource(
                "CPU_group_4482dec0faaf5ead891ff1659a9501000000", true, false)
                ->original_resource,
            "CPU");
  ASSERT_EQ(ParsePgFormattedResource(
                "custom_group_4482dec0faaf5ead891ff1659a9501000000", true, false)
                ->original_resource,
            "custom");
  ASSERT_EQ(ParsePgFormattedResource(
                "GPU_group_4482dec0faaf5ead891ff1659a9501000000", true, false)
                ->original_resource,
            "GPU");
  ASSERT_EQ(ParsePgFormattedResource(
                "CPU_group_4482dec0faaf5ead891ff1659a9501000000", true, false)
                ->bundle_index,
            -1);
  ASSERT_TRUE(!ParsePgFormattedResource(
      "CPU_group_4482dec0faaf5ead891ff1659a9501000000", false, true));
}

TEST_F(NewPlacementGroupResourceManagerTest, TestNewPrepareBundleResource) {
  // 1. create bundle spec.
  auto group_id = PlacementGroupID::Of(JobID::FromInt(1));
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto bundle_specs = GenBundleSpecifications(group_id, unit_resource, 1);
  /// 2. init local available resource.
  InitLocalAvailableResource(unit_resource);
  /// 3. prepare bundle resource.
  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundles(bundle_specs));
  /// 4. check remaining resources is correct.
  CheckAvailableResoueceEmpty("CPU");
}

TEST_F(NewPlacementGroupResourceManagerTest,
       TestNewPrepareBundleWithInsufficientResource) {
  // 1. create bundle spec.
  auto group_id = PlacementGroupID::Of(JobID::FromInt(1));
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 2.0});
  auto bundle_specs = GenBundleSpecifications(group_id, unit_resource, 1);
  /// 2. init local available resource.
  absl::flat_hash_map<std::string, double> init_unit_resource;
  init_unit_resource.insert({"CPU", 1.0});
  InitLocalAvailableResource(init_unit_resource);
  /// 3. prepare bundle resource.
  ASSERT_FALSE(new_placement_group_resource_manager_->PrepareBundles(bundle_specs));
}

TEST_F(NewPlacementGroupResourceManagerTest, TestNewPrepareBundleDuringDraining) {
  // 1. create bundle spec.
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto group1_id = PlacementGroupID::Of(JobID::FromInt(1));
  auto bundle1_specs = GenBundleSpecifications(group1_id, unit_resource, 1);
  auto group2_id = PlacementGroupID::Of(JobID::FromInt(2));
  auto bundle2_specs = GenBundleSpecifications(group2_id, unit_resource, 1);
  /// 2. init local available resource.
  absl::flat_hash_map<std::string, double> init_unit_resource;
  init_unit_resource.insert({"CPU", 2.0});
  InitLocalAvailableResource(init_unit_resource);

  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundles(bundle1_specs));
  // Drain the node, new bundle prepare will fail.
  rpc::DrainRayletRequest drain_request;
  drain_request.set_deadline_timestamp_ms(std::numeric_limits<int64_t>::max());
  cluster_resource_scheduler_->GetLocalResourceManager().SetLocalNodeDraining(
      drain_request);
  ASSERT_FALSE(new_placement_group_resource_manager_->PrepareBundles(bundle2_specs));
  // Prepared bundles can still be committed.
  new_placement_group_resource_manager_->CommitBundles(bundle1_specs);
  absl::flat_hash_map<std::string, double> remaining_resources_total = {
      {"CPU_group_" + group1_id.Hex(), 1.0},
      {"CPU_group_1_" + group1_id.Hex(), 1.0},
      {"CPU", 2.0},
      {"bundle_group_1_" + group1_id.Hex(), 1000},
      {"bundle_group_" + group1_id.Hex(), 1000}};
  absl::flat_hash_map<std::string, double> remaining_resources_avail = {
      {"CPU_group_" + group1_id.Hex(), 1.0},
      {"CPU_group_1_" + group1_id.Hex(), 1.0},
      {"CPU", 1.0},
      {"bundle_group_1_" + group1_id.Hex(), 1000},
      {"bundle_group_" + group1_id.Hex(), 1000}};
  auto remaining_resource_instance =
      ResourceMapToNodeResources(remaining_resources_total, remaining_resources_avail);
  CheckRemainingResourceCorrect(remaining_resource_instance);
}

TEST_F(NewPlacementGroupResourceManagerTest, TestNewCommitBundleResource) {
  // 1. create bundle spec.
  auto group_id = PlacementGroupID::Of(JobID::FromInt(1));
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto bundle_specs = GenBundleSpecifications(group_id, unit_resource, 1);
  /// 2. init local available resource.
  InitLocalAvailableResource(unit_resource);
  /// 3. prepare and commit bundle resource.
  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundles(bundle_specs));
  new_placement_group_resource_manager_->CommitBundles(bundle_specs);
  /// 4. check remaining resources is correct.
  absl::flat_hash_map<std::string, double> remaining_resources_total = {
      {"CPU_group_" + group_id.Hex(), 1.0},
      {"CPU_group_1_" + group_id.Hex(), 1.0},
      {"CPU", 1.0},
      {"bundle_group_1_" + group_id.Hex(), 1000},
      {"bundle_group_" + group_id.Hex(), 1000}};
  absl::flat_hash_map<std::string, double> remaining_resources_avail = {
      {"CPU_group_" + group_id.Hex(), 1.0},
      {"CPU_group_1_" + group_id.Hex(), 1.0},
      {"CPU", 0.0},
      {"bundle_group_1_" + group_id.Hex(), 1000},
      {"bundle_group_" + group_id.Hex(), 1000}};
  auto remaining_resource_instance =
      ResourceMapToNodeResources(remaining_resources_total, remaining_resources_avail);
  CheckRemainingResourceCorrect(remaining_resource_instance);
}

TEST_F(NewPlacementGroupResourceManagerTest, TestNewReturnBundleResource) {
  // 1. create bundle spec.
  auto group_id = PlacementGroupID::Of(JobID::FromInt(1));
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto bundle_spec = GenBundleCreation(group_id, 1, unit_resource);
  /// 2. init local available resource.
  InitLocalAvailableResource(unit_resource);
  /// 3. prepare and commit bundle resource.
  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundles(
      ConvertSingleSpecToVectorPtrs(bundle_spec)));
  new_placement_group_resource_manager_->CommitBundles(
      ConvertSingleSpecToVectorPtrs(bundle_spec));
  /// 4. return bundle resource.
  ASSERT_TRUE(new_placement_group_resource_manager_->ReturnBundle(bundle_spec).ok());
  /// 5. check remaining resources is correct.
  auto remaining_resource_instance =
      ResourceMapToNodeResources(unit_resource, unit_resource);
  CheckRemainingResourceCorrect(remaining_resource_instance);
}

TEST_F(NewPlacementGroupResourceManagerTest, TestNewMultipleBundlesCommitAndReturn) {
  // 1. create two bundles spec.
  auto group_id = PlacementGroupID::Of(JobID::FromInt(1));
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto first_bundle_spec = GenBundleCreation(group_id, 1, unit_resource);
  auto second_bundle_spec = GenBundleCreation(group_id, 2, unit_resource);
  /// 2. init local available resource.
  absl::flat_hash_map<std::string, double> init_unit_resource;
  init_unit_resource.insert({"CPU", 2.0});
  InitLocalAvailableResource(init_unit_resource);
  /// 3. prepare and commit two bundle resource.
  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundles(
      ConvertSingleSpecToVectorPtrs(first_bundle_spec)));
  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundles(
      ConvertSingleSpecToVectorPtrs(second_bundle_spec)));
  new_placement_group_resource_manager_->CommitBundles(
      ConvertSingleSpecToVectorPtrs(first_bundle_spec));
  new_placement_group_resource_manager_->CommitBundles(
      ConvertSingleSpecToVectorPtrs(second_bundle_spec));
  /// 4. check remaining resources is correct after commit phase.
  absl::flat_hash_map<std::string, double> remaining_resources_total = {
      {"CPU_group_" + group_id.Hex(), 2.0},
      {"CPU_group_1_" + group_id.Hex(), 1.0},
      {"CPU_group_2_" + group_id.Hex(), 1.0},
      {"CPU", 2.0},
      {"bundle_group_1_" + group_id.Hex(), 1000},
      {"bundle_group_2_" + group_id.Hex(), 1000},
      {"bundle_group_" + group_id.Hex(), 2000}};
  absl::flat_hash_map<std::string, double> remaining_resources_avail = {
      {"CPU_group_" + group_id.Hex(), 2.0},
      {"CPU_group_1_" + group_id.Hex(), 1.0},
      {"CPU_group_2_" + group_id.Hex(), 1.0},
      {"CPU", 0.0},
      {"bundle_group_1_" + group_id.Hex(), 1000},
      {"bundle_group_2_" + group_id.Hex(), 1000},
      {"bundle_group_" + group_id.Hex(), 2000}};
  auto remaining_resource_instance =
      ResourceMapToNodeResources(remaining_resources_total, remaining_resources_avail);
  CheckRemainingResourceCorrect(remaining_resource_instance);
  /// 5. return second bundle.
  ASSERT_TRUE(
      new_placement_group_resource_manager_->ReturnBundle(second_bundle_spec).ok());
  /// 6. check remaining resources is correct after return second bundle.
  remaining_resources_total = {{"CPU_group_" + group_id.Hex(), 2.0},
                               {"CPU_group_1_" + group_id.Hex(), 1.0},
                               {"CPU", 2.0},
                               {"bundle_group_1_" + group_id.Hex(), 1000},
                               {"bundle_group_" + group_id.Hex(), 2000}};
  remaining_resources_avail = {{"CPU_group_" + group_id.Hex(), 1.0},
                               {"CPU_group_1_" + group_id.Hex(), 1.0},
                               {"CPU", 1.0},
                               {"bundle_group_1_" + group_id.Hex(), 1000},
                               {"bundle_group_" + group_id.Hex(), 1000}};
  remaining_resource_instance =
      ResourceMapToNodeResources(remaining_resources_total, remaining_resources_avail);
  CheckRemainingResourceCorrect(remaining_resource_instance);
  /// 7. return first bundle.
  ASSERT_TRUE(
      new_placement_group_resource_manager_->ReturnBundle(first_bundle_spec).ok());
  /// 8. check remaining resources is correct after all bundle returned.
  remaining_resources_total = {{"CPU", 2.0}};
  remaining_resource_instance =
      ResourceMapToNodeResources(remaining_resources_total, remaining_resources_total);
  CheckRemainingResourceCorrect(remaining_resource_instance);
}

TEST_F(NewPlacementGroupResourceManagerTest, TestNewIdempotencyWithMultiPrepare) {
  // 1. create one bundle spec.
  auto group_id = PlacementGroupID::Of(JobID::FromInt(1));
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto bundle_specs = GenBundleSpecifications(group_id, unit_resource, 1);
  /// 2. init local available resource.
  absl::flat_hash_map<std::string, double> available_resource = {
      std::make_pair("CPU", 3.0)};
  InitLocalAvailableResource(available_resource);
  /// 3. prepare bundle resource 10 times.
  for (int i = 0; i < 10; i++) {
    ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundles(bundle_specs));
  }
  /// 4. check remaining resources is correct.
  absl::flat_hash_map<std::string, double> remaining_resources_total = {{"CPU", 3.0}};
  absl::flat_hash_map<std::string, double> remaining_resources_avail = {{"CPU", 2.0}};
  auto remaining_resource_instance =
      ResourceMapToNodeResources(remaining_resources_total, remaining_resources_avail);
  CheckRemainingResourceCorrect(remaining_resource_instance);
}

TEST_F(NewPlacementGroupResourceManagerTest, TestNewIdempotencyWithRandomOrder) {
  // 1. create one bundle spec.
  auto group_id = PlacementGroupID::Of(JobID::FromInt(1));
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto bundle_spec = GenBundleCreation(group_id, 1, unit_resource);
  /// 2. init local available resource.
  absl::flat_hash_map<std::string, double> available_resource = {
      std::make_pair("CPU", 3.0)};
  InitLocalAvailableResource(available_resource);
  /// 3. prepare bundle -> commit bundle -> prepare bundle.
  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundles(
      ConvertSingleSpecToVectorPtrs(bundle_spec)));
  new_placement_group_resource_manager_->CommitBundles(
      ConvertSingleSpecToVectorPtrs(bundle_spec));
  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundles(
      ConvertSingleSpecToVectorPtrs(bundle_spec)));
  /// 4. check remaining resources is correct.
  absl::flat_hash_map<std::string, double> remaining_resources_total = {
      {"CPU_group_" + group_id.Hex(), 1.0},
      {"CPU_group_1_" + group_id.Hex(), 1.0},
      {"CPU", 3.0},
      {"bundle_group_1_" + group_id.Hex(), 1000},
      {"bundle_group_" + group_id.Hex(), 1000}};
  absl::flat_hash_map<std::string, double> remaining_resources_avail = {
      {"CPU_group_" + group_id.Hex(), 1.0},
      {"CPU_group_1_" + group_id.Hex(), 1.0},
      {"CPU", 2.0},
      {"bundle_group_1_" + group_id.Hex(), 1000},
      {"bundle_group_" + group_id.Hex(), 1000}};
  auto remaining_resource_instance =
      ResourceMapToNodeResources(remaining_resources_total, remaining_resources_avail);
  CheckRemainingResourceCorrect(remaining_resource_instance);
  ASSERT_TRUE(new_placement_group_resource_manager_->ReturnBundle(bundle_spec).ok());
  // 5. prepare bundle -> commit bundle -> commit bundle.
  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundles(
      ConvertSingleSpecToVectorPtrs(bundle_spec)));
  new_placement_group_resource_manager_->CommitBundles(
      ConvertSingleSpecToVectorPtrs(bundle_spec));
  new_placement_group_resource_manager_->CommitBundles(
      ConvertSingleSpecToVectorPtrs(bundle_spec));
  // 6. check remaining resources is correct.
  CheckRemainingResourceCorrect(remaining_resource_instance);
  ASSERT_TRUE(new_placement_group_resource_manager_->ReturnBundle(bundle_spec).ok());
  // 7. prepare bundle -> return bundle -> commit bundle.
  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundles(
      ConvertSingleSpecToVectorPtrs(bundle_spec)));
  ASSERT_TRUE(new_placement_group_resource_manager_->ReturnBundle(bundle_spec).ok());
  new_placement_group_resource_manager_->CommitBundles(
      ConvertSingleSpecToVectorPtrs(bundle_spec));
  // 8. check remaining resources is correct.
  remaining_resource_instance =
      ResourceMapToNodeResources(available_resource, available_resource);
  CheckRemainingResourceCorrect(remaining_resource_instance);
}

TEST_F(NewPlacementGroupResourceManagerTest, TestPreparedResourceBatched) {
  // 1. create a placement group spec with 4 bundles and each required 1 CPU.
  auto group_id = PlacementGroupID::Of(JobID::FromInt(1));
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto bundle_specs = GenBundleSpecifications(group_id, unit_resource, 4);
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
  auto remaining_resource_instance =
      ResourceMapToNodeResources(remaining_resources, remaining_resources);
  CheckRemainingResourceCorrect(remaining_resource_instance);
  // 5. re-init the local available resource with 4 CPUs.
  available_resource = {std::make_pair("CPU", 4.0)};
  InitLocalAvailableResource(available_resource);
  // 6. re-prepare resources for the four bundles, but this time it should be
  // successfully.
  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundles(bundle_specs));
  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundles(bundle_specs));
  new_placement_group_resource_manager_->CommitBundles(bundle_specs);
  // 7. re-check remaining resources is correct.
  absl::flat_hash_map<std::string, double> remaining_resources_total = {
      {"CPU_group_" + group_id.Hex(), 4.0},
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
  absl::flat_hash_map<std::string, double> remaining_resources_avail = {
      {"CPU_group_" + group_id.Hex(), 4.0},
      {"CPU_group_1_" + group_id.Hex(), 1.0},
      {"CPU_group_2_" + group_id.Hex(), 1.0},
      {"CPU_group_3_" + group_id.Hex(), 1.0},
      {"CPU_group_4_" + group_id.Hex(), 1.0},
      {"CPU", 0.0},
      {"bundle_group_1_" + group_id.Hex(), 1000},
      {"bundle_group_2_" + group_id.Hex(), 1000},
      {"bundle_group_3_" + group_id.Hex(), 1000},
      {"bundle_group_4_" + group_id.Hex(), 1000},
      {"bundle_group_" + group_id.Hex(), 4000}};
  remaining_resource_instance =
      ResourceMapToNodeResources(remaining_resources_total, remaining_resources_avail);
  RAY_LOG(INFO) << "The current local resource view: "
                << cluster_resource_scheduler_->DebugString();
  CheckRemainingResourceCorrect(remaining_resource_instance);
}

TEST_F(NewPlacementGroupResourceManagerTest, TestCommiteResourceBatched) {
  // 1. create a placement group spec with 4 bundles and each required 1 CPU.
  auto group_id = PlacementGroupID::Of(JobID::FromInt(1));
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"GPU", 2.0});
  auto bundle_specs = GenBundleSpecifications(group_id, unit_resource, 4);
  // 2. init local available resource with 4 CPUs.
  absl::flat_hash_map<std::string, double> available_resource = {
      std::make_pair("GPU", 10.0)};
  InitLocalAvailableResource(available_resource);
  // 3. prepare resources for the four bundles and make sure it succeeds.
  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundles(bundle_specs));
  // 4. prepare resources for the four bundles.
  new_placement_group_resource_manager_->CommitBundles(bundle_specs);
  // make sure it keeps Idempotency.
  new_placement_group_resource_manager_->CommitBundles(bundle_specs);
  // 5. check remaining resources is correct.
  absl::flat_hash_map<std::string, double> remaining_resources_total = {
      {"GPU_group_" + group_id.Hex(), 8.0},
      {"GPU_group_1_" + group_id.Hex(), 2.0},
      {"GPU_group_2_" + group_id.Hex(), 2.0},
      {"GPU_group_3_" + group_id.Hex(), 2.0},
      {"GPU_group_4_" + group_id.Hex(), 2.0},
      {"GPU", 10.0},
      {"bundle_group_1_" + group_id.Hex(), 1000},
      {"bundle_group_2_" + group_id.Hex(), 1000},
      {"bundle_group_3_" + group_id.Hex(), 1000},
      {"bundle_group_4_" + group_id.Hex(), 1000},
      {"bundle_group_" + group_id.Hex(), 4000}};
  absl::flat_hash_map<std::string, double> remaining_resources_avail = {
      {"GPU_group_" + group_id.Hex(), 8.0},
      {"GPU_group_1_" + group_id.Hex(), 2.0},
      {"GPU_group_2_" + group_id.Hex(), 2.0},
      {"GPU_group_3_" + group_id.Hex(), 2.0},
      {"GPU_group_4_" + group_id.Hex(), 2.0},
      {"GPU", 2.0},
      {"bundle_group_1_" + group_id.Hex(), 1000},
      {"bundle_group_2_" + group_id.Hex(), 1000},
      {"bundle_group_3_" + group_id.Hex(), 1000},
      {"bundle_group_4_" + group_id.Hex(), 1000},
      {"bundle_group_" + group_id.Hex(), 4000}};
  auto remaining_resource_instance =
      ResourceMapToNodeResources(remaining_resources_total, remaining_resources_avail);
  RAY_LOG(INFO) << "The current local resource view: "
                << cluster_resource_scheduler_->DebugString();
  CheckRemainingResourceCorrect(remaining_resource_instance);
}

TEST_F(NewPlacementGroupResourceManagerTest, TestNewReturnBundleFailure) {
  // create bundle spec.
  auto group_id = PlacementGroupID::Of(JobID::FromInt(1));
  absl::flat_hash_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto bundle_spec = GenBundleCreation(group_id, 1, unit_resource);
  /// init local available resource.
  InitLocalAvailableResource(unit_resource);
  /// prepare and commit bundle resource.
  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundles(
      ConvertSingleSpecToVectorPtrs(bundle_spec)));
  new_placement_group_resource_manager_->CommitBundles(
      ConvertSingleSpecToVectorPtrs(bundle_spec));
  /// Occupy resources.
  const auto &placement_group_resources = bundle_spec.GetFormattedResources();
  auto resource_instances = std::make_shared<TaskResourceInstances>();
  ASSERT_TRUE(
      cluster_resource_scheduler_->GetLocalResourceManager().AllocateLocalTaskResources(
          placement_group_resources, resource_instances));
  /// return bundle resource failed with invalid error.
  ASSERT_TRUE(
      new_placement_group_resource_manager_->ReturnBundle(bundle_spec).IsInvalid());
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
