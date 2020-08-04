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

#include "ray/common/task/scheduling_resources.h"

#include <memory>

#include "gtest/gtest.h"
#include "ray/common/id.h"

namespace ray {
class SchedulingResourcesTest : public ::testing::Test {
 public:
  void SetUp() override {
    resource_set = std::make_shared<ResourceSet>();
    resource_id_set = std::make_shared<ResourceIdSet>();
  }

 protected:
  std::shared_ptr<ResourceSet> resource_set;
  std::shared_ptr<ResourceIdSet> resource_id_set;
};

TEST_F(SchedulingResourcesTest, AddBundleResources) {
  PlacementGroupID group_id = PlacementGroupID::FromRandom();
  std::vector<std::string> resource_labels = {"CPU"};
  std::vector<double> resource_capacity = {1.0};
  ResourceSet resource(resource_labels, resource_capacity);
  resource_set->AddBundleResources(group_id, 1, resource);
  resource_labels.pop_back();
  resource_labels.push_back("CPU_group_1_" + group_id.Hex());
  resource_labels.push_back("CPU_group_" + group_id.Hex());
  resource_capacity.push_back(1.0);
  ResourceSet result_resource(resource_labels, resource_capacity);
  ASSERT_EQ(1, resource_set->IsEqual(result_resource));
}

TEST_F(SchedulingResourcesTest, AddBundleResource) {
  PlacementGroupID group_id = PlacementGroupID::FromRandom();
  std::string wild_name = "CPU_group_" + group_id.Hex();
  std::string index_name = "CPU_group_1_" + group_id.Hex();
  std::vector<int64_t> whole_ids = {1, 2, 3};
  ResourceIds resource_ids(whole_ids);
  resource_id_set->AddBundleResourceIds(group_id, 1, "CPU", resource_ids);
  ASSERT_EQ(2, resource_id_set->AvailableResources().size());
  for (auto res : resource_id_set->AvailableResources()) {
    ASSERT_TRUE(res.first == wild_name || res.first == index_name) << res.first;
  }
}

TEST_F(SchedulingResourcesTest, ReturnBundleResources) {
  PlacementGroupID group_id = PlacementGroupID::FromRandom();
  std::vector<std::string> resource_labels = {"CPU"};
  std::vector<double> resource_capacity = {1.0};
  ResourceSet resource(resource_labels, resource_capacity);
  resource_set->AddBundleResources(group_id, 1, resource);
  resource_labels.pop_back();
  resource_labels.push_back("CPU_group_" + group_id.Hex());
  resource_labels.push_back("CPU_group_1_" + group_id.Hex());
  resource_capacity.push_back(1.0);
  ResourceSet result_resource(resource_labels, resource_capacity);
  ASSERT_EQ(1, resource_set->IsEqual(result_resource));
  resource_set->ReturnBundleResources(group_id, 1);
  ASSERT_EQ(1, resource_set->IsEqual(resource))
      << resource_set->ToString() << " vs " << resource.ToString();
}

TEST_F(SchedulingResourcesTest, MultipleBundlesAddRemove) {
  PlacementGroupID group_id = PlacementGroupID::FromRandom();
  std::vector<std::string> resource_labels = {"CPU"};
  std::vector<double> resource_capacity = {1.0};
  ResourceSet resource(resource_labels, resource_capacity);
  resource_set->AddBundleResources(group_id, 1, resource);
  resource_set->AddBundleResources(group_id, 2, resource);
  resource_labels = {
      "CPU_group_" + group_id.Hex(),
      "CPU_group_1_" + group_id.Hex(),
      "CPU_group_2_" + group_id.Hex(),
  };
  resource_capacity = {2.0, 1.0, 1.0};
  ResourceSet result_resource(resource_labels, resource_capacity);
  ASSERT_EQ(1, resource_set->IsEqual(result_resource))
      << resource_set->ToString() << " vs " << result_resource.ToString();

  // Return group 2.
  resource_set->ReturnBundleResources(group_id, 2);
  resource_labels = {
      "CPU_group_" + group_id.Hex(),
      "CPU_group_1_" + group_id.Hex(),
  };
  resource_capacity = {1.0, 1.0};
  ResourceSet result_resource2(resource_labels, resource_capacity);
  ASSERT_EQ(1, resource_set->IsEqual(result_resource2))
      << resource_set->ToString() << " vs " << result_resource2.ToString();

  // Return group 1.
  resource_set->ReturnBundleResources(group_id, 1);
  ASSERT_EQ(1, resource_set->IsEqual(resource))
      << resource_set->ToString() << " vs " << resource.ToString();
}
}  // namespace ray
