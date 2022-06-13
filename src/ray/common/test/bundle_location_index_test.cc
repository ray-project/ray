
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

#include "ray/common/bundle_location_index.h"

#include "gtest/gtest.h"

namespace ray {

class BundleLocationIndexTest : public ::testing::Test {
 public:
  PlacementGroupID pg_1 = PlacementGroupID::Of(JobID::FromInt(1));
  PlacementGroupID pg_2 = PlacementGroupID::Of(JobID::FromInt(2));
  BundleID bundle_0 = std::make_pair(pg_1, 0);
  BundleID bundle_1 = std::make_pair(pg_1, 2);
  BundleID bundle_2 = std::make_pair(pg_1, 3);

  BundleID pg_2_bundle_0 = std::make_pair(pg_2, 0);
  BundleID pg_2_bundle_1 = std::make_pair(pg_2, 1);

  NodeID node_0 = NodeID::FromRandom();
  NodeID node_1 = NodeID::FromRandom();
  NodeID node_2 = NodeID::FromRandom();
};

TEST_F(BundleLocationIndexTest, BesicTest) {
  BundleLocationIndex pg_location_index;

  ASSERT_FALSE(pg_location_index.GetBundleLocations(pg_1));
  ASSERT_FALSE(pg_location_index.GetBundleLocation(bundle_1));

  // test add and get
  auto bundle_locations = std::make_shared<BundleLocations>();
  (*bundle_locations)[bundle_0] = std::make_pair(node_0, nullptr);
  (*bundle_locations)[bundle_1] = std::make_pair(node_1, nullptr);
  pg_location_index.AddOrUpdateBundleLocations(bundle_locations);
  auto pg_bundles_location = pg_location_index.GetBundleLocations(pg_1);
  ASSERT_TRUE(pg_bundles_location);
  ASSERT_EQ((**pg_bundles_location)[bundle_0].first, node_0);
  ASSERT_EQ((**pg_bundles_location)[bundle_1].first, node_1);
  ASSERT_EQ(*(pg_location_index.GetBundleLocation(bundle_0)), node_0);
  ASSERT_EQ(*(pg_location_index.GetBundleLocation(bundle_1)), node_1);
  ASSERT_FALSE(pg_location_index.GetBundleLocation(bundle_2));

  bundle_locations = std::make_shared<BundleLocations>();
  (*bundle_locations)[bundle_2] = std::make_pair(node_2, nullptr);
  pg_location_index.AddOrUpdateBundleLocations(bundle_locations);
  bundle_locations = std::make_shared<BundleLocations>();
  (*bundle_locations)[pg_2_bundle_0] = std::make_pair(node_0, nullptr);
  (*bundle_locations)[pg_2_bundle_1] = std::make_pair(node_1, nullptr);

  pg_location_index.AddOrUpdateBundleLocations(bundle_locations);
  ASSERT_EQ(*(pg_location_index.GetBundleLocation(bundle_0)), node_0);
  ASSERT_EQ(*(pg_location_index.GetBundleLocation(bundle_1)), node_1);
  ASSERT_EQ(*(pg_location_index.GetBundleLocation(bundle_2)), node_2);
  ASSERT_EQ(*(pg_location_index.GetBundleLocation(pg_2_bundle_0)), node_0);
  ASSERT_EQ(*(pg_location_index.GetBundleLocation(pg_2_bundle_1)), node_1);
  ASSERT_EQ((*pg_location_index.GetBundleLocations(pg_1))->size(), 3);

  // test erase
  pg_location_index.Erase(node_0);
  ASSERT_FALSE(pg_location_index.GetBundleLocation(bundle_0));
  ASSERT_FALSE(pg_location_index.GetBundleLocation(pg_2_bundle_0));
  ASSERT_EQ(*(pg_location_index.GetBundleLocation(bundle_1)), node_1);

  pg_location_index.Erase(pg_1);
  ASSERT_FALSE(pg_location_index.GetBundleLocations(pg_1));
  ASSERT_FALSE(pg_location_index.GetBundleLocation(bundle_1));
  ASSERT_FALSE(pg_location_index.GetBundleLocation(bundle_2));
  ASSERT_EQ(*(pg_location_index.GetBundleLocation(pg_2_bundle_1)), node_1);

  // test add again
  bundle_locations = std::make_shared<BundleLocations>();
  (*bundle_locations)[bundle_0] = std::make_pair(node_0, nullptr);
  (*bundle_locations)[bundle_1] = std::make_pair(node_1, nullptr);
  pg_location_index.AddOrUpdateBundleLocations(bundle_locations);
  ASSERT_EQ(*(pg_location_index.GetBundleLocation(bundle_0)), node_0);
  ASSERT_EQ(*(pg_location_index.GetBundleLocation(bundle_1)), node_1);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
