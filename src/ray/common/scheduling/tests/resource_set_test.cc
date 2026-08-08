// Copyright 2023 The Ray Authors.
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

#include "ray/common/scheduling/resource_set.h"

#include <set>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "gtest/gtest.h"

namespace ray {
class NodeResourceSetTest : public ::testing::Test {};

TEST_F(NodeResourceSetTest, TestImplicitResourcePrefix) {
  // Test to make sure we don't accidentally change this constant
  // as autoscaler depends on it.
  ASSERT_EQ(std::string(kImplicitResourcePrefix), "node:__internal_implicit_resource_");
}

TEST_F(NodeResourceSetTest, TestRemoveNegative) {
  NodeResourceSet r1 = NodeResourceSet({{"CPU", -1}, {"custom1", 2}, {"custom2", -2}});
  r1.RemoveNegative();
  absl::flat_hash_map<std::string, double> expected = {{"custom1", 2}};
  ASSERT_EQ(r1.GetResourceMap(), expected);
}

TEST_F(NodeResourceSetTest, TestSetAndGet) {
  NodeResourceSet r1 = NodeResourceSet();
  // Default value for explicit resource is 0.
  ASSERT_EQ(r1.Get(ResourceID("non-exist")), 0);
  // Default value for implicit resource is 1.
  ASSERT_EQ(r1.Get(ResourceID(std::string(kImplicitResourcePrefix) + "a")), 1);

  r1.Set(ResourceID("exist"), 1);
  ASSERT_EQ(r1.Get(ResourceID("exist")), 1);
  r1.Set(ResourceID(std::string(kImplicitResourcePrefix) + "b"), 0.5);
  ASSERT_EQ(r1.Get(ResourceID(std::string(kImplicitResourcePrefix) + "b")), 0.5);

  // Set to the default value will remove it from the map.
  r1.Set(ResourceID("exist"), 0);
  r1.Set(ResourceID(std::string(kImplicitResourcePrefix) + "b"), 1);
  ASSERT_TRUE(r1.GetResourceMap().empty());
}

TEST_F(NodeResourceSetTest, TestHas) {
  NodeResourceSet r1 = NodeResourceSet();
  ASSERT_FALSE(r1.Has(ResourceID("non-exist")));
  // Every node implicitly has implicit resources.
  ASSERT_TRUE(r1.Has(ResourceID(std::string(kImplicitResourcePrefix) + "a")));
  r1.Set(ResourceID("exist"), 1);
  ASSERT_TRUE(r1.Has(ResourceID("exist")));
}

TEST_F(NodeResourceSetTest, TestOperator) {
  NodeResourceSet r1 = NodeResourceSet({{"CPU", 1}, {"custom1", 2}, {"custom2", 2}});
  ResourceSet r2 = ResourceSet({{"custom1", FixedPoint(1)}, {"custom2", FixedPoint(2)}});
  r1 -= r2;
  ASSERT_EQ(r1, NodeResourceSet({{"CPU", 1}, {"custom1", 1}}));

  NodeResourceSet r3 = NodeResourceSet({{"CPU", 1}, {"custom1", 2}, {"custom2", 2}});
  ResourceSet r4 =
      ResourceSet({{"CPU", FixedPoint(1)},
                   {"custom1", FixedPoint(1)},
                   {"custom3", FixedPoint(0)},
                   {std::string(kImplicitResourcePrefix) + "a", FixedPoint(0.5)}});
  ResourceSet r5 = ResourceSet(
      {{"CPU", FixedPoint(1)}, {"custom1", FixedPoint(1)}, {"custom3", FixedPoint(0.5)}});
  ASSERT_TRUE(r3 >= r4);
  ASSERT_FALSE(r3 >= r5);
}

TEST_F(NodeResourceSetTest, TestExplicitResourceIds) {
  NodeResourceSet r1 = NodeResourceSet(
      {{"CPU", 1}, {"custom1", 2}, {std::string(kImplicitResourcePrefix) + "a", 0.5}});
  ASSERT_EQ(r1.ExplicitResourceIds(),
            std::set<ResourceID>({ResourceID("CPU"), ResourceID("custom1")}));
}

class ResourceSetTest : public ::testing::Test {};

// std::hash<ResourceSet> must agree with operator==: equal sets hash equally,
// regardless of the order the resources were inserted.
TEST_F(ResourceSetTest, TestHashConsistentWithEquality) {
  absl::flat_hash_map<std::string, double> map_a = {
      {"CPU", 2}, {"GPU", 1}, {"custom1", 3}};
  absl::flat_hash_map<std::string, double> map_b = {
      {"custom1", 3}, {"CPU", 2}, {"GPU", 1}};
  ResourceSet a(map_a);
  ResourceSet b(map_b);
  ASSERT_EQ(a, b);
  EXPECT_EQ(std::hash<ResourceSet>()(a), std::hash<ResourceSet>()(b));

  // Different quantity -> not equal.
  absl::flat_hash_map<std::string, double> map_c = {
      {"CPU", 2}, {"GPU", 2}, {"custom1", 3}};
  ResourceSet c(map_c);
  ASSERT_NE(a, c);

  // Swapping quantities between resources must not collide.
  absl::flat_hash_map<std::string, double> map_d = {{"CPU", 1}, {"GPU", 2}};
  absl::flat_hash_map<std::string, double> map_e = {{"CPU", 2}, {"GPU", 1}};
  ResourceSet d(map_d);
  ResourceSet e(map_e);
  EXPECT_NE(std::hash<ResourceSet>()(d), std::hash<ResourceSet>()(e));

  // Repeated quantities on different resources must not cancel out.
  absl::flat_hash_map<std::string, double> map_f = {{"CPU", 1}, {"GPU", 1}};
  absl::flat_hash_map<std::string, double> map_g = {{"CPU", 2}, {"GPU", 2}};
  ResourceSet f(map_f);
  ResourceSet g(map_g);
  EXPECT_NE(std::hash<ResourceSet>()(f), std::hash<ResourceSet>()(g));

  // Usable as a hash-map key.
  absl::flat_hash_map<ResourceSet, int> counts;
  counts[a]++;
  counts[b]++;
  EXPECT_EQ(counts.size(), 1);
  EXPECT_EQ(counts[a], 2);
}

}  // namespace ray
