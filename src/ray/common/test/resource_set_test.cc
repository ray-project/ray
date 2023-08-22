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

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "gtest/gtest.h"
#include "ray/common/scheduling/cluster_resource_data.h"

namespace ray {
class NodeResourceSetTest : public ::testing::Test {};

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

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
