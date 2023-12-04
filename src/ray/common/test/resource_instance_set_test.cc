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

#include "ray/common/scheduling/resource_instance_set.h"

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "gtest/gtest.h"

namespace ray {
class NodeResourceInstanceSetTest : public ::testing::Test {};

TEST_F(NodeResourceInstanceSetTest, TestConstructor) {
  NodeResourceInstanceSet r1 =
      NodeResourceInstanceSet(NodeResourceSet({{"CPU", 2}, {"GPU", 2}}));
  ASSERT_EQ(r1.Get(ResourceID("CPU")), std::vector<FixedPoint>({FixedPoint(2)}));
  ASSERT_EQ(r1.Get(ResourceID("GPU")),
            std::vector<FixedPoint>({FixedPoint(1), FixedPoint(1)}));
}

TEST_F(NodeResourceInstanceSetTest, TestHas) {
  NodeResourceInstanceSet r1 =
      NodeResourceInstanceSet(NodeResourceSet({{"CPU", 2}, {"GPU", 2}}));
  ASSERT_TRUE(r1.Has(ResourceID("CPU")));
  ASSERT_FALSE(r1.Has(ResourceID("non-exist")));
  // Every node implicitly has implicit resources.
  ASSERT_TRUE(r1.Has(ResourceID(std::string(kImplicitResourcePrefix) + "a")));
}

TEST_F(NodeResourceInstanceSetTest, TestRemove) {
  NodeResourceInstanceSet r1 =
      NodeResourceInstanceSet(NodeResourceSet({{"CPU", 2}, {"GPU", 2}}));
  ASSERT_TRUE(r1.Has(ResourceID("GPU")));
  r1.Remove(ResourceID("GPU"));
  ASSERT_FALSE(r1.Has(ResourceID("GPU")));
  r1.Remove(ResourceID("non-exist"));
  ASSERT_FALSE(r1.Has(ResourceID("non-exist")));
}

TEST_F(NodeResourceInstanceSetTest, TestGet) {
  NodeResourceInstanceSet r1 =
      NodeResourceInstanceSet(NodeResourceSet({{"CPU", 2}, {"GPU", 2}}));
  ASSERT_EQ(r1.Get(ResourceID("CPU")), std::vector<FixedPoint>({FixedPoint(2)}));
  ASSERT_EQ(r1.Get(ResourceID(std::string(kImplicitResourcePrefix) + "a")),
            std::vector<FixedPoint>({FixedPoint(1)}));
  ASSERT_TRUE(r1.Get(ResourceID("non-exist")).empty());
}

TEST_F(NodeResourceInstanceSetTest, TestSet) {
  NodeResourceInstanceSet r1;
  r1.Set(ResourceID("CPU"), std::vector<FixedPoint>({FixedPoint(1)}));
  r1.Set(ResourceID(std::string(kImplicitResourcePrefix) + "a"),
         std::vector<FixedPoint>({FixedPoint(1)}));
  r1.Set(ResourceID(std::string(kImplicitResourcePrefix) + "b"),
         std::vector<FixedPoint>({FixedPoint(0.5)}));
  ASSERT_EQ(r1.Get(ResourceID("CPU")), std::vector<FixedPoint>({FixedPoint(1)}));
  ASSERT_EQ(r1.Get(ResourceID(std::string(kImplicitResourcePrefix) + "b")),
            std::vector<FixedPoint>({FixedPoint(0.5)}));
}

TEST_F(NodeResourceInstanceSetTest, TestSum) {
  NodeResourceInstanceSet r1;
  r1.Set(ResourceID("GPU"),
         std::vector<FixedPoint>({FixedPoint(1), FixedPoint(0.3), FixedPoint(0.5)}));
  r1.Set(ResourceID(std::string(kImplicitResourcePrefix) + "a"),
         std::vector<FixedPoint>({FixedPoint(0.7)}));
  ASSERT_EQ(r1.Sum(ResourceID("GPU")), 1.8);
  ASSERT_EQ(r1.Sum(ResourceID(std::string(kImplicitResourcePrefix) + "a")), 0.7);
  // Implicit resource has a total of 1 by default.
  ASSERT_EQ(r1.Sum(ResourceID(std::string(kImplicitResourcePrefix) + "b")), 1);
  ASSERT_EQ(r1.Sum(ResourceID("non-exist")), 0);
}

TEST_F(NodeResourceInstanceSetTest, TestOperator) {
  NodeResourceInstanceSet r1 =
      NodeResourceInstanceSet(NodeResourceSet({{"CPU", 2}, {"GPU", 2}}));
  NodeResourceInstanceSet r2 =
      NodeResourceInstanceSet(NodeResourceSet({{"CPU", 2}, {"GPU", 2}}));
  NodeResourceInstanceSet r3 =
      NodeResourceInstanceSet(NodeResourceSet({{"CPU", 2}, {"GPU", 1}}));
  ASSERT_TRUE(r1 == r2);
  ASSERT_FALSE(r1 == r3);
}

TEST_F(NodeResourceInstanceSetTest, TestTryAllocate) {
  NodeResourceInstanceSet r1 =
      NodeResourceInstanceSet(NodeResourceSet({{"CPU", 2}, {"GPU", 2}}));
  NodeResourceInstanceSet r2 =
      NodeResourceInstanceSet(NodeResourceSet({{"CPU", 2}, {"GPU", 2}}));
  // Allocation fails.
  auto allocations =
      r1.TryAllocate(ResourceSet({{"CPU", FixedPoint(1)}, {"GPU", FixedPoint(3)}}));
  ASSERT_FALSE(allocations);
  // Make sure nothing is allocated when allocation fails.
  ASSERT_TRUE(r1 == r2);

  allocations = r1.TryAllocate(
      ResourceSet({{"CPU", FixedPoint(1)},
                   {"GPU", FixedPoint(1)},
                   {std::string(kImplicitResourcePrefix) + "a", FixedPoint(0.3)}}));
  ASSERT_EQ(allocations->size(), 3);
  ASSERT_EQ((*allocations)[ResourceID("CPU")], std::vector<FixedPoint>({FixedPoint(1)}));
  ASSERT_EQ((*allocations)[ResourceID("GPU")],
            std::vector<FixedPoint>({FixedPoint(1), FixedPoint(0)}));
  ASSERT_EQ((*allocations)[ResourceID(std::string(kImplicitResourcePrefix) + "a")],
            std::vector<FixedPoint>({FixedPoint(0.3)}));
  ASSERT_EQ(r1.Get(ResourceID("CPU")), std::vector<FixedPoint>({FixedPoint(1)}));
  ASSERT_EQ(r1.Get(ResourceID("GPU")),
            std::vector<FixedPoint>({FixedPoint(0), FixedPoint(1)}));
  ASSERT_EQ(r1.Get(ResourceID(std::string(kImplicitResourcePrefix) + "a")),
            std::vector<FixedPoint>({FixedPoint(0.7)}));
}

TEST_F(NodeResourceInstanceSetTest, TestFree) {
  NodeResourceInstanceSet r1;
  r1.Set(ResourceID("GPU"), std::vector<FixedPoint>({FixedPoint(1), FixedPoint(0.3)}));
  r1.Set(ResourceID(std::string(kImplicitResourcePrefix) + "a"),
         std::vector<FixedPoint>({FixedPoint(0.4)}));
  r1.Free(ResourceID("GPU"), std::vector<FixedPoint>({FixedPoint(0), FixedPoint(0.7)}));
  r1.Free(ResourceID(std::string(kImplicitResourcePrefix) + "a"),
          std::vector<FixedPoint>({FixedPoint(0.6)}));
  ASSERT_EQ(r1.Get(ResourceID("GPU")),
            std::vector<FixedPoint>({FixedPoint(1), FixedPoint(1)}));
  ASSERT_EQ(r1.Resources().size(), 1);
}

TEST_F(NodeResourceInstanceSetTest, TestAdd) {
  NodeResourceInstanceSet r1;
  r1.Set(ResourceID("GPU"), std::vector<FixedPoint>({FixedPoint(1), FixedPoint(0.3)}));
  r1.Add(ResourceID("GPU"), std::vector<FixedPoint>({FixedPoint(0), FixedPoint(0.3)}));
  r1.Add(ResourceID("new"), std::vector<FixedPoint>({FixedPoint(2)}));
  ASSERT_EQ(r1.Get(ResourceID("GPU")),
            std::vector<FixedPoint>({FixedPoint(1), FixedPoint(0.6)}));
  ASSERT_EQ(r1.Get(ResourceID("new")), std::vector<FixedPoint>({FixedPoint(2)}));
}

TEST_F(NodeResourceInstanceSetTest, TestSubtract) {
  NodeResourceInstanceSet r1;
  r1.Set(ResourceID("GPU"), std::vector<FixedPoint>({FixedPoint(1), FixedPoint(1)}));
  auto underflow = r1.Subtract(
      ResourceID("GPU"), std::vector<FixedPoint>({FixedPoint(0.5), FixedPoint(0)}), true);
  ASSERT_EQ(r1.Get(ResourceID("GPU")),
            std::vector<FixedPoint>({FixedPoint(0.5), FixedPoint(1)}));
  ASSERT_EQ(underflow, std::vector<FixedPoint>({FixedPoint(0), FixedPoint(0)}));

  underflow = r1.Subtract(
      ResourceID("GPU"), std::vector<FixedPoint>({FixedPoint(1), FixedPoint(0)}), true);
  ASSERT_EQ(r1.Get(ResourceID("GPU")),
            std::vector<FixedPoint>({FixedPoint(-0.5), FixedPoint(1)}));
  ASSERT_EQ(underflow, std::vector<FixedPoint>({FixedPoint(0), FixedPoint(0)}));
}

TEST_F(NodeResourceInstanceSetTest, TestToNodeResourceSet) {
  NodeResourceInstanceSet r1 =
      NodeResourceInstanceSet(NodeResourceSet({{"CPU", 2}, {"GPU", 2}}));
  ASSERT_EQ(r1.ToNodeResourceSet(), NodeResourceSet({{"CPU", 2}, {"GPU", 2}}));
}

}  // namespace ray
