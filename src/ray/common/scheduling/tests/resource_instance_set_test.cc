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

#include <string>
#include <vector>

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

TEST_F(NodeResourceInstanceSetTest, TestTryAllocateOneResourceWithoutPlacementGroup) {
  // 1. Test non-unit resource
  {
    // Allocation succeed when demand is smaller than available
    NodeResourceInstanceSet r1 = NodeResourceInstanceSet(NodeResourceSet({{"CPU", 2}}));
    ResourceSet success_request = ResourceSet({{"CPU", FixedPoint(1)}});
    auto allocations = r1.TryAllocate(success_request);
    ASSERT_EQ(allocations->size(), 1);
    ASSERT_EQ((*allocations)[ResourceID("CPU")],
              std::vector<FixedPoint>({FixedPoint(1)}));
    ASSERT_EQ(r1.Get(ResourceID("CPU")), std::vector<FixedPoint>({FixedPoint(1)}));

    // Allocation failed when demand is larger than available
    ResourceSet fail_request = ResourceSet({{"CPU", FixedPoint(2)}});
    allocations = r1.TryAllocate(fail_request);
    ASSERT_FALSE(allocations);
    // Make sure nothing is allocated when allocation fails.
    ASSERT_EQ(r1.Get(ResourceID("CPU")), std::vector<FixedPoint>({FixedPoint(1)}));

    // Allocation succeed when demand equals available
    success_request = ResourceSet({{"CPU", FixedPoint(1)}});
    allocations = r1.TryAllocate(success_request);
    ASSERT_EQ(allocations->size(), 1);
    ASSERT_EQ((*allocations)[ResourceID("CPU")],
              std::vector<FixedPoint>({FixedPoint(1)}));
    ASSERT_EQ(r1.Get(ResourceID("CPU")), std::vector<FixedPoint>({FixedPoint(0)}));
  }

  // 2. Test unit resource
  {
    // Succees allocation with demand > 1
    NodeResourceInstanceSet r2 = NodeResourceInstanceSet(NodeResourceSet({{"GPU", 4}}));
    ResourceSet success_request = ResourceSet({{"GPU", FixedPoint(2)}});
    auto allocations = r2.TryAllocate(success_request);
    ASSERT_EQ(allocations->size(), 1);
    ASSERT_EQ((*allocations)[ResourceID("GPU")],
              std::vector<FixedPoint>(
                  {FixedPoint(1), FixedPoint(1), FixedPoint(0), FixedPoint(0)}));
    ASSERT_EQ(r2.Get(ResourceID("GPU")),
              std::vector<FixedPoint>(
                  {FixedPoint(0), FixedPoint(0), FixedPoint(1), FixedPoint(1)}));

    // Failed allocation when demand > available
    ResourceSet fail_request = ResourceSet({{"GPU", FixedPoint(3)}});
    allocations = r2.TryAllocate(fail_request);
    ASSERT_FALSE(allocations);
    // Make sure nothing is allocated when allocation fails.
    ASSERT_EQ(r2.Get(ResourceID("GPU")),
              std::vector<FixedPoint>(
                  {FixedPoint(0), FixedPoint(0), FixedPoint(1), FixedPoint(1)}));

    // Success allocation with fractional demand
    // Should be allocated with best fit
    success_request = ResourceSet({{"GPU", FixedPoint(0.4)}});
    allocations = r2.TryAllocate(success_request);
    ASSERT_EQ(allocations->size(), 1);
    ASSERT_EQ((*allocations)[ResourceID("GPU")],
              std::vector<FixedPoint>(
                  {FixedPoint(0), FixedPoint(0), FixedPoint(0.4), FixedPoint(0)}));
    ASSERT_EQ(r2.Get(ResourceID("GPU")),
              std::vector<FixedPoint>(
                  {FixedPoint(0), FixedPoint(0), FixedPoint(0.6), FixedPoint(1)}));

    success_request = ResourceSet({{"GPU", FixedPoint(0.7)}});
    allocations = r2.TryAllocate(success_request);
    ASSERT_EQ(allocations->size(), 1);
    ASSERT_EQ((*allocations)[ResourceID("GPU")],
              std::vector<FixedPoint>(
                  {FixedPoint(0), FixedPoint(0), FixedPoint(0), FixedPoint(0.7)}));
    ASSERT_EQ(r2.Get(ResourceID("GPU")),
              std::vector<FixedPoint>(
                  {FixedPoint(0), FixedPoint(0), FixedPoint(0.6), FixedPoint(0.3)}));

    success_request = ResourceSet({{"GPU", FixedPoint(0.3)}});
    allocations = r2.TryAllocate(success_request);
    ASSERT_EQ(allocations->size(), 1);
    ASSERT_EQ((*allocations)[ResourceID("GPU")],
              std::vector<FixedPoint>(
                  {FixedPoint(0), FixedPoint(0), FixedPoint(0), FixedPoint(0.3)}));
    ASSERT_EQ(r2.Get(ResourceID("GPU")),
              std::vector<FixedPoint>(
                  {FixedPoint(0), FixedPoint(0), FixedPoint(0.6), FixedPoint(0)}));
  }

  // 3. Test implicit resource
  {
    NodeResourceInstanceSet r3 = NodeResourceInstanceSet(
        NodeResourceSet(absl::flat_hash_map<std::string, double>{}));
    ResourceSet success_request =
        ResourceSet({{std::string(kImplicitResourcePrefix) + "a", FixedPoint(0.3)}});
    auto allocations = r3.TryAllocate(success_request);
    ASSERT_EQ(allocations->size(), 1);
    ASSERT_EQ((*allocations)[ResourceID(std::string(kImplicitResourcePrefix) + "a")],
              std::vector<FixedPoint>({FixedPoint(0.3)}));
    ASSERT_EQ(r3.Get(ResourceID(std::string(kImplicitResourcePrefix) + "a")),
              std::vector<FixedPoint>({FixedPoint(0.7)}));
  }
}

TEST_F(NodeResourceInstanceSetTest, TestTryAllocateMultipleResourcesWithoutPg) {
  // Case 1: Partial failure will not allocate anything
  NodeResourceInstanceSet r1 =
      NodeResourceInstanceSet(NodeResourceSet({{"CPU", 2}, {"GPU", 2}}));
  NodeResourceInstanceSet r2 =
      NodeResourceInstanceSet(NodeResourceSet({{"CPU", 2}, {"GPU", 2}}));
  ResourceSet fail_request =
      ResourceSet({{"CPU", FixedPoint(1)},
                   {"GPU", FixedPoint(3)},
                   {std::string(kImplicitResourcePrefix) + "a", FixedPoint(0.3)}});
  auto allocations = r1.TryAllocate(fail_request);
  ASSERT_FALSE(allocations);
  ASSERT_TRUE(r1 == r2);

  // Case 2: All success, will allocate all the resources
  ResourceSet success_request =
      ResourceSet({{"CPU", FixedPoint(1)},
                   {"GPU", FixedPoint(1)},
                   {std::string(kImplicitResourcePrefix) + "a", FixedPoint(0.3)}});
  allocations = r1.TryAllocate(success_request);
  ASSERT_EQ(allocations->size(), 3);
  ASSERT_EQ((*allocations)[ResourceID("CPU")], std::vector<FixedPoint>({FixedPoint(1)}));
  ASSERT_EQ(r1.Get(ResourceID("CPU")), std::vector<FixedPoint>({FixedPoint(1)}));
  ASSERT_EQ((*allocations)[ResourceID("GPU")],
            std::vector<FixedPoint>({FixedPoint(1), FixedPoint(0)}));
  ASSERT_EQ(r1.Get(ResourceID("GPU")),
            std::vector<FixedPoint>({FixedPoint(0), FixedPoint(1)}));
  ASSERT_EQ((*allocations)[ResourceID(std::string(kImplicitResourcePrefix) + "a")],
            std::vector<FixedPoint>({FixedPoint(0.3)}));
  ASSERT_EQ(r1.Get(ResourceID(std::string(kImplicitResourcePrefix) + "a")),
            std::vector<FixedPoint>({FixedPoint(0.7)}));
}

TEST_F(NodeResourceInstanceSetTest, TestTryAllocateWithSinglePgResourceAndBundleIndex) {
  // 1. Test non unit resource
  {
    // Success allocation when the index bundle have enough resources
    ResourceID cpu_resource("CPU");
    ResourceID pg_cpu_wildcard_resource("CPU_group_4482dec0faaf5ead891ff1659a9501000000");
    ResourceID pg_cpu_index_0_resource(
        "CPU_group_0_4482dec0faaf5ead891ff1659a9501000000");
    ResourceID pg_cpu_index_1_resource(
        "CPU_group_1_4482dec0faaf5ead891ff1659a9501000000");
    NodeResourceInstanceSet r1 = NodeResourceInstanceSet(
        NodeResourceSet(absl::flat_hash_map<std::string, double>{}));
    r1.Set(cpu_resource, std::vector<FixedPoint>({FixedPoint(1)}));
    r1.Set(pg_cpu_wildcard_resource, std::vector<FixedPoint>({FixedPoint(4)}));
    r1.Set(pg_cpu_index_0_resource, std::vector<FixedPoint>({FixedPoint(2)}));
    r1.Set(pg_cpu_index_1_resource, std::vector<FixedPoint>({FixedPoint(2)}));

    ResourceSet success_request = ResourceSet({{pg_cpu_wildcard_resource, FixedPoint(1)},
                                               {pg_cpu_index_1_resource, FixedPoint(1)}});
    auto allocations = r1.TryAllocate(success_request);
    ASSERT_EQ(allocations->size(), 2);
    ASSERT_EQ((*allocations)[pg_cpu_wildcard_resource],
              (*allocations)[pg_cpu_index_1_resource]);
    ASSERT_EQ((*allocations)[pg_cpu_wildcard_resource],
              std::vector<FixedPoint>({FixedPoint(1)}));
    ASSERT_EQ(r1.Get(cpu_resource), std::vector<FixedPoint>({FixedPoint(1)}));
    ASSERT_EQ(r1.Get(pg_cpu_wildcard_resource), std::vector<FixedPoint>({FixedPoint(3)}));
    ASSERT_EQ(r1.Get(pg_cpu_index_0_resource), std::vector<FixedPoint>({FixedPoint(2)}));
    ASSERT_EQ(r1.Get(pg_cpu_index_1_resource), std::vector<FixedPoint>({FixedPoint(1)}));

    // Failed allocation when the index bundle doesn't have enough resources, even though
    // the pg still has enough resources
    ResourceSet fail_request = ResourceSet({{pg_cpu_wildcard_resource, FixedPoint(2)},
                                            {pg_cpu_index_1_resource, FixedPoint(2)}});
    allocations = r1.TryAllocate(fail_request);
    ASSERT_FALSE(allocations);
    ASSERT_EQ(r1.Get(cpu_resource), std::vector<FixedPoint>({FixedPoint(1)}));
    ASSERT_EQ(r1.Get(pg_cpu_wildcard_resource), std::vector<FixedPoint>({FixedPoint(3)}));
    ASSERT_EQ(r1.Get(pg_cpu_index_0_resource), std::vector<FixedPoint>({FixedPoint(2)}));
    ASSERT_EQ(r1.Get(pg_cpu_index_1_resource), std::vector<FixedPoint>({FixedPoint(1)}));
  }

  // 2. Test unit resource
  {
    // Success allocation when the index bundle have enough resources
    ResourceID gpu_resource("GPU");
    ResourceID pg_gpu_wildcard_resource("GPU_group_4482dec0faaf5ead891ff1659a9501000000");
    ResourceID pg_gpu_index_0_resource(
        "GPU_group_0_4482dec0faaf5ead891ff1659a9501000000");
    ResourceID pg_gpu_index_1_resource(
        "GPU_group_1_4482dec0faaf5ead891ff1659a9501000000");
    NodeResourceInstanceSet r2 = NodeResourceInstanceSet(
        NodeResourceSet(absl::flat_hash_map<std::string, double>{}));
    r2.Set(
        gpu_resource,
        std::vector<FixedPoint>(
            {FixedPoint(0), FixedPoint(0), FixedPoint(0), FixedPoint(0), FixedPoint(1)}));
    r2.Set(
        pg_gpu_wildcard_resource,
        std::vector<FixedPoint>(
            {FixedPoint(1), FixedPoint(1), FixedPoint(1), FixedPoint(1), FixedPoint(0)}));
    r2.Set(
        pg_gpu_index_0_resource,
        std::vector<FixedPoint>(
            {FixedPoint(1), FixedPoint(1), FixedPoint(0), FixedPoint(0), FixedPoint(0)}));
    r2.Set(pg_gpu_index_1_resource,
           std::vector<FixedPoint>({{FixedPoint(0),
                                     FixedPoint(0),
                                     FixedPoint(1),
                                     FixedPoint(1),
                                     FixedPoint(0)}}));

    ResourceSet success_request = ResourceSet({{pg_gpu_wildcard_resource, FixedPoint(1)},
                                               {pg_gpu_index_1_resource, FixedPoint(1)}});
    auto allocations = r2.TryAllocate(success_request);
    ASSERT_EQ(allocations->size(), 2);
    // Make sure the allocations are consistent between wildcard resource and indexed
    // resource
    ASSERT_EQ((*allocations)[pg_gpu_wildcard_resource],
              (*allocations)[pg_gpu_index_1_resource]);
    ASSERT_EQ(
        (*allocations)[pg_gpu_wildcard_resource],
        std::vector<FixedPoint>(
            {FixedPoint(0), FixedPoint(0), FixedPoint(1), FixedPoint(0), FixedPoint(0)}));
    ASSERT_EQ(
        r2.Get(gpu_resource),
        std::vector<FixedPoint>(
            {FixedPoint(0), FixedPoint(0), FixedPoint(0), FixedPoint(0), FixedPoint(1)}));
    ASSERT_EQ(
        r2.Get(pg_gpu_wildcard_resource),
        std::vector<FixedPoint>(
            {FixedPoint(1), FixedPoint(1), FixedPoint(0), FixedPoint(1), FixedPoint(0)}));
    ASSERT_EQ(
        r2.Get(pg_gpu_index_0_resource),
        std::vector<FixedPoint>(
            {FixedPoint(1), FixedPoint(1), FixedPoint(0), FixedPoint(0), FixedPoint(0)}));
    ASSERT_EQ(
        r2.Get(pg_gpu_index_1_resource),
        std::vector<FixedPoint>(
            {FixedPoint(0), FixedPoint(0), FixedPoint(0), FixedPoint(1), FixedPoint(0)}));

    // Failed allocation when the index bundle doesn't have enough resources, even though
    // the pg still has enough resources
    ResourceSet fail_request = ResourceSet({{pg_gpu_wildcard_resource, FixedPoint(2)},
                                            {pg_gpu_index_1_resource, FixedPoint(2)}});
    allocations = r2.TryAllocate(fail_request);
    ASSERT_FALSE(allocations);
    ASSERT_EQ(
        r2.Get(gpu_resource),
        std::vector<FixedPoint>(
            {FixedPoint(0), FixedPoint(0), FixedPoint(0), FixedPoint(0), FixedPoint(1)}));
    ASSERT_EQ(
        r2.Get(pg_gpu_wildcard_resource),
        std::vector<FixedPoint>(
            {FixedPoint(1), FixedPoint(1), FixedPoint(0), FixedPoint(1), FixedPoint(0)}));
    ASSERT_EQ(
        r2.Get(pg_gpu_index_0_resource),
        std::vector<FixedPoint>(
            {FixedPoint(1), FixedPoint(1), FixedPoint(0), FixedPoint(0), FixedPoint(0)}));
    ASSERT_EQ(
        r2.Get(pg_gpu_index_1_resource),
        std::vector<FixedPoint>(
            {FixedPoint(0), FixedPoint(0), FixedPoint(0), FixedPoint(1), FixedPoint(0)}));
  }
}

TEST_F(NodeResourceInstanceSetTest, TestTryAllocateWithMultiplePgResourceAndBundleIndex) {
  // Case 1: Partial failure will not allocate anything
  ResourceID cpu_resource("CPU");
  ResourceID pg_cpu_wildcard_resource("CPU_group_4482dec0faaf5ead891ff1659a9501000000");
  ResourceID pg_cpu_index_0_resource("CPU_group_0_4482dec0faaf5ead891ff1659a9501000000");
  ResourceID pg_cpu_index_1_resource("CPU_group_1_4482dec0faaf5ead891ff1659a9501000000");
  ResourceID gpu_resource("GPU");
  ResourceID pg_gpu_wildcard_resource("GPU_group_4482dec0faaf5ead891ff1659a9501000000");
  ResourceID pg_gpu_index_0_resource("GPU_group_0_4482dec0faaf5ead891ff1659a9501000000");
  ResourceID pg_gpu_index_1_resource("GPU_group_1_4482dec0faaf5ead891ff1659a9501000000");

  NodeResourceInstanceSet r1 = NodeResourceInstanceSet(
      NodeResourceSet(absl::flat_hash_map<std::string, double>{}));
  r1.Set(cpu_resource, std::vector<FixedPoint>({FixedPoint(1)}));
  r1.Set(pg_cpu_wildcard_resource, std::vector<FixedPoint>({FixedPoint(4)}));
  r1.Set(pg_cpu_index_0_resource, std::vector<FixedPoint>({FixedPoint(2)}));
  r1.Set(pg_cpu_index_1_resource, std::vector<FixedPoint>({FixedPoint(2)}));
  r1.Set(
      gpu_resource,
      std::vector<FixedPoint>(
          {FixedPoint(0), FixedPoint(0), FixedPoint(0), FixedPoint(0), FixedPoint(1)}));
  r1.Set(
      pg_gpu_wildcard_resource,
      std::vector<FixedPoint>(
          {FixedPoint(1), FixedPoint(1), FixedPoint(1), FixedPoint(1), FixedPoint(0)}));
  r1.Set(
      pg_gpu_index_0_resource,
      std::vector<FixedPoint>(
          {FixedPoint(1), FixedPoint(1), FixedPoint(0), FixedPoint(0), FixedPoint(0)}));
  r1.Set(
      pg_gpu_index_1_resource,
      std::vector<FixedPoint>(
          {FixedPoint(0), FixedPoint(0), FixedPoint(1), FixedPoint(1), FixedPoint(0)}));

  ResourceSet fail_request = ResourceSet({{pg_cpu_wildcard_resource, FixedPoint(1)},
                                          {pg_gpu_wildcard_resource, FixedPoint(3)},
                                          {pg_cpu_index_1_resource, FixedPoint(1)},
                                          {pg_gpu_index_1_resource, FixedPoint(3)}});
  auto allocations = r1.TryAllocate(fail_request);
  ASSERT_FALSE(allocations);
  ASSERT_EQ(r1.Get(cpu_resource), std::vector<FixedPoint>({FixedPoint(1)}));
  ASSERT_EQ(r1.Get(pg_cpu_wildcard_resource), std::vector<FixedPoint>({FixedPoint(4)}));
  ASSERT_EQ(r1.Get(pg_cpu_index_0_resource), std::vector<FixedPoint>({FixedPoint(2)}));
  ASSERT_EQ(r1.Get(pg_cpu_index_1_resource), std::vector<FixedPoint>({FixedPoint(2)}));
  ASSERT_EQ(
      r1.Get(gpu_resource),
      std::vector<FixedPoint>(
          {FixedPoint(0), FixedPoint(0), FixedPoint(0), FixedPoint(0), FixedPoint(1)}));
  ASSERT_EQ(
      r1.Get(pg_gpu_wildcard_resource),
      std::vector<FixedPoint>(
          {FixedPoint(1), FixedPoint(1), FixedPoint(1), FixedPoint(1), FixedPoint(0)}));
  ASSERT_EQ(
      r1.Get(pg_gpu_index_0_resource),
      std::vector<FixedPoint>(
          {FixedPoint(1), FixedPoint(1), FixedPoint(0), FixedPoint(0), FixedPoint(0)}));
  ASSERT_EQ(
      r1.Get(pg_gpu_index_1_resource),
      std::vector<FixedPoint>(
          {FixedPoint(0), FixedPoint(0), FixedPoint(1), FixedPoint(1), FixedPoint(0)}));

  // Case 2: All success, will allocate all the resources
  ResourceSet success_request = ResourceSet({{pg_cpu_wildcard_resource, FixedPoint(1)},
                                             {pg_gpu_wildcard_resource, FixedPoint(1)},
                                             {pg_cpu_index_1_resource, FixedPoint(1)},
                                             {pg_gpu_index_1_resource, FixedPoint(1)}});
  allocations = r1.TryAllocate(success_request);
  ASSERT_EQ(allocations->size(), 4);
  // Make sure the allocations are consistent between wildcard resource and indexed
  // resource
  ASSERT_EQ((*allocations)[pg_cpu_wildcard_resource],
            (*allocations)[pg_cpu_index_1_resource]);
  ASSERT_EQ((*allocations)[pg_cpu_wildcard_resource],
            std::vector<FixedPoint>({FixedPoint(1)}));
  ASSERT_EQ((*allocations)[pg_gpu_wildcard_resource],
            (*allocations)[pg_gpu_index_1_resource]);
  ASSERT_EQ(
      (*allocations)[pg_gpu_wildcard_resource],
      std::vector<FixedPoint>(
          {FixedPoint(0), FixedPoint(0), FixedPoint(1), FixedPoint(0), FixedPoint(0)}));
  ASSERT_EQ(r1.Get(cpu_resource), std::vector<FixedPoint>({FixedPoint(1)}));
  ASSERT_EQ(r1.Get(pg_cpu_wildcard_resource), std::vector<FixedPoint>({FixedPoint(3)}));
  ASSERT_EQ(r1.Get(pg_cpu_index_0_resource), std::vector<FixedPoint>({FixedPoint(2)}));
  ASSERT_EQ(r1.Get(pg_cpu_index_1_resource), std::vector<FixedPoint>({FixedPoint(1)}));
  ASSERT_EQ(
      r1.Get(gpu_resource),
      std::vector<FixedPoint>(
          {FixedPoint(0), FixedPoint(0), FixedPoint(0), FixedPoint(0), FixedPoint(1)}));
  ASSERT_EQ(
      r1.Get(pg_gpu_wildcard_resource),
      std::vector<FixedPoint>(
          {FixedPoint(1), FixedPoint(1), FixedPoint(0), FixedPoint(1), FixedPoint(0)}));
  ASSERT_EQ(
      r1.Get(pg_gpu_index_0_resource),
      std::vector<FixedPoint>(
          {FixedPoint(1), FixedPoint(1), FixedPoint(0), FixedPoint(0), FixedPoint(0)}));
  ASSERT_EQ(
      r1.Get(pg_gpu_index_1_resource),
      std::vector<FixedPoint>(
          {FixedPoint(0), FixedPoint(0), FixedPoint(0), FixedPoint(1), FixedPoint(0)}));
}

TEST_F(NodeResourceInstanceSetTest, TestTryAllocateSinglePgResourceAndNoBundleIndex) {
  // 1. Test non unit resource
  {
    // Success allocation when found a bundle withe enough available resources
    ResourceID cpu_resource("CPU");
    ResourceID pg_cpu_wildcard_resource("CPU_group_4482dec0faaf5ead891ff1659a9501000000");
    ResourceID pg_cpu_index_0_resource(
        "CPU_group_0_4482dec0faaf5ead891ff1659a9501000000");
    ResourceID pg_cpu_index_1_resource(
        "CPU_group_1_4482dec0faaf5ead891ff1659a9501000000");
    ResourceID pg_cpu_index_2_resource(
        "CPU_group_2_4482dec0faaf5ead891ff1659a9501000000");
    NodeResourceInstanceSet r1 = NodeResourceInstanceSet(
        NodeResourceSet(absl::flat_hash_map<std::string, double>{}));
    r1.Set(cpu_resource, std::vector<FixedPoint>({FixedPoint(1)}));
    r1.Set(pg_cpu_wildcard_resource, std::vector<FixedPoint>({FixedPoint(5)}));
    r1.Set(pg_cpu_index_0_resource, std::vector<FixedPoint>({FixedPoint(1)}));
    r1.Set(pg_cpu_index_1_resource, std::vector<FixedPoint>({FixedPoint(2)}));
    r1.Set(pg_cpu_index_2_resource, std::vector<FixedPoint>({FixedPoint(2)}));

    ResourceSet success_request =
        ResourceSet({{pg_cpu_wildcard_resource, FixedPoint(2)}});
    auto allocations = r1.TryAllocate(success_request);
    ASSERT_EQ(allocations->size(), 2);
    ASSERT_EQ((*allocations)[pg_cpu_wildcard_resource],
              std::vector<FixedPoint>({FixedPoint(2)}));
    if (allocations->find(pg_cpu_index_1_resource) != allocations->end()) {
      ASSERT_EQ((*allocations)[pg_cpu_index_1_resource],
                std::vector<FixedPoint>({FixedPoint(2)}));
    } else {
      ASSERT_EQ((*allocations)[pg_cpu_index_2_resource],
                std::vector<FixedPoint>({FixedPoint(2)}));
    }
    ASSERT_EQ(r1.Get(cpu_resource), std::vector<FixedPoint>({FixedPoint(1)}));
    ASSERT_EQ(r1.Get(pg_cpu_wildcard_resource), std::vector<FixedPoint>({FixedPoint(3)}));
    ASSERT_EQ(r1.Get(pg_cpu_index_0_resource), std::vector<FixedPoint>({FixedPoint(1)}));
    if (allocations->find(pg_cpu_index_1_resource) != allocations->end()) {
      ASSERT_EQ(r1.Get(pg_cpu_index_1_resource),
                std::vector<FixedPoint>({FixedPoint(0)}));
      ASSERT_EQ(r1.Get(pg_cpu_index_2_resource),
                std::vector<FixedPoint>({FixedPoint(2)}));
    } else {
      ASSERT_EQ(r1.Get(pg_cpu_index_1_resource),
                std::vector<FixedPoint>({FixedPoint(2)}));
      ASSERT_EQ(r1.Get(pg_cpu_index_2_resource),
                std::vector<FixedPoint>({FixedPoint(0)}));
    }

    // Failed allocation when no index bundle have enough resources, even though the pg
    // still has enough resources
    ResourceSet fail_request = ResourceSet({
        {pg_cpu_wildcard_resource, FixedPoint(3)},
    });
    auto failed_allocations = r1.TryAllocate(fail_request);
    ASSERT_FALSE(failed_allocations);
    ASSERT_EQ(r1.Get(cpu_resource), std::vector<FixedPoint>({FixedPoint(1)}));
    ASSERT_EQ(r1.Get(pg_cpu_wildcard_resource), std::vector<FixedPoint>({FixedPoint(3)}));
    ASSERT_EQ(r1.Get(pg_cpu_index_0_resource), std::vector<FixedPoint>({FixedPoint(1)}));
    if (allocations->find(pg_cpu_index_1_resource) != allocations->end()) {
      ASSERT_EQ(r1.Get(pg_cpu_index_1_resource),
                std::vector<FixedPoint>({FixedPoint(0)}));
      ASSERT_EQ(r1.Get(pg_cpu_index_2_resource),
                std::vector<FixedPoint>({FixedPoint(2)}));
    } else {
      ASSERT_EQ(r1.Get(pg_cpu_index_1_resource),
                std::vector<FixedPoint>({FixedPoint(2)}));
      ASSERT_EQ(r1.Get(pg_cpu_index_2_resource),
                std::vector<FixedPoint>({FixedPoint(0)}));
    }
  }

  // 2. Test unit resource
  {
    // Success allocation when found a bundle with enough available resources
    ResourceID gpu_resource("GPU");
    ResourceID pg_gpu_wildcard_resource("GPU_group_4482dec0faaf5ead891ff1659a9501000000");
    ResourceID pg_gpu_index_0_resource(
        "GPU_group_0_4482dec0faaf5ead891ff1659a9501000000");
    ResourceID pg_gpu_index_1_resource(
        "GPU_group_1_4482dec0faaf5ead891ff1659a9501000000");
    ResourceID pg_gpu_index_2_resource(
        "GPU_group_2_4482dec0faaf5ead891ff1659a9501000000");
    NodeResourceInstanceSet r2 = NodeResourceInstanceSet(
        NodeResourceSet(absl::flat_hash_map<std::string, double>{}));
    r2.Set(gpu_resource,
           std::vector<FixedPoint>({FixedPoint(0),
                                    FixedPoint(0),
                                    FixedPoint(0),
                                    FixedPoint(0),
                                    FixedPoint(0),
                                    FixedPoint(1)}));
    r2.Set(pg_gpu_wildcard_resource,
           std::vector<FixedPoint>({FixedPoint(1),
                                    FixedPoint(1),
                                    FixedPoint(1),
                                    FixedPoint(1),
                                    FixedPoint(1),
                                    FixedPoint(0)}));
    r2.Set(pg_gpu_index_0_resource,
           std::vector<FixedPoint>({FixedPoint(1),
                                    FixedPoint(0),
                                    FixedPoint(0),
                                    FixedPoint(0),
                                    FixedPoint(0),
                                    FixedPoint(0)}));
    r2.Set(pg_gpu_index_1_resource,
           std::vector<FixedPoint>({FixedPoint(0),
                                    FixedPoint(1),
                                    FixedPoint(1),
                                    FixedPoint(0),
                                    FixedPoint(0),
                                    FixedPoint(0)}));
    r2.Set(pg_gpu_index_2_resource,
           std::vector<FixedPoint>({FixedPoint(0),
                                    FixedPoint(0),
                                    FixedPoint(0),
                                    FixedPoint(1),
                                    FixedPoint(1),
                                    FixedPoint(0)}));

    ResourceSet success_request =
        ResourceSet({{pg_gpu_wildcard_resource, FixedPoint(2)}});
    auto allocations = r2.TryAllocate(success_request);
    ASSERT_EQ(allocations->size(), 2);
    // Make sure the allocations are consistent between wildcard resource and indexed
    // resource
    if (allocations->find(pg_gpu_index_1_resource) != allocations->end()) {
      ASSERT_EQ((*allocations)[pg_gpu_wildcard_resource],
                (*allocations)[pg_gpu_index_1_resource]);
      ASSERT_EQ((*allocations)[pg_gpu_wildcard_resource],
                std::vector<FixedPoint>({FixedPoint(0),
                                         FixedPoint(1),
                                         FixedPoint(1),
                                         FixedPoint(0),
                                         FixedPoint(0),
                                         FixedPoint(0)}));
    } else {
      ASSERT_EQ((*allocations)[pg_gpu_wildcard_resource],
                (*allocations)[pg_gpu_index_2_resource]);
      ASSERT_EQ((*allocations)[pg_gpu_wildcard_resource],
                std::vector<FixedPoint>({FixedPoint(0),
                                         FixedPoint(0),
                                         FixedPoint(0),
                                         FixedPoint(1),
                                         FixedPoint(1),
                                         FixedPoint(0)}));
    }
    ASSERT_EQ(r2.Get(gpu_resource),
              std::vector<FixedPoint>({FixedPoint(0),
                                       FixedPoint(0),
                                       FixedPoint(0),
                                       FixedPoint(0),
                                       FixedPoint(0),
                                       FixedPoint(1)}));
    ASSERT_EQ(r2.Get(pg_gpu_index_0_resource),
              std::vector<FixedPoint>({FixedPoint(1),
                                       FixedPoint(0),
                                       FixedPoint(0),
                                       FixedPoint(0),
                                       FixedPoint(0),
                                       FixedPoint(0)}));
    if (allocations->find(pg_gpu_index_1_resource) != allocations->end()) {
      ASSERT_EQ(r2.Get(pg_gpu_wildcard_resource),
                std::vector<FixedPoint>({FixedPoint(1),
                                         FixedPoint(0),
                                         FixedPoint(0),
                                         FixedPoint(1),
                                         FixedPoint(1),
                                         FixedPoint(0)}));
      ASSERT_EQ(r2.Get(pg_gpu_index_1_resource),
                std::vector<FixedPoint>({FixedPoint(0),
                                         FixedPoint(0),
                                         FixedPoint(0),
                                         FixedPoint(0),
                                         FixedPoint(0),
                                         FixedPoint(0)}));
      ASSERT_EQ(r2.Get(pg_gpu_index_2_resource),
                std::vector<FixedPoint>({FixedPoint(0),
                                         FixedPoint(0),
                                         FixedPoint(0),
                                         FixedPoint(1),
                                         FixedPoint(1),
                                         FixedPoint(0)}));
    } else {
      ASSERT_EQ(r2.Get(pg_gpu_wildcard_resource),
                std::vector<FixedPoint>({FixedPoint(1),
                                         FixedPoint(1),
                                         FixedPoint(1),
                                         FixedPoint(0),
                                         FixedPoint(0),
                                         FixedPoint(0)}));
      ASSERT_EQ(r2.Get(pg_gpu_index_1_resource),
                std::vector<FixedPoint>({FixedPoint(0),
                                         FixedPoint(1),
                                         FixedPoint(1),
                                         FixedPoint(0),
                                         FixedPoint(0),
                                         FixedPoint(0)}));
      ASSERT_EQ(r2.Get(pg_gpu_index_2_resource),
                std::vector<FixedPoint>({FixedPoint(0),
                                         FixedPoint(0),
                                         FixedPoint(0),
                                         FixedPoint(0),
                                         FixedPoint(0),
                                         FixedPoint(0)}));
    }

    // Failed allocation when the index bundle doesn't have enough resources, even though
    // the pg still has enough resources
    ResourceSet fail_request = ResourceSet({{pg_gpu_wildcard_resource, FixedPoint(3)}});
    auto failed_allocations = r2.TryAllocate(fail_request);
    ASSERT_FALSE(failed_allocations);
    ASSERT_EQ(r2.Get(gpu_resource),
              std::vector<FixedPoint>({FixedPoint(0),
                                       FixedPoint(0),
                                       FixedPoint(0),
                                       FixedPoint(0),
                                       FixedPoint(0),
                                       FixedPoint(1)}));
    ASSERT_EQ(r2.Get(pg_gpu_index_0_resource),
              std::vector<FixedPoint>({FixedPoint(1),
                                       FixedPoint(0),
                                       FixedPoint(0),
                                       FixedPoint(0),
                                       FixedPoint(0),
                                       FixedPoint(0)}));
    if (allocations->find(pg_gpu_index_1_resource) != allocations->end()) {
      ASSERT_EQ(r2.Get(pg_gpu_wildcard_resource),
                std::vector<FixedPoint>({FixedPoint(1),
                                         FixedPoint(0),
                                         FixedPoint(0),
                                         FixedPoint(1),
                                         FixedPoint(1),
                                         FixedPoint(0)}));
      ASSERT_EQ(r2.Get(pg_gpu_index_1_resource),
                std::vector<FixedPoint>({FixedPoint(0),
                                         FixedPoint(0),
                                         FixedPoint(0),
                                         FixedPoint(0),
                                         FixedPoint(0),
                                         FixedPoint(0)}));
      ASSERT_EQ(r2.Get(pg_gpu_index_2_resource),
                std::vector<FixedPoint>({FixedPoint(0),
                                         FixedPoint(0),
                                         FixedPoint(0),
                                         FixedPoint(1),
                                         FixedPoint(1),
                                         FixedPoint(0)}));
    } else {
      ASSERT_EQ(r2.Get(pg_gpu_wildcard_resource),
                std::vector<FixedPoint>({FixedPoint(1),
                                         FixedPoint(1),
                                         FixedPoint(1),
                                         FixedPoint(0),
                                         FixedPoint(0),
                                         FixedPoint(0)}));
      ASSERT_EQ(r2.Get(pg_gpu_index_1_resource),
                std::vector<FixedPoint>({FixedPoint(0),
                                         FixedPoint(1),
                                         FixedPoint(1),
                                         FixedPoint(0),
                                         FixedPoint(0),
                                         FixedPoint(0)}));
      ASSERT_EQ(r2.Get(pg_gpu_index_2_resource),
                std::vector<FixedPoint>({FixedPoint(0),
                                         FixedPoint(0),
                                         FixedPoint(0),
                                         FixedPoint(0),
                                         FixedPoint(0),
                                         FixedPoint(0)}));
    }
  }
}

TEST_F(NodeResourceInstanceSetTest, TestTryAllocateMultiplePgResourceAndNoBundleIndex) {
  // Case 1: Partial failure will not allocate anything
  ResourceID cpu_resource("CPU");
  ResourceID pg_cpu_wildcard_resource("CPU_group_4482dec0faaf5ead891ff1659a9501000000");
  ResourceID pg_cpu_index_0_resource("CPU_group_0_4482dec0faaf5ead891ff1659a9501000000");
  ResourceID pg_cpu_index_1_resource("CPU_group_1_4482dec0faaf5ead891ff1659a9501000000");
  ResourceID pg_cpu_index_2_resource("CPU_group_2_4482dec0faaf5ead891ff1659a9501000000");
  ResourceID gpu_resource("GPU");
  ResourceID pg_gpu_wildcard_resource("GPU_group_4482dec0faaf5ead891ff1659a9501000000");
  ResourceID pg_gpu_index_0_resource("GPU_group_0_4482dec0faaf5ead891ff1659a9501000000");
  ResourceID pg_gpu_index_1_resource("GPU_group_1_4482dec0faaf5ead891ff1659a9501000000");
  ResourceID pg_gpu_index_2_resource("GPU_group_2_4482dec0faaf5ead891ff1659a9501000000");

  NodeResourceInstanceSet r1 = NodeResourceInstanceSet(
      NodeResourceSet(absl::flat_hash_map<std::string, double>{}));
  r1.Set(cpu_resource, std::vector<FixedPoint>({FixedPoint(1)}));
  r1.Set(pg_cpu_wildcard_resource, std::vector<FixedPoint>({FixedPoint(5)}));
  r1.Set(pg_cpu_index_0_resource, std::vector<FixedPoint>({FixedPoint(1)}));
  r1.Set(pg_cpu_index_1_resource, std::vector<FixedPoint>({FixedPoint(2)}));
  r1.Set(pg_cpu_index_2_resource, std::vector<FixedPoint>({FixedPoint(2)}));
  r1.Set(gpu_resource,
         std::vector<FixedPoint>({FixedPoint(0),
                                  FixedPoint(0),
                                  FixedPoint(0),
                                  FixedPoint(0),
                                  FixedPoint(0),
                                  FixedPoint(1)}));
  r1.Set(pg_gpu_wildcard_resource,
         std::vector<FixedPoint>({FixedPoint(1),
                                  FixedPoint(1),
                                  FixedPoint(1),
                                  FixedPoint(1),
                                  FixedPoint(1),
                                  FixedPoint(0)}));
  r1.Set(pg_gpu_index_0_resource,
         std::vector<FixedPoint>({FixedPoint(1),
                                  FixedPoint(0),
                                  FixedPoint(0),
                                  FixedPoint(0),
                                  FixedPoint(0),
                                  FixedPoint(0)}));
  r1.Set(pg_gpu_index_1_resource,
         std::vector<FixedPoint>({FixedPoint(0),
                                  FixedPoint(1),
                                  FixedPoint(1),
                                  FixedPoint(0),
                                  FixedPoint(0),
                                  FixedPoint(0)}));
  r1.Set(pg_gpu_index_2_resource,
         std::vector<FixedPoint>({FixedPoint(0),
                                  FixedPoint(0),
                                  FixedPoint(0),
                                  FixedPoint(1),
                                  FixedPoint(1),
                                  FixedPoint(0)}));

  ResourceSet fail_request = ResourceSet({{pg_cpu_wildcard_resource, FixedPoint(2)},
                                          {pg_gpu_wildcard_resource, FixedPoint(3)}});
  auto allocations = r1.TryAllocate(fail_request);
  ASSERT_FALSE(allocations);
  ASSERT_EQ(r1.Get(cpu_resource), std::vector<FixedPoint>({FixedPoint(1)}));
  ASSERT_EQ(r1.Get(pg_cpu_wildcard_resource), std::vector<FixedPoint>({FixedPoint(5)}));
  ASSERT_EQ(r1.Get(pg_cpu_index_0_resource), std::vector<FixedPoint>({FixedPoint(1)}));
  ASSERT_EQ(r1.Get(pg_cpu_index_1_resource), std::vector<FixedPoint>({FixedPoint(2)}));
  ASSERT_EQ(r1.Get(pg_cpu_index_2_resource), std::vector<FixedPoint>({FixedPoint(2)}));
  ASSERT_EQ(r1.Get(gpu_resource),
            std::vector<FixedPoint>({FixedPoint(0),
                                     FixedPoint(0),
                                     FixedPoint(0),
                                     FixedPoint(0),
                                     FixedPoint(0),
                                     FixedPoint(1)}));
  ASSERT_EQ(r1.Get(pg_gpu_wildcard_resource),
            std::vector<FixedPoint>({FixedPoint(1),
                                     FixedPoint(1),
                                     FixedPoint(1),
                                     FixedPoint(1),
                                     FixedPoint(1),
                                     FixedPoint(0)}));
  ASSERT_EQ(r1.Get(pg_gpu_index_0_resource),
            std::vector<FixedPoint>({FixedPoint(1),
                                     FixedPoint(0),
                                     FixedPoint(0),
                                     FixedPoint(0),
                                     FixedPoint(0),
                                     FixedPoint(0)}));
  ASSERT_EQ(r1.Get(pg_gpu_index_1_resource),
            std::vector<FixedPoint>({FixedPoint(0),
                                     FixedPoint(1),
                                     FixedPoint(1),
                                     FixedPoint(0),
                                     FixedPoint(0),
                                     FixedPoint(0)}));
  ASSERT_EQ(r1.Get(pg_gpu_index_2_resource),
            std::vector<FixedPoint>({FixedPoint(0),
                                     FixedPoint(0),
                                     FixedPoint(0),
                                     FixedPoint(1),
                                     FixedPoint(1),
                                     FixedPoint(0)}));

  // Case 2: All success, will allocate all the resources
  ResourceSet success_request = ResourceSet({{pg_cpu_wildcard_resource, FixedPoint(2)},
                                             {pg_gpu_wildcard_resource, FixedPoint(2)}});
  allocations = r1.TryAllocate(success_request);
  ASSERT_EQ(allocations->size(), 4);
  // Make sure the allocations are consistent between wildcard resource and indexed
  // resource
  if (allocations->find(pg_cpu_index_1_resource) != allocations->end()) {
    ASSERT_EQ((*allocations)[pg_cpu_wildcard_resource],
              (*allocations)[pg_cpu_index_1_resource]);
  } else {
    ASSERT_EQ((*allocations)[pg_cpu_wildcard_resource],
              (*allocations)[pg_cpu_index_2_resource]);
  }
  ASSERT_EQ((*allocations)[pg_cpu_wildcard_resource],
            std::vector<FixedPoint>({FixedPoint(2)}));
  if (allocations->find(pg_gpu_index_1_resource) != allocations->end()) {
    ASSERT_EQ((*allocations)[pg_gpu_wildcard_resource],
              std::vector<FixedPoint>({FixedPoint(0),
                                       FixedPoint(1),
                                       FixedPoint(1),
                                       FixedPoint(0),
                                       FixedPoint(0),
                                       FixedPoint(0)}));
    ASSERT_EQ((*allocations)[pg_gpu_wildcard_resource],
              (*allocations)[pg_gpu_index_1_resource]);
  } else {
    ASSERT_EQ((*allocations)[pg_gpu_wildcard_resource],
              std::vector<FixedPoint>({FixedPoint(0),
                                       FixedPoint(0),
                                       FixedPoint(0),
                                       FixedPoint(1),
                                       FixedPoint(1),
                                       FixedPoint(0)}));
    ASSERT_EQ((*allocations)[pg_gpu_wildcard_resource],
              (*allocations)[pg_gpu_index_2_resource]);
  }
  ASSERT_EQ(r1.Get(cpu_resource), std::vector<FixedPoint>({FixedPoint(1)}));
  ASSERT_EQ(r1.Get(pg_cpu_wildcard_resource), std::vector<FixedPoint>({FixedPoint(3)}));
  ASSERT_EQ(r1.Get(pg_cpu_index_0_resource), std::vector<FixedPoint>({FixedPoint(1)}));
  if (allocations->find(pg_cpu_index_1_resource) != allocations->end()) {
    ASSERT_EQ(r1.Get(pg_cpu_index_1_resource), std::vector<FixedPoint>({FixedPoint(0)}));
    ASSERT_EQ(r1.Get(pg_cpu_index_2_resource), std::vector<FixedPoint>({FixedPoint(2)}));
  } else {
    ASSERT_EQ(r1.Get(pg_cpu_index_1_resource), std::vector<FixedPoint>({FixedPoint(2)}));
    ASSERT_EQ(r1.Get(pg_cpu_index_2_resource), std::vector<FixedPoint>({FixedPoint(0)}));
  }
  ASSERT_EQ(r1.Get(gpu_resource),
            std::vector<FixedPoint>({FixedPoint(0),
                                     FixedPoint(0),
                                     FixedPoint(0),
                                     FixedPoint(0),
                                     FixedPoint(0),
                                     FixedPoint(1)}));
  ASSERT_EQ(r1.Get(pg_gpu_index_0_resource),
            std::vector<FixedPoint>({FixedPoint(1),
                                     FixedPoint(0),
                                     FixedPoint(0),
                                     FixedPoint(0),
                                     FixedPoint(0),
                                     FixedPoint(0)}));
  if (allocations->find(pg_gpu_index_1_resource) != allocations->end()) {
    ASSERT_EQ(r1.Get(pg_gpu_wildcard_resource),
              std::vector<FixedPoint>({FixedPoint(1),
                                       FixedPoint(0),
                                       FixedPoint(0),
                                       FixedPoint(1),
                                       FixedPoint(1),
                                       FixedPoint(0)}));
    ASSERT_EQ(r1.Get(pg_gpu_index_1_resource),
              std::vector<FixedPoint>({FixedPoint(0),
                                       FixedPoint(0),
                                       FixedPoint(0),
                                       FixedPoint(0),
                                       FixedPoint(0),
                                       FixedPoint(0)}));
    ASSERT_EQ(r1.Get(pg_gpu_index_2_resource),
              std::vector<FixedPoint>({FixedPoint(0),
                                       FixedPoint(0),
                                       FixedPoint(0),
                                       FixedPoint(1),
                                       FixedPoint(1),
                                       FixedPoint(0)}));
  } else {
    ASSERT_EQ(r1.Get(pg_gpu_wildcard_resource),
              std::vector<FixedPoint>({FixedPoint(1),
                                       FixedPoint(1),
                                       FixedPoint(1),
                                       FixedPoint(0),
                                       FixedPoint(0),
                                       FixedPoint(0)}));
    ASSERT_EQ(r1.Get(pg_gpu_index_1_resource),
              std::vector<FixedPoint>({FixedPoint(0),
                                       FixedPoint(1),
                                       FixedPoint(1),
                                       FixedPoint(0),
                                       FixedPoint(0),
                                       FixedPoint(0)}));
    ASSERT_EQ(r1.Get(pg_gpu_index_2_resource),
              std::vector<FixedPoint>({FixedPoint(0),
                                       FixedPoint(0),
                                       FixedPoint(0),
                                       FixedPoint(0),
                                       FixedPoint(0),
                                       FixedPoint(0)}));
  }
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
