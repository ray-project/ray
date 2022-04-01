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

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "gtest/gtest.h"
#include "ray/raylet/scheduling/cluster_resource_data.h"

namespace ray {

class ResourceRequestTest : public ::testing::Test {};

TEST_F(ResourceRequestTest, TestBasic) {
  auto cpu_id = ResourceID::CPU();
  auto gpu_id = ResourceID::GPU();
  auto custom_id1 = ResourceID("custom1");
  auto custom_id2 = ResourceID("custom2");

  absl::flat_hash_map<ResourceID, FixedPoint> resource_map(
      {{cpu_id, 1}, {custom_id1, 2}});

  ResourceRequest resource_request(resource_map);

  // Test Has
  ASSERT_TRUE(resource_request.Has(cpu_id));
  ASSERT_TRUE(resource_request.Has(custom_id1));
  ASSERT_FALSE(resource_request.Has(gpu_id));
  ASSERT_FALSE(resource_request.Has(custom_id2));

  // Test Get
  ASSERT_EQ(resource_request.Get(cpu_id), 1);
  ASSERT_EQ(resource_request.Get(custom_id1), 2);
  ASSERT_EQ(resource_request.Get(gpu_id), 0);
  ASSERT_EQ(resource_request.Get(custom_id2), 0);

  // Test Size and IsEmpty
  ASSERT_EQ(resource_request.Size(), 2);
  ASSERT_FALSE(resource_request.IsEmpty());

  // Test ResourceIds and ToMap
  auto resource_ids_it = resource_request.ResourceIds();
  auto resource_ids =
      absl::flat_hash_set<ResourceID>(resource_ids_it.begin(), resource_ids_it.end());
  ASSERT_EQ(resource_ids, absl::flat_hash_set<ResourceID>({cpu_id, custom_id1}));
  ASSERT_EQ(resource_request.ToMap(), resource_map);

  // Test Set
  resource_request.Set(gpu_id, 1);
  resource_request.Set(custom_id2, 2);
  ASSERT_TRUE(resource_request.Has(gpu_id));
  ASSERT_TRUE(resource_request.Has(custom_id2));
  ASSERT_EQ(resource_request.Get(gpu_id), 1);
  ASSERT_EQ(resource_request.Get(custom_id2), 2);
  // Set 0 will remove the resource
  resource_request.Set(cpu_id, 0);
  resource_request.Set(custom_id1, 0);
  ASSERT_FALSE(resource_request.Has(cpu_id));
  ASSERT_FALSE(resource_request.Has(custom_id1));

  // Test Clear
  resource_request.Clear();
  ASSERT_EQ(resource_request.Size(), 0);
  ASSERT_TRUE(resource_request.IsEmpty());
}

TEST_F(ResourceRequestTest, TestOperators) {
  auto cpu_id = ResourceID::CPU();
  auto custom_id1 = ResourceID("custom1");
  auto custom_id2 = ResourceID("custom2");
  ResourceRequest r1 = ResourceRequest({{cpu_id, 1}, {custom_id1, 2}});
  ResourceRequest r2 = r1;

  // === Test comparison operators ===
  // r1 = {CPU: 1, custom1: 2}, r2 = {CPU: 1, custom1: 2}
  ASSERT_TRUE(r1 == r2);
  ASSERT_TRUE(r1 <= r2);
  ASSERT_TRUE(r2 <= r1);
  ASSERT_TRUE(r1 >= r2);
  ASSERT_TRUE(r2 >= r1);

  // r1 = {CPU: 1, custom1: 2}, r2 = {CPU: 2, custom1: 2}
  r2.Set(cpu_id, 2);
  ASSERT_TRUE(r1 != r2);
  ASSERT_TRUE(r1 <= r2);
  ASSERT_TRUE(r2 >= r1);

  // r1 = {CPU: 1, custom1: 2}, r2 = {CPU: 2, custom1: 2, custom2: 2}
  r2.Set(custom_id2, 2);
  ASSERT_TRUE(r1 != r2);
  ASSERT_TRUE(r1 <= r2);
  ASSERT_TRUE(r2 >= r1);

  // r1 = {CPU: 1, custom1: 2}, r2 = {CPU: 2, custom1: 1, custom2: 2}
  r2.Set(custom_id1, 1);
  ASSERT_TRUE(r1 != r2);
  ASSERT_FALSE(r1 <= r2);
  ASSERT_FALSE(r2 >= r1);

  // r1 = {custom1: -2}, r2 = {}
  r1 = ResourceRequest({{custom_id1, -2}});
  r2.Clear();
  ASSERT_TRUE(r1 != r2);
  ASSERT_TRUE(r1 <= r2);
  ASSERT_TRUE(r2 >= r1);

  // r1 = {}, r2 = {custom1: -2}
  r1.Clear();
  r2 = ResourceRequest({{custom_id1, -2}});
  ASSERT_TRUE(r1 != r2);
  ASSERT_FALSE(r1 <= r2);
  ASSERT_FALSE(r2 >= r1);

  ResourceRequest r3, r4;
  absl::flat_hash_map<ResourceID, FixedPoint> expected;

  // === Test algebra operators ===
  //
  // r1 = {CPU: 1, custom1: 2}, r2 = {CPU: -1, custom2: 2}
  // r3 = r1 + r2 = {custom1: 2, custom2: 2}
  r1 = ResourceRequest({{cpu_id, 1}, {custom_id1, 2}});
  r2 = ResourceRequest({{cpu_id, -1}, {custom_id2, 2}});
  r3 = r1 + r2;
  expected = {{custom_id1, 2}, {custom_id2, 2}};
  ASSERT_EQ(r3.ToMap(), expected);
  r3 = r1;
  r3 += r2;
  ASSERT_EQ(r3.ToMap(), expected);

  // r4 = r1 - r2 = {cpu: 2, custom1: 2, custom2: -2}
  r4 = r1 - r2;
  expected = {{cpu_id, 2}, {custom_id1, 2}, {custom_id2, -2}};
  ASSERT_EQ(r4.ToMap(), expected);
  r4 = r1;
  r4 -= r2;
  ASSERT_EQ(r4.ToMap(), expected);

  // Test RemoveNegative
  r1 = ResourceRequest({{cpu_id, -1}, {custom_id1, 2}, {custom_id2, -2}});
  r1.RemoveNegative();
  expected = {{custom_id1, 2}};
  ASSERT_EQ(r1.ToMap(), expected);
}

class TaskResourceInstancesTest : public ::testing::Test {};

TEST_F(TaskResourceInstancesTest, TestBasic) {
  auto cpu_id = ResourceID::CPU();
  auto gpu_id = ResourceID::GPU();
  auto custom_id1 = ResourceID("custom1");

  absl::flat_hash_map<ResourceID, FixedPoint> resource_map({{cpu_id, 5}, {gpu_id, 5}});

  ResourceRequest resource_request(resource_map);
  TaskResourceInstances task_resource_instances(resource_request);

  // Test Has
  ASSERT_TRUE(task_resource_instances.Has(cpu_id));
  ASSERT_TRUE(task_resource_instances.Has(gpu_id));
  ASSERT_FALSE(task_resource_instances.Has(custom_id1));

  // Test Get
  // GPU is a unit resource, while CPU is not.
  auto cpu_instances = task_resource_instances.Get(cpu_id);
  auto gpu_instances = task_resource_instances.Get(gpu_id);
  ASSERT_EQ(cpu_instances, FixedPointVectorFromDouble({5}));
  ASSERT_EQ(gpu_instances, FixedPointVectorFromDouble({1, 1, 1, 1, 1}));

  // Test Set
  task_resource_instances.Set(custom_id1, FixedPointVectorFromDouble({1}));
  ASSERT_TRUE(task_resource_instances.Has(custom_id1));
  ASSERT_EQ(task_resource_instances.Get(custom_id1), FixedPointVectorFromDouble({1}));
  task_resource_instances.Set(custom_id1, FixedPointVectorFromDouble({2}));
  ASSERT_TRUE(task_resource_instances.Has(custom_id1));
  ASSERT_EQ(task_resource_instances.Get(custom_id1), FixedPointVectorFromDouble({2}));

  // Test Clear
  task_resource_instances.Remove(custom_id1);
  ASSERT_FALSE(task_resource_instances.Has(custom_id1));

  // Test ResourceIds
  auto resource_ids_it = task_resource_instances.ResourceIds();
  auto resource_ids =
      absl::flat_hash_set<ResourceID>(resource_ids_it.begin(), resource_ids_it.end());
  ASSERT_EQ(resource_ids, absl::flat_hash_set<ResourceID>({cpu_id, gpu_id}));

  // Test Size and IsEmpty
  ASSERT_EQ(task_resource_instances.Size(), 2);
  ASSERT_FALSE(task_resource_instances.IsEmpty());

  // Test Sum
  ASSERT_EQ(task_resource_instances.Sum(cpu_id), 5);
  ASSERT_EQ(task_resource_instances.Sum(gpu_id), 5);

  // Test ToResourceRequest
  ASSERT_EQ(task_resource_instances.ToResourceRequest(), resource_request);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
