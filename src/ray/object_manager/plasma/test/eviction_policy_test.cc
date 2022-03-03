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

#include "ray/object_manager/plasma/eviction_policy.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/object_manager/plasma/object_store.h"

using namespace ray;
using namespace testing;

namespace plasma {
TEST(LRUCacheTest, Test) {
  LRUCache cache("cache", 1024);
  EXPECT_EQ(1024, cache.Capacity());
  EXPECT_EQ(1024, cache.OriginalCapacity());

  {
    ObjectID key1 = ObjectID::FromRandom();
    int64_t size1 = 32;
    cache.Add(key1, size1);
    EXPECT_EQ(1024 - size1, cache.RemainingCapacity());
    ObjectID key2 = ObjectID::FromRandom();
    int64_t size2 = 64;
    cache.Add(key2, size2);
    EXPECT_EQ(1024 - size1 - size2, cache.RemainingCapacity());
    cache.Remove(key1);
    EXPECT_EQ(1024 - size2, cache.RemainingCapacity());
    cache.Remove(key2);
    EXPECT_EQ(1024, cache.RemainingCapacity());
  }

  {
    ObjectID key1 = ObjectID::FromRandom();
    int64_t size1 = 10;
    ObjectID key2 = ObjectID::FromRandom();
    int64_t size2 = 10;
    std::vector<ObjectID> keys{key1, key2};
    cache.Add(key1, size1);
    cache.Add(key2, size2);
    {
      std::vector<ObjectID> objects_to_evict;
      EXPECT_EQ(20, cache.ChooseObjectsToEvict(15, objects_to_evict));
      EXPECT_EQ(2, objects_to_evict.size());
    }

    {
      std::vector<ObjectID> objects_to_evict;
      EXPECT_EQ(20, cache.ChooseObjectsToEvict(30, objects_to_evict));
      EXPECT_EQ(2, objects_to_evict.size());
    }

    std::vector<ObjectID> foreach;
    cache.Foreach([&foreach](const ObjectID &key) {
      foreach
        .push_back(key);
    });
    reverse(foreach.begin(), foreach.end());
    EXPECT_EQ(foreach, keys);
  }

  cache.AdjustCapacity(1024);
  EXPECT_EQ(2048, cache.Capacity());
  EXPECT_EQ(1024, cache.OriginalCapacity());
}

class MockAllocator : public IAllocator {
 public:
  MOCK_METHOD1(Allocate, absl::optional<Allocation>(size_t bytes));
  MOCK_METHOD1(FallbackAllocate, absl::optional<Allocation>(size_t bytes));
  MOCK_METHOD1(Free, void(Allocation));
  MOCK_CONST_METHOD0(GetFootprintLimit, int64_t());
  MOCK_CONST_METHOD0(Allocated, int64_t());
  MOCK_CONST_METHOD0(FallbackAllocated, int64_t());
};

class MockObjectStore : public IObjectStore {
 public:
  MOCK_METHOD3(CreateObject,
               const LocalObject *(const ray::ObjectInfo &,
                                   plasma::flatbuf::ObjectSource,
                                   bool));
  MOCK_CONST_METHOD1(GetObject, const LocalObject *(const ObjectID &));
  MOCK_METHOD1(SealObject, const LocalObject *(const ObjectID &));
  MOCK_METHOD1(DeleteObject, bool(const ObjectID &));
  MOCK_CONST_METHOD0(GetNumBytesCreatedTotal, int64_t());
  MOCK_CONST_METHOD0(GetNumBytesUnsealed, int64_t());
  MOCK_CONST_METHOD0(GetNumObjectsUnsealed, int64_t());
  MOCK_CONST_METHOD1(GetDebugDump, void(std::stringstream &buffer));
};

TEST(EvictionPolicyTest, Test) {
  MockAllocator allocator;
  MockObjectStore store;
  EXPECT_CALL(allocator, GetFootprintLimit()).WillRepeatedly(Return(100));
  ObjectID key1 = ObjectID::FromRandom();
  ObjectID key2 = ObjectID::FromRandom();
  ObjectID key3 = ObjectID::FromRandom();
  ObjectID key4 = ObjectID::FromRandom();

  LocalObject object1{Allocation()};
  object1.object_info.data_size = 10;
  object1.object_info.metadata_size = 0;
  LocalObject object2{Allocation()};
  object2.object_info.data_size = 20;
  object2.object_info.metadata_size = 0;
  LocalObject object3{Allocation()};
  object3.object_info.data_size = 30;
  object3.object_info.metadata_size = 0;
  LocalObject object4{Allocation()};
  object4.object_info.data_size = 40;
  object4.object_info.metadata_size = 0;

  auto init_object_store = [&](EvictionPolicy &policy) {
    EXPECT_CALL(store, GetObject(_))
        .Times(4)
        .WillOnce(Return(&object1))
        .WillOnce(Return(&object2))
        .WillOnce(Return(&object3))
        .WillOnce(Return(&object4));
    policy.ObjectCreated(key1);
    policy.ObjectCreated(key2);
    policy.ObjectCreated(key3);
    policy.ObjectCreated(key4);

    EXPECT_CALL(allocator, Allocated()).WillRepeatedly(Return(10 + 20 + 30 + 40));
  };

  {
    EvictionPolicy policy(store, allocator);
    init_object_store(policy);
    std::vector<ObjectID> objects_to_evict;

    // Require 10, need to evict at least 20%, so the first two objects should be evicted.
    // [10,20,30,40] -> [30,40]
    EXPECT_EQ(-20, policy.RequireSpace(10, objects_to_evict));
    EXPECT_EQ(2, objects_to_evict.size());
  }

  {
    EvictionPolicy policy(store, allocator);
    init_object_store(policy);
    std::vector<ObjectID> objects_to_evict;

    // Require 30, need to evict 30, so the first two objects should be evicted.
    // [10,20,30,40] -> [30,40]
    EXPECT_EQ(0, policy.RequireSpace(30, objects_to_evict));
    EXPECT_EQ(2, objects_to_evict.size());
  }

  {
    EvictionPolicy policy(store, allocator);
    init_object_store(policy);
    std::vector<ObjectID> objects_to_evict;

    // Require 40, need to evict 40, so the first three objects should be evicted.
    // [10,20,30,40] -> [40]
    EXPECT_EQ(-20, policy.RequireSpace(40, objects_to_evict));
    EXPECT_EQ(3, objects_to_evict.size());
  }

  {
    EvictionPolicy policy(store, allocator);
    init_object_store(policy);

    EXPECT_CALL(store, GetObject(_)).WillRepeatedly(Return(&object1));
    EXPECT_TRUE(policy.IsObjectExists(key1));
    policy.BeginObjectAccess(key1);
    EXPECT_FALSE(policy.IsObjectExists(key1));
    policy.EndObjectAccess(key1);
    EXPECT_TRUE(policy.IsObjectExists(key1));
  }
}
}  // namespace plasma

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
