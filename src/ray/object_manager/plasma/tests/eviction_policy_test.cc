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

#include <algorithm>
#include <cstddef>
#include <optional>
#include <vector>

#include "gtest/gtest.h"
#include "ray/object_manager/plasma/object_store.h"

using ray::ObjectID;

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

// Hand-written fake allocator exposing settable return fields for the const getters
// used by EvictionPolicy.
class FakeAllocator : public IAllocator {
 public:
  std::optional<Allocation> Allocate(size_t bytes) override { return std::nullopt; }
  std::optional<Allocation> FallbackAllocate(size_t bytes) override {
    return std::nullopt;
  }
  void Free(Allocation allocation) override {}
  int64_t GetFootprintLimit() const override { return footprint_limit; }
  int64_t Allocated() const override { return allocated; }
  int64_t FallbackAllocated() const override { return fallback_allocated; }

  int64_t footprint_limit = 0;
  int64_t allocated = 0;
  int64_t fallback_allocated = 0;
};

// Hand-written fake object store. GetObject returns values from a settable queue in
// call order, repeating the last element once the queue is exhausted.
class FakeObjectStore : public IObjectStore {
 public:
  const LocalObject *CreateObject(const ray::ObjectInfo &,
                                  plasma::flatbuf::ObjectSource,
                                  bool) override {
    return nullptr;
  }
  const LocalObject *GetObject(const ObjectID &object_id) const override {
    get_object_call_count++;
    if (get_object_returns.empty()) {
      return nullptr;
    }
    if (get_object_index < get_object_returns.size()) {
      return get_object_returns[get_object_index++];
    }
    return get_object_returns.back();
  }
  const LocalObject *SealObject(const ObjectID &object_id) override { return nullptr; }
  bool DeleteObject(const ObjectID &object_id) override { return false; }

  std::vector<const LocalObject *> get_object_returns;
  mutable size_t get_object_index = 0;
  mutable int get_object_call_count = 0;
};

TEST(EvictionPolicyTest, Test) {
  FakeAllocator allocator;
  FakeObjectStore store;
  allocator.footprint_limit = 100;
  ObjectID key1 = ObjectID::FromRandom();
  ObjectID key2 = ObjectID::FromRandom();
  ObjectID key3 = ObjectID::FromRandom();
  ObjectID key4 = ObjectID::FromRandom();

  LocalObject object1{Allocation()};
  object1.object_info_.data_size = 10;
  object1.object_info_.metadata_size = 0;
  LocalObject object2{Allocation()};
  object2.object_info_.data_size = 20;
  object2.object_info_.metadata_size = 0;
  LocalObject object3{Allocation()};
  object3.object_info_.data_size = 30;
  object3.object_info_.metadata_size = 0;
  LocalObject object4{Allocation()};
  object4.object_info_.data_size = 40;
  object4.object_info_.metadata_size = 0;

  auto init_object_store = [&](EvictionPolicy &policy) {
    store.get_object_returns = {&object1, &object2, &object3, &object4};
    store.get_object_index = 0;
    int get_object_count_before = store.get_object_call_count;
    policy.ObjectCreated(key1);
    policy.ObjectCreated(key2);
    policy.ObjectCreated(key3);
    policy.ObjectCreated(key4);
    // Each ObjectCreated should query the store exactly once.
    EXPECT_EQ(store.get_object_call_count, get_object_count_before + 4);

    allocator.allocated = 10 + 20 + 30 + 40;
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

    // Any subsequent GetObject call returns object1.
    store.get_object_returns = {&object1};
    store.get_object_index = 0;
    EXPECT_TRUE(policy.IsObjectExists(key1));
    policy.BeginObjectAccess(key1);
    EXPECT_FALSE(policy.IsObjectExists(key1));
    policy.EndObjectAccess(key1);
    EXPECT_TRUE(policy.IsObjectExists(key1));
  }
}
}  // namespace plasma
