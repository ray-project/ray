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

#include "ray/object_manager/plasma/object_store.h"

#include <limits>

#include "absl/random/random.h"
#include "absl/strings/str_format.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

using namespace ray;
using namespace testing;

namespace plasma {
namespace {
template <typename T>
T Random(T max = std::numeric_limits<T>::max()) {
  static absl::BitGen bitgen;
  return absl::Uniform(bitgen, 0, max);
}

Allocation CreateAllocation(Allocation alloc, int64_t size) {
  alloc.size = size;
  alloc.offset = Random<ptrdiff_t>();
  alloc.mmap_size = Random<int64_t>();
  return alloc;
}

const std::string Serialize(const Allocation &allocation) {
  return absl::StrFormat("%p/%d/%d/%d/%d/%d/%d",
                         allocation.address,
                         allocation.size,
                         allocation.fd.first,
                         allocation.fd.second,
                         allocation.offset,
                         allocation.device_num,
                         allocation.mmap_size);
}

ObjectInfo CreateObjectInfo(ObjectID object_id, int64_t object_size) {
  ObjectInfo info;
  info.object_id = object_id;
  info.data_size = Random<int64_t>(object_size);
  info.metadata_size = object_size - info.data_size;
  info.owner_raylet_id = NodeID::FromRandom();
  info.owner_ip_address = "random_ip";
  info.owner_port = Random<int>();
  info.owner_worker_id = WorkerID::FromRandom();
  return info;
}

const ObjectID kId1 = ObjectID::FromRandom();
const ObjectID kId2 = []() {
  auto id = ObjectID::FromRandom();
  while (id == kId1) {
    id = ObjectID::FromRandom();
  }
  return id;
}();
}  // namespace

class MockAllocator : public IAllocator {
 public:
  MOCK_METHOD1(Allocate, absl::optional<Allocation>(size_t bytes));
  MOCK_METHOD1(FallbackAllocate, absl::optional<Allocation>(size_t bytes));
  MOCK_METHOD1(Free, void(Allocation));
  MOCK_CONST_METHOD0(GetFootprintLimit, int64_t());
  MOCK_CONST_METHOD0(Allocated, int64_t());
  MOCK_CONST_METHOD0(FallbackAllocated, int64_t());
};

TEST(ObjectStoreTest, PassThroughTest) {
  MockAllocator allocator;
  ObjectStore store(allocator);
  {
    auto info = CreateObjectInfo(kId1, 10);
    auto allocation = CreateAllocation(Allocation(), 10);
    auto alloc_str = Serialize(allocation);

    EXPECT_CALL(allocator, Allocate(10)).Times(1).WillOnce(Invoke([&](size_t bytes) {
      EXPECT_EQ(bytes, 10);
      return absl::optional<Allocation>(std::move(allocation));
    }));
    auto entry = store.CreateObject(info, {}, /*fallback_allocate*/ false);
    EXPECT_NE(entry, nullptr);
    EXPECT_EQ(entry->ref_count, 0);
    EXPECT_EQ(entry->state, ObjectState::PLASMA_CREATED);
    EXPECT_EQ(alloc_str, Serialize(entry->allocation));
    EXPECT_EQ(info, entry->object_info);

    // verify get
    auto entry1 = store.GetObject(kId1);
    EXPECT_EQ(entry1, entry);

    // get non exists
    auto entry2 = store.GetObject(kId2);
    EXPECT_EQ(entry2, nullptr);

    // seal object
    auto entry3 = store.SealObject(kId1);
    EXPECT_EQ(entry3, entry);
    EXPECT_EQ(entry3->state, ObjectState::PLASMA_SEALED);

    // seal non existing
    EXPECT_EQ(nullptr, store.SealObject(kId2));

    // delete sealed
    EXPECT_CALL(allocator, Free(_)).Times(1).WillOnce(Invoke([&](auto &&allocation) {
      EXPECT_EQ(alloc_str, Serialize(allocation));
    }));

    EXPECT_TRUE(store.DeleteObject(kId1));
    EXPECT_EQ(nullptr, store.GetObject(kId1));

    // delete already deleted
    EXPECT_FALSE(store.DeleteObject(kId1));

    // delete non existing
    EXPECT_FALSE(store.DeleteObject(kId2));
  }

  {
    auto allocation = CreateAllocation(Allocation(), 12);
    auto alloc_str = Serialize(allocation);
    auto info = CreateObjectInfo(kId2, 12);
    // allocation failure
    EXPECT_CALL(allocator, Allocate(12)).Times(1).WillOnce(Invoke([&](size_t bytes) {
      EXPECT_EQ(bytes, 12);
      return absl::optional<Allocation>();
    }));

    EXPECT_EQ(nullptr, store.CreateObject(info, {}, /*fallback_allocate*/ false));

    // fallback allocation successful
    EXPECT_CALL(allocator, FallbackAllocate(12))
        .Times(1)
        .WillOnce(Invoke([&](size_t bytes) {
          EXPECT_EQ(bytes, 12);
          return absl::optional<Allocation>(std::move(allocation));
        }));

    auto entry = store.CreateObject(info, {}, /*fallback_allocate*/ true);
    EXPECT_NE(entry, nullptr);
    EXPECT_EQ(entry->ref_count, 0);
    EXPECT_EQ(entry->state, ObjectState::PLASMA_CREATED);
    EXPECT_EQ(alloc_str, Serialize(entry->allocation));
    EXPECT_EQ(info, entry->object_info);

    // delete unsealed
    EXPECT_CALL(allocator, Free(_)).Times(1).WillOnce(Invoke([&](auto &&allocation) {
      EXPECT_EQ(alloc_str, Serialize(allocation));
    }));

    EXPECT_TRUE(store.DeleteObject(kId2));
  }
}
}  // namespace plasma

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
