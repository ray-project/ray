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

#include <functional>
#include <limits>
#include <optional>
#include <string>
#include <utility>

#include "absl/random/random.h"
#include "absl/strings/str_format.h"
#include "gtest/gtest.h"

using ray::NodeID;
using ray::ObjectID;
using ray::ObjectInfo;
using ray::WorkerID;

namespace plasma {
namespace {
template <typename T>
T Random(T max = std::numeric_limits<T>::max()) {
  static absl::BitGen bitgen;
  return absl::Uniform(bitgen, 0, max);
}

Allocation CreateAllocation(Allocation alloc,
                            int64_t size,
                            bool fallback_allocated = false) {
  alloc.size_ = size;
  alloc.offset_ = Random<ptrdiff_t>();
  alloc.mmap_size_ = Random<int64_t>();
  alloc.fallback_allocated_ = fallback_allocated;
  return alloc;
}

const std::string Serialize(const Allocation &allocation) {
  return absl::StrFormat("%p/%d/%d/%d/%d/%d/%d",
                         allocation.address_,
                         allocation.size_,
                         allocation.fd_.first,
                         allocation.fd_.second,
                         allocation.offset_,
                         allocation.device_num_,
                         allocation.mmap_size_);
}

ObjectInfo CreateObjectInfo(ObjectID object_id, int64_t object_size) {
  ObjectInfo info;
  info.object_id = object_id;
  info.data_size = Random<int64_t>(object_size);
  info.metadata_size = object_size - info.data_size;
  info.owner_node_id = NodeID::FromRandom();
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

// Hand-written fake allocator. Each method records how many times it was called
// and delegates to an optional, test-settable std::function hook so that tests can
// inject behavior (and assert on the argument) at the call site.
class FakeAllocator : public IAllocator {
 public:
  std::optional<Allocation> Allocate(size_t bytes) override {
    allocate_call_count++;
    if (allocate_hook) {
      return allocate_hook(bytes);
    }
    return std::nullopt;
  }
  std::optional<Allocation> FallbackAllocate(size_t bytes) override {
    fallback_allocate_call_count++;
    if (fallback_allocate_hook) {
      return fallback_allocate_hook(bytes);
    }
    return std::nullopt;
  }
  void Free(Allocation allocation) override {
    free_call_count++;
    if (free_hook) {
      free_hook(allocation);
    }
  }
  int64_t GetFootprintLimit() const override { return footprint_limit; }
  int64_t Allocated() const override { return allocated; }
  int64_t FallbackAllocated() const override { return fallback_allocated; }

  std::function<std::optional<Allocation>(size_t)> allocate_hook;
  std::function<std::optional<Allocation>(size_t)> fallback_allocate_hook;
  std::function<void(const Allocation &)> free_hook;

  int allocate_call_count = 0;
  int fallback_allocate_call_count = 0;
  int free_call_count = 0;

  int64_t footprint_limit = 0;
  int64_t allocated = 0;
  int64_t fallback_allocated = 0;
};

TEST(ObjectStoreTest, PassThroughTest) {
  FakeAllocator allocator;
  ObjectStore store(allocator);
  {
    auto info = CreateObjectInfo(kId1, 10);
    auto allocation = CreateAllocation(Allocation(), 10);
    auto alloc_str = Serialize(allocation);

    allocator.allocate_hook = [&](size_t bytes) {
      EXPECT_EQ(bytes, 10);
      return std::optional<Allocation>(std::move(allocation));
    };
    int allocate_count_before = allocator.allocate_call_count;
    auto entry = store.CreateObject(info, {}, /*fallback_allocate*/ false);
    EXPECT_EQ(allocator.allocate_call_count, allocate_count_before + 1);
    EXPECT_NE(entry, nullptr);
    EXPECT_EQ(entry->ref_count_, 0);
    EXPECT_EQ(entry->state_, ObjectState::PLASMA_CREATED);
    EXPECT_EQ(alloc_str, Serialize(entry->allocation_));
    EXPECT_EQ(info, entry->object_info_);
    EXPECT_FALSE(entry->allocation_.fallback_allocated_);

    // verify get
    auto entry1 = store.GetObject(kId1);
    EXPECT_EQ(entry1, entry);

    // get non exists
    auto entry2 = store.GetObject(kId2);
    EXPECT_EQ(entry2, nullptr);

    // seal object
    auto entry3 = store.SealObject(kId1);
    EXPECT_EQ(entry3, entry);
    EXPECT_EQ(entry3->state_, ObjectState::PLASMA_SEALED);

    // seal non existing
    EXPECT_EQ(nullptr, store.SealObject(kId2));

    // delete sealed
    allocator.free_hook = [&](const Allocation &allocation_arg) {
      EXPECT_EQ(alloc_str, Serialize(allocation_arg));
    };
    int free_count_before = allocator.free_call_count;
    EXPECT_TRUE(store.DeleteObject(kId1));
    EXPECT_EQ(allocator.free_call_count, free_count_before + 1);
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
    allocator.allocate_hook = [&](size_t bytes) {
      EXPECT_EQ(bytes, 12);
      return std::optional<Allocation>();
    };
    int allocate_count_before = allocator.allocate_call_count;
    EXPECT_EQ(nullptr, store.CreateObject(info, {}, /*fallback_allocate*/ false));
    EXPECT_EQ(allocator.allocate_call_count, allocate_count_before + 1);

    // fallback allocation successful
    allocation = CreateAllocation(Allocation(), 12, /* fallback_allocated */ true);
    alloc_str = Serialize(allocation);

    allocator.fallback_allocate_hook = [&](size_t bytes) {
      EXPECT_EQ(bytes, 12);
      return std::optional<Allocation>(std::move(allocation));
    };
    int fallback_allocate_count_before = allocator.fallback_allocate_call_count;
    auto entry = store.CreateObject(info, {}, /*fallback_allocate*/ true);
    EXPECT_EQ(allocator.fallback_allocate_call_count, fallback_allocate_count_before + 1);
    EXPECT_NE(entry, nullptr);
    EXPECT_EQ(entry->ref_count_, 0);
    EXPECT_EQ(entry->state_, ObjectState::PLASMA_CREATED);
    EXPECT_EQ(alloc_str, Serialize(entry->allocation_));
    EXPECT_EQ(info, entry->object_info_);
    EXPECT_TRUE(entry->allocation_.fallback_allocated_);

    // delete unsealed
    allocator.free_hook = [&](const Allocation &allocation_arg) {
      EXPECT_EQ(alloc_str, Serialize(allocation_arg));
    };
    int free_count_before = allocator.free_call_count;
    EXPECT_TRUE(store.DeleteObject(kId2));
    EXPECT_EQ(allocator.free_call_count, free_count_before + 1);
  }
}
}  // namespace plasma
