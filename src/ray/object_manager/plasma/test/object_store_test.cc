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
// namespace {
// template <typename T>
// T Random(T max = std::numeric_limits<T>::max()) {
//   static absl::BitGen bitgen;
//   return absl::Uniform(bitgen, 0, max);
// }

// Allocation CreateAllocation(Allocation alloc, int64_t size) {
//   alloc.size = size;
//   alloc.offset = Random<ptrdiff_t>();
//   alloc.mmap_size = Random<int64_t>();
//   return alloc;
// }

// const std::string Serialize(const Allocation &allocation) {
//   return absl::StrFormat("%p/%d/%d/%d/%d/%d/%d", allocation.address, allocation.size,
//                          allocation.fd.first, allocation.fd.second, allocation.offset,
//                          allocation.device_num, allocation.mmap_size);
// }

// ObjectInfo CreateObjectInfo(ObjectID object_id, int64_t object_size) {
//   ObjectInfo info;
//   info.object_id = object_id;
//   info.data_size = Random<int64_t>(object_size);
//   info.metadata_size = object_size - info.data_size;
//   info.owner_raylet_id = NodeID::FromRandom();
//   info.owner_ip_address = "random_ip";
//   info.owner_port = Random<int>();
//   info.owner_worker_id = WorkerID::FromRandom();
//   return info;
// }

// const ObjectID kId1 = ObjectID::FromRandom();
// const ObjectID kId2 = []() {
//   auto id = ObjectID::FromRandom();
//   while (id == kId1) {
//     id = ObjectID::FromRandom();
//   }
//   return id;
// }();
// }  // namespace

class MockAllocator : public IAllocator {
 public:
  MOCK_METHOD1(Allocate, absl::optional<Allocation>(size_t bytes));
  MOCK_METHOD1(FallbackAllocate, absl::optional<Allocation>(size_t bytes));
  MOCK_METHOD1(Free, void(Allocation));
  MOCK_CONST_METHOD0(GetFootprintLimit, int64_t());
  MOCK_CONST_METHOD0(Allocated, int64_t());
  MOCK_CONST_METHOD0(FallbackAllocated, int64_t());
};

class ObjectStoreTest : public ::testing::Test {
 public:
  ObjectStoreTest() {}

  /// Generate the metrics by iterating a table.
  /// It is used to test eager metrics update.
  ObjectStore::ObjectStoreMetrics GetMetrics(ObjectStore &store) {
    ObjectStore::ObjectStoreMetrics metrics;
    for (const auto &obj_entry : store.object_table_) {
      const auto &obj = obj_entry.second;
      metrics.num_bytes_created_total += obj->GetObjectSize();
      if (obj->state == ObjectState::PLASMA_CREATED) {
        metrics.num_objects_unsealed++;
        metrics.num_bytes_unsealed += obj->GetObjectSize();
      }

      if (obj->source == plasma::flatbuf::ObjectSource::CreatedByWorker) {
        metrics.num_objects_created_by_worker++;
        metrics.num_bytes_created_by_worker += obj->GetObjectSize();
      } else if (obj->source == plasma::flatbuf::ObjectSource::RestoredFromStorage) {
        metrics.num_objects_restored++;
        metrics.num_bytes_restored += obj->GetObjectSize();
      } else if (obj->source == plasma::flatbuf::ObjectSource::ReceivedFromRemoteRaylet) {
        metrics.num_objects_received++;
        metrics.num_bytes_received += obj->GetObjectSize();
      } else if (obj->source == plasma::flatbuf::ObjectSource::ErrorStoredByRaylet) {
        metrics.num_objects_errored++;
        metrics.num_bytes_errored += obj->GetObjectSize();
      }
    }
    return metrics;
  }

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
    return absl::StrFormat("%p/%d/%d/%d/%d/%d/%d", allocation.address, allocation.size,
                           allocation.fd.first, allocation.fd.second, allocation.offset,
                           allocation.device_num, allocation.mmap_size);
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
  const ObjectID kId2 = [this]() {
    auto id = ObjectID::FromRandom();
    while (id == kId1) {
      id = ObjectID::FromRandom();
    }
    return id;
  }();
};

TEST_F(ObjectStoreTest, PassThroughTest) {
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
    EXPECT_EQ(store.GetNumBytesCreatedTotal(), 10);
    EXPECT_EQ(store.GetNumBytesUnsealed(), 10);
    EXPECT_EQ(store.GetNumObjectsUnsealed(), 1);
    // verify metrics after creation
    EXPECT_EQ(GetMetrics(store), store.GetMetrics());

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
    EXPECT_EQ(store.GetNumBytesCreatedTotal(), 10);
    EXPECT_EQ(store.GetNumBytesUnsealed(), 0);
    EXPECT_EQ(store.GetNumObjectsUnsealed(), 0);
    // verify metrics after seal
    EXPECT_EQ(GetMetrics(store), store.GetMetrics());

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

    // verify metrics after deletion
    EXPECT_EQ(GetMetrics(store), store.GetMetrics());
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
    EXPECT_EQ(store.GetNumBytesCreatedTotal(), 22);
    EXPECT_EQ(store.GetNumBytesUnsealed(), 12);
    EXPECT_EQ(store.GetNumObjectsUnsealed(), 1);
    // verify metrics after fallback allocation
    EXPECT_EQ(GetMetrics(store), store.GetMetrics());

    // delete unsealed
    EXPECT_CALL(allocator, Free(_)).Times(1).WillOnce(Invoke([&](auto &&allocation) {
      EXPECT_EQ(alloc_str, Serialize(allocation));
    }));

    EXPECT_TRUE(store.DeleteObject(kId2));

    EXPECT_EQ(store.GetNumBytesCreatedTotal(), 22);
    EXPECT_EQ(store.GetNumBytesUnsealed(), 0);
    EXPECT_EQ(store.GetNumObjectsUnsealed(), 0);
    // verify metrics after fallback allocation
    EXPECT_EQ(GetMetrics(store), store.GetMetrics());
  }
}

TEST_F(ObjectStoreTest, SourceMetricsTest) {
  MockAllocator allocator;
  ObjectStore store(allocator);
  auto test_source = [&](plasma::flatbuf::ObjectSource source, int create, int del) {
    // verify metrics after creation
    std::vector<ObjectID> oids;
    for (int i = 0; i < create; i++) {
      auto oid = ObjectID::FromRandom();
      oids.emplace_back(oid);
      auto info = CreateObjectInfo(oid, 10);
      auto allocation = CreateAllocation(Allocation(), 10);
      auto alloc_str = Serialize(allocation);

      EXPECT_CALL(allocator, Allocate(10)).Times(1).WillOnce(Invoke([&](size_t bytes) {
        EXPECT_EQ(bytes, 10);
        return absl::optional<Allocation>(std::move(allocation));
      }));
      // create objects from a certain source.
      store.CreateObject(info, source, /*fallback_allocate*/ false);
    }
    EXPECT_EQ(GetMetrics(store), store.GetMetrics());

    // verify metrics after deletion
    for (int i = 0; i < del; i++) {
      ObjectID oid;
      if (oids.size() < i) {
        oid = oids[i];
      } else {
        // no op oid
        oid = ObjectID::FromRandom();
      }
      store.DeleteObject(oid);
    }
    EXPECT_EQ(GetMetrics(store), store.GetMetrics());
  };
  test_source(plasma::flatbuf::ObjectSource::CreatedByWorker, 4, 3);
  test_source(plasma::flatbuf::ObjectSource::RestoredFromStorage, 4, 4);
  test_source(plasma::flatbuf::ObjectSource::ReceivedFromRemoteRaylet, 10, 1);
  test_source(plasma::flatbuf::ObjectSource::ErrorStoredByRaylet, 2, 7);
}
}  // namespace plasma

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
