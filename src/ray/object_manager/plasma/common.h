// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <stddef.h>

#include <memory>
#include <string>
#include <unordered_map>

#include "gtest/gtest.h"
#include "ray/common/id.h"
#include "ray/object_manager/common.h"
#include "ray/object_manager/plasma/compat.h"
#include "ray/object_manager/plasma/plasma.h"
#include "ray/object_manager/plasma/plasma_generated.h"
#include "ray/util/macros.h"

namespace plasma {

using ray::NodeID;
using ray::ObjectID;
using ray::WorkerID;

enum class ObjectLocation : int32_t { Local, Remote, Nonexistent };

enum class ObjectState : int {
  /// Object was created but not sealed in the local Plasma Store.
  PLASMA_CREATED = 1,
  /// Object is sealed and stored in the local Plasma Store.
  PLASMA_SEALED = 2,
};

// Represents a chunk of allocated memory.
struct Allocation {
  /// Pointer to the allocated memory.
  void *address;
  /// Num bytes of the allocated memory.
  int64_t size;
  /// The file descriptor of the memory mapped file where the memory allocated.
  MEMFD_TYPE fd;
  /// The offset in bytes in the memory mapped file of the allocated memory.
  ptrdiff_t offset;
  /// Device number of the allocated memory.
  int device_num;
  /// the total size of this mapped memory.
  int64_t mmap_size;

  // only allow moves.
  RAY_DISALLOW_COPY_AND_ASSIGN(Allocation);
  Allocation(Allocation &&) noexcept = default;
  Allocation &operator=(Allocation &&) noexcept = default;

 private:
  // Only created by Allocator
  Allocation(void *address,
             int64_t size,
             MEMFD_TYPE fd,
             ptrdiff_t offset,
             int device_num,
             int64_t mmap_size)
      : address(address),
        size(size),
        fd(std::move(fd)),
        offset(offset),
        device_num(device_num),
        mmap_size(mmap_size) {}

  // Test only
  Allocation()
      : address(nullptr), size(0), fd(), offset(0), device_num(0), mmap_size(0) {}

  friend class PlasmaAllocator;
  friend class DummyAllocator;
  friend struct ObjectLifecycleManagerTest;
  FRIEND_TEST(ObjectStoreTest, PassThroughTest);
  FRIEND_TEST(EvictionPolicyTest, Test);
  friend struct GetRequestQueueTest;
};

/// This type is used by the Plasma store. It is here because it is exposed to
/// the eviction policy.
class LocalObject {
 public:
  LocalObject(Allocation allocation);

  RAY_DISALLOW_COPY_AND_ASSIGN(LocalObject);

  int64_t GetObjectSize() const { return object_info.GetObjectSize(); }

  bool Sealed() const { return state == ObjectState::PLASMA_SEALED; }

  int32_t GetRefCount() const { return ref_count; }

  const ray::ObjectInfo &GetObjectInfo() const { return object_info; }

  const Allocation &GetAllocation() const { return allocation; }

  const plasma::flatbuf::ObjectSource &GetSource() const { return source; }

  void ToPlasmaObject(PlasmaObject *object, bool check_sealed) const {
    RAY_DCHECK(object != nullptr);
    if (check_sealed) {
      RAY_DCHECK(Sealed());
    }
    object->store_fd = GetAllocation().fd;
    object->data_offset = GetAllocation().offset;
    object->metadata_offset = GetAllocation().offset + GetObjectInfo().data_size;
    object->data_size = GetObjectInfo().data_size;
    object->metadata_size = GetObjectInfo().metadata_size;
    object->device_num = GetAllocation().device_num;
    object->mmap_size = GetAllocation().mmap_size;
  }

 private:
  friend class ObjectStore;
  friend class ObjectLifecycleManager;
  FRIEND_TEST(ObjectStoreTest, PassThroughTest);
  friend struct ObjectLifecycleManagerTest;
  FRIEND_TEST(ObjectLifecycleManagerTest, RemoveReferenceOneRefNotSealed);
  friend struct ObjectStatsCollectorTest;
  FRIEND_TEST(EvictionPolicyTest, Test);
  friend struct GetRequestQueueTest;

  /// Allocation Info;
  Allocation allocation;
  /// Ray object info;
  ray::ObjectInfo object_info;
  /// Number of clients currently using this object.
  /// TODO: ref_count probably shouldn't belong to LocalObject.
  mutable int32_t ref_count;
  /// Unix epoch of when this object was created.
  int64_t create_time;
  /// How long creation of this object took.
  int64_t construct_duration;
  /// The state of the object, e.g., whether it is open or sealed.
  ObjectState state;
  /// The source of the object. Used for debugging purposes.
  plasma::flatbuf::ObjectSource source;
};
}  // namespace plasma
