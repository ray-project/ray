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

#include <gtest/gtest_prod.h>

#include <cstddef>
#include <utility>

#include "ray/common/id.h"
#include "ray/object_manager/common.h"
#include "ray/object_manager/plasma/plasma.h"
#include "ray/object_manager/plasma/plasma_generated.h"
#include "ray/util/compat.h"

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

inline constexpr std::string_view kCorruptedRequestErrorMessage =
    "This could be due to "
    "process forking in core worker or driver code which results in multiple processes "
    "sharing the same Plasma store socket. Please ensure that there are no "
    "process forking in any of the application core worker or driver code. Follow the "
    "link here to learn more about the issue and how to fix it: "
    "https://docs.ray.io/en/latest/ray-core/patterns/fork-new-processes.html";

// Represents a chunk of allocated memory.
struct Allocation {
  /// Pointer to the allocated memory.
  void *address_;
  /// Num bytes of the allocated memory.
  int64_t size_;
  /// The file descriptor of the memory mapped file where the memory allocated.
  MEMFD_TYPE fd_;
  /// The offset in bytes in the memory mapped file of the allocated memory.
  ptrdiff_t offset_;
  /// Device number of the allocated memory.
  int device_num_;
  /// the total size of this mapped memory.
  int64_t mmap_size_;
  /// if it was fallback allocated.
  bool fallback_allocated_;

  // only allow moves.
  Allocation(const Allocation &) = delete;
  Allocation &operator=(const Allocation &) = delete;
  Allocation(Allocation &&) noexcept = default;
  Allocation &operator=(Allocation &&) noexcept = default;

 private:
  // Only created by Allocator
  Allocation(void *address,
             int64_t size,
             MEMFD_TYPE fd,
             ptrdiff_t offset,
             int device_num,
             int64_t mmap_size,
             bool fallback_allocated)
      : address_(address),
        size_(size),
        fd_(std::move(fd)),
        offset_(offset),
        device_num_(device_num),
        mmap_size_(mmap_size),
        fallback_allocated_(fallback_allocated) {}

  // Test only
  Allocation()
      : address_(nullptr),
        size_(0),
        fd_(),
        offset_(0),
        device_num_(0),
        mmap_size_(0),
        fallback_allocated_(false) {}

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
  explicit LocalObject(Allocation allocation)
      : allocation_(std::move(allocation)), ref_count_(0) {}

  LocalObject(const LocalObject &) = delete;
  LocalObject &operator=(const LocalObject &) = delete;

  int64_t GetObjectSize() const { return object_info_.GetObjectSize(); }

  bool Sealed() const { return state_ == ObjectState::PLASMA_SEALED; }

  int32_t GetRefCount() const { return ref_count_; }

  const ray::ObjectInfo &GetObjectInfo() const { return object_info_; }

  const Allocation &GetAllocation() const { return allocation_; }

  const plasma::flatbuf::ObjectSource &GetSource() const { return source_; }

  ray::PlasmaObjectHeader *GetPlasmaObjectHeader() const {
    RAY_CHECK(object_info_.is_mutable) << "Object is not mutable";
    auto header_ptr = static_cast<uint8_t *>(allocation_.address_);
    return reinterpret_cast<ray::PlasmaObjectHeader *>(header_ptr);
  }

  void ToPlasmaObject(PlasmaObject *object, bool check_sealed) const {
    RAY_DCHECK(object != nullptr);
    if (check_sealed) {
      RAY_DCHECK(Sealed());
    }
    object->store_fd = GetAllocation().fd_;
    object->header_offset = GetAllocation().offset_;
    object->data_offset = GetAllocation().offset_;
    object->metadata_offset = GetAllocation().offset_ + GetObjectInfo().data_size;
    if (object_info_.is_mutable) {
      object->data_offset += sizeof(ray::PlasmaObjectHeader);
      object->metadata_offset += sizeof(ray::PlasmaObjectHeader);
    };
    object->data_size = GetObjectInfo().data_size;
    object->metadata_size = GetObjectInfo().metadata_size;
    // Senders and receivers of a channel may store different data and metadata
    // sizes locally depending on what data is written to the channel, but the
    // plasma store keeps the original data and metadata size.
    object->allocated_size = object->data_size + object->metadata_size;
    object->device_num = GetAllocation().device_num_;
    object->mmap_size = GetAllocation().mmap_size_;
    object->fallback_allocated = GetAllocation().fallback_allocated_;
    object->is_experimental_mutable_object = object_info_.is_mutable;
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
  Allocation allocation_;
  /// Ray object info;
  ray::ObjectInfo object_info_;
  /// Number of clients currently using this object.
  /// TODO: ref_count probably shouldn't belong to LocalObject.
  mutable int32_t ref_count_;
  /// Unix epoch of when this object was created.
  int64_t create_time_;
  /// How long creation of this object took.
  int64_t construct_duration_;
  /// The state of the object, e.g., whether it is open or sealed.
  ObjectState state_;
  /// The source of the object. Used for debugging purposes.
  plasma::flatbuf::ObjectSource source_;
};
}  // namespace plasma
