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

#include "ray/object_manager/plasma/object_lifecycle_manager.h"
#include "absl/time/clock.h"
#include "ray/common/ray_config.h"

namespace plasma {
using namespace flatbuf;

ObjectLifecycleManager::ObjectLifecycleManager(
    IAllocator &allocator, ray::DeleteObjectCallback delete_object_callback)
    : allocator_(allocator),
      object_store_(),
      eviction_policy_(object_store_, allocator_, allocator_.GetFootprintLimit()),
      delete_object_callback_(delete_object_callback),
      usage_log_interval_ns_(RayConfig::instance().object_store_usage_log_interval_s() *
                             1e9),
      last_usage_log_ns_(0),
      deletion_cache_(),
      num_bytes_in_use_(0) {}

const LocalObject *ObjectLifecycleManager::CreateObject(
    const ray::ObjectInfo &object_info, plasma::flatbuf::ObjectSource source,
    int device_num, bool fallback_allocator, PlasmaError *error) {
  RAY_LOG(DEBUG) << "attempting to create object " << object_info.object_id << " size "
                 << object_info.data_size;

  auto entry = object_store_.GetObject(object_info.object_id);
  if (entry != nullptr) {
    // There is already an object with the same ID in the Plasma Store, so
    // ignore this request.
    *error = PlasmaError::ObjectExists;
    return nullptr;
  }

  auto total_size = object_info.data_size + object_info.metadata_size;

  if (device_num != 0) {
    RAY_LOG(ERROR) << "device_num != 0 but CUDA not enabled";
    *error = PlasmaError::OutOfMemory;
    return nullptr;
  }
  *error = PlasmaError::OK;
  auto allocation = AllocateMemory(total_size,
                                   /*is_create=*/true, fallback_allocator, error);
  if (!allocation.address) {
    return nullptr;
  }
  entry = object_store_.CreateObject(std::move(allocation), object_info, source);
  eviction_policy_.ObjectCreated(object_info.object_id, true);
  return entry;
}

const LocalObject *ObjectLifecycleManager::GetObject(const ObjectID &object_id) const {
  return object_store_.GetObject(object_id);
}

const LocalObject *ObjectLifecycleManager::SealObject(const ObjectID &object_id) {
  return object_store_.SealObject(object_id);
}

void ObjectLifecycleManager::AbortObject(const ObjectID &object_id) {
  auto entry = object_store_.GetObject(object_id);
  RAY_CHECK(entry != nullptr) << "To abort an object it must be in the object table.";
  RAY_CHECK(entry->state != ObjectState::PLASMA_SEALED)
      << "To abort an object it must not have been sealed.";
  if (entry->ref_count > 0) {
    // A client was using this object.
    num_bytes_in_use_ -= entry->GetObjectSize();
    RAY_LOG(DEBUG) << "Erasing object " << object_id << " with nonzero ref count"
                   << object_id << ", num bytes in use is now " << num_bytes_in_use_;
  }

  eviction_policy_.RemoveObject(object_id);
  DeleteObjectImpl(object_id);
}

PlasmaError ObjectLifecycleManager::DeleteObject(const ObjectID &object_id) {
  auto entry = object_store_.GetObject(object_id);
  // TODO(rkn): This should probably not fail, but should instead throw an
  // error. Maybe we should also support deleting objects that have been
  // created but not sealed.
  if (entry == nullptr) {
    // To delete an object it must be in the object table.
    return PlasmaError::ObjectNonexistent;
  }

  if (entry->state != ObjectState::PLASMA_SEALED) {
    // To delete an object it must have been sealed.
    // Put it into deletion cache, it will be deleted later.
    deletion_cache_.emplace(object_id);
    return PlasmaError::ObjectNotSealed;
  }

  if (entry->ref_count != 0) {
    // To delete an object, there must be no clients currently using it.
    // Put it into deletion cache, it will be deleted later.
    deletion_cache_.emplace(object_id);
    return PlasmaError::ObjectInUse;
  }

  eviction_policy_.RemoveObject(object_id);
  DeleteObjectImpl(object_id);
  // Inform all subscribers that the object has been deleted.
  delete_object_callback_(object_id);
  return PlasmaError::OK;
}

int64_t ObjectLifecycleManager::RequireSpace(int64_t size) {
  std::vector<ObjectID> objects_to_evict;
  int64_t num_bytes_evicted =
      eviction_policy_.ChooseObjectsToEvict(size, &objects_to_evict);
  EvictObjects(objects_to_evict);
  return num_bytes_evicted;
}

void ObjectLifecycleManager::AddReference(const ObjectID &object_id) {
  auto entry = object_store_.GetObject(object_id);
  // If there are no other clients using this object, notify the eviction policy
  // that the object is being used.
  if (entry->ref_count == 0) {
    // Tell the eviction policy that this object is being used.
    eviction_policy_.BeginObjectAccess(object_id);
    num_bytes_in_use_ += entry->GetObjectSize();
  }
  // Increase reference count.
  entry->ref_count++;
  RAY_LOG(DEBUG) << "Object " << object_id << " in use by client"
                 << ", num bytes in use is now " << num_bytes_in_use_;
}

void ObjectLifecycleManager::RemoveReference(const ObjectID &object_id) {
  auto entry = object_store_.GetObject(object_id);
  entry->ref_count--;

  // If no more clients are using this object, notify the eviction policy
  // that the object is no longer being used.
  if (entry->ref_count == 0) {
    num_bytes_in_use_ -= entry->GetObjectSize();
    RAY_LOG(DEBUG) << "Releasing object no longer in use " << object_id
                   << ", num bytes in use is now " << num_bytes_in_use_;
    if (deletion_cache_.count(object_id) == 0) {
      // Tell the eviction policy that this object is no longer being used.
      eviction_policy_.EndObjectAccess(object_id);
    } else {
      // Above code does not really delete an object. Instead, it just put an
      // object to LRU cache which will be cleaned when the memory is not enough.
      deletion_cache_.erase(object_id);
      eviction_policy_.RemoveObject(object_id);
      // TODO: this looks like a bug:
      //  EvictObjects({object_id});
      DeleteObjectImpl(object_id);
      delete_object_callback_(object_id);
    }
  }
}

std::string ObjectLifecycleManager::EvictionPolicyDebugString() const {
  return eviction_policy_.DebugString();
}

Allocation ObjectLifecycleManager::AllocateMemory(size_t size, bool is_create,
                                                  bool fallback_allocator,
                                                  PlasmaError *error) {
  // Try to evict objects until there is enough space.
  Allocation allocation;
  int num_tries = 0;
  while (true) {
    // Allocate space for the new object. We use memalign instead of malloc
    // in order to align the allocated region to a 64-byte boundary. This is not
    // strictly necessary, but it is an optimization that could speed up the
    // computation of a hash of the data (see compute_object_hash_parallel in
    // plasma_client.cc). Note that even though this pointer is 64-byte aligned,
    // it is not guaranteed that the corresponding pointer in the client will be
    // 64-byte aligned, but in practice it often will be.
    allocation = allocator_.Memalign(kBlockSize, size);
    if (allocation.address != nullptr) {
      // If we manage to allocate the memory, return the pointer. If we cannot
      // allocate the space, but we are also not allowed to evict anything to
      // make more space, return an error to the client.
      *error = PlasmaError::OutOfMemory;
      break;
    }
    // Tell the eviction policy how much space we need to create this object.
    std::vector<ObjectID> objects_to_evict;
    int64_t space_needed = eviction_policy_.RequireSpace(size, &objects_to_evict);
    EvictObjects(objects_to_evict);
    // More space is still needed.
    if (space_needed > 0) {
      RAY_LOG(DEBUG) << "attempt to allocate " << size << " failed, need "
                     << space_needed;
      *error = PlasmaError::OutOfMemory;
      break;
    }

    // NOTE(ekl) if we can't achieve this after a number of retries, it's
    // because memory fragmentation in dlmalloc prevents us from allocating
    // even if our footprint tracker here still says we have free space.
    if (num_tries++ > 10) {
      *error = PlasmaError::OutOfMemory;
      break;
    }
  }

  // Fallback to allocating from the filesystem.
  if (allocation.address == nullptr && RayConfig::instance().plasma_unlimited() &&
      fallback_allocator) {
    RAY_LOG(INFO)
        << "Shared memory store full, falling back to allocating from filesystem: "
        << size;
    allocation = allocator_.DiskMemalignUnlimited(kBlockSize, size);
    if (allocation.address == nullptr) {
      RAY_LOG(ERROR) << "Plasma fallback allocator failed, likely out of disk space.";
    }
  } else if (!fallback_allocator) {
    RAY_LOG(DEBUG) << "Fallback allocation not enabled for this request.";
  }

  if (allocation.address != nullptr) {
    RAY_CHECK(allocation.fd.first != INVALID_FD);
    RAY_CHECK(allocation.fd.second != INVALID_UNIQUE_FD_ID);
    *error = PlasmaError::OK;
  }

  auto now = absl::GetCurrentTimeNanos();
  if (now - last_usage_log_ns_ > usage_log_interval_ns_) {
    RAY_LOG(INFO) << "Object store current usage " << (allocator_.Allocated() / 1e9)
                  << " / " << (allocator_.GetFootprintLimit() / 1e9) << " GB.";
    last_usage_log_ns_ = now;
  }
  return allocation;
}

void ObjectLifecycleManager::EvictObjects(const std::vector<ObjectID> &object_ids) {
  for (const auto &object_id : object_ids) {
    RAY_LOG(DEBUG) << "evicting object " << object_id.Hex();
    auto entry = object_store_.GetObject(object_id);
    // TODO(rkn): This should probably not fail, but should instead throw an
    // error. Maybe we should also support deleting objects that have been
    // created but not sealed.
    RAY_CHECK(entry != nullptr) << "To evict an object it must be in the object table.";
    RAY_CHECK(entry->state == ObjectState::PLASMA_SEALED)
        << "To evict an object it must have been sealed.";
    RAY_CHECK(entry->ref_count == 0)
        << "To evict an object, there must be no clients currently using it.";
    // Erase the object entry and send a deletion notification.
    DeleteObjectImpl(object_id);
    // Inform all subscribers that the object has been deleted.
    delete_object_callback_(object_id);
  }
}

void ObjectLifecycleManager::DeleteObjectImpl(const ObjectID &object_id) {
  allocator_.Free(object_store_.DeleteObject(object_id));
}

size_t ObjectLifecycleManager::GetNumBytesInUse() const { return num_bytes_in_use_; }

bool ObjectLifecycleManager::ContainsSealedObject(const ObjectID &object_id) {
  return object_store_.ContainsSealedObject(object_id);
}

size_t ObjectLifecycleManager::GetNumBytesCreatedTotal() const {
  return object_store_.GetNumBytesCreatedTotal();
}

size_t ObjectLifecycleManager::GetNumBytesUnsealed() const {
  return object_store_.GetNumBytesUnsealed();
}

size_t ObjectLifecycleManager::GetNumObjectsUnsealed() const {
  return object_store_.GetNumObjectsUnsealed();
}

void ObjectLifecycleManager::GetDebugDump(std::stringstream &buffer) const {
  return object_store_.GetDebugDump(buffer);
}

}  // namespace plasma
