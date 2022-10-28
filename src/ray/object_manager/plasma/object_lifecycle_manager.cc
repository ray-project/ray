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
    : object_store_(std::make_unique<ObjectStore>(allocator)),
      eviction_policy_(std::make_unique<EvictionPolicy>(*object_store_, allocator)),
      delete_object_callback_(delete_object_callback),
      earger_deletion_objects_(),
      stats_collector_(std::make_unique<ObjectStatsCollector>()) {}

std::pair<const LocalObject *, flatbuf::PlasmaError> ObjectLifecycleManager::CreateObject(
    const ray::ObjectInfo &object_info,
    plasma::flatbuf::ObjectSource source,
    bool fallback_allocator) {
  RAY_LOG(DEBUG) << "attempting to create object " << object_info.object_id << " size "
                 << object_info.data_size;
  if (object_store_->GetObject(object_info.object_id) != nullptr) {
    return {nullptr, PlasmaError::ObjectExists};
  }
  auto entry = CreateObjectInternal(object_info, source, fallback_allocator);

  if (entry == nullptr) {
    return {nullptr, PlasmaError::OutOfMemory};
  }
  eviction_policy_->ObjectCreated(object_info.object_id);
  stats_collector_->OnObjectCreated(*entry);
  return {entry, PlasmaError::OK};
}

const LocalObject *ObjectLifecycleManager::GetObject(const ObjectID &object_id) const {
  return object_store_->GetObject(object_id);
}

const LocalObject *ObjectLifecycleManager::SealObject(const ObjectID &object_id) {
  // TODO(scv119): should we check delete object from earger_deletion_objects_?
  auto entry = object_store_->SealObject(object_id);
  if (entry != nullptr) {
    stats_collector_->OnObjectSealed(*entry);
  }
  return entry;
}

flatbuf::PlasmaError ObjectLifecycleManager::AbortObject(const ObjectID &object_id) {
  auto entry = object_store_->GetObject(object_id);
  if (entry == nullptr) {
    RAY_LOG(ERROR) << "To abort an object it must be in the object table.";
    return PlasmaError::ObjectNonexistent;
  }
  if (entry->state == ObjectState::PLASMA_SEALED) {
    RAY_LOG(ERROR) << "To abort an object it must not have been sealed.";
    return PlasmaError::ObjectSealed;
  }

  bool abort_while_using = entry->ref_count > 0;
  DeleteObjectInternal(object_id);

  if (abort_while_using) {
    RAY_LOG(DEBUG) << "Erasing object " << object_id << " with nonzero ref count"
                   << object_id << ", num bytes in use is now " << GetNumBytesInUse();
  }
  return PlasmaError::OK;
}

PlasmaError ObjectLifecycleManager::DeleteObject(const ObjectID &object_id) {
  auto entry = object_store_->GetObject(object_id);
  if (entry == nullptr) {
    return PlasmaError::ObjectNonexistent;
  }

  // TODO(scv119): should we delete unsealed with ref_count 0?
  if (entry->state != ObjectState::PLASMA_SEALED) {
    // To delete an object it must have been sealed,
    // otherwise there might be memeory corruption.
    // Put it into deletion cache, it will be deleted later.
    earger_deletion_objects_.emplace(object_id);
    return PlasmaError::ObjectNotSealed;
  }

  if (entry->ref_count != 0) {
    // To delete an object, there must be no clients currently using it.
    // Put it into deletion cache, it will be deleted later.
    earger_deletion_objects_.emplace(object_id);
    return PlasmaError::ObjectInUse;
  }

  DeleteObjectInternal(object_id);
  return PlasmaError::OK;
}

int64_t ObjectLifecycleManager::RequireSpace(int64_t size) {
  std::vector<ObjectID> objects_to_evict;
  int64_t num_bytes_evicted =
      eviction_policy_->ChooseObjectsToEvict(size, objects_to_evict);
  EvictObjects(objects_to_evict);
  return num_bytes_evicted;
}

bool ObjectLifecycleManager::AddReference(const ObjectID &object_id) {
  auto entry = object_store_->GetObject(object_id);
  if (!entry) {
    RAY_LOG(ERROR) << object_id << " doesn't exist, add reference failed.";
    return false;
  }
  // If there are no other clients using this object, notify the eviction policy
  // that the object is being used.
  if (entry->ref_count == 0) {
    // Tell the eviction policy that this object is being used.
    eviction_policy_->BeginObjectAccess(object_id);
  }
  // Increase reference count.
  entry->ref_count++;
  stats_collector_->OnObjectRefIncreased(*entry);
  RAY_LOG(DEBUG) << "Object " << object_id << " reference has incremented"
                 << ", num bytes in use is now " << GetNumBytesInUse();
  return true;
}

bool ObjectLifecycleManager::RemoveReference(const ObjectID &object_id) {
  auto entry = object_store_->GetObject(object_id);
  if (!entry || entry->ref_count == 0) {
    RAY_LOG(ERROR)
        << object_id
        << " doesn't exist, or its ref count is already 0, remove reference failed.";
    return false;
  }

  entry->ref_count--;
  stats_collector_->OnObjectRefDecreased(*entry);

  if (entry->ref_count > 0) {
    return true;
  }

  RAY_LOG(DEBUG) << "Releasing object no longer in use " << object_id
                 << ", num bytes in use is now " << GetNumBytesInUse();

  eviction_policy_->EndObjectAccess(object_id);

  // TODO(scv119): handle this anomaly in upper layer.
  RAY_CHECK(entry->Sealed()) << object_id << " is not sealed while ref count becomes 0.";
  if (earger_deletion_objects_.count(object_id) > 0) {
    DeleteObjectInternal(object_id);
  }
  return true;
}

std::string ObjectLifecycleManager::EvictionPolicyDebugString() const {
  return eviction_policy_->DebugString();
}

const LocalObject *ObjectLifecycleManager::CreateObjectInternal(
    const ray::ObjectInfo &object_info,
    plasma::flatbuf::ObjectSource source,
    bool allow_fallback_allocation) {
  // Try to evict objects until there is enough space.
  // NOTE(ekl) if we can't achieve this after a number of retries, it's
  // because memory fragmentation in dlmalloc prevents us from allocating
  // even if our footprint tracker here still says we have free space.
  for (int num_tries = 0; num_tries <= 10; num_tries++) {
    auto result =
        object_store_->CreateObject(object_info, source, /*fallback_allocate*/ false);
    if (result != nullptr) {
      return result;
    }
    // Tell the eviction policy how much space we need to create this object.
    std::vector<ObjectID> objects_to_evict;
    int64_t space_needed =
        eviction_policy_->RequireSpace(object_info.GetObjectSize(), objects_to_evict);
    EvictObjects(objects_to_evict);
    // More space is still needed.
    if (space_needed > 0) {
      RAY_LOG(DEBUG) << "attempt to allocate " << object_info.GetObjectSize()
                     << " failed, need " << space_needed;
      break;
    }
  }

  if (!allow_fallback_allocation) {
    RAY_LOG(DEBUG) << "Fallback allocation not enabled for this request.";
    return nullptr;
  }

  RAY_LOG(INFO)
      << "Shared memory store full, falling back to allocating from filesystem: "
      << object_info.GetObjectSize();

  auto result =
      object_store_->CreateObject(object_info, source, /*fallback_allocate*/ true);

  if (result == nullptr) {
    RAY_LOG(ERROR) << "Plasma fallback allocator failed, likely out of disk space.";
  }
  return result;
}

void ObjectLifecycleManager::EvictObjects(const std::vector<ObjectID> &object_ids) {
  for (const auto &object_id : object_ids) {
    RAY_LOG(DEBUG) << "evicting object " << object_id.Hex();
    auto entry = object_store_->GetObject(object_id);
    // TODO(rkn): This should probably not fail, but should instead throw an
    // error. Maybe we should also support deleting objects that have been
    // created but not sealed.
    RAY_CHECK(entry != nullptr) << "To evict an object it must be in the object table.";
    RAY_CHECK(entry->state == ObjectState::PLASMA_SEALED)
        << "To evict an object it must have been sealed.";
    RAY_CHECK(entry->ref_count == 0)
        << "To evict an object, there must be no clients currently using it.";

    DeleteObjectInternal(object_id);
  }
}

void ObjectLifecycleManager::DeleteObjectInternal(const ObjectID &object_id) {
  auto entry = object_store_->GetObject(object_id);
  RAY_CHECK(entry != nullptr);

  bool aborted = entry->state == ObjectState::PLASMA_CREATED;

  stats_collector_->OnObjectDeleting(*entry);
  earger_deletion_objects_.erase(object_id);
  eviction_policy_->RemoveObject(object_id);
  object_store_->DeleteObject(object_id);

  if (!aborted) {
    // only send notification if it's not aborted.
    delete_object_callback_(object_id);
  }
}

int64_t ObjectLifecycleManager::GetNumBytesInUse() const {
  return stats_collector_->GetNumBytesInUse();
}

bool ObjectLifecycleManager::IsObjectSealed(const ObjectID &object_id) const {
  auto entry = GetObject(object_id);
  return entry && entry->state == ObjectState::PLASMA_SEALED;
}

int64_t ObjectLifecycleManager::GetNumBytesCreatedTotal() const {
  return stats_collector_->GetNumBytesCreatedTotal();
}

int64_t ObjectLifecycleManager::GetNumBytesUnsealed() const {
  return stats_collector_->GetNumBytesUnsealed();
}

int64_t ObjectLifecycleManager::GetNumObjectsUnsealed() const {
  return stats_collector_->GetNumObjectsUnsealed();
}

void ObjectLifecycleManager::RecordMetrics() const { stats_collector_->RecordMetrics(); }

void ObjectLifecycleManager::GetDebugDump(std::stringstream &buffer) const {
  return stats_collector_->GetDebugDump(buffer);
}

// For test only.
ObjectLifecycleManager::ObjectLifecycleManager(
    std::unique_ptr<IObjectStore> store,
    std::unique_ptr<IEvictionPolicy> eviction_policy,
    ray::DeleteObjectCallback delete_object_callback,
    std::unique_ptr<ObjectStatsCollector> stats_collector)
    : object_store_(std::move(store)),
      eviction_policy_(std::move(eviction_policy)),
      delete_object_callback_(delete_object_callback),
      earger_deletion_objects_(),
      stats_collector_(std::move(stats_collector)) {}

}  // namespace plasma
