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

#pragma once

#include <cstring>
#include <ctime>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "arrow/buffer.h"

#include "ray/common/id.h"
#include "ray/object_manager/format/object_manager_generated.h"
#include "ray/object_manager/plasma/quota_aware_policy.h"
#include "ray/object_manager/plasma/plasma_generated.h"

namespace plasma {

using flatbuf::PlasmaError;
using ray::ObjectID;
using ray::object_manager::protocol::ObjectInfoT;

namespace internal {

struct CudaIpcPlaceholder {};

}  //  namespace internal

enum class ObjectState : int {
  /// The object was not found.
  OBJECT_NOT_FOUND = 0,
  /// Object was created but not sealed in the local Plasma Store.
  PLASMA_CREATED = 1,
  /// Object is sealed and stored in the local Plasma Store.
  PLASMA_SEALED = 2,
  /// Object is evicted to external store.
  PLASMA_EVICTED = 3,
};

enum class ObjectStatus : int {
  /// The object was not found.
  OBJECT_NOT_FOUND = 0,
  /// The object was found.
  OBJECT_FOUND = 1
};

/// This type is used by the Plasma store. It is here because it is exposed to
/// the eviction policy.
struct ObjectTableEntry {
  ObjectTableEntry();

  ~ObjectTableEntry();

  /// Memory mapped file containing the object.
  int fd;
  /// Device number.
  int device_num;
  /// Size of the underlying map.
  int64_t map_size;
  /// Offset from the base of the mmap.
  ptrdiff_t offset;
  /// Pointer to the object data. Needed to free the object.
  uint8_t* pointer;
  /// Size of the object in bytes.
  int64_t data_size;
  /// Size of the object metadata in bytes.
  int64_t metadata_size;
  /// Number of clients currently using this object.
  int ref_count;
  /// Unix epoch of when this object was created.
  int64_t create_time;
  /// How long creation of this object took.
  int64_t construct_duration;

  /// The state of the object, e.g., whether it is open or sealed.
  ObjectState state;

#ifdef PLASMA_CUDA
  /// IPC GPU handle to share with clients.
  std::shared_ptr<::arrow::cuda::CudaIpcMemHandle> ipc_handle;
#else
  std::shared_ptr<internal::CudaIpcPlaceholder> ipc_handle;
#endif
};

void PlasmaObject_init(PlasmaObject* object, ObjectTableEntry* entry) {
  RAY_DCHECK(object != nullptr);
  RAY_DCHECK(entry != nullptr);
  RAY_DCHECK(entry->state == ObjectState::PLASMA_SEALED);
#ifdef PLASMA_CUDA
  if (entry->device_num != 0) {
    object->ipc_handle = entry->ipc_handle;
  }
#endif
  object->store_fd = entry->fd;
  object->data_offset = entry->offset;
  object->metadata_offset = entry->offset + entry->data_size;
  object->data_size = entry->data_size;
  object->metadata_size = entry->metadata_size;
  object->device_num = entry->device_num;
}

class ObjectDirectory {
 public:
  ObjectDirectory() : eviction_policy_(this, PlasmaAllocator::GetFootprintLimit()) {}

  /// Get the size of the object.
  ///
  /// \param object_id Object ID of the object.
  /// \return Object size in bytes.
  int64_t GetObjectSize(const ObjectID& object_id) {
   absl::MutexLock lock(&object_table_mutex_);
   auto& entry = object_table_[object_id];
   return entry->data_size + entry->metadata_size;
  }

  /// Get the state of the object.
  ///
  /// \param object_id Object ID of the object.
  /// \return OBJECT_NOT_FOUND if the object is not in the store, otherwise
  /// the object state in the entry.
  ObjectState GetObjectState(const ObjectID& object_id) {
    absl::MutexLock lock(&object_table_mutex_);
    auto it = object_table_.find(object_id);
    if (it != object_table_.end()) {
      return it->second->state;
    }
    return ObjectState::OBJECT_NOT_FOUND;
  }

  /// Set the state of the object.
  ///
  /// \param object_id Object ID of the object.
  /// \param state The new state for the object.
  void SetObjectState(const ObjectID& object_id, ObjectState state) {
    absl::MutexLock lock(&object_table_mutex_);
    auto it = object_table_.find(object_id);
    object_table_[object_id]->state = state;
  }

  void GetSealedObjectsInfo(std::vector<ObjectInfoT>* infos) {
    absl::MutexLock lock(&object_table_mutex_);
    for (const auto& entry : object_table_) {
      if (entry.second->state == ObjectState::PLASMA_SEALED) {
        ObjectInfoT info;
        info.object_id = entry.first.Binary();
        info.data_size = entry.second->data_size;
        info.metadata_size = entry.second->metadata_size;
        infos->push_back(info);
      }
    }
  }

  /// Check if the plasma store contains an object:
  ///
  /// \param object_id Object ID that will be checked.
  /// \return OBJECT_FOUND if the object is in the store, OBJECT_NOT_FOUND if
  /// not
  ObjectStatus ContainsObject(const ObjectID& object_id) {
    absl::MutexLock lock(&object_table_mutex_);
    auto entry = GetObjectTableEntry(object_id);
    return entry && (entry->state == ObjectState::PLASMA_SEALED ||
                    entry->state == ObjectState::PLASMA_EVICTED)
              ? ObjectStatus::OBJECT_FOUND
              : ObjectStatus::OBJECT_NOT_FOUND;
  }

  /// Forcefully delete an object in the store.
  ///
  /// \param object_id Object ID of the object to be deleted.
  /// \param evict_only Only free the memory of the object.
  void EraseObject(const ObjectID& object_id, bool evict_only=false) {
    int64_t buff_size;
    uint8_t* pointer;
    int device_num;
    {
      absl::MutexLock lock(&object_table_mutex_);
      if (evict_only) {
        auto& entry = object_table_[object_id];
        pointer = entry->pointer;
        buff_size = entry->data_size + entry->metadata_size;
        device_num = entry->device_num;
        entry->pointer = nullptr;
        entry->state = ObjectState::PLASMA_EVICTED;
      } else {
        auto entry_node = object_table_.extract(object_id);
        auto& entry = entry_node.mapped();
        pointer = entry->pointer;
        buff_size = entry->data_size + entry->metadata_size;
        device_num = entry->device_num;
      }
    }
  
    if (device_num == 0) {
      PlasmaAllocator::Free(pointer, buff_size);
    } else {
#ifdef PLASMA_CUDA
      RAY_CHECK_OK(FreeCudaMemory(device_num, buff_size, pointer));
#endif
    }
  }

  /// Seal a vector of objects. The objects are now immutable and can be accessed with
  /// get.
  ///
  /// \param object_ids The vector of Object IDs of the objects to be sealed.
  /// \param infos The summary info of sealed objects.
  void SealObjects(const std::vector<ObjectID>& object_ids,
                   std::vector<ObjectInfoT> *infos) {
    infos->reserve(object_ids.size());
    RAY_LOG(DEBUG) << "sealing " << object_ids.size() << " objects";
    absl::MutexLock lock(&object_table_mutex_);
    for (size_t i = 0; i < object_ids.size(); ++i) {
      ObjectInfoT object_info;
      auto entry = GetObjectTableEntry(object_ids[i]);
      RAY_CHECK(entry != nullptr);
      RAY_CHECK(entry->state == ObjectState::PLASMA_CREATED);
      // Set the state of object to SEALED.
      entry->state = ObjectState::PLASMA_SEALED;
      // Set object construction duration.
      entry->construct_duration = std::time(nullptr) - entry->create_time;

      object_info.object_id = object_ids[i].Binary();
      object_info.data_size = entry->data_size;
      object_info.metadata_size = entry->metadata_size;
      infos->push_back(object_info);
    }
  }

  /// Delete a specific object by object_id that have been created in the hash table.
  ///
  /// \param object_id Object ID of the object to be deleted.
  /// \return One of the following error codes:
  ///  - PlasmaError::OK, if the object was delete successfully.
  ///  - PlasmaError::ObjectNonexistent, if ths object isn't existed.
  ///  - PlasmaError::ObjectInUse, if the object is in use.
  PlasmaError DeleteObject(const ObjectID& object_id) {
    {
      absl::MutexLock lock(&object_table_mutex_);
      auto it = object_table_.find(object_id);
      // TODO(rkn): This should probably not fail, but should instead throw an
      // error. Maybe we should also support deleting objects that have been
      // created but not sealed.
      if (it == object_table_.end()) {
        // To delete an object it must be in the object table.
        return PlasmaError::ObjectNonexistent;
      }
      auto& entry = it->second;
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
    }
    eviction_policy_.RemoveObject(object_id);
    EraseObject(object_id);
    return PlasmaError::OK;
  }

  void CheckObjectEvictable(const ObjectID& object_id) {
    absl::MutexLock lock(&object_table_mutex_);
    auto it = object_table_.find(object_id);
    // TODO(rkn): This should probably not fail, but should instead throw an
    // error. Maybe we should also support deleting objects that have been
    // created but not sealed.
    RAY_CHECK(it != object_table_.end()) << "To evict an object it must be in the object table.";
    auto& entry = it->second;
    RAY_CHECK(entry->state == ObjectState::PLASMA_SEALED)
        << "To evict an object it must have been sealed.";
    RAY_CHECK(entry->ref_count == 0)
        << "To evict an object, there must be no clients currently using it.";
  }

  std::shared_ptr<arrow::MutableBuffer> GetArrowBuffer(const ObjectID& object_id) {
    absl::MutexLock lock(&object_table_mutex_);
    auto it = object_table_.find(object_id);
    RAY_CHECK(it != object_table_.end());
    auto &entry = it->second;
    RAY_CHECK(entry->pointer != nullptr);
    return std::make_shared<arrow::MutableBuffer>(
        entry->pointer, entry->data_size + entry->metadata_size);
  }

  void MarkObjectAsReconstructed(const ObjectID& object_id, PlasmaObject* object) {
    absl::MutexLock lock(&object_table_mutex_);
    auto it = object_table_.find(object_id);
    RAY_CHECK(it != object_table_.end());
    auto &entry = it->second;

    entry->state = ObjectState::PLASMA_SEALED;
    entry->construct_duration =  std::time(nullptr) - entry->create_time;
    PlasmaObject_init(object, entry.get());
  }

  void RegisterObjectToClient(const ObjectID& object_id, Client* client, bool is_create) {
    absl::MutexLock lock(&object_table_mutex_);
    // Notify the eviction policy that this object was created. This must be done
    // immediately before the call to AddToClientObjectIds so that the
    // eviction policy does not have an opportunity to evict the object.
    eviction_policy_.ObjectCreated(object_id, client, is_create);
    // Record that this client is using this object.
    AddToClientObjectIds(object_id, object_table_[object_id].get(), client);
  }

  void RegisterSealedObjectToClient(const ObjectID& object_id, Client* client, PlasmaObject* object) {
    absl::MutexLock lock(&object_table_mutex_);
    auto it = object_table_.find(object_id);
    RAY_CHECK(it != object_table_.end());
    auto &entry = it->second;
    PlasmaObject_init(object, entry.get());
    // Record that this client is using this object.
    AddToClientObjectIds(object_id, entry.get(), client);
  }

  void MemcpyToObject(const ObjectID& object_id, const std::string &data, const std::string &metadata) {
    absl::MutexLock lock(&object_table_mutex_);
    auto it = object_table_.find(object_id);
    RAY_CHECK(it != object_table_.end());
    auto &entry = it->second;
    // Write the inlined data and metadata into the allocated object.
    std::memcpy(entry->pointer, data.data(), data.size());
    std::memcpy(entry->pointer + data.size(), metadata.data(), metadata.size());
  }

 private:
  /// Get an entry from the object table and return NULL if the object_id
  /// is not present.
  ///
  /// \param object_id The object_id of the entry we are looking for.
  /// \return The entry associated with the object_id or NULL if the object_id
  ///         is not present.
  ObjectTableEntry* GetObjectTableEntry(const ObjectID& object_id) {
    auto it = object_table_.find(object_id);
    if (it == object_table_.end()) {
      return nullptr;
    }
    return it->second.get();
  }

  // If this client is not already using the object, add the client to the
  // object's list of clients, otherwise do nothing.
  void AddToClientObjectIds(
      const ObjectID& object_id, ObjectTableEntry* entry, Client* client) {
    // Check if this client is already using the object.
    if (client->object_ids.find(object_id) != client->object_ids.end()) {
      return;
    }
    // If there are no other clients using this object, notify the eviction policy
    // that the object is being used.
    if (entry->ref_count == 0) {
      // Tell the eviction policy that this object is being used.
      eviction_policy_.BeginObjectAccess(object_id);
    }
    // Increase reference count.
    entry->ref_count++;

    // Add object id to the list of object ids that this client is using.
    client->object_ids.insert(object_id);
  }
 
  /// A mutex to protect plasma memory allocator.
  // absl::Mutex plasma_allocator_mutex_;
  /// A mutex to protect 'objects_'.
  absl::Mutex object_table_mutex_;
  /// Mapping from ObjectIDs to information about the object.
  absl::flat_hash_map<ObjectID, std::unique_ptr<ObjectTableEntry>> object_table_;
  absl::flat_hash_set<ObjectID> deletion_cache_;
  /// A mutex to protect 'eviction_policy_'.
  absl::Mutex eviction_policy_mutex_;
  /// The state that is managed by the eviction policy.
  QuotaAwarePolicy eviction_policy_;
};

extern std::unique_ptr<ObjectDirectory> object_directory;

}  // namespace plasma
