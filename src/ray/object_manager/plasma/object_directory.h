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
#include "ray/object_manager/plasma/external_store.h"
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
  ObjectTableEntry() : pointer(nullptr), ref_count(0) {}

  ~ObjectTableEntry() { pointer = nullptr; }

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

  int64_t ObjectSize() const {
    return data_size + metadata_size;
  }

  void FreeObject() {
    int64_t buff_size = ObjectSize();
    if (device_num == 0) {
      PlasmaAllocator::Free(pointer, buff_size);
    } else {
#ifdef PLASMA_CUDA
      ARROW_ASSIGN_OR_RAISE(auto context, manager_->GetContext(device_num - 1));
      RAY_CHECK_OK(context->Free(pointer, buff_size));
#endif
    }
    pointer = nullptr;
    state = ObjectState::PLASMA_EVICTED;
  }

  Status AllocateMemory(int device_id, size_t size) {
    if (device_id == 0) {
      // Allocate space for the new object. We use memalign instead of malloc
      // in order to align the allocated region to a 64-byte boundary. This is not
      // strictly necessary, but it is an optimization that could speed up the
      // computation of a hash of the data (see compute_object_hash_parallel in
      // plasma_client.cc). Note that even though this pointer is 64-byte aligned,
      // it is not guaranteed that the corresponding pointer in the client will be
      // 64-byte aligned, but in practice it often will be.
      uint8_t* address = reinterpret_cast<uint8_t*>(PlasmaAllocator::Memalign(kBlockSize, size));
      if (!address) {
        return Status::ObjectStoreFull("Cannot allocate object.");
      }
      pointer = address;
      GetMallocMapinfo(pointer, &fd, &map_size, &offset);
      RAY_CHECK(fd != -1);
    } else {
#ifdef PLASMA_CUDA
      RAY_DCHECK(device_id != 0);
      ARROW_ASSIGN_OR_RAISE(auto context, manager_->GetContext(device_id - 1));
      ARROW_ASSIGN_OR_RAISE(auto cuda_buffer, context->Allocate(static_cast<int64_t>(size)));
      // The IPC handle will keep the buffer memory alive
      Status s = cuda_buffer->ExportForIpc().Value(&ipc_handle);
      if (!s.ok()) {
        RAY_LOG(ERROR) << "Failed to allocate CUDA memory: " << s.ToString();
        return s;
      }
      pointer = reinterpret_cast<uint8_t*>(cuda_buffer->address());
#else
    RAY_LOG(ERROR) << "device_num != 0 but CUDA not enabled";
    return Status::OutOfMemory("CUDA is not enabled.");
#endif
    }
    state = ObjectState::PLASMA_CREATED;
    device_num = device_id;
    create_time = std::time(nullptr);
    construct_duration = -1;
  }
};

void PlasmaObject_init(PlasmaObject* object, ObjectTableEntry* entry) {
  RAY_DCHECK(object != nullptr);
  RAY_DCHECK(entry != nullptr);
  // RAY_DCHECK(entry->state == ObjectState::PLASMA_SEALED);
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
  void EraseObject(const ObjectID& object_id) {
    absl::MutexLock lock(&object_table_mutex_);
    auto entry_node = object_table_.extract(object_id);
    auto& entry = entry_node.mapped();
    entry->FreeObject();
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

  void EvictObjects(const std::vector<ObjectID>& object_ids,
                    const std::shared_ptr<ExternalStore> &external_store,
                    const std::function<void(const std::vector<ObjectInfoT>&)> &notifications_callback) {
    absl::MutexLock lock(&object_table_mutex_);
    if (object_ids.empty()) {
      return;
    }

    std::vector<ObjectTableEntry*> evicted_objects_entries;
    std::vector<std::shared_ptr<arrow::Buffer>> evicted_object_data;
    std::vector<ObjectInfoT> infos;
    for (const auto& object_id : object_ids) {
      RAY_LOG(DEBUG) << "evicting object " << object_id.Hex();
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
      // If there is a backing external store, then mark object for eviction to
      // external store, free the object data pointer and keep a placeholder
      // entry in ObjectTable
      if (external_store) {
        evicted_objects_entries.push_back(entry.get());
        evicted_object_data.emplace_back(object_directory->GetArrowBuffer(object_id));
      } else {
        // If there is no backing external store, just erase the object entry
        // and send a deletion notification.
        entry->FreeObject();
        // Inform all subscribers that the object has been deleted.
        ObjectInfoT notification;
        notification.object_id = object_id.Binary();
        notification.is_deletion = true;
        infos.emplace_back(notification);
      }
    }

    if (external_store) {
      RAY_CHECK_OK(external_store->Put(object_ids, evicted_object_data));
      for (auto entry : evicted_objects_entries) {
        entry->FreeObject();
      }
    } else {
      notifications_callback(infos);
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

  /// Record the fact that a particular client is no longer using an object.
  ///
  /// \param object_id The object ID of the object that is being released.
  /// \param client The client making this request.
  void ReleaseObject(const ObjectID& object_id, Client* client,
                     const std::shared_ptr<ExternalStore> &external_store,
                     const std::function<void(const std::vector<ObjectInfoT>&)> &notifications_callback) {
    auto entry = GetObjectTableEntry(object_id);
    RAY_CHECK(entry != nullptr);
    // Remove the client from the object's array of clients.
    RAY_CHECK(RemoveFromClientObjectIds(object_id, entry, client, external_store, notifications_callback) == 1);
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

  int RemoveFromClientObjectIds(
      const ObjectID& object_id, ObjectTableEntry* entry, Client* client,
      const std::shared_ptr<ExternalStore> &external_store,
      const std::function<void(const std::vector<ObjectInfoT>&)> &notifications_callback) {
    auto it = client->object_ids.find(object_id);
    if (it != client->object_ids.end()) {
      client->object_ids.erase(it);
      // Decrease reference count.
      entry->ref_count--;

      // If no more clients are using this object, notify the eviction policy
      // that the object is no longer being used.
      if (entry->ref_count == 0) {
        if (deletion_cache_.count(object_id) == 0) {
          // Tell the eviction policy that this object is no longer being used.
          eviction_policy_.EndObjectAccess(object_id);
        } else {
          // Above code does not really delete an object. Instead, it just put an
          // object to LRU cache which will be cleaned when the memory is not enough.
          deletion_cache_.erase(object_id);
          EvictObjects({object_id}, external_store, notifications_callback);
        }
      }
      // Return 1 to indicate that the client was removed.
      return 1;
    } else {
      // Return 0 to indicate that the client was not removed.
      return 0;
    }
  }
 
  /// A mutex to protect plasma memory allocator.
  // absl::Mutex plasma_allocator_mutex_;
  /// A mutex to protect 'objects_'.
  absl::Mutex object_table_mutex_;
  /// Mapping from ObjectIDs to information about the object.
  absl::flat_hash_map<ObjectID, std::unique_ptr<ObjectTableEntry>> object_table_;
  /// Store objects that were requested to be deleted, but could not be deleted because
  /// it is referenced by some clients, etc.
  absl::flat_hash_set<ObjectID> deletion_cache_;
  /// A mutex to protect 'eviction_policy_'.
  absl::Mutex eviction_policy_mutex_;
  /// The state that is managed by the eviction policy.
  QuotaAwarePolicy eviction_policy_;
};

extern std::unique_ptr<ObjectDirectory> object_directory;

}  // namespace plasma
