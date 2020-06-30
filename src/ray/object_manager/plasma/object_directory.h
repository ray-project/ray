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
  ObjectTableEntry() : map_size(0), pointer(nullptr), ref_count(0) {}

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

  int64_t ObjectSize() const { return data_size + metadata_size; }

  void FreeObject();

  Status AllocateMemory(int device_id, size_t size);

  std::shared_ptr<arrow::MutableBuffer> GetArrowBuffer() {
    RAY_CHECK(pointer);
    return std::make_shared<arrow::MutableBuffer>(pointer, data_size + metadata_size);
  }
};


class ObjectDirectory {
 public:
  ObjectDirectory(
    const std::shared_ptr<ExternalStore> &external_store,
    const std::function<void(const std::vector<ObjectInfoT>&)> &notifications_callback);
  /// Get the size of the object.
  ///
  /// \param object_id Object ID of the object.
  /// \return Object size in bytes.
  int64_t GetObjectSize(const ObjectID& object_id) {
   // TODO(suquark): This public function is not protected by a lock,
   // because it will be called from the eviction policy, and it will
   // be deadlocked. We should get rid of this function later.
   return object_table_[object_id]->ObjectSize();
  }

  void GetSealedObjectsInfo(std::vector<ObjectInfoT>* infos);

  /// Check if the plasma store contains an object:
  ///
  /// \param object_id Object ID that will be checked.
  /// \return OBJECT_FOUND if the object is in the store, OBJECT_NOT_FOUND if
  /// not
  ObjectStatus ContainsObject(const ObjectID& object_id);

  void GetObjects(const std::vector<ObjectID>& object_ids,
                  Client* client,
                  std::vector<ObjectID> *sealed_objects,
                  std::vector<ObjectID> *reconstructed_objects,
                  std::vector<ObjectID> *nonexistent_objects);

  /// Create a new object. The client must do a call to release_object to tell
  /// the store when it is done with the object.
  ///
  /// \param object_id Object ID of the object to be created.
  /// \param evict_if_full If this is true, then when the object store is full,
  ///        try to evict objects that are not currently referenced before
  ///        creating the object. Else, do not evict any objects and
  ///        immediately return an PlasmaError::OutOfMemory.
  /// \param data_size Size in bytes of the object to be created.
  /// \param metadata_size Size in bytes of the object metadata.
  /// \param device_num The number of the device where the object is being
  ///        created.
  ///        device_num = 0 corresponds to the host,
  ///        device_num = 1 corresponds to GPU0,
  ///        device_num = 2 corresponds to GPU1, etc.
  /// \param client The client that created the object.
  /// \param result The object that has been created.
  /// \return One of the following error codes:
  ///  - Status::OK, if the object was created successfully.
  ///  - Status::ObjectExists, if an object with this ID is already
  ///    present in the store. In this case, the client should not call
  ///    plasma_release.
  ///  - PlasmaError::OutOfMemory, if the store is out of memory and
  ///    cannot create the object. In this case, the client should not call
  ///    plasma_release.
  Status CreateObject(const ObjectID& object_id, bool evict_if_full,
                      int64_t data_size, int64_t metadata_size,
                      int device_num, Client* client,
                      PlasmaObject* result);

  /// Seal a vector of objects. The objects are now immutable and can be accessed with
  /// get.
  ///
  /// \param object_ids The vector of Object IDs of the objects to be sealed.
  void SealObjects(const std::vector<ObjectID>& object_ids) {
    absl::MutexLock lock(&object_table_mutex_);
    SealObjectsInternal(object_ids);
  }

  /// Create and seal a new object, and release it from the client.
  ///
  /// \param object_id Object ID of the object to be created.
  /// \param evict_if_full If this is true, then when the object store is full,
  ///        try to evict objects that are not currently referenced before
  ///        creating the object. Else, do not evict any objects and
  ///        immediately return an PlasmaError::OutOfMemory.
  /// \param data_size Size in bytes of the object to be created.
  /// \param metadata_size Size in bytes of the object metadata.
  /// \param device_num The number of the device where the object is being
  ///        created.
  ///        device_num = 0 corresponds to the host,
  ///        device_num = 1 corresponds to GPU0,
  ///        device_num = 2 corresponds to GPU1, etc.
  /// \param client The client that created the object.
  /// \param result The object that has been created.
  /// \return One of the following error codes:
  ///  - Status::OK, if the object was created successfully.
  ///  - Status::ObjectExists, if an object with this ID is already
  ///    present in the store. In this case, the client should not call
  ///    plasma_release.
  ///  - PlasmaError::OutOfMemory, if the store is out of memory and
  ///    cannot create the object. In this case, the client should not call
  ///    plasma_release.
  Status CreateAndSealObject(const ObjectID& object_id, bool evict_if_full,
                             const std::string &data, const std::string &metadata,
                             int device_num, Client* client, PlasmaObject* result);

  /// Evict objects returned by the eviction policy.
  /// This code path should only be used for testing.
  ///
  /// \param num_bytes The amount of memory we could like to evict.
  void EvictObjects(int64_t num_bytes, int64_t *num_bytes_evicted);

  /// Delete a specific object by object_id that have been created in the hash table.
  ///
  /// \param object_id Object ID of the object to be deleted.
  /// \return One of the following error codes:
  ///  - PlasmaError::OK, if the object was delete successfully.
  ///  - PlasmaError::ObjectNonexistent, if ths object isn't existed.
  ///  - PlasmaError::ObjectInUse, if the object is in use.
  PlasmaError DeleteObject(const ObjectID& object_id);

  /// Record the fact that a particular client is no longer using an object.
  ///
  /// \param object_id The object ID of the object that is being released.
  /// \param client The client making this request.
  void ReleaseObject(const ObjectID& object_id, Client* client) {
    absl::MutexLock lock(&object_table_mutex_);
    auto entry = GetObjectTableEntry(object_id);
    RAY_CHECK(entry != nullptr);
    // Remove the client from the object's array of clients.
    RAY_CHECK(RemoveFromClientObjectIds(object_id, entry, client) == 1);
  }

  /// Abort a created but unsealed object. If the client is not the
  /// creator, then the abort will fail.
  ///
  /// \param object_id Object ID of the object to be aborted.
  /// \param client The client who created the object. If this does not
  ///   match the creator of the object, then the abort will fail.
  /// \return 1 if the abort succeeds, else 0.
  int AbortObject(const ObjectID& object_id, Client* client);

  void DisconnectClient(Client* client);

  void MarkObjectAsReconstructed(const ObjectID& object_id, PlasmaObject* object);

  void RegisterSealedObjectToClient(const ObjectID& object_id, Client* client, PlasmaObject* object);

  bool SetClientOption(Client *client, const std::string &client_name, int64_t output_memory_quota) {
    client->name = client_name;
    return eviction_policy_.SetClientQuota(client, output_memory_quota);
  }

  void RefreshObjects(const std::vector<ObjectID>& object_ids) {
    eviction_policy_.RefreshObjects(object_ids);
  }

  std::string DebugString() const {
    return eviction_policy_.DebugString();
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

  /// Evict objects.
  ///
  /// \param object_ids Object IDs of the objects to be evicted.
  void EvictObjectsInternal(const std::vector<ObjectID>& object_ids);

  Status CreateObjectInternal(const ObjectID& object_id, bool evict_if_full,
                              int64_t data_size, int64_t metadata_size,
                              int device_num, Client* client,
                              ObjectTableEntry** new_entry);
 
  /// Seal a vector of objects. The objects are now immutable and can be accessed with
  /// get.
  ///
  /// \param object_ids The vector of Object IDs of the objects to be sealed.
  void SealObjectsInternal(const std::vector<ObjectID>& object_ids);

  // If this client is not already using the object, add the client to the
  // object's list of clients, otherwise do nothing.
  void AddToClientObjectIds(
      const ObjectID& object_id, ObjectTableEntry* entry, Client* client);

  int RemoveFromClientObjectIds(
      const ObjectID& object_id, ObjectTableEntry* entry, Client* client);

  /// Forcefully delete an object in the store.
  ///
  /// \param object_id Object ID of the object to be deleted.
  /// \param evict_only Only free the memory of the object.
  void EraseObject(const ObjectID& object_id) {
    auto entry_node = object_table_.extract(object_id);
    auto& entry = entry_node.mapped();
    entry->FreeObject();
  }

  /// Allocate memory
  Status AllocateMemory(const ObjectID& object_id, ObjectTableEntry* entry, size_t size, bool evict_if_full,
                        Client* client, bool is_create, int device_num);

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
  /// Manages worker threads for handling asynchronous/multi-threaded requests
  /// for reading/writing data to/from external store.
  std::shared_ptr<ExternalStore> external_store_;
  /// A callback function for notifications about objects creation and removal.
  std::function<void(const std::vector<ObjectInfoT>&)> notifications_callback_;
};

extern std::unique_ptr<ObjectDirectory> object_directory;

}  // namespace plasma
