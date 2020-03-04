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

#ifndef PLASMA_STORE_H
#define PLASMA_STORE_H

#include <deque>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "plasma/common.h"
#include "plasma/events.h"
#include "plasma/external_store.h"
#include "plasma/plasma.h"
#include "plasma/protocol.h"
#include "plasma/quota_aware_policy.h"

namespace arrow {
class Status;
}  // namespace arrow

namespace plasma {

namespace flatbuf {
struct ObjectInfoT;
enum class PlasmaError;
}  // namespace flatbuf

using flatbuf::ObjectInfoT;
using flatbuf::PlasmaError;

struct GetRequest;

struct NotificationQueue {
  /// The object notifications for clients. We notify the client about the
  /// objects in the order that the objects were sealed or deleted.
  std::deque<std::unique_ptr<uint8_t[]>> object_notifications;
};

class PlasmaStore {
 public:
  using NotificationMap = std::unordered_map<int, NotificationQueue>;

  // TODO: PascalCase PlasmaStore methods.
  PlasmaStore(EventLoop* loop, std::string directory, bool hugepages_enabled,
              const std::string& socket_name,
              std::shared_ptr<ExternalStore> external_store);

  ~PlasmaStore();

  /// Get a const pointer to the internal PlasmaStoreInfo object.
  const PlasmaStoreInfo* GetPlasmaStoreInfo();

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
  ///  - PlasmaError::OK, if the object was created successfully.
  ///  - PlasmaError::ObjectExists, if an object with this ID is already
  ///    present in the store. In this case, the client should not call
  ///    plasma_release.
  ///  - PlasmaError::OutOfMemory, if the store is out of memory and
  ///    cannot create the object. In this case, the client should not call
  ///    plasma_release.
  PlasmaError CreateObject(const ObjectID& object_id, bool evict_if_full,
                           int64_t data_size, int64_t metadata_size, int device_num,
                           Client* client, PlasmaObject* result);

  /// Abort a created but unsealed object. If the client is not the
  /// creator, then the abort will fail.
  ///
  /// \param object_id Object ID of the object to be aborted.
  /// \param client The client who created the object. If this does not
  ///   match the creator of the object, then the abort will fail.
  /// \return 1 if the abort succeeds, else 0.
  int AbortObject(const ObjectID& object_id, Client* client);

  /// Delete a specific object by object_id that have been created in the hash table.
  ///
  /// \param object_id Object ID of the object to be deleted.
  /// \return One of the following error codes:
  ///  - PlasmaError::OK, if the object was delete successfully.
  ///  - PlasmaError::ObjectNonexistent, if ths object isn't existed.
  ///  - PlasmaError::ObjectInUse, if the object is in use.
  PlasmaError DeleteObject(ObjectID& object_id);

  /// Evict objects returned by the eviction policy.
  ///
  /// \param object_ids Object IDs of the objects to be evicted.
  void EvictObjects(const std::vector<ObjectID>& object_ids);

  /// Process a get request from a client. This method assumes that we will
  /// eventually have these objects sealed. If one of the objects has not yet
  /// been sealed, the client that requested the object will be notified when it
  /// is sealed.
  ///
  /// For each object, the client must do a call to release_object to tell the
  /// store when it is done with the object.
  ///
  /// \param client The client making this request.
  /// \param object_ids Object IDs of the objects to be gotten.
  /// \param timeout_ms The timeout for the get request in milliseconds.
  void ProcessGetRequest(Client* client, const std::vector<ObjectID>& object_ids,
                         int64_t timeout_ms);

  /// Seal a vector of objects. The objects are now immutable and can be accessed with
  /// get.
  ///
  /// \param object_ids The vector of Object IDs of the objects to be sealed.
  /// \param digests The vector of digests of the objects. This is used to tell if two
  /// objects with the same object ID are the same.
  void SealObjects(const std::vector<ObjectID>& object_ids,
                   const std::vector<std::string>& digests);

  /// Check if the plasma store contains an object:
  ///
  /// \param object_id Object ID that will be checked.
  /// \return OBJECT_FOUND if the object is in the store, OBJECT_NOT_FOUND if
  /// not
  ObjectStatus ContainsObject(const ObjectID& object_id);

  /// Record the fact that a particular client is no longer using an object.
  ///
  /// \param object_id The object ID of the object that is being released.
  /// \param client The client making this request.
  void ReleaseObject(const ObjectID& object_id, Client* client);

  /// Subscribe a file descriptor to updates about new sealed objects.
  ///
  /// \param client The client making this request.
  void SubscribeToUpdates(Client* client);

  /// Connect a new client to the PlasmaStore.
  ///
  /// \param listener_sock The socket that is listening to incoming connections.
  void ConnectClient(int listener_sock);

  /// Disconnect a client from the PlasmaStore.
  ///
  /// \param client_fd The client file descriptor that is disconnected.
  void DisconnectClient(int client_fd);

  NotificationMap::iterator SendNotifications(NotificationMap::iterator it);

  arrow::Status ProcessMessage(Client* client);

 private:
  void PushNotification(ObjectInfoT* object_notification);

  void PushNotifications(std::vector<ObjectInfoT>& object_notifications);

  void PushNotification(ObjectInfoT* object_notification, int client_fd);

  void AddToClientObjectIds(const ObjectID& object_id, ObjectTableEntry* entry,
                            Client* client);

  /// Remove a GetRequest and clean up the relevant data structures.
  ///
  /// \param get_request The GetRequest to remove.
  void RemoveGetRequest(GetRequest* get_request);

  /// Remove all of the GetRequests for a given client.
  ///
  /// \param client The client whose GetRequests should be removed.
  void RemoveGetRequestsForClient(Client* client);

  void ReturnFromGet(GetRequest* get_req);

  void UpdateObjectGetRequests(const ObjectID& object_id);

  int RemoveFromClientObjectIds(const ObjectID& object_id, ObjectTableEntry* entry,
                                Client* client);

  void EraseFromObjectTable(const ObjectID& object_id);

  uint8_t* AllocateMemory(size_t size, bool evict_if_full, int* fd, int64_t* map_size,
                          ptrdiff_t* offset, Client* client, bool is_create);
#ifdef PLASMA_CUDA
  Status AllocateCudaMemory(int device_num, int64_t size, uint8_t** out_pointer,
                            std::shared_ptr<CudaIpcMemHandle>* out_ipc_handle);

  Status FreeCudaMemory(int device_num, int64_t size, uint8_t* out_pointer);
#endif

  /// Event loop of the plasma store.
  EventLoop* loop_;
  /// The plasma store information, including the object tables, that is exposed
  /// to the eviction policy.
  PlasmaStoreInfo store_info_;
  /// The state that is managed by the eviction policy.
  QuotaAwarePolicy eviction_policy_;
  /// Input buffer. This is allocated only once to avoid mallocs for every
  /// call to process_message.
  std::vector<uint8_t> input_buffer_;
  /// A hash table mapping object IDs to a vector of the get requests that are
  /// waiting for the object to arrive.
  std::unordered_map<ObjectID, std::vector<GetRequest*>> object_get_requests_;
  /// The pending notifications that have not been sent to subscribers because
  /// the socket send buffers were full. This is a hash table from client file
  /// descriptor to an array of object_ids to send to that client.
  /// TODO(pcm): Consider putting this into the Client data structure and
  /// reorganize the code slightly.
  NotificationMap pending_notifications_;

  std::unordered_map<int, std::unique_ptr<Client>> connected_clients_;

  std::unordered_set<ObjectID> deletion_cache_;

  /// Manages worker threads for handling asynchronous/multi-threaded requests
  /// for reading/writing data to/from external store.
  std::shared_ptr<ExternalStore> external_store_;
#ifdef PLASMA_CUDA
  arrow::cuda::CudaDeviceManager* manager_;
#endif
};

int StartPlasmaStore(const std::string socket_name, int64_t system_memory,
                     bool hugepages_enabled, const std::string plasma_directory,
                     const std:string external_store_endpoint);

}  // namespace plasma

#endif  // PLASMA_STORE_H
