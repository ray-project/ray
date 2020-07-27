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

#include <deque>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "ray/common/status.h"
#include "ray/object_manager/format/object_manager_generated.h"
#include "ray/object_manager/notification/object_store_notification_manager.h"
#include "ray/object_manager/plasma/common.h"
#include "ray/object_manager/plasma/connection.h"
#include "ray/object_manager/plasma/external_store.h"
#include "ray/object_manager/plasma/plasma.h"
#include "ray/object_manager/plasma/protocol.h"
#include "ray/object_manager/plasma/quota_aware_policy.h"

namespace plasma {

using ray::Status;

namespace flatbuf {
enum class PlasmaError;
}  // namespace flatbuf

using ray::object_manager::protocol::ObjectInfoT;
using flatbuf::PlasmaError;

struct GetRequest;

class PlasmaStore {
 public:
  // TODO: PascalCase PlasmaStore methods.
  PlasmaStore(boost::asio::io_service &main_service, std::string directory, bool hugepages_enabled,
              const std::string& socket_name,
              std::shared_ptr<ExternalStore> external_store);

  ~PlasmaStore();

  /// Start this store.
  void Start();

  /// Stop this store.
  void Stop();

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
                           const std::shared_ptr<Client> &client, PlasmaObject* result);

  /// Abort a created but unsealed object. If the client is not the
  /// creator, then the abort will fail.
  ///
  /// \param object_id Object ID of the object to be aborted.
  /// \param client The client who created the object. If this does not
  ///   match the creator of the object, then the abort will fail.
  /// \return 1 if the abort succeeds, else 0.
  int AbortObject(const ObjectID& object_id, const std::shared_ptr<Client> &client);

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
  void ProcessGetRequest(const std::shared_ptr<Client> &client, const std::vector<ObjectID>& object_ids,
                         int64_t timeout_ms);

  /// Seal a vector of objects. The objects are now immutable and can be accessed with
  /// get.
  ///
  /// \param object_ids The vector of Object IDs of the objects to be sealed.
  void SealObjects(const std::vector<ObjectID>& object_ids);

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
  void ReleaseObject(const ObjectID& object_id, const std::shared_ptr<Client> &client);

  /// Subscribe a file descriptor to updates about new sealed objects.
  ///
  /// \param client The client making this request.
  void SubscribeToUpdates(const std::shared_ptr<Client> &client);

  /// Connect a new client to the PlasmaStore.
  ///
  /// \param error The error code from the acceptor.
  void ConnectClient(const boost::system::error_code &error);

  /// Disconnect a client from the PlasmaStore.
  ///
  /// \param client The client that is disconnected.
  void DisconnectClient(const std::shared_ptr<Client> &client);

  void SendNotifications(
    const std::shared_ptr<Client> &client, const std::vector<ObjectInfoT> &object_info);

  Status ProcessMessage(const std::shared_ptr<Client> &client, plasma::flatbuf::MessageType type,
                        const std::vector<uint8_t> &message);

  void SetNotificationListener(
      const std::shared_ptr<ray::ObjectStoreNotificationManager> &notification_listener) {
    notification_listener_ = notification_listener;
    if (notification_listener_) {
      // Push notifications to the new subscriber about existing sealed objects.
      for (const auto& entry : store_info_.objects) {
        if (entry.second->state == ObjectState::PLASMA_SEALED) {
          ObjectInfoT info;
          info.object_id = entry.first.Binary();
          info.data_size = entry.second->data_size;
          info.metadata_size = entry.second->metadata_size;
          notification_listener_->ProcessStoreAdd(info);
        }
      }
    }
  }

 private:
  void PushNotification(ObjectInfoT* object_notification);

  void PushNotifications(const std::vector<ObjectInfoT>& object_notifications);

  void AddToClientObjectIds(const ObjectID& object_id, ObjectTableEntry* entry,
                            const std::shared_ptr<Client> &client);

  /// Remove a GetRequest and clean up the relevant data structures.
  ///
  /// \param get_request The GetRequest to remove.
  void RemoveGetRequest(GetRequest* get_request);

  /// Remove all of the GetRequests for a given client.
  ///
  /// \param client The client whose GetRequests should be removed.
  void RemoveGetRequestsForClient(const std::shared_ptr<Client> &client);

  void ReturnFromGet(GetRequest* get_req);

  void UpdateObjectGetRequests(const ObjectID& object_id);

  int RemoveFromClientObjectIds(const ObjectID& object_id, ObjectTableEntry* entry,
                                const std::shared_ptr<Client> &client);

  void EraseFromObjectTable(const ObjectID& object_id);

  uint8_t* AllocateMemory(size_t size, bool evict_if_full, MEMFD_TYPE* fd, int64_t* map_size,
                          ptrdiff_t* offset, const std::shared_ptr<Client> &client, bool is_create);
#ifdef PLASMA_CUDA
  Status AllocateCudaMemory(int device_num, int64_t size, uint8_t** out_pointer,
                            std::shared_ptr<CudaIpcMemHandle>* out_ipc_handle);

  Status FreeCudaMemory(int device_num, int64_t size, uint8_t* out_pointer);
#endif

  // Start listening for clients.
  void DoAccept();

  // A reference to the asio io context.
  boost::asio::io_service& io_context_;
  /// The name of the socket this object store listens on.
  std::string socket_name_;
  /// An acceptor for new clients.
  boost::asio::basic_socket_acceptor<ray::local_stream_protocol> acceptor_;
  /// The socket to listen on for new clients.
  ray::local_stream_socket socket_;

  /// The plasma store information, including the object tables, that is exposed
  /// to the eviction policy.
  PlasmaStoreInfo store_info_;
  /// The state that is managed by the eviction policy.
  QuotaAwarePolicy eviction_policy_;
  /// A hash table mapping object IDs to a vector of the get requests that are
  /// waiting for the object to arrive.
  std::unordered_map<ObjectID, std::vector<GetRequest*>> object_get_requests_;
  /// The registered client for receiving notifications.
  std::unordered_set<std::shared_ptr<Client>> notification_clients_;

  std::unordered_set<ObjectID> deletion_cache_;

  /// Manages worker threads for handling asynchronous/multi-threaded requests
  /// for reading/writing data to/from external store.
  std::shared_ptr<ExternalStore> external_store_;
#ifdef PLASMA_CUDA
  arrow::cuda::CudaDeviceManager* manager_;
#endif
  std::shared_ptr<ray::ObjectStoreNotificationManager> notification_listener_;
};

}  // namespace plasma
