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
#include "ray/object_manager/plasma/events.h"
#include "ray/object_manager/plasma/external_store.h"
#include "ray/object_manager/plasma/object_directory.h"
#include "ray/object_manager/plasma/plasma.h"
#include "ray/object_manager/plasma/protocol.h"

namespace plasma {

using ray::Status;

namespace flatbuf {
enum class PlasmaError;
}  // namespace flatbuf

using ray::object_manager::protocol::ObjectInfoT;
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
  PlasmaStore(EventLoop* loop, const std::string& socket_name);

  ~PlasmaStore();

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

  Status ProcessMessage(Client* client);

  void SetNotificationListener(
      const std::shared_ptr<ray::ObjectStoreNotificationManager> &notification_listener) {
    notification_listener_ = notification_listener;
    if (notification_listener_) {
      // Push notifications to the new subscriber about existing sealed objects.
      std::vector<ObjectInfoT> infos;
      object_directory->GetSealedObjectsInfo(&infos);
      for (const auto& info : infos) {
        notification_listener_->ProcessStoreAdd(info);
      }
    }
  }

  /// Push notifications to listeners.
  void PushNotifications(const std::vector<ObjectInfoT>& object_notifications);

 private:
  /// Seal a vector of objects. The objects are now immutable and can be accessed with
  /// get.
  ///
  /// \param object_ids The vector of Object IDs of the objects to be sealed.
  void SealObjects(const std::vector<ObjectID>& object_ids);

  /// Create and seal an object.
  ///
  /// \param object_id The Object ID of the new objects to be created and sealed.
  /// \param client The current client that is associated with the object.
  PlasmaError CreateAndSealObject(const ObjectID& object_id, bool evict_if_full,
                                  const std::string &data, const std::string &metadata,
                                  int device_num, Client* client,  PlasmaObject* result);

  void PushNotifications(const std::vector<ObjectInfoT>& object_notifications, int client_fd);

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

  /// Event loop of the plasma store.
  EventLoop* loop_;
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
#ifdef PLASMA_CUDA
  arrow::cuda::CudaDeviceManager* manager_;
#endif
  std::shared_ptr<ray::ObjectStoreNotificationManager> notification_listener_;
};

}  // namespace plasma
