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

// PLASMA STORE: This is a simple object store server process
//
// It accepts incoming client connections on a unix domain socket
// (name passed in via the -s option of the executable) and uses a
// single thread to serve the clients. Each client establishes a
// connection and can create objects, wait for objects and seal
// objects through that connection.
//
// It keeps a hash table that maps object_ids (which are 20 byte long,
// just enough to store and SHA1 hash) to memory mapped files.

#include "ray/object_manager/plasma/store.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

#include <ctime>
#include <deque>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "ray/object_manager/format/object_manager_generated.h"
#include "ray/object_manager/plasma/common.h"
#include "ray/object_manager/plasma/fling.h"
#include "ray/object_manager/plasma/io.h"
#include "ray/object_manager/plasma/malloc.h"
#include "ray/object_manager/plasma/plasma_allocator.h"
#include "ray/object_manager/plasma/protocol.h"

#ifdef PLASMA_CUDA
#include "arrow/gpu/cuda_api.h"

using arrow::cuda::CudaBuffer;
using arrow::cuda::CudaContext;
using arrow::cuda::CudaDeviceManager;
#endif

namespace fb = plasma::flatbuf;

namespace plasma {

struct GetRequest {
  GetRequest(Client* client, const std::vector<ObjectID>& object_ids);
  /// The client that called get.
  Client* client;
  /// The ID of the timer that will time out and cause this wait to return to
  ///  the client if it hasn't already returned.
  int64_t timer;
  /// The object IDs involved in this request. This is used in the reply.
  std::vector<ObjectID> object_ids;
  /// The object information for the objects in this request. This is used in
  /// the reply.
  std::unordered_map<ObjectID, PlasmaObject> objects;
  /// The minimum number of objects to wait for in this request.
  int64_t num_objects_to_wait_for_;
  /// The number of object requests in this wait request that are already
  /// satisfied.
  int64_t num_satisfied_;

  void SatisfyWithSealedObject(const ObjectID& object_id) {
    object_directory->RegisterSealedObjectToClient(object_id, client, &objects[object_id]);
    num_satisfied_ += 1;
  }

  void SatisfyWithReconstructedObject(const ObjectID& object_id) {
    object_directory->MarkObjectAsReconstructed(object_id, &objects[object_id]);
    num_satisfied_ += 1;
  }

  bool Fulfilled() {
    return num_satisfied_ == num_objects_to_wait_for_;
  }
};

GetRequest::GetRequest(Client* client, const std::vector<ObjectID>& object_ids)
    : client(client),
      timer(-1),
      object_ids(object_ids.begin(), object_ids.end()),
      objects(object_ids.size()),
      num_satisfied_(0) {
  std::unordered_set<ObjectID> unique_ids(object_ids.begin(), object_ids.end());
  num_objects_to_wait_for_ = unique_ids.size();
}

Client::Client(int fd) : fd(fd), notification_fd(-1) {}

PlasmaStore::PlasmaStore(EventLoop* loop, const std::string& socket_name,
                         std::shared_ptr<ExternalStore> external_store)
    : loop_(loop),
      external_store_(external_store) {
#ifdef PLASMA_CUDA
  auto maybe_manager = CudaDeviceManager::Instance();
  DCHECK_OK(maybe_manager.status());
  manager_ = *maybe_manager;
#endif
}

// TODO(pcm): Get rid of this destructor by using RAII to clean up data.
PlasmaStore::~PlasmaStore() {}

// A helper function for object creation.
PlasmaError CreateObjectStatusToPlasmaError(const Status& status) {
  if (status.ok()) {
    return PlasmaError::OK;
  } else if (status.IsObjectExists()) {
    return PlasmaError::ObjectExists;
  } else if (status.IsOutOfMemory()) {
    return PlasmaError::OutOfMemory;
  } else {
    RAY_LOG(FATAL) << "Unexpected error: " << status.ToString();
  }
}

void PlasmaStore::RemoveGetRequest(GetRequest* get_request) {
  // Remove the get request from each of the relevant object_get_requests hash
  // tables if it is present there. It should only be present there if the get
  // request timed out or if it was issued by a client that has disconnected.
  for (ObjectID& object_id : get_request->object_ids) {
    auto object_request_iter = object_get_requests_.find(object_id);
    if (object_request_iter != object_get_requests_.end()) {
      auto& get_requests = object_request_iter->second;
      // Erase get_req from the vector.
      auto it = std::find(get_requests.begin(), get_requests.end(), get_request);
      if (it != get_requests.end()) {
        get_requests.erase(it);
        // If the vector is empty, remove the object ID from the map.
        if (get_requests.empty()) {
          object_get_requests_.erase(object_request_iter);
        }
      }
    }
  }
  // Remove the get request.
  if (get_request->timer != -1) {
    RAY_CHECK(loop_->RemoveTimer(get_request->timer) == kEventLoopOk);
  }
  delete get_request;
}

void PlasmaStore::RemoveGetRequestsForClient(Client* client) {
  std::unordered_set<GetRequest*> get_requests_to_remove;
  for (auto const& pair : object_get_requests_) {
    for (GetRequest* get_request : pair.second) {
      if (get_request->client == client) {
        get_requests_to_remove.insert(get_request);
      }
    }
  }

  // It shouldn't be possible for a given client to be in the middle of multiple get
  // requests.
  RAY_CHECK(get_requests_to_remove.size() <= 1);
  for (GetRequest* get_request : get_requests_to_remove) {
    RemoveGetRequest(get_request);
  }
}

void PlasmaStore::ReturnFromGet(GetRequest* get_req) {
  // Figure out how many file descriptors we need to send.
  std::unordered_set<int> fds_to_send;
  std::vector<int> store_fds;
  std::vector<int64_t> mmap_sizes;
  for (const auto& object_id : get_req->object_ids) {
    PlasmaObject& object = get_req->objects[object_id];
    int fd = object.store_fd;
    if (object.data_size != -1 && fds_to_send.count(fd) == 0 && fd != -1) {
      fds_to_send.insert(fd);
      store_fds.push_back(fd);
      // TODO(suquark): remove GetMmapSize
      mmap_sizes.push_back(GetMmapSize(fd));
    }
  }

  // Send the get reply to the client.
  Status s = SendGetReply(get_req->client->fd, &get_req->object_ids[0], get_req->objects,
                          get_req->object_ids.size(), store_fds, mmap_sizes);
  WarnIfSigpipe(s.ok() ? 0 : -1, get_req->client->fd);
  // If we successfully sent the get reply message to the client, then also send
  // the file descriptors.
  if (s.ok()) {
    // Send all of the file descriptors for the present objects.
    for (int store_fd : store_fds) {
      // Only send the file descriptor if it hasn't been sent (see analogous
      // logic in GetStoreFd in client.cc).
      if (get_req->client->used_fds.find(store_fd) == get_req->client->used_fds.end()) {
        WarnIfSigpipe(send_fd(get_req->client->fd, store_fd), get_req->client->fd);
        get_req->client->used_fds.insert(store_fd);
      }
    }
  }

  // Remove the get request from each of the relevant object_get_requests hash
  // tables if it is present there. It should only be present there if the get
  // request timed out.
  RemoveGetRequest(get_req);
}

void PlasmaStore::UpdateObjectGetRequests(const ObjectID& object_id) {
  auto it = object_get_requests_.find(object_id);
  // If there are no get requests involving this object, then return.
  if (it == object_get_requests_.end()) {
    return;
  }

  auto& get_requests = it->second;

  // After finishing the loop below, get_requests and it will have been
  // invalidated by the removal of object_id from object_get_requests_.
  size_t index = 0;
  size_t num_requests = get_requests.size();
  for (size_t i = 0; i < num_requests; ++i) {
    auto get_req = get_requests[index];
    get_req->SatisfyWithSealedObject(object_id);

    // If this get request is done, reply to the client.
    if (get_req->Fulfilled()) {
      ReturnFromGet(get_req);
    } else {
      // The call to ReturnFromGet will remove the current element in the
      // array, so we only increment the counter in the else branch.
      index += 1;
    }
  }

  // No get requests should be waiting for this object anymore. The object ID
  // may have been removed from the object_get_requests_ by ReturnFromGet, but
  // if the get request has not returned yet, then remove the object ID from the
  // map here.
  it = object_get_requests_.find(object_id);
  if (it != object_get_requests_.end()) {
    object_get_requests_.erase(object_id);
  }
}

void PlasmaStore::ProcessGetRequest(Client* client,
                                    const std::vector<ObjectID>& object_ids,
                                    int64_t timeout_ms) {
  // Create a get request for this object.
  auto get_req = new GetRequest(client, object_ids);
  std::vector<ObjectID> sealed_objects;
  std::vector<ObjectID> reconstructed_objects;
  std::vector<ObjectID> nonexistent_objects;
  object_directory->GetObjects(object_ids, client, &sealed_objects, &reconstructed_objects, &nonexistent_objects);
  for (const auto& object_id: sealed_objects) {
    get_req->SatisfyWithSealedObject(object_id);
  }
  for (const auto& object_id: reconstructed_objects) {
    get_req->SatisfyWithReconstructedObject(object_id);
  }
  for (const auto& object_id: nonexistent_objects) {
    // Add a placeholder plasma object to the get request to indicate that the
    // object is not present. This will be parsed by the client. We set the
    // data size to -1 to indicate that the object is not present.
    get_req->objects[object_id].data_size = -1;
    // Add the get request to the relevant data structures.
    object_get_requests_[object_id].push_back(get_req);
  }
  // If all of the objects are present already or if the timeout is 0, return to
  // the client.
  if (get_req->Fulfilled() || timeout_ms == 0) {
    ReturnFromGet(get_req);
  } else if (timeout_ms != -1) {
    // Set a timer that will cause the get request to return to the client. Note
    // that a timeout of -1 is used to indicate that no timer should be set.
    get_req->timer = loop_->AddTimer(timeout_ms, [this, get_req](int64_t timer_id) {
      ReturnFromGet(get_req);
      return kEventLoopTimerDone;
    });
  }
}

void PlasmaStore::SealObjects(const std::vector<ObjectID>& object_ids) {
  object_directory->SealObjects(object_ids);
  for (size_t i = 0; i < object_ids.size(); ++i) {
    UpdateObjectGetRequests(object_ids[i]);
  }
}

PlasmaError PlasmaStore::CreateAndSealObject(const ObjectID& object_id, bool evict_if_full,
                                             const std::string &data, const std::string &metadata,
                                             int device_num, Client* client,
                                             PlasmaObject* result) {
  Status status = object_directory->CreateAndSealObject(
    object_id, evict_if_full, data, metadata, device_num, client, result);
  if (!status.ok()) {
    return CreateObjectStatusToPlasmaError(status);
  }
  UpdateObjectGetRequests(object_id);
  return PlasmaError::OK;
}

void PlasmaStore::ConnectClient(int listener_sock) {
  int client_fd = AcceptClient(listener_sock);

  Client* client = new Client(client_fd);
  connected_clients_[client_fd] = std::unique_ptr<Client>(client);

  // Add a callback to handle events on this socket.
  // TODO(pcm): Check return value.
  loop_->AddFileEvent(client_fd, kEventLoopRead, [this, client](int events) {
    Status s = ProcessMessage(client);
    if (!s.ok()) {
      RAY_LOG(FATAL) << "Failed to process file event: " << s;
    }
  });
  RAY_LOG(DEBUG) << "New connection with fd " << client_fd;
}

void PlasmaStore::DisconnectClient(int client_fd) {
  RAY_CHECK(client_fd > 0);
  auto it = connected_clients_.find(client_fd);
  RAY_CHECK(it != connected_clients_.end());
  loop_->RemoveFileEvent(client_fd);
  // Close the socket.
  close(client_fd);
  RAY_LOG(DEBUG) << "Disconnecting client on fd " << client_fd;
  // Release all the objects that the client was using.
  auto client = it->second.get();
  object_directory->DisconnectClient(client);

  /// Remove all of the client's GetRequests.
  RemoveGetRequestsForClient(client);

  if (client->notification_fd > 0) {
    // This client has subscribed for notifications.
    auto notify_fd = client->notification_fd;
    loop_->RemoveFileEvent(notify_fd);
    // Close socket.
    close(notify_fd);
    // Remove notification queue for this fd from global map.
    pending_notifications_.erase(notify_fd);
    // Reset fd.
    client->notification_fd = -1;
  }

  connected_clients_.erase(it);
}

/// Send notifications about sealed objects to the subscribers. This is called
/// in SealObject. If the socket's send buffer is full, the notification will
/// be buffered, and this will be called again when the send buffer has room.
/// Since we call erase on pending_notifications_, all iterators get
/// invalidated, which is why we return a valid iterator to the next client to
/// be used in PushNotification.
///
/// \param it Iterator that points to the client to send the notification to.
/// \return Iterator pointing to the next client.
PlasmaStore::NotificationMap::iterator PlasmaStore::SendNotifications(
    PlasmaStore::NotificationMap::iterator it) {
  int client_fd = it->first;
  auto& notifications = it->second.object_notifications;

  int num_processed = 0;
  bool closed = false;
  // Loop over the array of pending notifications and send as many of them as
  // possible.
  for (size_t i = 0; i < notifications.size(); ++i) {
    auto& notification = notifications.at(i);
    // Decode the length, which is the first bytes of the message.
    int64_t size = *(reinterpret_cast<int64_t*>(notification.get()));

    // Attempt to send a notification about this object ID.
    ssize_t nbytes = send(client_fd, notification.get(), sizeof(int64_t) + size, 0);
    if (nbytes >= 0) {
      RAY_CHECK(nbytes == static_cast<ssize_t>(sizeof(int64_t)) + size);
    } else if (nbytes == -1 &&
               (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
      RAY_LOG(DEBUG) << "The socket's send buffer is full, so we are caching this "
                          "notification and will send it later.";
      // Add a callback to the event loop to send queued notifications whenever
      // there is room in the socket's send buffer. Callbacks can be added
      // more than once here and will be overwritten. The callback is removed
      // at the end of the method.
      // TODO(pcm): Introduce status codes and check in case the file descriptor
      // is added twice.
      loop_->AddFileEvent(client_fd, kEventLoopWrite, [this, client_fd](int events) {
        SendNotifications(pending_notifications_.find(client_fd));
      });
      break;
    } else {
      RAY_LOG(WARNING) << "Failed to send notification to client on fd " << client_fd;
      if (errno == EPIPE) {
        closed = true;
        break;
      }
    }
    num_processed += 1;
  }
  // Remove the sent notifications from the array.
  notifications.erase(notifications.begin(), notifications.begin() + num_processed);

  // If we have sent all notifications, remove the fd from the event loop.
  if (notifications.empty()) {
    loop_->RemoveFileEvent(client_fd);
  }

  // Stop sending notifications if the pipe was broken.
  if (closed) {
    close(client_fd);
    return pending_notifications_.erase(it);
  } else {
    return ++it;
  }
}

void PlasmaStore::PushNotifications(const std::vector<ObjectInfoT>& object_info) {
  if (notification_listener_) {
    for (const auto& info : object_info) {
      if (!info.is_deletion) {
        notification_listener_->ProcessStoreAdd(info);
      } else {
        notification_listener_->ProcessStoreRemove(ObjectID::FromBinary(info.object_id));
      }
    }
  }

  auto it = pending_notifications_.begin();
  while (it != pending_notifications_.end()) {
    auto notifications = CreatePlasmaNotificationBuffer(object_info);
    it->second.object_notifications.emplace_back(std::move(notifications));
    it = SendNotifications(it);
  }
}

void PlasmaStore::PushNotifications(const std::vector<ObjectInfoT>& object_info, int client_fd) {
  auto it = pending_notifications_.find(client_fd);
  if (it != pending_notifications_.end()) {
    auto notifications = CreatePlasmaNotificationBuffer(object_info);
    it->second.object_notifications.emplace_back(std::move(notifications));
    SendNotifications(it);
  }
}

// Subscribe to notifications about sealed objects.
void PlasmaStore::SubscribeToUpdates(Client* client) {
  RAY_LOG(DEBUG) << "subscribing to updates on fd " << client->fd;
  if (client->notification_fd > 0) {
    // This client has already subscribed. Return.
    return;
  }

  // TODO(rkn): The store could block here if the client doesn't send a file
  // descriptor.
  int fd = recv_fd(client->fd);
  if (fd < 0) {
    // This may mean that the client died before sending the file descriptor.
    RAY_LOG(WARNING) << "Failed to receive file descriptor from client on fd "
                       << client->fd << ".";
    return;
  }

  // Add this fd to global map, which is needed for this client to receive notifications.
  pending_notifications_[fd];
  client->notification_fd = fd;

  // Push notifications to the new subscriber about existing sealed objects.
  std::vector<ObjectInfoT> infos;
  object_directory->GetSealedObjectsInfo(&infos);
  PushNotifications(infos, fd);
}

Status PlasmaStore::ProcessMessage(Client* client) {
  fb::MessageType type;
  Status s = ReadMessage(client->fd, &type, &input_buffer_);
  RAY_CHECK(s.ok() || s.IsIOError());

  uint8_t* input = input_buffer_.data();
  size_t input_size = input_buffer_.size();
  ObjectID object_id;
  PlasmaObject object = {};

  // Process the different types of requests.
  switch (type) {
    case fb::MessageType::PlasmaCreateRequest: {
      bool evict_if_full;
      int64_t data_size;
      int64_t metadata_size;
      int device_num;
      RAY_RETURN_NOT_OK(ReadCreateRequest(input, input_size, &object_id, &evict_if_full,
                                      &data_size, &metadata_size, &device_num));
      PlasmaError error_code = CreateObjectStatusToPlasmaError(
        object_directory->CreateObject(
          object_id, evict_if_full, data_size, metadata_size, device_num, client, &object));
      int64_t mmap_size = 0;
      if (error_code == PlasmaError::OK && device_num == 0) {
        mmap_size = GetMmapSize(object.store_fd);
      }
      HANDLE_SIGPIPE(
          SendCreateReply(client->fd, object_id, &object, error_code, mmap_size),
          client->fd);
      // Only send the file descriptor if it hasn't been sent (see analogous
      // logic in GetStoreFd in client.cc). Similar in ReturnFromGet.
      if (error_code == PlasmaError::OK && device_num == 0 &&
          client->used_fds.find(object.store_fd) == client->used_fds.end()) {
        WarnIfSigpipe(send_fd(client->fd, object.store_fd), client->fd);
        client->used_fds.insert(object.store_fd);
      }
    } break;
    case fb::MessageType::PlasmaCreateAndSealRequest: {
      bool evict_if_full;
      std::string data;
      std::string metadata;
      RAY_RETURN_NOT_OK(ReadCreateAndSealRequest(input, input_size, &object_id,
                                             &evict_if_full, &data, &metadata));
      // CreateAndSeal currently only supports device_num = 0, which corresponds
      // to the host.
      PlasmaError error_code = CreateAndSealObject(
        object_id, evict_if_full, data, metadata, /*device_num=*/0, client, &object);
      // Reply to the client.
      HANDLE_SIGPIPE(SendCreateAndSealReply(client->fd, error_code), client->fd);
    } break;
    case fb::MessageType::PlasmaCreateAndSealBatchRequest: {
      bool evict_if_full;
      std::vector<ObjectID> object_ids;
      std::vector<std::string> data;
      std::vector<std::string> metadata;

      RAY_RETURN_NOT_OK(ReadCreateAndSealBatchRequest(
          input, input_size, &object_ids, &evict_if_full, &data, &metadata));

      PlasmaError error_code = PlasmaError::OK;
      for (size_t i = 0; i < object_ids.size(); i++) {
        // CreateAndSeal currently only supports device_num = 0, which corresponds
        // to the host.
        error_code = CreateAndSealObject(
          object_ids[i], evict_if_full, data[i], metadata[i], /*device_num=*/0, client, &object);
        if (error_code != PlasmaError::OK) {
          break;
        }
      }
      HANDLE_SIGPIPE(SendCreateAndSealBatchReply(client->fd, error_code), client->fd);
    } break;
    case fb::MessageType::PlasmaAbortRequest: {
      RAY_RETURN_NOT_OK(ReadAbortRequest(input, input_size, &object_id));
      RAY_CHECK(object_directory->AbortObject(object_id, client) == 1)
          << "To abort an object, the only client currently using it must be the creator.";
      HANDLE_SIGPIPE(SendAbortReply(client->fd, object_id), client->fd);
    } break;
    case fb::MessageType::PlasmaGetRequest: {
      std::vector<ObjectID> object_ids_to_get;
      int64_t timeout_ms;
      RAY_RETURN_NOT_OK(ReadGetRequest(input, input_size, object_ids_to_get, &timeout_ms));
      ProcessGetRequest(client, object_ids_to_get, timeout_ms);
    } break;
    case fb::MessageType::PlasmaReleaseRequest: {
      RAY_RETURN_NOT_OK(ReadReleaseRequest(input, input_size, &object_id));
      object_directory->ReleaseObject(object_id, client);
    } break;
    case fb::MessageType::PlasmaDeleteRequest: {
      std::vector<ObjectID> object_ids;
      std::vector<PlasmaError> error_codes;
      RAY_RETURN_NOT_OK(ReadDeleteRequest(input, input_size, &object_ids));
      error_codes.reserve(object_ids.size());
      for (auto& object_id : object_ids) {
        error_codes.push_back(object_directory->DeleteObject(object_id));
      }
      HANDLE_SIGPIPE(SendDeleteReply(client->fd, object_ids, error_codes), client->fd);
    } break;
    case fb::MessageType::PlasmaContainsRequest: {
      RAY_RETURN_NOT_OK(ReadContainsRequest(input, input_size, &object_id));
      if (object_directory->ContainsObject(object_id) == ObjectStatus::OBJECT_FOUND) {
        HANDLE_SIGPIPE(SendContainsReply(client->fd, object_id, 1), client->fd);
      } else {
        HANDLE_SIGPIPE(SendContainsReply(client->fd, object_id, 0), client->fd);
      }
    } break;
    case fb::MessageType::PlasmaSealRequest: {
      RAY_RETURN_NOT_OK(ReadSealRequest(input, input_size, &object_id));
      SealObjects({object_id});
      HANDLE_SIGPIPE(SendSealReply(client->fd, object_id, PlasmaError::OK), client->fd);
    } break;
    case fb::MessageType::PlasmaEvictRequest: {
      // This code path should only be used for testing.
      int64_t num_bytes;
      RAY_RETURN_NOT_OK(ReadEvictRequest(input, input_size, &num_bytes));
      int64_t num_bytes_evicted;
      object_directory->EvictObjects(num_bytes, &num_bytes_evicted);
      HANDLE_SIGPIPE(SendEvictReply(client->fd, num_bytes_evicted), client->fd);
    } break;
    case fb::MessageType::PlasmaRefreshLRURequest: {
      std::vector<ObjectID> object_ids;
      RAY_RETURN_NOT_OK(ReadRefreshLRURequest(input, input_size, &object_ids));
      eviction_policy_.RefreshObjects(object_ids);
      HANDLE_SIGPIPE(SendRefreshLRUReply(client->fd), client->fd);
    } break;
    case fb::MessageType::PlasmaSubscribeRequest:
      SubscribeToUpdates(client);
      break;
    case fb::MessageType::PlasmaConnectRequest: {
      HANDLE_SIGPIPE(SendConnectReply(client->fd, PlasmaAllocator::GetFootprintLimit()),
                     client->fd);
    } break;
    case fb::MessageType::PlasmaDisconnectClient:
      RAY_LOG(DEBUG) << "Disconnecting client on fd " << client->fd;
      DisconnectClient(client->fd);
      break;
    case fb::MessageType::PlasmaSetOptionsRequest: {
      std::string client_name;
      int64_t output_memory_quota;
      RAY_RETURN_NOT_OK(
          ReadSetOptionsRequest(input, input_size, &client_name, &output_memory_quota));
      client->name = client_name;
      bool success = eviction_policy_.SetClientQuota(client, output_memory_quota);
      HANDLE_SIGPIPE(SendSetOptionsReply(client->fd, success ? PlasmaError::OK
                                                             : PlasmaError::OutOfMemory),
                     client->fd);
    } break;
    case fb::MessageType::PlasmaGetDebugStringRequest: {
      HANDLE_SIGPIPE(SendGetDebugStringReply(client->fd, eviction_policy_.DebugString()),
                     client->fd);
    } break;
    default:
      // This code should be unreachable.
      RAY_CHECK(0);
  }
  return Status::OK();
}

}  // namespace plasma
