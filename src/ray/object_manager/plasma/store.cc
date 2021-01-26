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

#include <boost/bind.hpp>
#include <chrono>
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
#include "ray/object_manager/plasma/malloc.h"
#include "ray/object_manager/plasma/plasma_allocator.h"
#include "ray/object_manager/plasma/protocol.h"
#include "ray/util/asio_util.h"
#include "ray/util/util.h"

namespace fb = plasma::flatbuf;

namespace {

ray::ObjectID GetCreateRequestObjectId(const std::vector<uint8_t> &message) {
  uint8_t *input = (uint8_t *)message.data();
  size_t input_size = message.size();
  auto request = flatbuffers::GetRoot<fb::PlasmaCreateRequest>(input);
  RAY_DCHECK(plasma::VerifyFlatbuffer(request, input, input_size));
  return ray::ObjectID::FromBinary(request->object_id()->str());
}

}  // namespace

namespace plasma {

struct GetRequest {
  GetRequest(boost::asio::io_service &io_context, const std::shared_ptr<Client> &client,
             const std::vector<ObjectID> &object_ids);
  /// The client that called get.
  std::shared_ptr<Client> client;
  /// The object IDs involved in this request. This is used in the reply.
  std::vector<ObjectID> object_ids;
  /// The object information for the objects in this request. This is used in
  /// the reply.
  std::unordered_map<ObjectID, PlasmaObject> objects;
  /// The minimum number of objects to wait for in this request.
  int64_t num_objects_to_wait_for;
  /// The number of object requests in this wait request that are already
  /// satisfied.
  int64_t num_satisfied;

  void AsyncWait(int64_t timeout_ms,
                 std::function<void(const boost::system::error_code &)> on_timeout) {
    // Set an expiry time relative to now.
    timer_.expires_from_now(std::chrono::milliseconds(timeout_ms));
    timer_.async_wait(on_timeout);
  }

  void CancelTimer() { timer_.cancel(); }

 private:
  /// The timer that will time out and cause this wait to return to
  /// the client if it hasn't already returned.
  boost::asio::steady_timer timer_;
};

GetRequest::GetRequest(boost::asio::io_service &io_context,
                       const std::shared_ptr<Client> &client,
                       const std::vector<ObjectID> &object_ids)
    : client(client),
      object_ids(object_ids.begin(), object_ids.end()),
      objects(object_ids.size()),
      num_satisfied(0),
      timer_(io_context) {
  std::unordered_set<ObjectID> unique_ids(object_ids.begin(), object_ids.end());
  num_objects_to_wait_for = unique_ids.size();
}

PlasmaStore::PlasmaStore(boost::asio::io_service &main_service, std::string directory,
                         bool hugepages_enabled, const std::string &socket_name,
                         uint32_t delay_on_oom_ms,
                         ray::SpillObjectsCallback spill_objects_callback,
                         std::function<void()> object_store_full_callback)
    : io_context_(main_service),
      socket_name_(socket_name),
      acceptor_(main_service, ParseUrlEndpoint(socket_name)),
      socket_(main_service),
      eviction_policy_(&store_info_, PlasmaAllocator::GetFootprintLimit()),
      spill_objects_callback_(spill_objects_callback),
      delay_on_oom_ms_(delay_on_oom_ms),
      usage_log_interval_ns_(RayConfig::instance().object_store_usage_log_interval_s() *
                             1e9),
      create_request_queue_(
          /*evict_if_full=*/RayConfig::instance().object_pinning_enabled(),
          /*oom_grace_period_s=*/RayConfig::instance().oom_grace_period_s(),
          spill_objects_callback, object_store_full_callback,
          /*get_time=*/
          []() { return absl::GetCurrentTimeNanos(); }) {
  store_info_.directory = directory;
  store_info_.hugepages_enabled = hugepages_enabled;
}

// TODO(pcm): Get rid of this destructor by using RAII to clean up data.
PlasmaStore::~PlasmaStore() {}

void PlasmaStore::Start() {
  // Start listening for clients.
  DoAccept();
}

void PlasmaStore::Stop() { acceptor_.close(); }

const PlasmaStoreInfo *PlasmaStore::GetPlasmaStoreInfo() { return &store_info_; }

// If this client is not already using the object, add the client to the
// object's list of clients, otherwise do nothing.
void PlasmaStore::AddToClientObjectIds(const ObjectID &object_id, ObjectTableEntry *entry,
                                       const std::shared_ptr<Client> &client) {
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

// Allocate memory
uint8_t *PlasmaStore::AllocateMemory(size_t size, bool evict_if_full, MEMFD_TYPE *fd,
                                     int64_t *map_size, ptrdiff_t *offset,
                                     const std::shared_ptr<Client> &client,
                                     bool is_create, PlasmaError *error) {
  // First free up space from the client's LRU queue if quota enforcement is on.
  if (evict_if_full) {
    std::vector<ObjectID> client_objects_to_evict;
    bool quota_ok = eviction_policy_.EnforcePerClientQuota(client.get(), size, is_create,
                                                           &client_objects_to_evict);
    if (!quota_ok) {
      *error = PlasmaError::OutOfMemory;
      return nullptr;
    }
    EvictObjects(client_objects_to_evict);
  }

  // Try to evict objects until there is enough space.
  uint8_t *pointer = nullptr;
  while (true) {
    // Allocate space for the new object. We use memalign instead of malloc
    // in order to align the allocated region to a 64-byte boundary. This is not
    // strictly necessary, but it is an optimization that could speed up the
    // computation of a hash of the data (see compute_object_hash_parallel in
    // plasma_client.cc). Note that even though this pointer is 64-byte aligned,
    // it is not guaranteed that the corresponding pointer in the client will be
    // 64-byte aligned, but in practice it often will be.
    pointer = reinterpret_cast<uint8_t *>(PlasmaAllocator::Memalign(kBlockSize, size));
    if (pointer || !evict_if_full) {
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
    // More space is still needed. Try to spill objects to external storage to
    // make room.
    if (space_needed > 0) {
      *error = PlasmaError::OutOfMemory;
      break;
    }
  }

  if (pointer != nullptr) {
    GetMallocMapinfo(pointer, fd, map_size, offset);
    RAY_CHECK(*fd != INVALID_FD);
    *error = PlasmaError::OK;
  }

  auto now = absl::GetCurrentTimeNanos();
  if (now - last_usage_log_ns_ > usage_log_interval_ns_) {
    RAY_LOG(INFO) << "Object store current usage " << (PlasmaAllocator::Allocated() / 1e9)
                  << " / " << (PlasmaAllocator::GetFootprintLimit() / 1e9) << " GB.";
    last_usage_log_ns_ = now;
  }
  return pointer;
}

PlasmaError PlasmaStore::HandleCreateObjectRequest(const std::shared_ptr<Client> &client,
                                                   const std::vector<uint8_t> &message,
                                                   bool evict_if_full,
                                                   PlasmaObject *object) {
  uint8_t *input = (uint8_t *)message.data();
  size_t input_size = message.size();
  ObjectID object_id;

  NodeID owner_raylet_id;
  std::string owner_ip_address;
  int owner_port;
  WorkerID owner_worker_id;
  int64_t data_size;
  int64_t metadata_size;
  int device_num;
  ReadCreateRequest(input, input_size, &object_id, &owner_raylet_id, &owner_ip_address,
                    &owner_port, &owner_worker_id, &data_size, &metadata_size,
                    &device_num);
  auto error = CreateObject(object_id, owner_raylet_id, owner_ip_address, owner_port,
                            owner_worker_id, evict_if_full, data_size, metadata_size,
                            device_num, client, object);
  if (error == PlasmaError::OutOfMemory) {
    RAY_LOG(DEBUG) << "Not enough memory to create the object " << object_id
                   << ", data_size=" << data_size << ", metadata_size=" << metadata_size;
  }
  return error;
}

PlasmaError PlasmaStore::CreateObject(
    const ObjectID &object_id, const NodeID &owner_raylet_id,
    const std::string &owner_ip_address, int owner_port, const WorkerID &owner_worker_id,
    bool evict_if_full, int64_t data_size, int64_t metadata_size, int device_num,
    const std::shared_ptr<Client> &client, PlasmaObject *result) {
  RAY_LOG(DEBUG) << "creating object " << object_id.Hex() << " size " << data_size;

  auto entry = GetObjectTableEntry(&store_info_, object_id);
  if (entry != nullptr) {
    // There is already an object with the same ID in the Plasma Store, so
    // ignore this request.
    return PlasmaError::ObjectExists;
  }

  MEMFD_TYPE fd = INVALID_FD;
  int64_t map_size = 0;
  ptrdiff_t offset = 0;
  uint8_t *pointer = nullptr;
  auto total_size = data_size + metadata_size;

  if (device_num == 0) {
    PlasmaError error = PlasmaError::OK;
    pointer = AllocateMemory(total_size, evict_if_full, &fd, &map_size, &offset, client,
                             true, &error);
    if (!pointer) {
      return error;
    }
  } else {
    RAY_LOG(ERROR) << "device_num != 0 but CUDA not enabled";
    return PlasmaError::OutOfMemory;
  }

  auto ptr = std::unique_ptr<ObjectTableEntry>(new ObjectTableEntry());
  entry = store_info_.objects.emplace(object_id, std::move(ptr)).first->second.get();
  entry->data_size = data_size;
  entry->metadata_size = metadata_size;
  entry->pointer = pointer;
  // TODO(pcm): Set the other fields.
  entry->fd = fd;
  entry->map_size = map_size;
  entry->offset = offset;
  entry->state = ObjectState::PLASMA_CREATED;
  entry->device_num = device_num;
  entry->owner_raylet_id = owner_raylet_id;
  entry->owner_ip_address = owner_ip_address;
  entry->owner_port = owner_port;
  entry->owner_worker_id = owner_worker_id;
  entry->create_time = std::time(nullptr);
  entry->construct_duration = -1;

  result->store_fd = fd;
  result->data_offset = offset;
  result->metadata_offset = offset + data_size;
  result->data_size = data_size;
  result->metadata_size = metadata_size;
  result->device_num = device_num;
  if (device_num == 0) {
    result->mmap_size = GetMmapSize(fd);
  }

  // Notify the eviction policy that this object was created. This must be done
  // immediately before the call to AddToClientObjectIds so that the
  // eviction policy does not have an opportunity to evict the object.
  eviction_policy_.ObjectCreated(object_id, client.get(), true);
  // Record that this client is using this object.
  AddToClientObjectIds(object_id, store_info_.objects[object_id].get(), client);
  return PlasmaError::OK;
}

void PlasmaObject_init(PlasmaObject *object, ObjectTableEntry *entry) {
  RAY_DCHECK(object != nullptr);
  RAY_DCHECK(entry != nullptr);
  RAY_DCHECK(entry->state == ObjectState::PLASMA_SEALED);
  object->store_fd = entry->fd;
  object->data_offset = entry->offset;
  object->metadata_offset = entry->offset + entry->data_size;
  object->data_size = entry->data_size;
  object->metadata_size = entry->metadata_size;
  object->device_num = entry->device_num;
}

void PlasmaStore::RemoveGetRequest(GetRequest *get_request) {
  // Remove the get request from each of the relevant object_get_requests hash
  // tables if it is present there. It should only be present there if the get
  // request timed out or if it was issued by a client that has disconnected.
  for (ObjectID &object_id : get_request->object_ids) {
    auto object_request_iter = object_get_requests_.find(object_id);
    if (object_request_iter != object_get_requests_.end()) {
      auto &get_requests = object_request_iter->second;
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
  get_request->CancelTimer();
  delete get_request;
}

void PlasmaStore::RemoveGetRequestsForClient(const std::shared_ptr<Client> &client) {
  std::unordered_set<GetRequest *> get_requests_to_remove;
  for (auto const &pair : object_get_requests_) {
    for (GetRequest *get_request : pair.second) {
      if (get_request->client == client) {
        get_requests_to_remove.insert(get_request);
      }
    }
  }

  // It shouldn't be possible for a given client to be in the middle of multiple get
  // requests.
  RAY_CHECK(get_requests_to_remove.size() <= 1);
  for (GetRequest *get_request : get_requests_to_remove) {
    RemoveGetRequest(get_request);
  }
}

void PlasmaStore::ReturnFromGet(GetRequest *get_req) {
  // Figure out how many file descriptors we need to send.
  std::unordered_set<MEMFD_TYPE> fds_to_send;
  std::vector<MEMFD_TYPE> store_fds;
  std::vector<int64_t> mmap_sizes;
  for (const auto &object_id : get_req->object_ids) {
    PlasmaObject &object = get_req->objects[object_id];
    MEMFD_TYPE fd = object.store_fd;
    if (object.data_size != -1 && fds_to_send.count(fd) == 0 && fd != INVALID_FD) {
      fds_to_send.insert(fd);
      store_fds.push_back(fd);
      mmap_sizes.push_back(GetMmapSize(fd));
    }
  }
  // Send the get reply to the client.
  Status s = SendGetReply(get_req->client, &get_req->object_ids[0], get_req->objects,
                          get_req->object_ids.size(), store_fds, mmap_sizes);
  // If we successfully sent the get reply message to the client, then also send
  // the file descriptors.
  if (s.ok()) {
    // Send all of the file descriptors for the present objects.
    for (MEMFD_TYPE store_fd : store_fds) {
      Status send_fd_status = get_req->client->SendFd(store_fd);
      if (!send_fd_status.ok()) {
        RAY_LOG(ERROR) << "Failed to send mmap results to client on fd "
                       << get_req->client;
      }
    }
  } else {
    RAY_LOG(ERROR) << "Failed to send Get reply to client on fd " << get_req->client;
  }

  // Remove the get request from each of the relevant object_get_requests hash
  // tables if it is present there. It should only be present there if the get
  // request timed out.
  RemoveGetRequest(get_req);
}

void PlasmaStore::UpdateObjectGetRequests(const ObjectID &object_id) {
  auto it = object_get_requests_.find(object_id);
  // If there are no get requests involving this object, then return.
  if (it == object_get_requests_.end()) {
    return;
  }

  auto &get_requests = it->second;

  // After finishing the loop below, get_requests and it will have been
  // invalidated by the removal of object_id from object_get_requests_.
  size_t index = 0;
  size_t num_requests = get_requests.size();
  for (size_t i = 0; i < num_requests; ++i) {
    auto get_req = get_requests[index];
    auto entry = GetObjectTableEntry(&store_info_, object_id);
    RAY_CHECK(entry != nullptr);

    PlasmaObject_init(&get_req->objects[object_id], entry);
    get_req->num_satisfied += 1;
    // Record the fact that this client will be using this object and will
    // be responsible for releasing this object.
    AddToClientObjectIds(object_id, entry, get_req->client);

    // If this get request is done, reply to the client.
    if (get_req->num_satisfied == get_req->num_objects_to_wait_for) {
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

void PlasmaStore::ProcessGetRequest(const std::shared_ptr<Client> &client,
                                    const std::vector<ObjectID> &object_ids,
                                    int64_t timeout_ms) {
  // Create a get request for this object.
  auto get_req = new GetRequest(io_context_, client, object_ids);
  for (auto object_id : object_ids) {
    // Check if this object is already present
    // locally. If so, record that the object is being used and mark it as accounted for.
    auto entry = GetObjectTableEntry(&store_info_, object_id);
    if (entry && entry->state == ObjectState::PLASMA_SEALED) {
      // Update the get request to take into account the present object.
      PlasmaObject_init(&get_req->objects[object_id], entry);
      get_req->num_satisfied += 1;
      // If necessary, record that this client is using this object. In the case
      // where entry == NULL, this will be called from SealObject.
      AddToClientObjectIds(object_id, entry, client);
    } else if (entry && entry->state == ObjectState::PLASMA_EVICTED) {
      // Make sure the object pointer is not already allocated
      RAY_CHECK(!entry->pointer);

      PlasmaError error = PlasmaError::OK;
      entry->pointer = AllocateMemory(entry->data_size + entry->metadata_size,
                                      /*evict=*/true, &entry->fd, &entry->map_size,
                                      &entry->offset, client, false, &error);
      if (entry->pointer) {
        // TODO(suquark): Not sure if this old behavior is still compatible
        // with our current object spilling mechanics.
        entry->state = ObjectState::PLASMA_CREATED;
        entry->create_time = std::time(nullptr);
        eviction_policy_.ObjectCreated(object_id, client.get(), false);
        AddToClientObjectIds(object_id, store_info_.objects[object_id].get(), client);
      } else {
        // We are out of memory and cannot allocate memory for this object.
        // Change the state of the object back to PLASMA_EVICTED so some
        // other request can try again.
        entry->state = ObjectState::PLASMA_EVICTED;
      }
    } else {
      // Add a placeholder plasma object to the get request to indicate that the
      // object is not present. This will be parsed by the client. We set the
      // data size to -1 to indicate that the object is not present.
      get_req->objects[object_id].data_size = -1;
      // Add the get request to the relevant data structures.
      object_get_requests_[object_id].push_back(get_req);
    }
  }

  // If all of the objects are present already or if the timeout is 0, return to
  // the client.
  if (get_req->num_satisfied == get_req->num_objects_to_wait_for || timeout_ms == 0) {
    ReturnFromGet(get_req);
  } else if (timeout_ms != -1) {
    // Set a timer that will cause the get request to return to the client. Note
    // that a timeout of -1 is used to indicate that no timer should be set.
    get_req->AsyncWait(timeout_ms, [this, get_req](const boost::system::error_code &ec) {
      if (ec != boost::asio::error::operation_aborted) {
        // Timer was not cancelled, take necessary action.
        ReturnFromGet(get_req);
      }
    });
  }
}

int PlasmaStore::RemoveFromClientObjectIds(const ObjectID &object_id,
                                           ObjectTableEntry *entry,
                                           const std::shared_ptr<Client> &client) {
  auto it = client->object_ids.find(object_id);
  if (it != client->object_ids.end()) {
    client->object_ids.erase(it);
    // Decrease reference count.
    entry->ref_count--;

    // If no more clients are using this object, notify the eviction policy
    // that the object is no longer being used.
    if (entry->ref_count == 0) {
      RAY_LOG(DEBUG) << "Releasing object no longer in use " << object_id;
      if (deletion_cache_.count(object_id) == 0) {
        // Tell the eviction policy that this object is no longer being used.
        eviction_policy_.EndObjectAccess(object_id);
      } else {
        // Above code does not really delete an object. Instead, it just put an
        // object to LRU cache which will be cleaned when the memory is not enough.
        deletion_cache_.erase(object_id);
        EvictObjects({object_id});
      }
    }
    // Return 1 to indicate that the client was removed.
    return 1;
  } else {
    // Return 0 to indicate that the client was not removed.
    return 0;
  }
}

void PlasmaStore::EraseFromObjectTable(const ObjectID &object_id) {
  auto &object = store_info_.objects[object_id];
  auto buff_size = object->data_size + object->metadata_size;
  if (object->device_num == 0) {
    PlasmaAllocator::Free(object->pointer, buff_size);
  }
  store_info_.objects.erase(object_id);
}

void PlasmaStore::ReleaseObject(const ObjectID &object_id,
                                const std::shared_ptr<Client> &client) {
  auto entry = GetObjectTableEntry(&store_info_, object_id);
  RAY_CHECK(entry != nullptr);
  // Remove the client from the object's array of clients.
  RAY_CHECK(RemoveFromClientObjectIds(object_id, entry, client) == 1);
}

// Check if an object is present.
ObjectStatus PlasmaStore::ContainsObject(const ObjectID &object_id) {
  auto entry = GetObjectTableEntry(&store_info_, object_id);
  return entry && (entry->state == ObjectState::PLASMA_SEALED ||
                   entry->state == ObjectState::PLASMA_EVICTED)
             ? ObjectStatus::OBJECT_FOUND
             : ObjectStatus::OBJECT_NOT_FOUND;
}

void PlasmaStore::SealObjects(const std::vector<ObjectID> &object_ids) {
  std::vector<ObjectInfoT> infos;

  for (size_t i = 0; i < object_ids.size(); ++i) {
    RAY_LOG(DEBUG) << "sealing object " << object_ids[i];
    ObjectInfoT object_info;
    auto entry = GetObjectTableEntry(&store_info_, object_ids[i]);
    RAY_CHECK(entry != nullptr);
    RAY_CHECK(entry->state == ObjectState::PLASMA_CREATED);
    // Set the state of object to SEALED.
    entry->state = ObjectState::PLASMA_SEALED;
    // Set object construction duration.
    entry->construct_duration = std::time(nullptr) - entry->create_time;

    object_info.object_id = object_ids[i].Binary();
    object_info.data_size = entry->data_size;
    object_info.owner_raylet_id = entry->owner_raylet_id.Binary();
    object_info.owner_ip_address = entry->owner_ip_address;
    object_info.owner_port = entry->owner_port;
    object_info.owner_worker_id = entry->owner_worker_id.Binary();
    object_info.metadata_size = entry->metadata_size;
    infos.push_back(object_info);
  }

  PushNotifications(infos);

  for (size_t i = 0; i < object_ids.size(); ++i) {
    UpdateObjectGetRequests(object_ids[i]);
  }
}

int PlasmaStore::AbortObject(const ObjectID &object_id,
                             const std::shared_ptr<Client> &client) {
  auto entry = GetObjectTableEntry(&store_info_, object_id);
  RAY_CHECK(entry != nullptr) << "To abort an object it must be in the object table.";
  RAY_CHECK(entry->state != ObjectState::PLASMA_SEALED)
      << "To abort an object it must not have been sealed.";
  auto it = client->object_ids.find(object_id);
  if (it == client->object_ids.end()) {
    // If the client requesting the abort is not the creator, do not
    // perform the abort.
    return 0;
  } else {
    // The client requesting the abort is the creator. Free the object.
    EraseFromObjectTable(object_id);
    client->object_ids.erase(it);
    return 1;
  }
}

PlasmaError PlasmaStore::DeleteObject(ObjectID &object_id) {
  auto entry = GetObjectTableEntry(&store_info_, object_id);
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

  // Prepare the notification before deleting the object.
  ObjectInfoT notification;
  notification.object_id = object_id.Binary();
  notification.owner_raylet_id = entry->owner_raylet_id.Binary();
  notification.owner_ip_address = entry->owner_ip_address;
  notification.owner_port = entry->owner_port;
  notification.owner_worker_id = entry->owner_worker_id.Binary();
  notification.is_deletion = true;

  eviction_policy_.RemoveObject(object_id);
  EraseFromObjectTable(object_id);
  // Inform all subscribers that the object has been deleted.
  PushNotification(&notification);
  return PlasmaError::OK;
}

void PlasmaStore::EvictObjects(const std::vector<ObjectID> &object_ids) {
  if (object_ids.size() == 0) {
    return;
  }
  for (const auto &object_id : object_ids) {
    RAY_LOG(DEBUG) << "evicting object " << object_id.Hex();
    auto entry = GetObjectTableEntry(&store_info_, object_id);
    // TODO(rkn): This should probably not fail, but should instead throw an
    // error. Maybe we should also support deleting objects that have been
    // created but not sealed.
    RAY_CHECK(entry != nullptr) << "To evict an object it must be in the object table.";
    RAY_CHECK(entry->state == ObjectState::PLASMA_SEALED)
        << "To evict an object it must have been sealed.";
    RAY_CHECK(entry->ref_count == 0)
        << "To evict an object, there must be no clients currently using it.";

    // Prepare the notification before deleting the object.
    ObjectInfoT notification;
    notification.object_id = object_id.Binary();
    notification.owner_raylet_id = entry->owner_raylet_id.Binary();
    notification.owner_ip_address = entry->owner_ip_address;
    notification.owner_port = entry->owner_port;
    notification.owner_worker_id = entry->owner_worker_id.Binary();
    notification.is_deletion = true;
    // Erase the object entry and send a deletion notification.
    EraseFromObjectTable(object_id);
    // Inform all subscribers that the object has been deleted.
    PushNotification(&notification);
  }
}

void PlasmaStore::ConnectClient(const boost::system::error_code &error) {
  if (!error) {
    // Accept a new local client and dispatch it to the node manager.
    auto new_connection = Client::Create(
        boost::bind(&PlasmaStore::ProcessMessage, this, _1, _2, _3), std::move(socket_));
  }
  // We're ready to accept another client.
  DoAccept();
}

void PlasmaStore::DisconnectClient(const std::shared_ptr<Client> &client) {
  client->Close();
  RAY_LOG(DEBUG) << "Disconnecting client on fd " << client;
  // Release all the objects that the client was using.
  eviction_policy_.ClientDisconnected(client.get());
  std::unordered_map<ObjectID, ObjectTableEntry *> sealed_objects;
  for (const auto &object_id : client->object_ids) {
    auto it = store_info_.objects.find(object_id);
    if (it == store_info_.objects.end()) {
      continue;
    }

    if (it->second->state == ObjectState::PLASMA_SEALED) {
      // Add sealed objects to a temporary list of object IDs. Do not perform
      // the remove here, since it potentially modifies the object_ids table.
      sealed_objects[it->first] = it->second.get();
    } else {
      // Abort unsealed object.
      // Don't call AbortObject() because client->object_ids would be modified.
      EraseFromObjectTable(object_id);
    }
  }

  /// Remove all of the client's GetRequests.
  RemoveGetRequestsForClient(client);

  for (const auto &entry : sealed_objects) {
    RemoveFromClientObjectIds(entry.first, entry.second, client);
  }

  if (notification_clients_.find(client) != notification_clients_.end()) {
    // Remove notification for this client from global map.
    notification_clients_.erase(client);
  }

  create_request_queue_.RemoveDisconnectedClientRequests(client);
}

/// Send notifications about sealed objects to the subscribers. This is called
/// in SealObject. If the socket's send buffer is full, the notification will
/// be buffered, and this will be called again when the send buffer has room.
///
/// \param client The client to push notifications to.
/// \param object_info The notifications.
void PlasmaStore::SendNotifications(const std::shared_ptr<Client> &client,
                                    const std::vector<ObjectInfoT> &object_info) {
  namespace protocol = ray::object_manager::protocol;
  flatbuffers::FlatBufferBuilder fbb;
  std::vector<flatbuffers::Offset<protocol::ObjectInfo>> info;
  for (size_t i = 0; i < object_info.size(); ++i) {
    info.push_back(protocol::CreateObjectInfo(fbb, &object_info[i]));
  }
  auto info_array = fbb.CreateVector(info);
  auto message = protocol::CreatePlasmaNotification(fbb, info_array);
  fbb.Finish(message);

  // In C++14, we can use unique_ptr instead.
  auto size = new int64_t;
  *size = fbb.GetSize();
  auto data = new uint8_t[fbb.GetSize()];
  std::memcpy(data, fbb.GetBufferPointer(), fbb.GetSize());

  std::vector<boost::asio::const_buffer> buffers{
      boost::asio::const_buffer(size, sizeof(*size)),
      boost::asio::const_buffer(data, fbb.GetSize()),
  };
  client->WriteBufferAsync(buffers, [this, client, size, data](const Status &s) {
    if (!s.ok()) {
      RAY_LOG(WARNING) << "Failed to send notification to client on fd " << client;
      if (s.IsIOError()) {
        client->Close();
        notification_clients_.erase(client);
      }
    }
    delete size;
    delete[] data;
  });
}

void PlasmaStore::PushNotification(ObjectInfoT *object_info) {
  PushNotifications({*object_info});
}

void PlasmaStore::PushNotifications(const std::vector<ObjectInfoT> &object_info) {
  if (notification_listener_) {
    for (const auto &info : object_info) {
      if (!info.is_deletion) {
        notification_listener_->ProcessStoreAdd(info);
      } else {
        notification_listener_->ProcessStoreRemove(ObjectID::FromBinary(info.object_id));
      }
    }
  }

  for (const auto &client : notification_clients_) {
    SendNotifications(client, object_info);
  }
}

// Subscribe to notifications about sealed objects.
void PlasmaStore::SubscribeToUpdates(const std::shared_ptr<Client> &client) {
  // Add this fd to global map, which is needed for this client to receive notifications.
  notification_clients_.insert(client);

  std::vector<ObjectInfoT> infos;
  // Push notifications to the new subscriber about existing sealed objects.
  for (const auto &entry : store_info_.objects) {
    if (entry.second->state == ObjectState::PLASMA_SEALED) {
      ObjectInfoT info;
      info.object_id = entry.first.Binary();
      info.data_size = entry.second->data_size;
      info.metadata_size = entry.second->metadata_size;
      info.owner_raylet_id = entry.second->owner_raylet_id.Binary();
      info.owner_ip_address = entry.second->owner_ip_address;
      info.owner_port = entry.second->owner_port;
      info.owner_worker_id = entry.second->owner_worker_id.Binary();
      infos.push_back(info);
    }
  }
  SendNotifications(client, infos);
}

Status PlasmaStore::ProcessMessage(const std::shared_ptr<Client> &client,
                                   fb::MessageType type,
                                   const std::vector<uint8_t> &message) {
  // Global lock is used here so that we allow raylet to access some of methods
  // that are required for object spilling directly without releasing a lock.
  std::lock_guard<std::recursive_mutex> guard(mutex_);
  // TODO(suquark): We should convert these interfaces to const later.
  uint8_t *input = (uint8_t *)message.data();
  size_t input_size = message.size();
  ObjectID object_id;

  // Process the different types of requests.
  switch (type) {
  case fb::MessageType::PlasmaCreateRequest: {
    const auto &object_id = GetCreateRequestObjectId(message);
    const auto &request = flatbuffers::GetRoot<fb::PlasmaCreateRequest>(input);

    auto handle_create = [this, client, message](bool evict_if_full,
                                                 PlasmaObject *result) {
      return HandleCreateObjectRequest(client, message, evict_if_full, result);
    };

    if (request->try_immediately()) {
      RAY_LOG(DEBUG) << "Received request to create object " << object_id
                     << " immediately";
      auto result_error =
          create_request_queue_.TryRequestImmediately(object_id, client, handle_create);
      const auto &result = result_error.first;
      const auto &error = result_error.second;
      if (SendCreateReply(client, object_id, result, error).ok() &&
          error == PlasmaError::OK && result.device_num == 0) {
        static_cast<void>(client->SendFd(result.store_fd));
      }
    } else {
      auto req_id = create_request_queue_.AddRequest(object_id, client, handle_create);
      RAY_LOG(DEBUG) << "Received create request for object " << object_id
                     << " assigned request ID " << req_id;
      ProcessCreateRequests();
      ReplyToCreateClient(client, object_id, req_id);
    }
  } break;
  case fb::MessageType::PlasmaCreateRetryRequest: {
    auto request = flatbuffers::GetRoot<fb::PlasmaCreateRetryRequest>(input);
    RAY_DCHECK(plasma::VerifyFlatbuffer(request, input, input_size));
    const auto &object_id = ObjectID::FromBinary(request->object_id()->str());
    ReplyToCreateClient(client, object_id, request->request_id());
  } break;
  case fb::MessageType::PlasmaAbortRequest: {
    RAY_RETURN_NOT_OK(ReadAbortRequest(input, input_size, &object_id));
    RAY_CHECK(AbortObject(object_id, client) == 1) << "To abort an object, the only "
                                                      "client currently using it "
                                                      "must be the creator.";
    RAY_RETURN_NOT_OK(SendAbortReply(client, object_id));
  } break;
  case fb::MessageType::PlasmaGetRequest: {
    std::vector<ObjectID> object_ids_to_get;
    int64_t timeout_ms;
    RAY_RETURN_NOT_OK(ReadGetRequest(input, input_size, object_ids_to_get, &timeout_ms));
    ProcessGetRequest(client, object_ids_to_get, timeout_ms);
  } break;
  case fb::MessageType::PlasmaReleaseRequest: {
    RAY_RETURN_NOT_OK(ReadReleaseRequest(input, input_size, &object_id));
    ReleaseObject(object_id, client);
  } break;
  case fb::MessageType::PlasmaDeleteRequest: {
    std::vector<ObjectID> object_ids;
    std::vector<PlasmaError> error_codes;
    RAY_RETURN_NOT_OK(ReadDeleteRequest(input, input_size, &object_ids));
    error_codes.reserve(object_ids.size());
    for (auto &object_id : object_ids) {
      error_codes.push_back(DeleteObject(object_id));
    }
    RAY_RETURN_NOT_OK(SendDeleteReply(client, object_ids, error_codes));
  } break;
  case fb::MessageType::PlasmaContainsRequest: {
    RAY_RETURN_NOT_OK(ReadContainsRequest(input, input_size, &object_id));
    if (ContainsObject(object_id) == ObjectStatus::OBJECT_FOUND) {
      RAY_RETURN_NOT_OK(SendContainsReply(client, object_id, 1));
    } else {
      RAY_RETURN_NOT_OK(SendContainsReply(client, object_id, 0));
    }
  } break;
  case fb::MessageType::PlasmaSealRequest: {
    RAY_RETURN_NOT_OK(ReadSealRequest(input, input_size, &object_id));
    SealObjects({object_id});
    RAY_RETURN_NOT_OK(SendSealReply(client, object_id, PlasmaError::OK));
  } break;
  case fb::MessageType::PlasmaEvictRequest: {
    // This code path should only be used for testing.
    int64_t num_bytes;
    RAY_RETURN_NOT_OK(ReadEvictRequest(input, input_size, &num_bytes));
    std::vector<ObjectID> objects_to_evict;
    int64_t num_bytes_evicted =
        eviction_policy_.ChooseObjectsToEvict(num_bytes, &objects_to_evict);
    EvictObjects(objects_to_evict);
    RAY_RETURN_NOT_OK(SendEvictReply(client, num_bytes_evicted));
  } break;
  case fb::MessageType::PlasmaRefreshLRURequest: {
    std::vector<ObjectID> object_ids;
    RAY_RETURN_NOT_OK(ReadRefreshLRURequest(input, input_size, &object_ids));
    eviction_policy_.RefreshObjects(object_ids);
    RAY_RETURN_NOT_OK(SendRefreshLRUReply(client));
  } break;
  case fb::MessageType::PlasmaSubscribeRequest:
    SubscribeToUpdates(client);
    break;
  case fb::MessageType::PlasmaConnectRequest: {
    RAY_RETURN_NOT_OK(SendConnectReply(client, PlasmaAllocator::GetFootprintLimit()));
  } break;
  case fb::MessageType::PlasmaDisconnectClient:
    RAY_LOG(DEBUG) << "Disconnecting client on fd " << client;
    DisconnectClient(client);
    return Status::Disconnected("The Plasma Store client is disconnected.");
    break;
  case fb::MessageType::PlasmaSetOptionsRequest: {
    std::string client_name;
    int64_t output_memory_quota;
    RAY_RETURN_NOT_OK(
        ReadSetOptionsRequest(input, input_size, &client_name, &output_memory_quota));
    client->name = client_name;
    bool success = eviction_policy_.SetClientQuota(client.get(), output_memory_quota);
    RAY_RETURN_NOT_OK(SendSetOptionsReply(
        client, success ? PlasmaError::OK : PlasmaError::OutOfMemory));
  } break;
  case fb::MessageType::PlasmaGetDebugStringRequest: {
    RAY_RETURN_NOT_OK(SendGetDebugStringReply(client, eviction_policy_.DebugString()));
  } break;
  default:
    // This code should be unreachable.
    RAY_CHECK(0);
  }
  return Status::OK();
}

void PlasmaStore::DoAccept() {
  acceptor_.async_accept(socket_, boost::bind(&PlasmaStore::ConnectClient, this,
                                              boost::asio::placeholders::error));
}

void PlasmaStore::ProcessCreateRequests() {
  // Only try to process requests if the timer is not set. If the timer is set,
  // that means that the first request is currently not serviceable because
  // there is not enough memory. In that case, we should wait for the timer to
  // expire before trying any requests again.
  if (create_timer_) {
    return;
  }

  auto status = create_request_queue_.ProcessRequests();
  uint32_t retry_after_ms = 0;
  if (!status.ok()) {
    retry_after_ms = delay_on_oom_ms_;
  }

  if (retry_after_ms > 0) {
    // Try to process requests later, after space has been made.
    create_timer_ = execute_after(io_context_,
                                  [this]() {
                                    RAY_LOG(DEBUG)
                                        << "OOM timer finished, retrying create requests";
                                    create_timer_ = nullptr;
                                    ProcessCreateRequests();
                                  },
                                  retry_after_ms);
  }
}

void PlasmaStore::ReplyToCreateClient(const std::shared_ptr<Client> &client,
                                      const ObjectID &object_id, uint64_t req_id) {
  PlasmaObject result = {};
  PlasmaError error;
  bool finished = create_request_queue_.GetRequestResult(req_id, &result, &error);
  if (finished) {
    RAY_LOG(DEBUG) << "Finishing create object " << object_id << " request ID " << req_id;
    if (SendCreateReply(client, object_id, result, error).ok() &&
        error == PlasmaError::OK && result.device_num == 0) {
      static_cast<void>(client->SendFd(result.store_fd));
    }
  } else {
    static_cast<void>(SendUnfinishedCreateReply(client, object_id, req_id));
  }
}

bool PlasmaStore::IsObjectSpillable(const ObjectID &object_id) {
  // The lock is acquired when a request is received to the plasma store.
  // recursive mutex is used here to allow
  std::lock_guard<std::recursive_mutex> guard(mutex_);
  auto entry = GetObjectTableEntry(&store_info_, object_id);
  return entry->ref_count == 1;
}

}  // namespace plasma
