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

#include "absl/container/flat_hash_set.h"
#include "ray/common/asio/asio_util.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/object_manager/plasma/common.h"
#include "ray/object_manager/plasma/malloc.h"
#include "ray/object_manager/plasma/plasma_allocator.h"
#include "ray/object_manager/plasma/protocol.h"
#include "ray/object_manager/plasma/get_request_queue.h"
#include "ray/util/util.h"

namespace fb = plasma::flatbuf;

namespace plasma {
namespace {

ray::ObjectID GetCreateRequestObjectId(const std::vector<uint8_t> &message) {
  uint8_t *input = (uint8_t *)message.data();
  size_t input_size = message.size();
  auto request = flatbuffers::GetRoot<fb::PlasmaCreateRequest>(input);
  RAY_DCHECK(plasma::VerifyFlatbuffer(request, input, input_size));
  return ray::ObjectID::FromBinary(request->object_id()->str());
}

void ToPlasmaObject(const LocalObject &entry, PlasmaObject *object, bool check_sealed) {
  RAY_DCHECK(object != nullptr);
  if (check_sealed) {
    RAY_DCHECK(entry.state == ObjectState::PLASMA_SEALED);
  }
  object->store_fd = entry.allocation.fd;
  object->data_offset = entry.allocation.offset;
  object->metadata_offset = entry.allocation.offset + entry.object_info.data_size;
  object->data_size = entry.object_info.data_size;
  object->metadata_size = entry.object_info.metadata_size;
  object->device_num = entry.allocation.device_num;
  object->mmap_size = entry.allocation.mmap_size;
}
}  // namespace

PlasmaStore::PlasmaStore(instrumented_io_context &main_service, IAllocator &allocator,
                         const std::string &socket_name, uint32_t delay_on_oom_ms,
                         float object_spilling_threshold,
                         ray::SpillObjectsCallback spill_objects_callback,
                         std::function<void()> object_store_full_callback,
                         ray::AddObjectCallback add_object_callback,
                         ray::DeleteObjectCallback delete_object_callback)
    : io_context_(main_service),
      socket_name_(socket_name),
      acceptor_(main_service, ParseUrlEndpoint(socket_name)),
      socket_(main_service),
      allocator_(allocator),
      object_store_(allocator_),
      eviction_policy_(object_store_, allocator_),
      spill_objects_callback_(spill_objects_callback),
      add_object_callback_(add_object_callback),
      delete_object_callback_(delete_object_callback),
      delay_on_oom_ms_(delay_on_oom_ms),
      object_spilling_threshold_(object_spilling_threshold),
      usage_log_interval_ns_(RayConfig::instance().object_store_usage_log_interval_s() *
                             1e9),
      create_request_queue_(
          /*oom_grace_period_s=*/RayConfig::instance().oom_grace_period_s(),
          spill_objects_callback, object_store_full_callback,
          /*get_time=*/
          []() { return absl::GetCurrentTimeNanos(); },
          [this]() { return GetDebugDump(); }) {
  const auto event_stats_print_interval_ms =
      RayConfig::instance().event_stats_print_interval_ms();
  if (event_stats_print_interval_ms > 0 && RayConfig::instance().event_stats()) {
    PrintDebugDump();
  }
}

// TODO(pcm): Get rid of this destructor by using RAII to clean up data.
PlasmaStore::~PlasmaStore() {}

void PlasmaStore::Start() {
  // Start listening for clients.
  DoAccept();
}

void PlasmaStore::Stop() { acceptor_.close(); }

// If this client is not already using the object, add the client to the
// object's list of clients, otherwise do nothing.
// TODO: MissionToMars Can be move to GetRequestQueue
void PlasmaStore::AddToClientObjectIds(const ObjectID &object_id,
                                       const LocalObject *entry,
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
    num_bytes_in_use_ += entry->GetObjectSize();
  }
  // Increase reference count.
  entry->ref_count++;
  RAY_LOG(DEBUG) << "Object " << object_id << " in use by client"
                 << ", num bytes in use is now " << num_bytes_in_use_;

  // Add object id to the list of object ids that this client is using.
  client->object_ids.insert(object_id);
}

const LocalObject *PlasmaStore::CreateObjectInternal(const ray::ObjectInfo &object_info,
                                                     plasma::flatbuf::ObjectSource source,
                                                     bool allow_fallback_allocation) {
  // Try to evict objects until there is enough space.
  // NOTE(ekl) if we can't achieve this after a number of retries, it's
  // because memory fragmentation in dlmalloc prevents us from allocating
  // even if our footprint tracker here still says we have free space.
  for (int num_tries = 0; num_tries <= 10; num_tries++) {
    auto result =
        object_store_.CreateObject(object_info, source, /*fallback_allocate*/ false);
    if (result != nullptr) {
      return result;
    }
    // Tell the eviction policy how much space we need to create this object.
    std::vector<ObjectID> objects_to_evict;
    int64_t space_needed =
        eviction_policy_.RequireSpace(object_info.GetObjectSize(), &objects_to_evict);
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
      object_store_.CreateObject(object_info, source, /*fallback_allocate*/ true);

  if (result == nullptr) {
    RAY_LOG(ERROR) << "Plasma fallback allocator failed, likely out of disk space.";
  }
  return result;
}

PlasmaError PlasmaStore::HandleCreateObjectRequest(const std::shared_ptr<Client> &client,
                                                   const std::vector<uint8_t> &message,
                                                   bool fallback_allocator,
                                                   PlasmaObject *object,
                                                   bool *spilling_required) {
  uint8_t *input = (uint8_t *)message.data();
  size_t input_size = message.size();
  ray::ObjectInfo object_info;
  fb::ObjectSource source;
  int device_num;
  ReadCreateRequest(input, input_size, &object_info, &source, &device_num);

  if (device_num != 0) {
    RAY_LOG(ERROR) << "device_num != 0 but CUDA not enabled";
    return PlasmaError::OutOfMemory;
  }

  auto error = CreateObject(object_info, source, client, fallback_allocator, object);
  if (error == PlasmaError::OutOfMemory) {
    RAY_LOG(DEBUG) << "Not enough memory to create the object " << object_info.object_id
                   << ", data_size=" << object_info.data_size
                   << ", metadata_size=" << object_info.metadata_size;
  }

  // Trigger object spilling if current usage is above the specified threshold.
  if (spilling_required != nullptr) {
    const int64_t footprint_limit = allocator_.GetFootprintLimit();
    if (footprint_limit != 0) {
      const float allocated_percentage =
          static_cast<float>(allocator_.Allocated()) / footprint_limit;
      if (allocated_percentage > object_spilling_threshold_) {
        RAY_LOG(DEBUG) << "Triggering object spilling because current usage "
                       << allocated_percentage << "% is above threshold "
                       << object_spilling_threshold_ << "%.";
        *spilling_required = true;
      }
    }
  }
  return error;
}

PlasmaError PlasmaStore::CreateObject(const ray::ObjectInfo &object_info,
                                      fb::ObjectSource source,
                                      const std::shared_ptr<Client> &client,
                                      bool fallback_allocator, PlasmaObject *result) {
  RAY_LOG(DEBUG) << "attempting to create object " << object_info.object_id << " size "
                 << object_info.data_size;

  if (object_store_.GetObject(object_info.object_id) != nullptr) {
    return PlasmaError::ObjectExists;
  }
  auto entry = CreateObjectInternal(object_info, source, fallback_allocator);
  {
    // TODO(scv119) use RAY_LOG_EVERY_MS
    auto now = absl::GetCurrentTimeNanos();
    if (now - last_usage_log_ns_ > usage_log_interval_ns_) {
      RAY_LOG(INFO) << "Object store current usage " << (allocator_.Allocated() / 1e9)
                    << " / " << (allocator_.GetFootprintLimit() / 1e9) << " GB.";
      last_usage_log_ns_ = now;
    }
  }
  if (entry == nullptr) {
    return PlasmaError::OutOfMemory;
  }

  RAY_LOG(DEBUG) << "create object " << object_info.object_id << " succeeded";
  ToPlasmaObject(*entry, result, /* check sealed */ false);

  // Notify the eviction policy that this object was created. This must be done
  // immediately before the call to AddToClientObjectIds so that the
  // eviction policy does not have an opportunity to evict the object.
  eviction_policy_.ObjectCreated(object_info.object_id, true);
  // Record that this client is using this object.
  AddToClientObjectIds(object_info.object_id, entry, client);
  return PlasmaError::OK;
}

void PlasmaStore::RemoveGetRequest(const std::shared_ptr<GetRequest> &get_request) {
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
  get_request->MarkRemoved();
}

void PlasmaStore::RemoveGetRequestsForClient(const std::shared_ptr<Client> &client) {
  std::unordered_set<std::shared_ptr<GetRequest>> get_requests_to_remove;
  for (auto const &pair : object_get_requests_) {
    for (const auto &get_request : pair.second) {
      if (get_request->client == client) {
        get_requests_to_remove.insert(get_request);
      }
    }
  }

  // It shouldn't be possible for a given client to be in the middle of multiple get
  // requests.
  RAY_CHECK(get_requests_to_remove.size() <= 1);
  for (const auto &get_request : get_requests_to_remove) {
    RemoveGetRequest(get_request);
  }
}

void PlasmaStore::ReturnFromGet(const std::shared_ptr<GetRequest> &get_req) {
  // If the get request is already removed, do no-op. This can happen because the boost
  // timer is not atomic. See https://github.com/ray-project/ray/pull/15071.
  if (get_req->IsRemoved()) {
    return;
  }

  // Figure out how many file descriptors we need to send.
  absl::flat_hash_set<MEMFD_TYPE> fds_to_send;
  std::vector<MEMFD_TYPE> store_fds;
  std::vector<int64_t> mmap_sizes;
  for (const auto &object_id : get_req->object_ids) {
    PlasmaObject &object = get_req->objects[object_id];
    MEMFD_TYPE fd = object.store_fd;
    if (object.data_size != -1 && fds_to_send.count(fd) == 0 && fd.first != INVALID_FD) {
      fds_to_send.insert(fd);
      store_fds.push_back(fd);
      mmap_sizes.push_back(object.mmap_size);
      if (get_req->is_from_worker) {
        total_consumed_bytes_ += object.data_size + object.metadata_size;
      }
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

/// TODO: MissionToMars Can be move to get_request_queue.h
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
    auto entry = object_store_.GetObject(object_id);
    RAY_CHECK(entry != nullptr);
    ToPlasmaObject(*entry, &get_req->objects[object_id], /* check sealed */ true);
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
                                    int64_t timeout_ms, bool is_from_worker) {
  // Create a get request for this object.
  auto get_req = std::make_shared<GetRequest>(
      GetRequest(io_context_, client, object_ids, is_from_worker));
  for (auto object_id : object_ids) {
    // Check if this object is already present
    // locally. If so, record that the object is being used and mark it as accounted for.
    auto entry = object_store_.GetObject(object_id);
    if (entry && entry->state == ObjectState::PLASMA_SEALED) {
      // Update the get request to take into account the present object.
      ToPlasmaObject(*entry, &get_req->objects[object_id], /* checksealed */ true);
      get_req->num_satisfied += 1;
      // If necessary, record that this client is using this object. In the case
      // where entry == NULL, this will be called from SealObject.
      AddToClientObjectIds(object_id, entry, client);
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
                                           const LocalObject *entry,
                                           const std::shared_ptr<Client> &client) {
  auto it = client->object_ids.find(object_id);
  if (it != client->object_ids.end()) {
    client->object_ids.erase(it);
    // Decrease reference count.
    entry->ref_count--;
    RAY_LOG(DEBUG) << "Object " << object_id << " no longer in use by client";

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
  auto object = object_store_.GetObject(object_id);
  if (object == nullptr) {
    RAY_LOG(WARNING) << object_id << " has already been deleted.";
    return;
  }
  RAY_CHECK(object->allocation.device_num == 0)
      << object_id << "'s device_num is " << object->allocation.device_num
      << "but CUDA not enabled";
  RAY_LOG(DEBUG) << "Erasing object: " << object_id
                 << ", address: " << static_cast<void *>(object->allocation.address)
                 << ", size:" << object->GetObjectSize();
  if (object->ref_count > 0) {
    // A client was using this object.
    num_bytes_in_use_ -= object->GetObjectSize();
    RAY_LOG(DEBUG) << "Erasing object " << object_id << " with nonzero ref count"
                   << object_id << ", num bytes in use is now " << num_bytes_in_use_;
  }
  object_store_.DeleteObject(object_id);
}

void PlasmaStore::ReleaseObject(const ObjectID &object_id,
                                const std::shared_ptr<Client> &client) {
  auto entry = object_store_.GetObject(object_id);
  RAY_CHECK(entry != nullptr);
  // Remove the client from the object's array of clients.
  RAY_CHECK(RemoveFromClientObjectIds(object_id, entry, client) == 1);
}

// Check if an object is present.
ObjectStatus PlasmaStore::ContainsObject(const ObjectID &object_id) {
  auto entry = object_store_.GetObject(object_id);
  return entry && entry->state == ObjectState::PLASMA_SEALED
             ? ObjectStatus::OBJECT_FOUND
             : ObjectStatus::OBJECT_NOT_FOUND;
}

void PlasmaStore::SealObjects(const std::vector<ObjectID> &object_ids) {
  for (size_t i = 0; i < object_ids.size(); ++i) {
    RAY_LOG(DEBUG) << "sealing object " << object_ids[i];
    auto entry = object_store_.SealObject(object_ids[i]);
    RAY_CHECK(entry) << object_ids[i] << " doesn't exist or has already been sealed.";
    add_object_callback_(entry->object_info);
  }

  for (size_t i = 0; i < object_ids.size(); ++i) {
    UpdateObjectGetRequests(object_ids[i]);
  }
}

int PlasmaStore::AbortObject(const ObjectID &object_id,
                             const std::shared_ptr<Client> &client) {
  auto entry = object_store_.GetObject(object_id);
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
  EraseFromObjectTable(object_id);
  // Inform all subscribers that the object has been deleted.
  delete_object_callback_(object_id);
  return PlasmaError::OK;
}

void PlasmaStore::EvictObjects(const std::vector<ObjectID> &object_ids) {
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
    EraseFromObjectTable(object_id);
    // Inform all subscribers that the object has been deleted.
    delete_object_callback_(object_id);
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
  std::unordered_map<ObjectID, const LocalObject *> sealed_objects;
  for (const auto &object_id : client->object_ids) {
    auto entry = object_store_.GetObject(object_id);
    if (entry == nullptr) {
      continue;
    }

    if (entry->state == ObjectState::PLASMA_SEALED) {
      // Add sealed objects to a temporary list of object IDs. Do not perform
      // the remove here, since it potentially modifies the object_ids table.
      sealed_objects[object_id] = entry;
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

  create_request_queue_.RemoveDisconnectedClientRequests(client);
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
    const size_t object_size = request->data_size() + request->metadata_size();

    auto handle_create = [this, client, message](bool fallback_allocator,
                                                 PlasmaObject *result,
                                                 bool *spilling_required) {
      return HandleCreateObjectRequest(client, message, fallback_allocator, result,
                                       spilling_required);
    };

    if (request->try_immediately()) {
      RAY_LOG(DEBUG) << "Received request to create object " << object_id
                     << " immediately";
      auto result_error = create_request_queue_.TryRequestImmediately(
          object_id, client, handle_create, object_size);
      const auto &result = result_error.first;
      const auto &error = result_error.second;
      if (SendCreateReply(client, object_id, result, error).ok() &&
          error == PlasmaError::OK && result.device_num == 0) {
        static_cast<void>(client->SendFd(result.store_fd));
      }
    } else {
      auto req_id =
          create_request_queue_.AddRequest(object_id, client, handle_create, object_size);
      RAY_LOG(DEBUG) << "Received create request for object " << object_id
                     << " assigned request ID " << req_id << ", " << object_size
                     << " bytes";
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
    bool is_from_worker;
    RAY_RETURN_NOT_OK(ReadGetRequest(input, input_size, object_ids_to_get, &timeout_ms,
                                     &is_from_worker));
    ProcessGetRequest(client, object_ids_to_get, timeout_ms, is_from_worker);
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
  case fb::MessageType::PlasmaConnectRequest: {
    RAY_RETURN_NOT_OK(SendConnectReply(client, allocator_.GetFootprintLimit()));
  } break;
  case fb::MessageType::PlasmaDisconnectClient:
    RAY_LOG(DEBUG) << "Disconnecting client on fd " << client;
    DisconnectClient(client);
    return Status::Disconnected("The Plasma Store client is disconnected.");
    break;
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

    if (!dumped_on_oom_) {
      RAY_LOG(INFO) << "Plasma store at capacity\n" << GetDebugDump();
      dumped_on_oom_ = true;
    }
  } else {
    dumped_on_oom_ = false;
  }

  if (retry_after_ms > 0) {
    // Try to process requests later, after space has been made.
    create_timer_ = execute_after(io_context_,
                                  [this]() {
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

int64_t PlasmaStore::GetConsumedBytes() {
  std::lock_guard<std::recursive_mutex> guard(mutex_);
  return total_consumed_bytes_;
}

bool PlasmaStore::IsObjectSpillable(const ObjectID &object_id) {
  // The lock is acquired when a request is received to the plasma store.
  // recursive mutex is used here to allow
  std::lock_guard<std::recursive_mutex> guard(mutex_);
  auto entry = object_store_.GetObject(object_id);
  if (!entry) {
    // Object already evicted or deleted.
    return false;
  }
  return entry->ref_count == 1;
}

void PlasmaStore::PrintDebugDump() const {
  RAY_LOG(INFO) << GetDebugDump();

  stats_timer_ = execute_after(io_context_, [this]() { PrintDebugDump(); },
                               RayConfig::instance().event_stats_print_interval_ms());
}

std::string PlasmaStore::GetDebugDump() const {
  // TODO(swang): We might want to optimize this if it gets called more often.
  std::stringstream buffer;
  buffer << "========== Plasma store: =================\n";
  buffer << "Current usage: " << (allocator_.Allocated() / 1e9) << " / "
         << (allocator_.GetFootprintLimit() / 1e9) << " GB\n";
  buffer << "- num bytes created total: " << object_store_.GetNumBytesCreatedTotal()
         << "\n";
  auto num_pending_requests = create_request_queue_.NumPendingRequests();
  auto num_pending_bytes = create_request_queue_.NumPendingBytes();
  buffer << num_pending_requests << " pending objects of total size "
         << num_pending_bytes / 1024 / 1024 << "MB\n";
  object_store_.GetDebugDump(buffer);
  return buffer.str();
}

}  // namespace plasma
