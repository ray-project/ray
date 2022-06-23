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

// PLASMA CLIENT: Client library for using the plasma store and manager

#include "ray/object_manager/plasma/client.h"

#include <algorithm>
#include <boost/asio.hpp>
#include <cstring>
#include <deque>
#include <mutex>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/ray_config.h"
#include "ray/object_manager/plasma/connection.h"
#include "ray/object_manager/plasma/plasma.h"
#include "ray/object_manager/plasma/protocol.h"
#include "ray/object_manager/plasma/shared_memory.h"

namespace fb = plasma::flatbuf;

namespace plasma {

using fb::MessageType;
using fb::PlasmaError;

// ----------------------------------------------------------------------
// PlasmaBuffer

/// A Buffer class that automatically releases the backing plasma object
/// when it goes out of scope. This is returned by Get.
class PlasmaBuffer : public SharedMemoryBuffer {
 public:
  ~PlasmaBuffer();

  PlasmaBuffer(std::shared_ptr<PlasmaClient::Impl> client,
               const ObjectID &object_id,
               const std::shared_ptr<Buffer> &buffer)
      : SharedMemoryBuffer(buffer, 0, buffer->Size()),
        client_(client),
        object_id_(object_id) {}

 private:
  std::shared_ptr<PlasmaClient::Impl> client_;
  ObjectID object_id_;
};

/// A mutable Buffer class that keeps the backing data alive by keeping a
/// PlasmaClient shared pointer. This is returned by Create. Release will
/// be called in the associated Seal call.
class RAY_NO_EXPORT PlasmaMutableBuffer : public SharedMemoryBuffer {
 public:
  PlasmaMutableBuffer(std::shared_ptr<PlasmaClient::Impl> client,
                      uint8_t *mutable_data,
                      int64_t data_size)
      : SharedMemoryBuffer(mutable_data, data_size), client_(client) {}

 private:
  std::shared_ptr<PlasmaClient::Impl> client_;
};

// ----------------------------------------------------------------------
// PlasmaClient::Impl

struct ObjectInUseEntry {
  /// A count of the number of times this client has called PlasmaClient::Create
  /// or
  /// PlasmaClient::Get on this object ID minus the number of calls to
  /// PlasmaClient::Release.
  /// When this count reaches zero, we remove the entry from the ObjectsInUse
  /// and decrement a count in the relevant ClientMmapTableEntry.
  int count;
  /// Cached information to read the object.
  PlasmaObject object;
  /// A flag representing whether the object has been sealed.
  bool is_sealed;
};

class PlasmaClient::Impl : public std::enable_shared_from_this<PlasmaClient::Impl> {
 public:
  Impl();
  ~Impl();

  // PlasmaClient method implementations

  Status Connect(const std::string &store_socket_name,
                 const std::string &manager_socket_name,
                 int release_delay = 0,
                 int num_retries = -1);

  Status SetClientOptions(const std::string &client_name, int64_t output_memory_quota);

  Status CreateAndSpillIfNeeded(const ObjectID &object_id,
                                const ray::rpc::Address &owner_address,
                                int64_t data_size,
                                const uint8_t *metadata,
                                int64_t metadata_size,
                                std::shared_ptr<Buffer> *data,
                                fb::ObjectSource source,
                                int device_num = 0);

  Status RetryCreate(const ObjectID &object_id,
                     uint64_t request_id,
                     const uint8_t *metadata,
                     uint64_t *retry_with_request_id,
                     std::shared_ptr<Buffer> *data);

  Status TryCreateImmediately(const ObjectID &object_id,
                              const ray::rpc::Address &owner_address,
                              int64_t data_size,
                              const uint8_t *metadata,
                              int64_t metadata_size,
                              std::shared_ptr<Buffer> *data,
                              fb::ObjectSource source,
                              int device_num);

  Status Get(const std::vector<ObjectID> &object_ids,
             int64_t timeout_ms,
             std::vector<ObjectBuffer> *object_buffers,
             bool is_from_worker);

  Status Get(const ObjectID *object_ids,
             int64_t num_objects,
             int64_t timeout_ms,
             ObjectBuffer *object_buffers,
             bool is_from_worker);

  Status Release(const ObjectID &object_id);

  Status Contains(const ObjectID &object_id, bool *has_object);

  Status Abort(const ObjectID &object_id);

  Status Seal(const ObjectID &object_id);

  Status Delete(const std::vector<ObjectID> &object_ids);

  Status Evict(int64_t num_bytes, int64_t &num_bytes_evicted);

  Status Disconnect();

  std::string DebugString();

  bool IsInUse(const ObjectID &object_id);

  int64_t store_capacity() { return store_capacity_; }

 private:
  /// Helper method to read and process the reply of a create request.
  Status HandleCreateReply(const ObjectID &object_id,
                           const uint8_t *metadata,
                           uint64_t *retry_with_request_id,
                           std::shared_ptr<Buffer> *data);

  /// Check if store_fd has already been received from the store. If yes,
  /// return it. Otherwise, receive it from the store (see analogous logic
  /// in store.cc).
  ///
  /// \param store_fd File descriptor to fetch from the store.
  /// \return The pointer corresponding to store_fd.
  uint8_t *GetStoreFdAndMmap(MEMFD_TYPE store_fd, int64_t map_size);

  /// This is a helper method for marking an object as unused by this client.
  ///
  /// \param object_id The object ID we mark unused.
  /// \return The return status.
  Status MarkObjectUnused(const ObjectID &object_id);

  /// Common helper for Get() variants
  Status GetBuffers(const ObjectID *object_ids,
                    int64_t num_objects,
                    int64_t timeout_ms,
                    const std::function<std::shared_ptr<Buffer>(
                        const ObjectID &, const std::shared_ptr<Buffer> &)> &wrap_buffer,
                    ObjectBuffer *object_buffers,
                    bool is_from_worker);

  uint8_t *LookupMmappedFile(MEMFD_TYPE store_fd_val);

  void IncrementObjectCount(const ObjectID &object_id,
                            PlasmaObject *object,
                            bool is_sealed);

  /// The boost::asio IO context for the client.
  instrumented_io_context main_service_;
  /// The connection to the store service.
  std::shared_ptr<StoreConn> store_conn_;
  /// Table of dlmalloc buffer files that have been memory mapped so far. This
  /// is a hash table mapping a file descriptor to a struct containing the
  /// address of the corresponding memory-mapped file.
  absl::flat_hash_map<MEMFD_TYPE, std::unique_ptr<ClientMmapTableEntry>> mmap_table_;
  /// Used to clean up old fd entries in mmap_table_ that are no longer needed,
  /// since their fd has been reused. TODO(ekl) we should be more proactive about
  /// unmapping unused segments.
  absl::flat_hash_map<MEMFD_TYPE_NON_UNIQUE, MEMFD_TYPE> dedup_fd_table_;
  /// A hash table of the object IDs that are currently being used by this
  /// client.
  absl::flat_hash_map<ObjectID, std::unique_ptr<ObjectInUseEntry>> objects_in_use_;
  /// The amount of memory available to the Plasma store. The client needs this
  /// information to make sure that it does not delay in releasing so much
  /// memory that the store is unable to evict enough objects to free up space.
  int64_t store_capacity_;
  /// A hash set to record the ids that users want to delete but still in use.
  std::unordered_set<ObjectID> deletion_cache_;
  /// A mutex which protects this class.
  std::recursive_mutex client_mutex_;
};

PlasmaBuffer::~PlasmaBuffer() { RAY_UNUSED(client_->Release(object_id_)); }

PlasmaClient::Impl::Impl() : store_capacity_(0) {}

PlasmaClient::Impl::~Impl() {}

// If the file descriptor fd has been mmapped in this client process before,
// return the pointer that was returned by mmap, otherwise mmap it and store the
// pointer in a hash table.
uint8_t *PlasmaClient::Impl::GetStoreFdAndMmap(MEMFD_TYPE store_fd_val,
                                               int64_t map_size) {
  auto entry = mmap_table_.find(store_fd_val);
  if (entry != mmap_table_.end()) {
    return entry->second->pointer();
  } else {
    MEMFD_TYPE fd;
    RAY_CHECK_OK(store_conn_->RecvFd(&fd.first));
    fd.second = store_fd_val.second;
    // Close and erase the old duplicated fd entry that is no longer needed.
    if (dedup_fd_table_.find(store_fd_val.first) != dedup_fd_table_.end()) {
      RAY_LOG(INFO) << "Erasing re-used mmap entry for fd " << store_fd_val.first;
      mmap_table_.erase(dedup_fd_table_[store_fd_val.first]);
    }
    dedup_fd_table_[store_fd_val.first] = store_fd_val;
    mmap_table_[store_fd_val] = std::make_unique<ClientMmapTableEntry>(fd, map_size);
    return mmap_table_[store_fd_val]->pointer();
  }
}

// Get a pointer to a file that we know has been memory mapped in this client
// process before.
uint8_t *PlasmaClient::Impl::LookupMmappedFile(MEMFD_TYPE store_fd_val) {
  auto entry = mmap_table_.find(store_fd_val);
  RAY_CHECK(entry != mmap_table_.end());
  return entry->second->pointer();
}

bool PlasmaClient::Impl::IsInUse(const ObjectID &object_id) {
  std::lock_guard<std::recursive_mutex> guard(client_mutex_);

  const auto elem = objects_in_use_.find(object_id);
  return (elem != objects_in_use_.end());
}

void PlasmaClient::Impl::IncrementObjectCount(const ObjectID &object_id,
                                              PlasmaObject *object,
                                              bool is_sealed) {
  // Increment the count of the object to track the fact that it is being used.
  // The corresponding decrement should happen in PlasmaClient::Release.
  auto elem = objects_in_use_.find(object_id);
  ObjectInUseEntry *object_entry;
  if (elem == objects_in_use_.end()) {
    // Add this object ID to the hash table of object IDs in use. The
    // corresponding call to free happens in PlasmaClient::Release.
    objects_in_use_[object_id] = std::make_unique<ObjectInUseEntry>();
    objects_in_use_[object_id]->object = *object;
    objects_in_use_[object_id]->count = 0;
    objects_in_use_[object_id]->is_sealed = is_sealed;
    object_entry = objects_in_use_[object_id].get();
  } else {
    object_entry = elem->second.get();
    RAY_CHECK(object_entry->count > 0);
  }
  // Increment the count of the number of instances of this object that are
  // being used by this client. The corresponding decrement should happen in
  // PlasmaClient::Release.
  object_entry->count += 1;
}

Status PlasmaClient::Impl::HandleCreateReply(const ObjectID &object_id,
                                             const uint8_t *metadata,
                                             uint64_t *retry_with_request_id,
                                             std::shared_ptr<Buffer> *data) {
  std::vector<uint8_t> buffer;
  RAY_RETURN_NOT_OK(PlasmaReceive(store_conn_, MessageType::PlasmaCreateReply, &buffer));
  ObjectID id;
  PlasmaObject object;
  MEMFD_TYPE store_fd;
  int64_t mmap_size;

  if (retry_with_request_id) {
    RAY_RETURN_NOT_OK(ReadCreateReply(buffer.data(),
                                      buffer.size(),
                                      &id,
                                      retry_with_request_id,
                                      &object,
                                      &store_fd,
                                      &mmap_size));
    if (*retry_with_request_id > 0) {
      // The client should retry the request.
      return Status::OK();
    }
  } else {
    uint64_t unused = 0;
    RAY_RETURN_NOT_OK(ReadCreateReply(
        buffer.data(), buffer.size(), &id, &unused, &object, &store_fd, &mmap_size));
    RAY_CHECK(unused == 0);
  }

  // If the CreateReply included an error, then the store will not send a file
  // descriptor.
  if (object.device_num == 0) {
    // The metadata should come right after the data.
    RAY_CHECK(object.metadata_offset == object.data_offset + object.data_size);
    *data = std::make_shared<PlasmaMutableBuffer>(
        shared_from_this(),
        GetStoreFdAndMmap(store_fd, mmap_size) + object.data_offset,
        object.data_size);
    // If plasma_create is being called from a transfer, then we will not copy the
    // metadata here. The metadata will be written along with the data streamed
    // from the transfer.
    if (metadata != NULL) {
      // Copy the metadata to the buffer.
      memcpy((*data)->Data() + object.data_size, metadata, object.metadata_size);
    }
  } else {
    RAY_LOG(FATAL) << "GPU is not enabled.";
  }

  // Increment the count of the number of instances of this object that this
  // client is using. A call to PlasmaClient::Release is required to decrement
  // this count. Cache the reference to the object.
  IncrementObjectCount(object_id, &object, false);
  // We increment the count a second time (and the corresponding decrement will
  // happen in a PlasmaClient::Release call in plasma_seal) so even if the
  // buffer returned by PlasmaClient::Create goes out of scope, the object does
  // not get released before the call to PlasmaClient::Seal happens.
  IncrementObjectCount(object_id, &object, false);
  return Status::OK();
}

Status PlasmaClient::Impl::CreateAndSpillIfNeeded(const ObjectID &object_id,
                                                  const ray::rpc::Address &owner_address,
                                                  int64_t data_size,
                                                  const uint8_t *metadata,
                                                  int64_t metadata_size,
                                                  std::shared_ptr<Buffer> *data,
                                                  fb::ObjectSource source,
                                                  int device_num) {
  std::unique_lock<std::recursive_mutex> guard(client_mutex_);
  uint64_t retry_with_request_id = 0;

  RAY_LOG(DEBUG) << "called plasma_create on conn " << store_conn_ << " with size "
                 << data_size << " and metadata size " << metadata_size;
  RAY_RETURN_NOT_OK(SendCreateRequest(store_conn_,
                                      object_id,
                                      owner_address,
                                      data_size,
                                      metadata_size,
                                      source,
                                      device_num,
                                      /*try_immediately=*/false));
  Status status = HandleCreateReply(object_id, metadata, &retry_with_request_id, data);

  while (retry_with_request_id > 0) {
    guard.unlock();
    // TODO(sang): Consider using exponential backoff here.
    std::this_thread::sleep_for(
        std::chrono::milliseconds(RayConfig::instance().object_store_full_delay_ms()));
    guard.lock();
    RAY_LOG(DEBUG) << "Retrying request for object " << object_id << " with request ID "
                   << retry_with_request_id;
    status = RetryCreate(
        object_id, retry_with_request_id, metadata, &retry_with_request_id, data);
  }

  return status;
}

Status PlasmaClient::Impl::RetryCreate(const ObjectID &object_id,
                                       uint64_t request_id,
                                       const uint8_t *metadata,
                                       uint64_t *retry_with_request_id,
                                       std::shared_ptr<Buffer> *data) {
  std::lock_guard<std::recursive_mutex> guard(client_mutex_);
  RAY_RETURN_NOT_OK(SendCreateRetryRequest(store_conn_, object_id, request_id));
  return HandleCreateReply(object_id, metadata, retry_with_request_id, data);
}

Status PlasmaClient::Impl::TryCreateImmediately(const ObjectID &object_id,
                                                const ray::rpc::Address &owner_address,
                                                int64_t data_size,
                                                const uint8_t *metadata,
                                                int64_t metadata_size,
                                                std::shared_ptr<Buffer> *data,
                                                fb::ObjectSource source,
                                                int device_num) {
  std::lock_guard<std::recursive_mutex> guard(client_mutex_);

  RAY_LOG(DEBUG) << "called plasma_create on conn " << store_conn_ << " with size "
                 << data_size << " and metadata size " << metadata_size;
  RAY_RETURN_NOT_OK(SendCreateRequest(store_conn_,
                                      object_id,
                                      owner_address,
                                      data_size,
                                      metadata_size,
                                      source,
                                      device_num,
                                      /*try_immediately=*/true));
  return HandleCreateReply(object_id, metadata, nullptr, data);
}

Status PlasmaClient::Impl::GetBuffers(
    const ObjectID *object_ids,
    int64_t num_objects,
    int64_t timeout_ms,
    const std::function<std::shared_ptr<Buffer>(
        const ObjectID &, const std::shared_ptr<Buffer> &)> &wrap_buffer,
    ObjectBuffer *object_buffers,
    bool is_from_worker) {
  // Fill out the info for the objects that are already in use locally.
  bool all_present = true;
  for (int64_t i = 0; i < num_objects; ++i) {
    auto object_entry = objects_in_use_.find(object_ids[i]);
    if (object_entry == objects_in_use_.end()) {
      // This object is not currently in use by this client, so we need to send
      // a request to the store.
      all_present = false;
    } else if (!object_entry->second->is_sealed) {
      // This client created the object but hasn't sealed it. If we call Get
      // with no timeout, we will deadlock, because this client won't be able to
      // call Seal.
      RAY_CHECK(timeout_ms != -1)
          << "Plasma client called get on an unsealed object that it created";
      RAY_LOG(WARNING)
          << "Attempting to get an object that this client created but hasn't sealed.";
      all_present = false;
    } else {
      PlasmaObject *object = &object_entry->second->object;
      std::shared_ptr<Buffer> physical_buf;

      if (object->device_num == 0) {
        uint8_t *data = LookupMmappedFile(object->store_fd);
        physical_buf = std::make_shared<SharedMemoryBuffer>(
            data + object->data_offset, object->data_size + object->metadata_size);
      } else {
        RAY_LOG(FATAL) << "GPU library is not enabled.";
      }
      physical_buf = wrap_buffer(object_ids[i], physical_buf);
      object_buffers[i].data =
          SharedMemoryBuffer::Slice(physical_buf, 0, object->data_size);
      object_buffers[i].metadata = SharedMemoryBuffer::Slice(
          physical_buf, object->data_size, object->metadata_size);
      object_buffers[i].device_num = object->device_num;
      // Increment the count of the number of instances of this object that this
      // client is using. Cache the reference to the object.
      IncrementObjectCount(object_ids[i], object, true);
    }
  }

  if (all_present) {
    return Status::OK();
  }

  // If we get here, then the objects aren't all currently in use by this
  // client, so we need to send a request to the plasma store.
  RAY_RETURN_NOT_OK(SendGetRequest(
      store_conn_, &object_ids[0], num_objects, timeout_ms, is_from_worker));
  std::vector<uint8_t> buffer;
  RAY_RETURN_NOT_OK(PlasmaReceive(store_conn_, MessageType::PlasmaGetReply, &buffer));
  std::vector<ObjectID> received_object_ids(num_objects);
  std::vector<PlasmaObject> object_data(num_objects);
  PlasmaObject *object;
  std::vector<MEMFD_TYPE> store_fds;
  std::vector<int64_t> mmap_sizes;
  RAY_RETURN_NOT_OK(ReadGetReply(buffer.data(),
                                 buffer.size(),
                                 received_object_ids.data(),
                                 object_data.data(),
                                 num_objects,
                                 store_fds,
                                 mmap_sizes));

  // We mmap all of the file descriptors here so that we can avoid look them up
  // in the subsequent loop based on just the store file descriptor and without
  // having to know the relevant file descriptor received from recv_fd.
  for (size_t i = 0; i < store_fds.size(); i++) {
    GetStoreFdAndMmap(store_fds[i], mmap_sizes[i]);
  }

  for (int64_t i = 0; i < num_objects; ++i) {
    RAY_DCHECK(received_object_ids[i] == object_ids[i]);
    object = &object_data[i];
    if (object_buffers[i].data) {
      // If the object was already in use by the client, then the store should
      // have returned it.
      RAY_DCHECK(object->data_size != -1);
      // We've already filled out the information for this object, so we can
      // just continue.
      continue;
    }
    // If we are here, the object was not currently in use, so we need to
    // process the reply from the object store.
    if (object->data_size != -1) {
      std::shared_ptr<Buffer> physical_buf;
      if (object->device_num == 0) {
        uint8_t *data = LookupMmappedFile(object->store_fd);
        physical_buf = std::make_shared<SharedMemoryBuffer>(
            data + object->data_offset, object->data_size + object->metadata_size);
      } else {
        RAY_LOG(FATAL) << "Arrow GPU library is not enabled.";
      }
      // Finish filling out the return values.
      physical_buf = wrap_buffer(object_ids[i], physical_buf);
      object_buffers[i].data =
          SharedMemoryBuffer::Slice(physical_buf, 0, object->data_size);
      object_buffers[i].metadata = SharedMemoryBuffer::Slice(
          physical_buf, object->data_size, object->metadata_size);
      object_buffers[i].device_num = object->device_num;
      // Increment the count of the number of instances of this object that this
      // client is using. Cache the reference to the object.
      IncrementObjectCount(received_object_ids[i], object, true);
    } else {
      // The object was not retrieved.  The caller can detect this condition
      // by checking the boolean value of the metadata/data buffers.
      RAY_DCHECK(!object_buffers[i].metadata);
      RAY_DCHECK(!object_buffers[i].data);
    }
  }
  return Status::OK();
}

Status PlasmaClient::Impl::Get(const std::vector<ObjectID> &object_ids,
                               int64_t timeout_ms,
                               std::vector<ObjectBuffer> *out,
                               bool is_from_worker) {
  std::lock_guard<std::recursive_mutex> guard(client_mutex_);

  const auto wrap_buffer = [=](const ObjectID &object_id,
                               const std::shared_ptr<Buffer> &buffer) {
    return std::make_shared<PlasmaBuffer>(shared_from_this(), object_id, buffer);
  };
  const size_t num_objects = object_ids.size();
  *out = std::vector<ObjectBuffer>(num_objects);
  return GetBuffers(
      &object_ids[0], num_objects, timeout_ms, wrap_buffer, &(*out)[0], is_from_worker);
}

Status PlasmaClient::Impl::MarkObjectUnused(const ObjectID &object_id) {
  auto object_entry = objects_in_use_.find(object_id);
  RAY_CHECK(object_entry != objects_in_use_.end());
  RAY_CHECK(object_entry->second->count == 0);

  // Remove the entry from the hash table of objects currently in use.
  objects_in_use_.erase(object_id);
  return Status::OK();
}

Status PlasmaClient::Impl::Release(const ObjectID &object_id) {
  std::lock_guard<std::recursive_mutex> guard(client_mutex_);

  // If the client is already disconnected, ignore release requests.
  if (!store_conn_) {
    return Status::OK();
  }
  auto object_entry = objects_in_use_.find(object_id);
  RAY_CHECK(object_entry != objects_in_use_.end());

  object_entry->second->count -= 1;
  RAY_CHECK(object_entry->second->count >= 0);
  // Check if the client is no longer using this object.
  if (object_entry->second->count == 0) {
    // Tell the store that the client no longer needs the object.
    RAY_RETURN_NOT_OK(MarkObjectUnused(object_id));
    RAY_RETURN_NOT_OK(SendReleaseRequest(store_conn_, object_id));
    auto iter = deletion_cache_.find(object_id);
    if (iter != deletion_cache_.end()) {
      deletion_cache_.erase(object_id);
      RAY_RETURN_NOT_OK(Delete({object_id}));
    }
  }
  return Status::OK();
}

// This method is used to query whether the plasma store contains an object.
Status PlasmaClient::Impl::Contains(const ObjectID &object_id, bool *has_object) {
  std::lock_guard<std::recursive_mutex> guard(client_mutex_);

  // Check if we already have a reference to the object.
  if (objects_in_use_.count(object_id) > 0) {
    *has_object = 1;
  } else {
    // If we don't already have a reference to the object, check with the store
    // to see if we have the object.
    RAY_RETURN_NOT_OK(SendContainsRequest(store_conn_, object_id));
    std::vector<uint8_t> buffer;
    RAY_RETURN_NOT_OK(
        PlasmaReceive(store_conn_, MessageType::PlasmaContainsReply, &buffer));
    ObjectID object_id2;
    RAY_DCHECK(buffer.size() > 0);
    RAY_RETURN_NOT_OK(
        ReadContainsReply(buffer.data(), buffer.size(), &object_id2, has_object));
  }
  return Status::OK();
}

Status PlasmaClient::Impl::Seal(const ObjectID &object_id) {
  std::lock_guard<std::recursive_mutex> guard(client_mutex_);

  // Make sure this client has a reference to the object before sending the
  // request to Plasma.
  auto object_entry = objects_in_use_.find(object_id);

  if (object_entry == objects_in_use_.end()) {
    return Status::ObjectNotFound("Seal() called on an object without a reference to it");
  }
  if (object_entry->second->is_sealed) {
    return Status::ObjectAlreadySealed("Seal() called on an already sealed object");
  }

  object_entry->second->is_sealed = true;
  /// Send the seal request to Plasma.
  RAY_RETURN_NOT_OK(SendSealRequest(store_conn_, object_id));
  std::vector<uint8_t> buffer;
  RAY_RETURN_NOT_OK(PlasmaReceive(store_conn_, MessageType::PlasmaSealReply, &buffer));
  ObjectID sealed_id;
  RAY_RETURN_NOT_OK(ReadSealReply(buffer.data(), buffer.size(), &sealed_id));
  RAY_CHECK(sealed_id == object_id);
  // We call PlasmaClient::Release to decrement the number of instances of this
  // object
  // that are currently being used by this client. The corresponding increment
  // happened in plasma_create and was used to ensure that the object was not
  // released before the call to PlasmaClient::Seal.
  return Release(object_id);
}

Status PlasmaClient::Impl::Abort(const ObjectID &object_id) {
  std::lock_guard<std::recursive_mutex> guard(client_mutex_);
  auto object_entry = objects_in_use_.find(object_id);
  RAY_CHECK(object_entry != objects_in_use_.end())
      << "Plasma client called abort on an object without a reference to it";
  RAY_CHECK(!object_entry->second->is_sealed)
      << "Plasma client called abort on a sealed object";

  // Make sure that the Plasma client only has one reference to the object. If
  // it has more, then the client needs to release the buffer before calling
  // abort.
  if (object_entry->second->count > 1) {
    return Status::Invalid("Plasma client cannot have a reference to the buffer.");
  }

  // Send the abort request.
  RAY_RETURN_NOT_OK(SendAbortRequest(store_conn_, object_id));
  // Decrease the reference count to zero, then remove the object.
  object_entry->second->count--;
  RAY_RETURN_NOT_OK(MarkObjectUnused(object_id));

  std::vector<uint8_t> buffer;
  ObjectID id;
  RAY_RETURN_NOT_OK(PlasmaReceive(store_conn_, MessageType::PlasmaAbortReply, &buffer));
  return ReadAbortReply(buffer.data(), buffer.size(), &id);
}

Status PlasmaClient::Impl::Delete(const std::vector<ObjectID> &object_ids) {
  std::lock_guard<std::recursive_mutex> guard(client_mutex_);

  std::vector<ObjectID> not_in_use_ids;
  for (auto &object_id : object_ids) {
    // If the object is in used, skip it.
    if (objects_in_use_.count(object_id) == 0) {
      not_in_use_ids.push_back(object_id);
    } else {
      deletion_cache_.emplace(object_id);
    }
  }
  if (not_in_use_ids.size() > 0) {
    RAY_RETURN_NOT_OK(SendDeleteRequest(store_conn_, not_in_use_ids));
    std::vector<uint8_t> buffer;
    RAY_RETURN_NOT_OK(
        PlasmaReceive(store_conn_, MessageType::PlasmaDeleteReply, &buffer));
    RAY_DCHECK(buffer.size() > 0);
    std::vector<PlasmaError> error_codes;
    not_in_use_ids.clear();
    RAY_RETURN_NOT_OK(
        ReadDeleteReply(buffer.data(), buffer.size(), &not_in_use_ids, &error_codes));
  }
  return Status::OK();
}

Status PlasmaClient::Impl::Evict(int64_t num_bytes, int64_t &num_bytes_evicted) {
  std::lock_guard<std::recursive_mutex> guard(client_mutex_);

  // Send a request to the store to evict objects.
  RAY_RETURN_NOT_OK(SendEvictRequest(store_conn_, num_bytes));
  // Wait for a response with the number of bytes actually evicted.
  std::vector<uint8_t> buffer;
  RAY_RETURN_NOT_OK(PlasmaReceive(store_conn_, MessageType::PlasmaEvictReply, &buffer));
  return ReadEvictReply(buffer.data(), buffer.size(), num_bytes_evicted);
}

Status PlasmaClient::Impl::Connect(const std::string &store_socket_name,
                                   const std::string &manager_socket_name,
                                   int release_delay,
                                   int num_retries) {
  std::lock_guard<std::recursive_mutex> guard(client_mutex_);

  /// The local stream socket that connects to store.
  ray::local_stream_socket socket(main_service_);
  RAY_RETURN_NOT_OK(ray::ConnectSocketRetry(socket, store_socket_name));
  store_conn_.reset(new StoreConn(std::move(socket)));
  // Send a ConnectRequest to the store to get its memory capacity.
  RAY_RETURN_NOT_OK(SendConnectRequest(store_conn_));
  std::vector<uint8_t> buffer;
  RAY_RETURN_NOT_OK(PlasmaReceive(store_conn_, MessageType::PlasmaConnectReply, &buffer));
  RAY_RETURN_NOT_OK(ReadConnectReply(buffer.data(), buffer.size(), &store_capacity_));
  return Status::OK();
}

Status PlasmaClient::Impl::Disconnect() {
  std::lock_guard<std::recursive_mutex> guard(client_mutex_);

  // NOTE: We purposefully do not finish sending release calls for objects in
  // use, so that we don't duplicate PlasmaClient::Release calls (when handling
  // a SIGTERM, for example).

  // Close the connections to Plasma. The Plasma store will release the objects
  // that were in use by us when handling the SIGPIPE.
  store_conn_.reset();
  return Status::OK();
}

std::string PlasmaClient::Impl::DebugString() {
  std::lock_guard<std::recursive_mutex> guard(client_mutex_);
  if (!SendGetDebugStringRequest(store_conn_).ok()) {
    return "error sending request";
  }
  std::vector<uint8_t> buffer;
  if (!PlasmaReceive(store_conn_, MessageType::PlasmaGetDebugStringReply, &buffer).ok()) {
    return "error receiving reply";
  }
  std::string debug_string;
  if (!ReadGetDebugStringReply(buffer.data(), buffer.size(), &debug_string).ok()) {
    return "error parsing reply";
  }
  return debug_string;
}

// ----------------------------------------------------------------------
// PlasmaClient

PlasmaClient::PlasmaClient() : impl_(std::make_shared<PlasmaClient::Impl>()) {}

PlasmaClient::~PlasmaClient() {}

Status PlasmaClient::Connect(const std::string &store_socket_name,
                             const std::string &manager_socket_name,
                             int release_delay,
                             int num_retries) {
  return impl_->Connect(
      store_socket_name, manager_socket_name, release_delay, num_retries);
}

Status PlasmaClient::CreateAndSpillIfNeeded(const ObjectID &object_id,
                                            const ray::rpc::Address &owner_address,
                                            int64_t data_size,
                                            const uint8_t *metadata,
                                            int64_t metadata_size,
                                            std::shared_ptr<Buffer> *data,
                                            fb::ObjectSource source,
                                            int device_num) {
  return impl_->CreateAndSpillIfNeeded(object_id,
                                       owner_address,
                                       data_size,
                                       metadata,
                                       metadata_size,
                                       data,
                                       source,
                                       device_num);
}

Status PlasmaClient::TryCreateImmediately(const ObjectID &object_id,
                                          const ray::rpc::Address &owner_address,
                                          int64_t data_size,
                                          const uint8_t *metadata,
                                          int64_t metadata_size,
                                          std::shared_ptr<Buffer> *data,
                                          fb::ObjectSource source,
                                          int device_num) {
  return impl_->TryCreateImmediately(object_id,
                                     owner_address,
                                     data_size,
                                     metadata,
                                     metadata_size,
                                     data,
                                     source,
                                     device_num);
}

Status PlasmaClient::Get(const std::vector<ObjectID> &object_ids,
                         int64_t timeout_ms,
                         std::vector<ObjectBuffer> *object_buffers,
                         bool is_from_worker) {
  return impl_->Get(object_ids, timeout_ms, object_buffers, is_from_worker);
}

Status PlasmaClient::Release(const ObjectID &object_id) {
  return impl_->Release(object_id);
}

Status PlasmaClient::Contains(const ObjectID &object_id, bool *has_object) {
  return impl_->Contains(object_id, has_object);
}

Status PlasmaClient::Abort(const ObjectID &object_id) { return impl_->Abort(object_id); }

Status PlasmaClient::Seal(const ObjectID &object_id) { return impl_->Seal(object_id); }

Status PlasmaClient::Delete(const ObjectID &object_id) {
  return impl_->Delete(std::vector<ObjectID>{object_id});
}

Status PlasmaClient::Delete(const std::vector<ObjectID> &object_ids) {
  return impl_->Delete(object_ids);
}

Status PlasmaClient::Evict(int64_t num_bytes, int64_t &num_bytes_evicted) {
  return impl_->Evict(num_bytes, num_bytes_evicted);
}

Status PlasmaClient::Disconnect() { return impl_->Disconnect(); }

std::string PlasmaClient::DebugString() { return impl_->DebugString(); }

bool PlasmaClient::IsInUse(const ObjectID &object_id) {
  return impl_->IsInUse(object_id);
}

int64_t PlasmaClient::store_capacity() { return impl_->store_capacity(); }

}  // namespace plasma
