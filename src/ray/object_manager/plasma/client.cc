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

#include "ray/object_manager/plasma/client.h"

#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/ray_config.h"
#include "ray/common/status.h"
#include "ray/object_manager/plasma/connection.h"
#include "ray/object_manager/plasma/plasma.h"
#include "ray/object_manager/plasma/protocol.h"

namespace plasma {

using plasma::flatbuf::MessageType;
using plasma::flatbuf::PlasmaError;

/// A Buffer class that automatically releases the backing plasma object
/// when it goes out of scope. This is returned by Get.
class PlasmaBuffer : public SharedMemoryBuffer {
 public:
  PlasmaBuffer(std::shared_ptr<PlasmaClient> client,
               const ObjectID &object_id,
               const std::shared_ptr<Buffer> &buffer)
      : SharedMemoryBuffer(buffer, 0, buffer->Size()),
        client_(std::move(client)),
        object_id_(object_id) {}

  ~PlasmaBuffer() override { RAY_UNUSED(client_->Release(object_id_)); }

  PlasmaBuffer(const PlasmaBuffer &) = delete;
  PlasmaBuffer &operator=(const PlasmaBuffer &) = delete;

 private:
  std::shared_ptr<PlasmaClient> client_;
  ObjectID object_id_;
};

/// A mutable Buffer class that keeps the backing data alive by keeping a
/// PlasmaClient shared pointer. This is returned by Create. Release will
/// be called in the associated Seal call.
class PlasmaMutableBuffer : public SharedMemoryBuffer {
 public:
  PlasmaMutableBuffer(std::shared_ptr<PlasmaClient> client,
                      uint8_t *mutable_data,
                      int64_t data_size)
      : SharedMemoryBuffer(mutable_data, data_size), client_(std::move(client)) {}

 private:
  std::shared_ptr<PlasmaClient> client_;
};

// If the file descriptor fd has been mmapped in this client process before,
// return the pointer that was returned by mmap, otherwise mmap it and store the
// pointer in a hash table.
uint8_t *PlasmaClient::GetStoreFdAndMmap(MEMFD_TYPE store_fd_val, int64_t map_size) {
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
uint8_t *PlasmaClient::LookupMmappedFile(MEMFD_TYPE store_fd_val) const {
  auto entry = mmap_table_.find(store_fd_val);
  RAY_CHECK(entry != mmap_table_.end());
  return entry->second->pointer();
}

bool PlasmaClient::IsInUse(const ObjectID &object_id) {
  std::lock_guard<std::recursive_mutex> guard(client_mutex_);

  const auto elem = objects_in_use_.find(object_id);
  return (elem != objects_in_use_.end());
}

void PlasmaClient::InsertObjectInUse(const ObjectID &object_id,
                                     std::unique_ptr<PlasmaObject> object,
                                     bool is_sealed) {
  auto inserted =
      objects_in_use_.insert({object_id, std::make_unique<ObjectInUseEntry>()});
  RAY_CHECK(inserted.second) << "Object already in use";
  auto it = inserted.first;

  // Add this object ID to the hash table of object IDs in use. The
  // corresponding call to free happens in PlasmaClient::Release.
  it->second->object = std::move(*object);
  // Count starts at 1 to pin the object.
  it->second->count = 1;
  it->second->is_sealed = is_sealed;
}

void PlasmaClient::IncrementObjectCount(const ObjectID &object_id) {
  // Increment the count of the object to track the fact that it is being used.
  // The corresponding decrement should happen in PlasmaClient::Release.
  auto object_entry = objects_in_use_.find(object_id);
  RAY_CHECK(object_entry != objects_in_use_.end());
  object_entry->second->count += 1;
  RAY_LOG(DEBUG) << "IncrementObjectCount " << object_id
                 << " count is now: " << object_entry->second->count;
}

Status PlasmaClient::HandleCreateReply(const ObjectID &object_id,
                                       bool is_experimental_mutable_object,
                                       const uint8_t *metadata,
                                       uint64_t *retry_with_request_id,
                                       std::shared_ptr<Buffer> *data) {
  std::vector<uint8_t> buffer;
  RAY_RETURN_NOT_OK(PlasmaReceive(store_conn_, MessageType::PlasmaCreateReply, &buffer));
  ObjectID id;
  auto object = std::make_unique<PlasmaObject>();
  MEMFD_TYPE store_fd;
  int64_t mmap_size;

  if (retry_with_request_id != nullptr) {
    RAY_RETURN_NOT_OK(ReadCreateReply(buffer.data(),
                                      buffer.size(),
                                      &id,
                                      retry_with_request_id,
                                      object.get(),
                                      &store_fd,
                                      &mmap_size));
    if (*retry_with_request_id > 0) {
      // The client should retry the request.
      return Status::OK();
    }
  } else {
    uint64_t unused = 0;
    RAY_RETURN_NOT_OK(ReadCreateReply(
        buffer.data(), buffer.size(), &id, &unused, object.get(), &store_fd, &mmap_size));
    RAY_CHECK_EQ(unused, 0ul);
  }

  // If the CreateReply included an error, then the store will not send a file
  // descriptor.
  RAY_CHECK_EQ(object->device_num, 0) << "GPU is not enabled.";
  // The metadata should come right after the data.
  RAY_CHECK_EQ(object->metadata_offset, object->data_offset + object->data_size);
  RAY_LOG(DEBUG) << "GetStoreFdAndMmap " << store_fd.first << ", " << store_fd.second
                 << ", size " << mmap_size << " for object id " << id;
  *data = std::make_shared<PlasmaMutableBuffer>(
      shared_from_this(),
      GetStoreFdAndMmap(store_fd, mmap_size) + object->data_offset,
      object->data_size);
  // If plasma_create is being called from a transfer, then we will not copy the
  // metadata here. The metadata will be written along with the data streamed
  // from the transfer.
  if (metadata != nullptr) {
    // Copy the metadata to the buffer.
    memcpy((*data)->Data() + object->data_size, metadata, object->metadata_size);
  }

  // Add the object as in use. A call to PlasmaClient::Release is required to
  // decrement the initial ref count of 1. Cache the reference to the object.
  InsertObjectInUse(object_id, std::move(object), /*is_sealed=*/false);
  // We increment the count a second time (and the corresponding decrement will
  // happen in a PlasmaClient::Release call in plasma_seal) so even if the
  // buffer returned by PlasmaClient::Create goes out of scope, the object does
  // not get released before the call to PlasmaClient::Seal happens.
  IncrementObjectCount(object_id);

  if (is_experimental_mutable_object) {
    // Pin experimental mutable objects when they are first created so that
    // they are not evicted before the writer has a chance to register the
    // object.
    // TODO(swang): GC these once they are deleted by the
    // experimental::MutableObjectManager. This can be done by pinning the object using
    // the shared_ptr to the memory buffer that is held by the
    // experimental::MutableObjectManager.
    IncrementObjectCount(object_id);
  }

  // Create IPC was successful.
  auto object_entry = objects_in_use_.find(object_id);
  RAY_CHECK(object_entry != objects_in_use_.end());
  auto &entry = object_entry->second;
  RAY_CHECK(!entry->is_sealed);

  return Status::OK();
}

Status PlasmaClient::CreateAndSpillIfNeeded(const ObjectID &object_id,
                                            const ray::rpc::Address &owner_address,
                                            bool is_experimental_mutable_object,
                                            int64_t data_size,
                                            const uint8_t *metadata,
                                            int64_t metadata_size,
                                            std::shared_ptr<Buffer> *data,
                                            plasma::flatbuf::ObjectSource source,
                                            int device_num) {
  uint64_t retry_with_request_id = 0;
  Status status;
  {
    std::unique_lock<std::recursive_mutex> guard(client_mutex_);

    RAY_LOG(DEBUG) << "called plasma_create on conn " << store_conn_ << " with size "
                   << data_size << " and metadata size " << metadata_size;
    RAY_RETURN_NOT_OK(SendCreateRequest(store_conn_,
                                        object_id,
                                        owner_address,
                                        is_experimental_mutable_object,
                                        data_size,
                                        metadata_size,
                                        source,
                                        device_num,
                                        /*try_immediately=*/false));
    status = HandleCreateReply(object_id,
                               is_experimental_mutable_object,
                               metadata,
                               &retry_with_request_id,
                               data);
  }

  while (retry_with_request_id > 0) {
    // TODO(sang): Consider using exponential backoff here.
    std::this_thread::sleep_for(
        std::chrono::milliseconds(RayConfig::instance().object_store_full_delay_ms()));
    std::unique_lock<std::recursive_mutex> guard(client_mutex_);
    RAY_LOG(DEBUG) << "Retrying request for object " << object_id << " with request ID "
                   << retry_with_request_id;
    RAY_RETURN_NOT_OK(
        SendCreateRetryRequest(store_conn_, object_id, retry_with_request_id));
    status = HandleCreateReply(object_id,
                               is_experimental_mutable_object,
                               metadata,
                               &retry_with_request_id,
                               data);
  }

  return status;
}

Status PlasmaClient::TryCreateImmediately(const ObjectID &object_id,
                                          const ray::rpc::Address &owner_address,
                                          int64_t data_size,
                                          const uint8_t *metadata,
                                          int64_t metadata_size,
                                          std::shared_ptr<Buffer> *data,
                                          plasma::flatbuf::ObjectSource source,
                                          int device_num) {
  std::lock_guard<std::recursive_mutex> guard(client_mutex_);

  RAY_LOG(DEBUG) << "called plasma_create on conn " << store_conn_ << " with size "
                 << data_size << " and metadata size " << metadata_size;
  RAY_RETURN_NOT_OK(SendCreateRequest(store_conn_,
                                      object_id,
                                      owner_address,
                                      /*is_mutable=*/false,
                                      data_size,
                                      metadata_size,
                                      source,
                                      device_num,
                                      /*try_immediately=*/true));
  return HandleCreateReply(
      object_id, /*is_experimental_mutable_object=*/false, metadata, nullptr, data);
}

Status PlasmaClient::GetBuffers(const ObjectID *object_ids,
                                int64_t num_objects,
                                int64_t timeout_ms,
                                ObjectBuffer *object_buffers) {
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
      RAY_CHECK_NE(timeout_ms, -1)
          << "Plasma client called get on an unsealed object that it created";
      RAY_LOG(WARNING)
          << "Attempting to get an object that this client created but hasn't sealed.";
      all_present = false;
    } else {
      PlasmaObject *object = &object_entry->second->object;

      RAY_LOG(DEBUG) << "Plasma Get " << object_ids[i]
                     << ", data size: " << object->data_size
                     << ", metadata size: " << object->metadata_size;
      RAY_CHECK_EQ(object->device_num, 0) << "GPU library is not enabled.";

      uint8_t *data = LookupMmappedFile(object->store_fd);
      auto physical_buf = std::make_shared<PlasmaBuffer>(
          shared_from_this(),
          object_ids[i],
          std::make_shared<SharedMemoryBuffer>(
              data + object->data_offset, object->data_size + object->metadata_size));

      object_buffers[i].data =
          SharedMemoryBuffer::Slice(physical_buf, 0, object->data_size);
      object_buffers[i].metadata = SharedMemoryBuffer::Slice(
          physical_buf, object->data_size, object->metadata_size);
      object_buffers[i].device_num = object->device_num;
      // Increment the count of the number of instances of this object that this
      // client is using. Cache the reference to the object.
      IncrementObjectCount(object_ids[i]);
    }
  }

  if (all_present) {
    return Status::OK();
  }

  // If we get here, then the objects aren't all currently in use by this
  // client, so we need to send a request to the plasma store.
  for (int64_t i = 0; i < num_objects; i++) {
    RAY_LOG(DEBUG) << "Sending get request " << object_ids[i];
  }
  RAY_RETURN_NOT_OK(SendGetRequest(store_conn_, &object_ids[0], num_objects, timeout_ms));
  std::vector<uint8_t> buffer;
  RAY_RETURN_NOT_OK(PlasmaReceive(store_conn_, MessageType::PlasmaGetReply, &buffer));
  std::vector<ObjectID> received_object_ids(num_objects);
  std::vector<PlasmaObject> object_data(num_objects);
  std::vector<MEMFD_TYPE> store_fds;
  std::vector<int64_t> mmap_sizes;
  ReadGetReply(buffer.data(),
               buffer.size(),
               received_object_ids.data(),
               object_data.data(),
               num_objects,
               store_fds,
               mmap_sizes);

  // We mmap all of the file descriptors here so that we can avoid look them up
  // in the subsequent loop based on just the store file descriptor and without
  // having to know the relevant file descriptor received from recv_fd.
  for (size_t i = 0; i < store_fds.size(); i++) {
    RAY_LOG(DEBUG) << "GetStoreFdAndMmap " << store_fds[i].first << ", "
                   << store_fds[i].second << ", size " << mmap_sizes[i]
                   << " for object id " << received_object_ids[i];
    GetStoreFdAndMmap(store_fds[i], mmap_sizes[i]);
  }

  std::unique_ptr<PlasmaObject> object;
  for (int64_t i = 0; i < num_objects; ++i) {
    RAY_DCHECK(received_object_ids[i] == object_ids[i]);
    object = std::make_unique<PlasmaObject>(object_data[i]);
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
      if (objects_in_use_.find(received_object_ids[i]) == objects_in_use_.end()) {
        // Increment the count of the number of instances of this object that this
        // client is using. Cache the reference to the object.
        InsertObjectInUse(received_object_ids[i], std::move(object), /*is_sealed=*/true);
      } else {
        IncrementObjectCount(received_object_ids[i]);
      }
      auto &object_entry = objects_in_use_[received_object_ids[i]];

      RAY_LOG(DEBUG) << "Plasma Get " << received_object_ids[i]
                     << ", data size: " << object_entry->object.data_size
                     << ", metadata size: " << object_entry->object.metadata_size;
      RAY_CHECK_EQ(object_entry->object.device_num, 0)
          << "Arrow GPU library is not enabled.";
      uint8_t *data = LookupMmappedFile(object_entry->object.store_fd);

      // Finish filling out the return values.
      auto physical_buf = std::make_shared<PlasmaBuffer>(
          shared_from_this(),
          object_ids[i],
          std::make_shared<SharedMemoryBuffer>(
              data + object_entry->object.data_offset,
              object_entry->object.data_size + object_entry->object.metadata_size));
      object_buffers[i].data =
          SharedMemoryBuffer::Slice(physical_buf, 0, object_entry->object.data_size);
      object_buffers[i].metadata =
          SharedMemoryBuffer::Slice(physical_buf,
                                    object_entry->object.data_size,
                                    object_entry->object.metadata_size);
      object_buffers[i].device_num = object_entry->object.device_num;
    } else {
      // The object was not retrieved.  The caller can detect this condition
      // by checking the boolean value of the metadata/data buffers.
      RAY_DCHECK(!object_buffers[i].metadata);
      RAY_DCHECK(!object_buffers[i].data);
    }
  }
  return Status::OK();
}

Status PlasmaClient::GetExperimentalMutableObject(
    const ObjectID &object_id, std::unique_ptr<MutableObject> *mutable_object) {
#if defined(_WIN32)
  return Status::NotImplemented("Not supported on Windows.");
#endif

  // First make sure the object is in scope. The ObjectBuffer will keep the
  // value pinned in the plasma store.
  std::vector<ObjectBuffer> object_buffers;
  RAY_RETURN_NOT_OK(Get({object_id}, /*timeout_ms=*/0, &object_buffers));
  if (!object_buffers[0].data) {
    return Status::Invalid(
        "Experimental mutable object must be in the local object store to register as "
        "reader or writer");
  }

  // Now that the value is pinned, get the object as a MutableObject, which is
  // used to implement channels. The returned MutableObject will pin the
  // object in the local object store.

  std::unique_lock<std::recursive_mutex> guard(client_mutex_);

  auto object_entry = objects_in_use_.find(object_id);
  if (object_entry == objects_in_use_.end()) {
    return Status::ObjectNotFound("MutableObject must be in use before getting");
  }

  if (!object_entry->second->object.is_experimental_mutable_object) {
    return Status::ObjectNotFound("Cannot get normal plasma objects as mutable objects");
  }

  // Pin experimental mutable object so that it is not evicted before the
  // caller has a chance to register the object.
  // TODO(swang): GC once they are deleted by the experimental::MutableObjectManager. This
  // can be done by pinning the object using the shared_ptr to the memory buffer that is
  // held by the experimental::MutableObjectManager.
  IncrementObjectCount(object_id);

  const auto &object = object_entry->second->object;
  *mutable_object =
      std::make_unique<MutableObject>(LookupMmappedFile(object.store_fd), object);
  return Status::OK();
}

Status PlasmaClient::Get(const std::vector<ObjectID> &object_ids,
                         int64_t timeout_ms,
                         std::vector<ObjectBuffer> *out) {
  std::lock_guard<std::recursive_mutex> guard(client_mutex_);
  const size_t num_objects = object_ids.size();
  *out = std::vector<ObjectBuffer>(num_objects);
  return GetBuffers(object_ids.data(), num_objects, timeout_ms, out->data());
}

Status PlasmaClient::MarkObjectUnused(const ObjectID &object_id) {
  auto object_entry = objects_in_use_.find(object_id);
  RAY_CHECK(object_entry != objects_in_use_.end());
  RAY_CHECK_EQ(object_entry->second->count, 0);

  // Remove the entry from the hash table of objects currently in use.
  objects_in_use_.erase(object_id);
  return Status::OK();
}

Status PlasmaClient::Release(const ObjectID &object_id) {
  std::lock_guard<std::recursive_mutex> guard(client_mutex_);

  // If the client is already disconnected, ignore release requests.
  if (!store_conn_) {
    return Status::OK();
  }
  const auto object_entry = objects_in_use_.find(object_id);
  RAY_CHECK(object_entry != objects_in_use_.end());

  object_entry->second->count -= 1;
  RAY_LOG(DEBUG) << "Decrement object count " << object_id << " count is now "
                 << object_entry->second->count;
  RAY_CHECK_GE(object_entry->second->count, 0);

  if (object_entry->second->count == 0) {
    RAY_LOG(DEBUG) << "Releasing object no longer in use " << object_id;
    // object_entry is invalidated in MarkObjectUnused, need to read the fd beforehand.
    // If the fd may be unmapped, we wait for the plasma server to send a ReleaseReply.
    // Otherwise, skip the reply to boost performance.
    // Q: since both server and client knows this fd is fallback allocated, why do we
    //    need to pass it in PlasmaReleaseRequest?
    // A: because we wanna be idempotent, and in the 2nd call, the server does not know
    //    about the object.
    const MEMFD_TYPE fd = object_entry->second->object.store_fd;
    bool may_unmap = object_entry->second->object.fallback_allocated;
    // Tell the store that the client no longer needs the object.
    RAY_RETURN_NOT_OK(MarkObjectUnused(object_id));
    RAY_RETURN_NOT_OK(SendReleaseRequest(store_conn_, object_id, may_unmap));
    if (may_unmap) {
      // Now, since the object release may unmap the mmap, we wait for a reply.
      std::vector<uint8_t> buffer;
      RAY_RETURN_NOT_OK(
          PlasmaReceive(store_conn_, MessageType::PlasmaReleaseReply, &buffer));
      ObjectID released_object_id;

      // `should_unmap` is set to true by the plasma server, when the mmap section is
      // fallback-allocated and is no longer used.
      bool should_unmap;
      RAY_RETURN_NOT_OK(ReadReleaseReply(
          buffer.data(), buffer.size(), &released_object_id, &should_unmap));
      if (should_unmap) {
        auto mmap_entry = mmap_table_.find(fd);
        // Release call is idempotent: if we already released, it's ok.
        if (mmap_entry != mmap_table_.end()) {
          mmap_table_.erase(mmap_entry);
        }
      }
    }
    auto iter = deletion_cache_.find(object_id);
    if (iter != deletion_cache_.end()) {
      deletion_cache_.erase(object_id);
      RAY_RETURN_NOT_OK(Delete({object_id}));
    }
  }
  return Status::OK();
}

// This method is used to query whether the plasma store contains an object.
Status PlasmaClient::Contains(const ObjectID &object_id, bool *has_object) {
  std::lock_guard<std::recursive_mutex> guard(client_mutex_);

  // Check if we already have a reference to the object.
  if (objects_in_use_.count(object_id) > 0) {
    *has_object = true;
  } else {
    // If we don't already have a reference to the object, check with the store
    // to see if we have the object.
    RAY_RETURN_NOT_OK(SendContainsRequest(store_conn_, object_id));
    std::vector<uint8_t> buffer;
    RAY_RETURN_NOT_OK(
        PlasmaReceive(store_conn_, MessageType::PlasmaContainsReply, &buffer));
    ObjectID object_id2;
    RAY_DCHECK(buffer.size() > 0);
    ReadContainsReply(buffer.data(), buffer.size(), &object_id2, has_object);
  }
  return Status::OK();
}

Status PlasmaClient::Seal(const ObjectID &object_id) {
  std::lock_guard<std::recursive_mutex> guard(client_mutex_);
  RAY_LOG(DEBUG) << "Seal " << object_id;

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
  // Send the seal request to Plasma. This is the normal Seal path, used for
  // immutable objects and the initial Create call for mutable objects.
  RAY_RETURN_NOT_OK(SendSealRequest(store_conn_, object_id));
  std::vector<uint8_t> buffer;
  RAY_RETURN_NOT_OK(PlasmaReceive(store_conn_, MessageType::PlasmaSealReply, &buffer));
  ObjectID sealed_id;
  RAY_RETURN_NOT_OK(ReadSealReply(buffer.data(), buffer.size(), &sealed_id));
  RAY_CHECK_EQ(sealed_id, object_id);
  // We call PlasmaClient::Release to decrement the number of instances of this
  // object
  // that are currently being used by this client. The corresponding increment
  // happened in plasma_create and was used to ensure that the object was not
  // released before the call to PlasmaClient::Seal.
  RAY_RETURN_NOT_OK(Release(object_id));

  return Status::OK();
}

Status PlasmaClient::Abort(const ObjectID &object_id) {
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
  ReadAbortReply(buffer.data(), buffer.size(), &id);
  return Status::OK();
}

Status PlasmaClient::Delete(const std::vector<ObjectID> &object_ids) {
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
    ReadDeleteReply(buffer.data(), buffer.size(), &not_in_use_ids, &error_codes);
  }
  return Status::OK();
}

Status PlasmaClient::Connect(const std::string &store_socket_name,
                             const std::string &manager_socket_name,
                             int num_retries) {
  std::lock_guard<std::recursive_mutex> guard(client_mutex_);

  /// The local stream socket that connects to store.
  ray::local_stream_socket socket(main_service_);
  RAY_RETURN_NOT_OK(ray::ConnectSocketRetry(socket, store_socket_name));
  store_conn_ =
      std::make_shared<StoreConn>(std::move(socket), exit_on_connection_failure_);
  // Send a ConnectRequest to the store to get its memory capacity.
  RAY_RETURN_NOT_OK(SendConnectRequest(store_conn_));
  std::vector<uint8_t> buffer;
  RAY_RETURN_NOT_OK(PlasmaReceive(store_conn_, MessageType::PlasmaConnectReply, &buffer));
  ReadConnectReply(buffer.data(), buffer.size());

  return Status::OK();
}

void PlasmaClient::Disconnect() {
  std::lock_guard<std::recursive_mutex> guard(client_mutex_);

  // NOTE: We purposefully do not finish sending release calls for objects in
  // use, so that we don't duplicate PlasmaClient::Release calls (when handling
  // a SIGTERM, for example).

  // Close the connections to Plasma. The Plasma store will release the objects
  // that were in use by us when handling the SIGPIPE.
  store_conn_.reset();
}

StatusOr<std::string> PlasmaClient::GetMemoryUsage() {
  std::lock_guard<std::recursive_mutex> guard(client_mutex_);
  auto request_status = SendGetDebugStringRequest(store_conn_);
  if (!request_status.ok()) {
    return request_status;
  }
  std::vector<uint8_t> buffer;
  auto recv_status =
      PlasmaReceive(store_conn_, MessageType::PlasmaGetDebugStringReply, &buffer);
  if (!recv_status.ok()) {
    return recv_status;
  }
  std::string debug_string;
  auto response_status =
      ReadGetDebugStringReply(buffer.data(), buffer.size(), &debug_string);
  if (!response_status.ok()) {
    return response_status;
  }
  return debug_string;
}

}  // namespace plasma
