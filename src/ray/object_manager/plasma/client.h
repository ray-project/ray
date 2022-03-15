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

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "ray/common/buffer.h"
#include "ray/common/status.h"
#include "ray/object_manager/plasma/common.h"
#include "ray/util/visibility.h"
#include "src/ray/protobuf/common.pb.h"

namespace plasma {

using ray::Buffer;
using ray::SharedMemoryBuffer;
using ray::Status;

/// Object buffer data structure.
struct ObjectBuffer {
  /// The data buffer.
  std::shared_ptr<SharedMemoryBuffer> data;
  /// The metadata buffer.
  std::shared_ptr<SharedMemoryBuffer> metadata;
  /// The device number.
  int device_num;
};

class PlasmaClientInterface {
 public:
  virtual ~PlasmaClientInterface(){};

  /// Tell Plasma that the client no longer needs the object. This should be
  /// called after Get() or Create() when the client is done with the object.
  /// After this call, the buffer returned by Get() is no longer valid.
  ///
  /// \param object_id The ID of the object that is no longer needed.
  /// \return The return status.
  virtual Status Release(const ObjectID &object_id) = 0;

  /// Disconnect from the local plasma instance, including the local store and
  /// manager.
  ///
  /// \return The return status.
  virtual Status Disconnect() = 0;

  /// Get some objects from the Plasma Store. This function will block until the
  /// objects have all been created and sealed in the Plasma Store or the
  /// timeout expires.
  ///
  /// If an object was not retrieved, the corresponding metadata and data
  /// fields in the ObjectBuffer structure will evaluate to false.
  /// Objects are automatically released by the client when their buffers
  /// get out of scope.
  ///
  /// \param object_ids The IDs of the objects to get.
  /// \param timeout_ms The amount of time in milliseconds to wait before this
  ///        request times out. If this value is -1, then no timeout is set.
  /// \param[out] object_buffers The object results.
  /// \param is_from_worker Whether or not if the Get request comes from a Ray workers.
  /// \return The return status.
  virtual Status Get(const std::vector<ObjectID> &object_ids,
                     int64_t timeout_ms,
                     std::vector<ObjectBuffer> *object_buffers,
                     bool is_from_worker) = 0;

  /// Seal an object in the object store. The object will be immutable after
  /// this
  /// call.
  ///
  /// \param object_id The ID of the object to seal.
  /// \return The return status.
  virtual Status Seal(const ObjectID &object_id) = 0;

  /// Abort an unsealed object in the object store. If the abort succeeds, then
  /// it will be as if the object was never created at all. The unsealed object
  /// must have only a single reference (the one that would have been removed by
  /// calling Seal).
  ///
  /// \param object_id The ID of the object to abort.
  /// \return The return status.
  virtual Status Abort(const ObjectID &object_id) = 0;

  /// Create an object in the Plasma Store. Any metadata for this object must be
  /// be passed in when the object is created.
  ///
  /// If this request cannot be fulfilled immediately, this call will block until
  /// enough objects have been spilled to make space. If spilling cannot free
  /// enough space, an out of memory error will be returned.
  ///
  /// \param object_id The ID to use for the newly created object.
  /// \param owner_address The address of the object's owner.
  /// \param data_size The size in bytes of the space to be allocated for this
  /// object's
  ///        data (this does not include space used for metadata).
  /// \param metadata The object's metadata. If there is no metadata, this
  /// pointer should be NULL.
  /// \param metadata_size The size in bytes of the metadata. If there is no
  ///        metadata, this should be 0.
  /// \param data The address of the newly created object will be written here.
  /// \param device_num The number of the device where the object is being
  ///        created.
  ///        device_num = 0 corresponds to the host,
  ///        device_num = 1 corresponds to GPU0,
  ///        device_num = 2 corresponds to GPU1, etc.
  /// \return The return status.
  ///
  /// The returned object must be released once it is done with.  It must also
  /// be either sealed or aborted.
  virtual Status CreateAndSpillIfNeeded(const ObjectID &object_id,
                                        const ray::rpc::Address &owner_address,
                                        int64_t data_size,
                                        const uint8_t *metadata,
                                        int64_t metadata_size,
                                        std::shared_ptr<Buffer> *data,
                                        plasma::flatbuf::ObjectSource source,
                                        int device_num = 0) = 0;

  /// Delete a list of objects from the object store. This currently assumes that the
  /// object is present, has been sealed and not used by another client. Otherwise,
  /// it is a no operation.
  ///
  /// \param object_ids The list of IDs of the objects to delete.
  /// \return The return status. If all the objects are non-existent, return OK.
  virtual Status Delete(const std::vector<ObjectID> &object_ids) = 0;
};

class PlasmaClient : public PlasmaClientInterface {
 public:
  PlasmaClient();
  ~PlasmaClient();

  /// Connect to the local plasma store. Return the resulting connection.
  ///
  /// \param store_socket_name The name of the UNIX domain socket to use to
  ///        connect to the Plasma store.
  /// \param manager_socket_name The name of the UNIX domain socket to use to
  ///        connect to the local Plasma manager. If this is "", then this
  ///        function will not connect to a manager.
  ///        Note that plasma manager is no longer supported, this function
  ///        will return failure if this is not "".
  /// \param release_delay Deprecated (not used).
  /// \param num_retries number of attempts to connect to IPC socket, default 50
  /// \return The return status.
  Status Connect(const std::string &store_socket_name,
                 const std::string &manager_socket_name = "",
                 int release_delay = 0,
                 int num_retries = -1);

  /// Create an object in the Plasma Store. Any metadata for this object must be
  /// be passed in when the object is created.
  ///
  /// If this request cannot be fulfilled immediately, this call will block until
  /// enough objects have been spilled to make space. If spilling cannot free
  /// enough space, an out of memory error will be returned.
  ///
  /// \param object_id The ID to use for the newly created object.
  /// \param owner_address The address of the object's owner.
  /// \param data_size The size in bytes of the space to be allocated for this
  /// object's
  ///        data (this does not include space used for metadata).
  /// \param metadata The object's metadata. If there is no metadata, this
  /// pointer should be NULL.
  /// \param metadata_size The size in bytes of the metadata. If there is no
  ///        metadata, this should be 0.
  /// \param data The address of the newly created object will be written here.
  /// \param device_num The number of the device where the object is being
  ///        created.
  ///        device_num = 0 corresponds to the host,
  ///        device_num = 1 corresponds to GPU0,
  ///        device_num = 2 corresponds to GPU1, etc.
  /// \return The return status.
  ///
  /// The returned object must be released once it is done with.  It must also
  /// be either sealed or aborted.
  Status CreateAndSpillIfNeeded(const ObjectID &object_id,
                                const ray::rpc::Address &owner_address,
                                int64_t data_size,
                                const uint8_t *metadata,
                                int64_t metadata_size,
                                std::shared_ptr<Buffer> *data,
                                plasma::flatbuf::ObjectSource source,
                                int device_num = 0);

  /// Create an object in the Plasma Store. Any metadata for this object must be
  /// be passed in when the object is created.
  ///
  /// The plasma store will attempt to fulfill this request immediately. If it
  /// cannot be fulfilled immediately, an error will be returned to the client.
  ///
  /// \param object_id The ID to use for the newly created object.
  /// \param owner_address The address of the object's owner.
  /// \param data_size The size in bytes of the space to be allocated for this
  /// object's
  ///        data (this does not include space used for metadata).
  /// \param metadata The object's metadata. If there is no metadata, this
  /// pointer
  ///        should be NULL.
  /// \param metadata_size The size in bytes of the metadata. If there is no
  ///        metadata, this should be 0.
  /// \param data The address of the newly created object will be written here.
  /// \param device_num The number of the device where the object is being
  ///        created.
  ///        device_num = 0 corresponds to the host,
  ///        device_num = 1 corresponds to GPU0,
  ///        device_num = 2 corresponds to GPU1, etc.
  /// \return The return status.
  ///
  /// The returned object must be released once it is done with.  It must also
  /// be either sealed or aborted.
  Status TryCreateImmediately(const ObjectID &object_id,
                              const ray::rpc::Address &owner_address,
                              int64_t data_size,
                              const uint8_t *metadata,
                              int64_t metadata_size,
                              std::shared_ptr<Buffer> *data,
                              plasma::flatbuf::ObjectSource source,
                              int device_num = 0);

  /// Get some objects from the Plasma Store. This function will block until the
  /// objects have all been created and sealed in the Plasma Store or the
  /// timeout expires.
  ///
  /// If an object was not retrieved, the corresponding metadata and data
  /// fields in the ObjectBuffer structure will evaluate to false.
  /// Objects are automatically released by the client when their buffers
  /// get out of scope.
  ///
  /// \param object_ids The IDs of the objects to get.
  /// \param timeout_ms The amount of time in milliseconds to wait before this
  ///        request times out. If this value is -1, then no timeout is set.
  /// \param[out] object_buffers The object results.
  /// \param is_from_worker Whether or not if the Get request comes from a Ray workers.
  /// \return The return status.
  Status Get(const std::vector<ObjectID> &object_ids,
             int64_t timeout_ms,
             std::vector<ObjectBuffer> *object_buffers,
             bool is_from_worker);

  /// Tell Plasma that the client no longer needs the object. This should be
  /// called after Get() or Create() when the client is done with the object.
  /// After this call, the buffer returned by Get() is no longer valid.
  ///
  /// \param object_id The ID of the object that is no longer needed.
  /// \return The return status.
  Status Release(const ObjectID &object_id);

  /// Check if the object store contains a particular object and the object has
  /// been sealed. The result will be stored in has_object.
  ///
  /// @todo: We may want to indicate if the object has been created but not
  /// sealed.
  ///
  /// \param object_id The ID of the object whose presence we are checking.
  /// \param has_object The function will write true at this address if
  ///        the object is present and false if it is not present.
  /// \return The return status.
  Status Contains(const ObjectID &object_id, bool *has_object);

  /// Abort an unsealed object in the object store. If the abort succeeds, then
  /// it will be as if the object was never created at all. The unsealed object
  /// must have only a single reference (the one that would have been removed by
  /// calling Seal).
  ///
  /// \param object_id The ID of the object to abort.
  /// \return The return status.
  Status Abort(const ObjectID &object_id);

  /// Seal an object in the object store. The object will be immutable after
  /// this
  /// call.
  ///
  /// \param object_id The ID of the object to seal.
  /// \return The return status.
  Status Seal(const ObjectID &object_id);

  /// Delete an object from the object store. This currently assumes that the
  /// object is present, has been sealed and not used by another client. Otherwise,
  /// it is a no operation.
  ///
  /// \todo We may want to allow the deletion of objects that are not present or
  ///       haven't been sealed.
  ///
  /// \param object_id The ID of the object to delete.
  /// \return The return status.
  Status Delete(const ObjectID &object_id);

  /// Delete a list of objects from the object store. This currently assumes that the
  /// object is present, has been sealed and not used by another client. Otherwise,
  /// it is a no operation.
  ///
  /// \param object_ids The list of IDs of the objects to delete.
  /// \return The return status. If all the objects are non-existent, return OK.
  Status Delete(const std::vector<ObjectID> &object_ids);

  /// Delete objects until we have freed up num_bytes bytes or there are no more
  /// released objects that can be deleted.
  ///
  /// \param num_bytes The number of bytes to try to free up.
  /// \param num_bytes_evicted Out parameter for total number of bytes of space
  /// retrieved.
  /// \return The return status.
  Status Evict(int64_t num_bytes, int64_t &num_bytes_evicted);

  /// Disconnect from the local plasma instance, including the local store and
  /// manager.
  ///
  /// \return The return status.
  Status Disconnect();

  /// Get the current debug string from the plasma store server.
  ///
  /// \return The debug string.
  std::string DebugString();

  /// Get the memory capacity of the store.
  ///
  /// \return Memory capacity of the store in bytes.
  int64_t store_capacity();

 private:
  /// Retry a previous create call using the returned request ID.
  ///
  /// \param object_id The ID to use for the newly created object.
  /// \param request_id The request ID returned by the previous Create call.
  /// \param metadata The object's metadata. If there is no metadata, this
  /// pointer should be NULL.
  /// \param retry_with_request_id If the request is not yet fulfilled, this
  ///        will be set to a unique ID with which the client should retry.
  /// \param data The address of the newly created object will be written here.
  Status RetryCreate(const ObjectID &object_id,
                     uint64_t request_id,
                     const uint8_t *metadata,
                     uint64_t *retry_with_request_id,
                     std::shared_ptr<Buffer> *data);

  friend class PlasmaBuffer;
  friend class PlasmaMutableBuffer;
  bool IsInUse(const ObjectID &object_id);

  class Impl;
  std::shared_ptr<Impl> impl_;
};

}  // namespace plasma
