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

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/buffer.h"
#include "ray/common/status.h"
#include "ray/common/status_or.h"
#include "ray/object_manager/common.h"
#include "ray/object_manager/plasma/common.h"
#include "ray/object_manager/plasma/connection.h"
#include "ray/object_manager/plasma/shared_memory.h"
#include "src/ray/protobuf/common.pb.h"

namespace plasma {

using ray::Buffer;
using ray::PlasmaObjectHeader;
using ray::SharedMemoryBuffer;
using ray::Status;
using ray::StatusOr;

struct MutableObject {
  MutableObject(uint8_t *base_ptr, const PlasmaObject &object_info)
      : header(
            reinterpret_cast<PlasmaObjectHeader *>(base_ptr + object_info.header_offset)),
        buffer(std::make_shared<SharedMemoryBuffer>(base_ptr + object_info.data_offset,
                                                    object_info.allocated_size)),
        allocated_size(object_info.allocated_size) {}

  PlasmaObjectHeader *header;
  std::shared_ptr<SharedMemoryBuffer> buffer;
  const int64_t allocated_size;
};

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
  virtual ~PlasmaClientInterface() = default;

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
  virtual Status Connect(const std::string &store_socket_name,
                         const std::string &manager_socket_name = "",
                         int num_retries = -1) = 0;

  /// Tell Plasma that the client no longer needs the object. This should be
  /// called after Get() or Create() when the client is done with the object.
  /// After this call, the buffer returned by Get() is no longer valid.
  ///
  /// \param object_id The ID of the object that is no longer needed.
  virtual Status Release(const ObjectID &object_id) = 0;

  /// Check if the object store contains a particular object and the object has
  /// been sealed. The result will be stored in has_object.
  ///
  /// @todo: We may want to indicate if the object has been created but not
  /// sealed.
  ///
  /// \param object_id The ID of the object whose presence we are checking.
  /// \param has_object The function will write true at this address if
  ///        the object is present and false if it is not present.
  virtual Status Contains(const ObjectID &object_id, bool *has_object) = 0;

  /// Disconnect from the local plasma instance, including the local store and
  /// manager.
  virtual void Disconnect() = 0;

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
  virtual Status Get(const std::vector<ObjectID> &object_ids,
                     int64_t timeout_ms,
                     std::vector<ObjectBuffer> *object_buffers) = 0;

  /// Get an experimental mutable object.
  ///
  /// \param[in] object_id The ID of the object.
  /// \param[in] mutable_object Struct containing pointers for the object
  /// header, which is used to synchronize with other writers and readers, and
  /// the object data and metadata, which is read by the application.
  virtual Status GetExperimentalMutableObject(
      const ObjectID &object_id, std::unique_ptr<MutableObject> *mutable_object) = 0;

  /// Seal an object in the object store. The object will be immutable after
  /// this call.
  ///
  /// \param object_id The ID of the object to seal.
  virtual Status Seal(const ObjectID &object_id) = 0;

  /// Abort an unsealed object in the object store. If the abort succeeds, then
  /// it will be as if the object was never created at all. The unsealed object
  /// must have only a single reference (the one that would have been removed by
  /// calling Seal).
  ///
  /// \param object_id The ID of the object to abort.
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
  ///
  /// The returned object must be released once it is done with.  It must also
  /// be either sealed or aborted.
  virtual Status CreateAndSpillIfNeeded(const ObjectID &object_id,
                                        const ray::rpc::Address &owner_address,
                                        bool is_mutable,
                                        int64_t data_size,
                                        const uint8_t *metadata,
                                        int64_t metadata_size,
                                        std::shared_ptr<Buffer> *data,
                                        plasma::flatbuf::ObjectSource source,
                                        int device_num = 0) = 0;

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
  ///
  /// The returned object must be released once it is done with.  It must also
  /// be either sealed or aborted.
  virtual Status TryCreateImmediately(const ObjectID &object_id,
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
  /// \return Returns ok if all the objects are non-existent.
  virtual Status Delete(const std::vector<ObjectID> &object_ids) = 0;

  /// Get the current debug string from the plasma store server.
  ///
  /// \return the debug string if successful, otherwise return an error status.
  virtual StatusOr<std::string> GetMemoryUsage() = 0;
};

class PlasmaClient : public std::enable_shared_from_this<PlasmaClient>,
                     public PlasmaClientInterface {
 public:
  PlasmaClient() : exit_on_connection_failure_(false) {}

  explicit PlasmaClient(bool exit_on_connection_failure)
      : exit_on_connection_failure_(exit_on_connection_failure) {}

  Status Connect(const std::string &store_socket_name,
                 const std::string &manager_socket_name = "",
                 int num_retries = -1) override;

  Status CreateAndSpillIfNeeded(const ObjectID &object_id,
                                const ray::rpc::Address &owner_address,
                                bool is_experimental_mutable_object,
                                int64_t data_size,
                                const uint8_t *metadata,
                                int64_t metadata_size,
                                std::shared_ptr<Buffer> *data,
                                plasma::flatbuf::ObjectSource source,
                                int device_num = 0) override;

  Status TryCreateImmediately(const ObjectID &object_id,
                              const ray::rpc::Address &owner_address,
                              int64_t data_size,
                              const uint8_t *metadata,
                              int64_t metadata_size,
                              std::shared_ptr<Buffer> *data,
                              plasma::flatbuf::ObjectSource source,
                              int device_num = 0) override;

  Status Get(const std::vector<ObjectID> &object_ids,
             int64_t timeout_ms,
             std::vector<ObjectBuffer> *object_buffers) override;

  Status GetExperimentalMutableObject(
      const ObjectID &object_id, std::unique_ptr<MutableObject> *mutable_object) override;

  Status Release(const ObjectID &object_id) override;

  Status Contains(const ObjectID &object_id, bool *has_object) override;

  Status Abort(const ObjectID &object_id) override;

  Status Seal(const ObjectID &object_id) override;

  Status Delete(const std::vector<ObjectID> &object_ids) override;

  void Disconnect() override;

  StatusOr<std::string> GetMemoryUsage() override;

 private:
  bool IsInUse(const ObjectID &object_id);

  /// Helper method to read and process the reply of a create request.
  Status HandleCreateReply(const ObjectID &object_id,
                           bool is_experimental_mutable_object,
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
                    ObjectBuffer *object_buffers);

  uint8_t *LookupMmappedFile(MEMFD_TYPE store_fd_val) const;

  ray::PlasmaObjectHeader *GetPlasmaObjectHeader(const PlasmaObject &object) const {
    auto base_ptr = LookupMmappedFile(object.store_fd);
    auto header_ptr = base_ptr + object.header_offset;
    return reinterpret_cast<ray::PlasmaObjectHeader *>(header_ptr);
  }

  void InsertObjectInUse(const ObjectID &object_id,
                         std::unique_ptr<PlasmaObject> object,
                         bool is_sealed);

  void IncrementObjectCount(const ObjectID &object_id);

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

  struct ObjectInUseEntry {
    /// A count of the number of times this client has called PlasmaClient::Create
    /// or
    /// PlasmaClient::Get on this object ID minus the number of calls to
    /// PlasmaClient::Release.
    /// When this count reaches zero, we remove the entry from the ObjectsInUse
    /// and decrement a count in the relevant ClientMmapTableEntry.
    int count = 0;
    /// Cached information to read the object.
    PlasmaObject object;
    /// A flag representing whether the object has been sealed.
    bool is_sealed = false;
  };
  /// A hash table of the object IDs that are currently being used by this
  /// client.
  absl::flat_hash_map<ObjectID, std::unique_ptr<ObjectInUseEntry>> objects_in_use_;

  /// A hash set to record the ids that users want to delete but still in use.
  std::unordered_set<ObjectID> deletion_cache_;
  /// A mutex which protects this class.
  std::recursive_mutex client_mutex_;
  /// Whether the current process should exit when read or write to the connection fails.
  /// It should only be turned on when the plasma client is in a core worker.
  bool exit_on_connection_failure_;
};

}  // namespace plasma
