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

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/file_system_monitor.h"
#include "ray/common/ray_config.h"
#include "ray/common/status.h"
#include "ray/object_manager/common.h"
#include "ray/object_manager/plasma/common.h"
#include "ray/object_manager/plasma/connection.h"
#include "ray/object_manager/plasma/create_request_queue.h"
#include "ray/object_manager/plasma/eviction_policy.h"
#include "ray/object_manager/plasma/get_request_queue.h"
#include "ray/object_manager/plasma/object_lifecycle_manager.h"
#include "ray/object_manager/plasma/object_store.h"
#include "ray/object_manager/plasma/plasma.h"
#include "ray/object_manager/plasma/plasma_allocator.h"
#include "ray/object_manager/plasma/protocol.h"

namespace plasma {

using ray::Status;

namespace flatbuf {
enum class PlasmaError;
}  // namespace flatbuf

using flatbuf::PlasmaError;

class PlasmaStore {
 public:
  PlasmaStore(instrumented_io_context &main_service,
              IAllocator &allocator,
              ray::FileSystemMonitor &fs_monitor,
              const std::string &socket_name,
              uint32_t delay_on_oom_ms,
              float object_spilling_threshold,
              ray::SpillObjectsCallback spill_objects_callback,
              std::function<void()> object_store_full_callback,
              ray::AddObjectCallback add_object_callback,
              ray::DeleteObjectCallback delete_object_callback);

  ~PlasmaStore();

  /// Start this store.
  void Start();

  /// Stop this store.
  void Stop();

  /// Return true if the given object id has only one reference.
  /// Only one reference means there's only a raylet that pins the object
  /// so it is safe to spill the object.
  /// NOTE: Avoid using this method outside object spilling context (e.g., unless you
  /// absolutely know what's going on). This method won't work correctly if it is used
  /// before the object is pinned by raylet for the first time.
  bool IsObjectSpillable(const ObjectID &object_id) LOCKS_EXCLUDED(mutex_);

  /// Return the plasma object bytes that are consumed by core workers.
  int64_t GetConsumedBytes();

  /// Get the available memory for new objects to be created. This includes
  /// memory that is currently being used for created but unsealed objects.
  void GetAvailableMemory(std::function<void(size_t)> callback) const
      LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock lock(&mutex_);
    RAY_CHECK((object_lifecycle_mgr_.GetNumBytesUnsealed() > 0 &&
               object_lifecycle_mgr_.GetNumObjectsUnsealed() > 0) ||
              (object_lifecycle_mgr_.GetNumBytesUnsealed() == 0 &&
               object_lifecycle_mgr_.GetNumObjectsUnsealed() == 0))
        << "Tracking for available memory in the plasma store has gone out of sync. "
           "Please file a GitHub issue.";
    RAY_CHECK(object_lifecycle_mgr_.GetNumBytesInUse() >=
              object_lifecycle_mgr_.GetNumBytesUnsealed());
    // We do not count unsealed objects as in use because these may have been
    // created by the object manager.
    int64_t num_bytes_in_use = object_lifecycle_mgr_.GetNumBytesInUse() -
                               object_lifecycle_mgr_.GetNumBytesUnsealed();
    size_t available = 0;
    if (num_bytes_in_use < allocator_.GetFootprintLimit()) {
      available = allocator_.GetFootprintLimit() - num_bytes_in_use;
    }
    callback(available);
  }

 private:
  /// Create a new object. The client must do a call to release_object to tell
  /// the store when it is done with the object.
  ///
  /// \param object_info Ray object info.
  /// \param client The client that created the object.
  /// \param fallback_allocator Whether to allow falling back to the fs allocator
  /// \param result The object that has been created.
  /// \return One of the following error codes:
  ///  - PlasmaError::OK, if the object was created successfully.
  ///  - PlasmaError::ObjectExists, if an object with this ID is already
  ///    present in the store. In this case, the client should not call
  ///    plasma_release.
  ///  - PlasmaError::OutOfMemory, if the store is out of memory and
  ///    cannot create the object. In this case, the client should not call
  ///    plasma_release.
  PlasmaError CreateObject(const ray::ObjectInfo &object_info,
                           plasma::flatbuf::ObjectSource source,
                           const std::shared_ptr<Client> &client,
                           bool fallback_allocator,
                           PlasmaObject *result) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Abort a created but unsealed object. If the client is not the
  /// creator, then the abort will fail.
  ///
  /// \param object_id Object ID of the object to be aborted.
  /// \param client The client who created the object. If this does not
  ///   match the creator of the object, then the abort will fail.
  /// \return 1 if the abort succeeds, else 0.
  int AbortObject(const ObjectID &object_id, const std::shared_ptr<Client> &client)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Delete a specific object by object_id that have been created in the hash table.
  ///
  /// \param object_id Object ID of the object to be deleted.
  /// \return One of the following error codes:
  ///  - PlasmaError::OK, if the object was delete successfully.
  ///  - PlasmaError::ObjectNonexistent, if ths object isn't existed.
  ///  - PlasmaError::ObjectInUse, if the object is in use.
  PlasmaError DeleteObject(ObjectID &object_id) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

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
  void ProcessGetRequest(const std::shared_ptr<Client> &client,
                         const std::vector<ObjectID> &object_ids,
                         int64_t timeout_ms,
                         bool is_from_worker) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Process queued requests to create an object.
  void ProcessCreateRequests() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Seal a vector of objects. The objects are now immutable and can be accessed with
  /// get.
  ///
  /// \param object_ids The vector of Object IDs of the objects to be sealed.
  void SealObjects(const std::vector<ObjectID> &object_ids)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Record the fact that a particular client is no longer using an object.
  ///
  /// \param object_id The object ID of the object that is being released.
  /// \param client The client making this request.
  void ReleaseObject(const ObjectID &object_id, const std::shared_ptr<Client> &client)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Connect a new client to the PlasmaStore.
  ///
  /// \param error The error code from the acceptor.
  void ConnectClient(const boost::system::error_code &error)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Disconnect a client from the PlasmaStore.
  ///
  /// \param client The client that is disconnected.
  void DisconnectClient(const std::shared_ptr<Client> &client)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status ProcessMessage(const std::shared_ptr<Client> &client,
                        plasma::flatbuf::MessageType type,
                        const std::vector<uint8_t> &message) LOCKS_EXCLUDED(mutex_);

  PlasmaError HandleCreateObjectRequest(const std::shared_ptr<Client> &client,
                                        const std::vector<uint8_t> &message,
                                        bool fallback_allocator,
                                        PlasmaObject *object,
                                        bool *spilling_required)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void ReplyToCreateClient(const std::shared_ptr<Client> &client,
                           const ObjectID &object_id,
                           uint64_t req_id) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void AddToClientObjectIds(const ObjectID &object_id,
                            const std::shared_ptr<ClientInterface> &client)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void ReturnFromGet(const std::shared_ptr<GetRequest> &get_request);

  int RemoveFromClientObjectIds(const ObjectID &object_id,
                                const std::shared_ptr<Client> &client)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Start listening for clients.
  void DoAccept();

  void PrintAndRecordDebugDump() const LOCKS_EXCLUDED(mutex_);

  std::string GetDebugDump() const EXCLUSIVE_LOCKS_REQUIRED(mutex_);

 private:
  friend class GetRequestQueue;

  void ScheduleRecordMetrics() const LOCKS_EXCLUDED(mutex_);

  // A reference to the asio io context.
  instrumented_io_context &io_context_;
  /// The name of the socket this object store listens on.
  std::string socket_name_;
  /// An acceptor for new clients.
  boost::asio::basic_socket_acceptor<ray::local_stream_protocol> acceptor_;
  /// The socket to listen on for new clients.
  ray::local_stream_socket socket_;

  /// This mutex is used in order to make plasma store threas-safe with raylet.
  /// Raylet's local_object_manager needs to ping access plasma store's method in order to
  /// figure out the correct view of the object store. recursive_mutex is used to avoid
  /// deadlock while we keep the simplest possible change. NOTE(sang): Avoid adding more
  /// interface that node manager or object manager can access the plasma store with this
  /// mutex if it is not absolutely necessary.
  mutable absl::Mutex mutex_;

  /// The allocator that allocates mmaped memory.
  IAllocator &allocator_ GUARDED_BY(mutex_);

  /// Monitor the disk utilization.
  ray::FileSystemMonitor &fs_monitor_;

  /// A callback to asynchronously notify that an object is sealed.
  /// NOTE: This function should guarantee the thread-safety because the callback is
  /// shared with the main raylet thread.
  const ray::AddObjectCallback add_object_callback_;

  /// A callback to asynchronously notify that an object is deleted.
  /// NOTE: This function should guarantee the thread-safety because the callback is
  /// shared with the main raylet thread.
  const ray::DeleteObjectCallback delete_object_callback_;

  ObjectLifecycleManager object_lifecycle_mgr_ GUARDED_BY(mutex_);

  /// The amount of time to wait before retrying a creation request after an
  /// OOM error.
  const uint32_t delay_on_oom_ms_;

  /// The percentage of object store memory used above which spilling is triggered.
  const float object_spilling_threshold_;

  /// A timer that is set when the first request in the queue is not
  /// serviceable because there is not enough memory. The request will be
  /// retried when this timer expires.
  std::shared_ptr<boost::asio::deadline_timer> create_timer_ GUARDED_BY(mutex_);

  /// Timer for printing debug information.
  mutable std::shared_ptr<boost::asio::deadline_timer> stats_timer_ GUARDED_BY(mutex_);

  /// Timer for recording object store metrics.
  mutable std::shared_ptr<boost::asio::deadline_timer> metric_timer_ GUARDED_BY(mutex_);

  /// Queue of object creation requests.
  CreateRequestQueue create_request_queue_ GUARDED_BY(mutex_);

  /// Total plasma object bytes that are consumed by core workers.
  std::atomic<int64_t> total_consumed_bytes_;

  /// Whether we have dumped debug information on OOM yet. This limits dump
  /// (which can be expensive) to once per OOM event.
  bool dumped_on_oom_ GUARDED_BY(mutex_) = false;

  GetRequestQueue get_request_queue_ GUARDED_BY(mutex_);
};

}  // namespace plasma
