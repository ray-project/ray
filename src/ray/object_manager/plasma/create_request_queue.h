// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "ray/common/file_system_monitor.h"
#include "ray/common/status.h"
#include "ray/object_manager/common.h"
#include "ray/object_manager/plasma/common.h"
#include "ray/object_manager/plasma/connection.h"
#include "ray/object_manager/plasma/plasma.h"
#include "ray/object_manager/plasma/protocol.h"

namespace plasma {

class CreateRequestQueue {
 public:
  using CreateObjectCallback = std::function<PlasmaError(
      bool fallback_allocator, PlasmaObject *result, bool *spilling_required)>;

  CreateRequestQueue(ray::FileSystemMonitor &fs_monitor,
                     int64_t oom_grace_period_s,
                     ray::SpillObjectsCallback spill_objects_callback,
                     std::function<void()> trigger_global_gc,
                     std::function<int64_t()> get_time,
                     std::function<std::string()> dump_debug_info_callback = nullptr)
      : fs_monitor_(fs_monitor),
        oom_grace_period_ns_(oom_grace_period_s * 1e9),
        spill_objects_callback_(spill_objects_callback),
        trigger_global_gc_(trigger_global_gc),
        get_time_(get_time),
        dump_debug_info_callback_(dump_debug_info_callback) {}

  /// Add a request to the queue. The caller should use the returned request ID
  /// to later get the result of the request.
  ///
  /// The request may not get tried immediately if the head of the queue is not
  /// serviceable.
  ///
  /// \param object_id The ID of the object to create.
  /// \param client The client that sent the request. This is used as a key to
  /// drop this request if the client disconnects.
  /// \param create_callback A callback to attempt to create the object.
  /// \param object_size Object size in bytes.
  /// \return A request ID that can be used to get the result.
  uint64_t AddRequest(const ObjectID &object_id,
                      const std::shared_ptr<ClientInterface> &client,
                      const CreateObjectCallback &create_callback,
                      const size_t object_size);

  /// Get the result of a request.
  ///
  /// This method should only be called with a request ID returned by a
  /// previous call to add a request. The result will be popped, so this method
  /// should not be called again with the same request ID once a result is
  /// returned. If either of these is violated, an error will be returned.
  ///
  /// \param[in] req_id The request ID that was returned to the caller by a
  /// previous call to add a request.
  /// \param[out] result The resulting object information will be stored here,
  /// if the request was finished and successful.
  /// \param[out] error The error code returned by the creation handler will be
  /// stored here, if the request finished. This will also return an error if
  /// there is no information about this request ID.
  /// \return Whether the result and error are ready. This returns false if the
  /// request is still pending.
  bool GetRequestResult(uint64_t req_id, PlasmaObject *result, PlasmaError *error);

  /// Try to fulfill a request immediately, for clients that cannot retry.
  ///
  /// \param object_id The ID of the object to create.
  /// \param client The client that sent the request. This is used as a key to
  /// drop this request if the client disconnects.
  /// \param create_callback A callback to attempt to create the object.
  /// \param object_size Object size in bytes.
  /// \return The result of the call. This will return an out-of-memory error
  /// if there are other requests queued or there is not enough space left in
  /// the object store, this will return an out-of-memory error.
  std::pair<PlasmaObject, PlasmaError> TryRequestImmediately(
      const ObjectID &object_id,
      const std::shared_ptr<ClientInterface> &client,
      const CreateObjectCallback &create_callback,
      size_t object_size);

  /// Process requests in the queue.
  ///
  /// This will try to process as many requests in the queue as possible, in
  /// FIFO order. If the first request is not serviceable, this will break and
  /// the caller should try again later.
  ///
  /// \return Bad status for the first request in the queue if it failed to be
  /// serviced, or OK if all requests were fulfilled.
  Status ProcessRequests();

  /// Remove all requests that were made by a client that is now disconnected.
  ///
  /// \param client The client that was disconnected.
  void RemoveDisconnectedClientRequests(const std::shared_ptr<ClientInterface> &client);

  size_t NumPendingRequests() const { return queue_.size(); }

  size_t NumPendingBytes() const { return num_bytes_pending_; }

 private:
  struct CreateRequest {
    CreateRequest(const ObjectID &object_id,
                  uint64_t request_id,
                  const std::shared_ptr<ClientInterface> &client,
                  CreateObjectCallback create_callback,
                  size_t object_size)
        : object_id(object_id),
          request_id(request_id),
          client(client),
          create_callback(create_callback),
          object_size(object_size) {}

    // The ObjectID to create.
    const ObjectID object_id;

    // A request ID that can be returned to the caller to get the result once
    // ready.
    const uint64_t request_id;

    // A pointer to the client, used as a key to delete requests that were made
    // by a client that is now disconnected.
    const std::shared_ptr<ClientInterface> client;

    // A callback to attempt to create the object.
    const CreateObjectCallback create_callback;

    const size_t object_size;

    // The results of the creation call. These should be sent back to the
    // client once ready.
    PlasmaError error = PlasmaError::OK;
    PlasmaObject result = {};
  };

  /// Process a single request. Sets the request's error result to the error
  /// returned by the request handler inside. Returns OK if the request can be
  /// finished.
  Status ProcessRequest(bool fallback_allocator,
                        std::unique_ptr<CreateRequest> &request,
                        bool *spilling_required);

  /// Finish a queued request and remove it from the queue.
  void FinishRequest(std::list<std::unique_ptr<CreateRequest>>::iterator request_it);

  /// Monitor the disk utilization.
  ray::FileSystemMonitor &fs_monitor_;

  /// The next request ID to assign, so that the caller can get the results of
  /// a request by retrying. Start at 1 because 0 means "do not retry".
  uint64_t next_req_id_ = 1;

  /// Grace period until we throw the OOM error to the application.
  /// -1 means grace period is infinite.
  const int64_t oom_grace_period_ns_;

  /// A callback to trigger object spilling. It tries to spill objects upto max
  /// throughput. It returns true if space is made by object spilling, and false if
  /// there's no more space to be made.
  const ray::SpillObjectsCallback spill_objects_callback_;

  /// A callback to trigger global GC in the cluster if the object store is
  /// full.
  const std::function<void()> trigger_global_gc_;

  /// A callback to return the current time.
  const std::function<int64_t()> get_time_;

  /// Sink for debug info.
  const std::function<std::string()> dump_debug_info_callback_;

  /// Queue of object creation requests to respond to. Requests will be placed
  /// on this queue if the object store does not have enough room at the time
  /// that the client made the creation request, but space may be made through
  /// object spilling. Once the raylet notifies us that objects have been
  /// spilled, we will attempt to process these requests again and respond to
  /// the client if successful or out of memory. If more objects must be
  /// spilled, the request will be replaced at the head of the queue.
  /// TODO(swang): We should also queue objects here even if there is no room
  /// in the object store. Then, the client does not need to poll on an
  /// OutOfMemory error and we can just respond to them once there is enough
  /// space made, or after a timeout.
  std::list<std::unique_ptr<CreateRequest>> queue_;

  /// A buffer of the results of fulfilled requests. The value will be null
  /// while the request is pending and will be set once the request has
  /// finished.
  absl::flat_hash_map<uint64_t, std::unique_ptr<CreateRequest>> fulfilled_requests_;

  /// Last time global gc was invoked in ms.
  uint64_t last_global_gc_ms_;

  /// The time OOM timer first starts. It becomes -1 upon every creation success.
  int64_t oom_start_time_ns_ = -1;

  size_t num_bytes_pending_ = 0;

  friend class CreateRequestQueueTest;
};

}  // namespace plasma
