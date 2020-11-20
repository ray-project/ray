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

#include "ray/common/status.h"
#include "ray/object_manager/plasma/common.h"
#include "ray/object_manager/plasma/connection.h"
#include "ray/object_manager/plasma/plasma.h"
#include "ray/object_manager/plasma/protocol.h"

namespace plasma {

class CreateRequestQueue {
 public:
  using CreateObjectCallback =
      std::function<PlasmaError(bool evict_if_full, PlasmaObject *result)>;

  CreateRequestQueue(int32_t max_retries, bool evict_if_full,
                     std::function<void()> on_store_full)
      : max_retries_(max_retries),
        evict_if_full_(evict_if_full),
        on_store_full_(on_store_full) {
    RAY_LOG(DEBUG) << "Starting plasma::CreateRequestQueue with " << max_retries_
                   << " retries on OOM, evict if full? " << (evict_if_full_ ? 1 : 0);
  }

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
  /// \return A request ID that can be used to get the result.
  uint64_t AddRequest(const ObjectID &object_id,
                      const std::shared_ptr<ClientInterface> &client,
                      const CreateObjectCallback &create_callback);

  bool GetRequestResult(uint64_t req_id, PlasmaObject *result, PlasmaError *error);

  /// Try to fulfill a request immediately, for clients that cannot retry.
  ///
  /// \param object_id The ID of the object to create.
  /// \param client The client that sent the request. This is used as a key to
  /// drop this request if the client disconnects.
  /// \param create_callback A callback to attempt to create the object.
  /// \return The result of the call. This will return an out-of-memory error
  /// if there are other requests queued or there is not enough space left in
  /// the object store, this will return an out-of-memory error.
  std::pair<PlasmaObject, PlasmaError> TryRequestImmediately(
      const ObjectID &object_id, const std::shared_ptr<ClientInterface> &client,
      const CreateObjectCallback &create_callback);

  /// Process requests in the queue.
  ///
  /// This will try to process as many requests in the queue as possible, in
  /// FIFO order. If the first request is not serviceable, this will break and
  /// the caller should try again later.
  ///
  /// \return Bad PlasmaError for the first request in the queue if it failed to be
  /// serviced, or OK if all requests were fulfilled.
  Status ProcessRequests();

  /// Remove all requests that were made by a client that is now disconnected.
  ///
  /// \param client The client that was disconnected.
  void RemoveDisconnectedClientRequests(const std::shared_ptr<ClientInterface> &client);

 private:
  struct CreateRequest {
    CreateRequest(const ObjectID &object_id, uint64_t request_id,
                  const std::shared_ptr<ClientInterface> &client,
                  CreateObjectCallback create_callback)
        : object_id(object_id),
          request_id(request_id),
          client(client),
          create_callback(create_callback) {}

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

    // The results of the creation call. These should be sent back to the
    // client once ready.
    PlasmaError error = PlasmaError::OK;
    PlasmaObject result = {};
  };

  /// Process a single request. Sets the request's error result to the error
  /// returned by the request handler inside. Returns OK if the request can be
  /// finished.
  Status ProcessRequest(std::unique_ptr<CreateRequest> &request);

  /// Finish a queued request and remove it from the queue.
  void FinishRequest(std::list<std::unique_ptr<CreateRequest>>::iterator request_it);

  uint64_t next_req_id_ = 1;

  /// The maximum number of times to retry each request upon OOM.
  const int32_t max_retries_;

  /// The number of times the request at the head of the queue has been tried.
  int32_t num_retries_ = 0;

  /// On the first attempt to create an object, whether to evict from the
  /// object store to make space. If the first attempt fails, then we will
  /// always try to evict.
  const bool evict_if_full_;

  /// A callback to call if the object store is full.
  const std::function<void()> on_store_full_;

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

  absl::flat_hash_map<uint64_t, std::unique_ptr<CreateRequest>> fulfilled_requests_;

  friend class CreateRequestQueueTest;
};

}  // namespace plasma
