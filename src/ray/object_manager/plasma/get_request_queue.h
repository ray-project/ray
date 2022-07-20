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

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/object_manager/plasma/connection.h"
#include "ray/object_manager/plasma/object_lifecycle_manager.h"

namespace plasma {
struct GetRequest;
using ObjectReadyCallback = std::function<void(
    const ObjectID &object_id, const std::shared_ptr<GetRequest> &get_request)>;
using AllObjectReadyCallback =
    std::function<void(const std::shared_ptr<GetRequest> &get_request)>;

struct GetRequest {
  GetRequest(instrumented_io_context &io_context,
             const std::shared_ptr<ClientInterface> &client,
             const std::vector<ObjectID> &object_ids,
             bool is_from_worker,
             int64_t num_unique_objects_to_wait_for);
  /// The client that called get.
  std::shared_ptr<ClientInterface> client;
  /// The object IDs involved in this request. This is used in the reply.
  std::vector<ObjectID> object_ids;
  /// The object information for the objects in this request. This is used in
  /// the reply.
  absl::flat_hash_map<ObjectID, PlasmaObject> objects;
  /// The minimum number of objects to wait for in this request.
  const int64_t num_unique_objects_to_wait_for;
  /// The number of object requests in this wait request that are already
  /// satisfied.
  int64_t num_unique_objects_satisfied;
  /// Whether or not the request comes from the core worker. It is used to track the size
  /// of total objects that are consumed by core worker.
  const bool is_from_worker;

  void AsyncWait(int64_t timeout_ms,
                 std::function<void(const boost::system::error_code &)> on_timeout);

  void CancelTimer();

  /// Mark that the get request is removed.
  void MarkRemoved();

  bool IsRemoved() const;

 private:
  /// The timer that will time out and cause this wait to return to
  /// the client if it hasn't already returned.
  boost::asio::steady_timer timer_;
  /// Whether or not if this get request is removed.
  /// Once the get request is removed, any operation on top of the get request shouldn't
  /// happen.
  bool is_removed_ = false;
};

class GetRequestQueue {
 public:
  GetRequestQueue(instrumented_io_context &io_context,
                  IObjectLifecycleManager &object_lifecycle_mgr,
                  ObjectReadyCallback object_callback,
                  AllObjectReadyCallback all_objects_callback)
      : io_context_(io_context),
        object_lifecycle_mgr_(object_lifecycle_mgr),
        object_satisfied_callback_(object_callback),
        all_objects_satisfied_callback_(all_objects_callback) {}

  /// Add a get request to get request queue. Note this will call callback functions
  /// directly if all objects has been satisfied, otherwise store the request
  /// in queue.
  /// \param client the client where the request comes from.
  /// \param object_ids the object ids to get.
  /// \param timeout_ms timeout in millisecond, -1 is used to indicate that no timer
  /// should be set. \param is_from_worker whether the get request from a worker or not.
  /// \param object_callback the callback function called once any object has been
  /// satisfied. \param all_objects_callback the callback function called when all objects
  /// has been satisfied.
  void AddRequest(const std::shared_ptr<ClientInterface> &client,
                  const std::vector<ObjectID> &object_ids,
                  int64_t timeout_ms,
                  bool is_from_worker);

  /// Remove all of the GetRequests for a given client.
  ///
  /// \param client The client whose GetRequests should be removed.
  void RemoveGetRequestsForClient(const std::shared_ptr<ClientInterface> &client);

  /// Handle a sealed object, should be called when an object sealed. Mark
  /// the object satisfied and call object callbacks.
  /// \param object_id the object_id to mark.
  void MarkObjectSealed(const ObjectID &object_id);

 private:
  /// Remove a GetRequest and clean up the relevant data structures.
  ///
  /// \param get_request The GetRequest to remove.
  void RemoveGetRequest(const std::shared_ptr<GetRequest> &get_request);

  /// Only for tests.
  bool IsGetRequestExist(const ObjectID &object_id);
  int64_t GetRequestCount(const ObjectID &object_id);

  /// Called when objects satisfied. Call get request callback function and
  /// remove get request in queue.
  /// \param get_request the get request to be completed.
  void OnGetRequestCompleted(const std::shared_ptr<GetRequest> &get_request);

  instrumented_io_context &io_context_;

  /// A hash table mapping object IDs to a vector of the get requests that are
  /// waiting for the object to arrive.
  absl::flat_hash_map<ObjectID, std::vector<std::shared_ptr<GetRequest>>>
      object_get_requests_;

  IObjectLifecycleManager &object_lifecycle_mgr_;

  ObjectReadyCallback object_satisfied_callback_;
  AllObjectReadyCallback all_objects_satisfied_callback_;

  friend struct GetRequestQueueTest;
};

}  // namespace plasma
