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

struct GetRequest {
  GetRequest(instrumented_io_context &io_context,
             const std::shared_ptr<ClientInterface> &client,
             const std::vector<ObjectID> &object_ids, bool is_from_worker,
             std::function<void(const std::shared_ptr<GetRequest> &get_req)> &callback);
  /// The client that called get.
  std::shared_ptr<ClientInterface> client;
  /// The object IDs involved in this request. This is used in the reply.
  std::vector<ObjectID> object_ids;
  std::unordered_map<ObjectID, bool> object_satisfied;
  /// The object information for the objects in this request. This is used in
  /// the reply.
  std::unordered_map<ObjectID, PlasmaObject> objects;
  /// The minimum number of objects to wait for in this request.
  int64_t num_objects_to_wait_for;
  /// The number of object requests in this wait request that are already
  /// satisfied.
  int64_t num_satisfied;
  /// Whether or not the request comes from the core worker. It is used to track the size
  /// of total objects that are consumed by core worker.
  bool is_from_worker;

  std::function<void(const std::shared_ptr<GetRequest> &get_req)> callback;

  void AsyncWait(int64_t timeout_ms,
                 std::function<void(const boost::system::error_code &)> on_timeout) {
    RAY_CHECK(!is_removed_);
    // Set an expiry time relative to now.
    timer_.expires_from_now(std::chrono::milliseconds(timeout_ms));
    timer_.async_wait(on_timeout);
  }

  void CancelTimer() {
    RAY_CHECK(!is_removed_);
    timer_.cancel();
  }

  /// Mark that the get request is removed.
  void MarkRemoved() {
    RAY_CHECK(!is_removed_);
    is_removed_ = true;
  }

  bool IsRemoved() const { return is_removed_; }

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
  using ObjectReadyCallback =
      std::function<void(const std::shared_ptr<GetRequest> &get_req)>;

  GetRequestQueue(instrumented_io_context &io_context,
                  IObjectLifecycleManager &object_lifecycle_mgr)
      : io_context_(io_context), object_lifecycle_mgr_(object_lifecycle_mgr) {}
  void AddRequest(const std::shared_ptr<ClientInterface> &client,
                  const std::vector<ObjectID> &object_ids, int64_t timeout_ms,
                  bool is_from_worker, ObjectReadyCallback callback);

  /// Remove all of the GetRequests for a given client.
  ///
  /// \param client The client whose GetRequests should be removed.
  void RemoveGetRequestsForClient(const std::shared_ptr<ClientInterface> &client);

  void ObjectSealed(const ObjectID &object_id);

 private:
  /// Remove a GetRequest and clean up the relevant data structures.
  ///
  /// \param get_request The GetRequest to remove.
  void RemoveGetRequest(const std::shared_ptr<GetRequest> &get_request);

  /// Only for tests.
  bool IsGetRequestExist(const ObjectID &object_id);

  instrumented_io_context &io_context_;

  /// The object store stores created objects.
  /// A hash table mapping object IDs to a vector of the get requests that are
  /// waiting for the object to arrive.
  std::unordered_map<ObjectID, std::vector<std::shared_ptr<GetRequest>>>
      object_get_requests_;

  IObjectLifecycleManager &object_lifecycle_mgr_;

  FRIEND_TEST(GetRequestQueueTest, TestAddRequest);
};

}  // namespace plasma