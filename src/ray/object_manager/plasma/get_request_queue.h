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
#include "ray/object_manager/plasma/connection.h"

namespace plasma {

struct GetRequest {
  GetRequest(instrumented_io_context &io_context, const std::shared_ptr<Client> &client,
             const std::vector<ObjectID> &object_ids, bool is_from_worker);
  /// The client that called get.
  std::shared_ptr<Client> client;
  /// The object IDs involved in this request. This is used in the reply.
  std::vector<ObjectID> object_ids;
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

GetRequest::GetRequest(instrumented_io_context &io_context,
                       const std::shared_ptr<Client> &client,
                       const std::vector<ObjectID> &object_ids, bool is_from_worker)
    : client(client),
      object_ids(object_ids.begin(), object_ids.end()),
      objects(object_ids.size()),
      num_satisfied(0),
      is_from_worker(is_from_worker),
      timer_(io_context) {
  std::unordered_set<ObjectID> unique_ids(object_ids.begin(), object_ids.end());
  num_objects_to_wait_for = unique_ids.size();
};

class GetRequestQueue {
 public:
  using ObjectReadyCallback = std::function<void(const ObjectID &object_id, std::shared_ptr<Client> client)>;

  GetRequestQueue(instrumented_io_context &io_context);
  void AddRequest(const std::shared_ptr<Client> &client, const std::vector<ObjectID> &object_ids, bool is_from_worker,
  			ObjectReadyCallback &callback);
  void RemoveDisconnectedClientRequests(const std::shared_ptr<ClientInterface> &client);
};

}  // namespace plasma