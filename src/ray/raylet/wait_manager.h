// Copyright 2022 The Ray Authors.
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

#include "ray/common/id.h"

namespace ray {
namespace raylet {

/// This class is responsible for handling wait requests with `fetch_local=true`.
/// It is not thread safe and is expected to run ONLY in
/// the NodeManager.io_service_ thread.
class WaitManager {
 public:
  WaitManager(
      const std::function<bool(const ray::ObjectID &)> is_object_local,
      const std::function<void(std::function<void()>, int64_t delay_ms)> delay_executor)
      : is_object_local_(is_object_local),
        delay_executor_(delay_executor),
        next_wait_id_(0) {}

  using WaitCallback = std::function<void(const std::vector<ray::ObjectID> &ready,
                                          const std::vector<ray::ObjectID> &remaining)>;
  /// Wait until either num_required_objects are locally available
  /// or timeout_ms has elapsed, then invoke the provided callback.
  ///
  /// \param object_ids The object ids to wait on.
  /// \param timeout_ms The time in milliseconds to wait before invoking the callback.
  /// \param num_required_objects The minimum number of objects required before
  /// invoking the callback.
  /// \param callback Invoked when either timeout_ms is satisfied OR num_required_objects
  /// is satisfied.
  void Wait(const std::vector<ray::ObjectID> &object_ids,
            int64_t timeout_ms,
            uint64_t num_required_objects,
            const WaitCallback &callback);

  /// This is invoked whenever an object becomes locally available.
  ///
  /// \param object_id The object ID of the object that is locally
  /// available.
  void HandleObjectLocal(const ray::ObjectID &object_id);

  std::string DebugString() const;

 private:
  struct WaitRequest {
    WaitRequest(int64_t timeout_ms,
                const WaitCallback &callback,
                const std::vector<ObjectID> &object_ids,
                uint64_t num_required_objects)
        : timeout_ms(timeout_ms),
          callback(callback),
          object_ids(object_ids),
          num_required_objects(num_required_objects) {}
    /// The period of time to wait before invoking the callback.
    const int64_t timeout_ms;
    /// The callback invoked when Wait is complete.
    WaitCallback callback;
    /// Ordered input object_ids.
    const std::vector<ObjectID> object_ids;
    /// The number of required objects.
    const uint64_t num_required_objects;
    /// The objects that have been locally available.
    std::unordered_set<ObjectID> ready;
  };

  /// Completion handler for Wait.
  /// A wait is complete once the required objects are local
  /// at some point. There is no guarantee that they are
  /// still local and will always be local during and after this
  /// method call.
  void WaitComplete(uint64_t wait_id);

  /// A callback which should return true if a given object is
  /// locally available.
  const std::function<bool(const ObjectID &)> is_object_local_;

  const std::function<void(std::function<void()>, int64_t delay_ms)> delay_executor_;

  /// A set of active wait requests.
  std::unordered_map<uint64_t, WaitRequest> wait_requests_;

  /// Map from object to wait requests that are waiting for this object.
  std::unordered_map<ObjectID, std::unordered_set<uint64_t>> object_to_wait_requests_;

  uint64_t next_wait_id_;

  friend class WaitManagerTest;
};

}  // namespace raylet
}  // namespace ray
