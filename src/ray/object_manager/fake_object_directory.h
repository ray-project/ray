// Copyright The Ray Authors.
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

#include <functional>
#include <string>
#include <vector>

#include "ray/object_manager/object_directory.h"

namespace ray {

// Hand-written fake for IObjectDirectory. Methods are no-ops that record their
// calls; a settable hook lets tests drive SubscribeObjectLocations callbacks.
class FakeObjectDirectory : public IObjectDirectory {
 public:
  void HandleNodeRemoved(const NodeID &node_id) override {
    handle_node_removed_calls.push_back(node_id);
  }

  void SubscribeObjectLocations(const UniqueID &callback_id,
                                const ObjectID &object_id,
                                const rpc::Address &owner_address,
                                const OnLocationsFound &callback) override {
    subscribed_objects.push_back(object_id);
    if (on_subscribe) {
      on_subscribe(callback_id, object_id, owner_address, callback);
    }
  }

  void UnsubscribeObjectLocations(const UniqueID &callback_id,
                                  const ObjectID &object_id) override {
    unsubscribed_objects.push_back(object_id);
  }

  void ReportObjectAdded(const ObjectID &object_id,
                         const NodeID &node_id,
                         const ObjectInfo &object_info) override {
    added_objects.push_back(object_id);
  }

  void ReportObjectRemoved(const ObjectID &object_id,
                           const NodeID &node_id,
                           const ObjectInfo &object_info) override {
    removed_objects.push_back(object_id);
  }

  void ReportObjectSpilled(const ObjectID &object_id,
                           const NodeID &node_id,
                           const rpc::Address &owner_address,
                           const std::string &spilled_url,
                           const ObjectID &generator_id,
                           const bool spilled_to_local_storage) override {
    spilled_objects.push_back(object_id);
  }

  void RecordMetrics(uint64_t duration_ms) override {}

  std::string DebugString() const override { return "FakeObjectDirectory"; }

  // Optional hook to drive location-found callbacks; recorded calls otherwise.
  std::function<void(
      const UniqueID &, const ObjectID &, const rpc::Address &, const OnLocationsFound &)>
      on_subscribe;
  std::vector<NodeID> handle_node_removed_calls;
  std::vector<ObjectID> subscribed_objects;
  std::vector<ObjectID> unsubscribed_objects;
  std::vector<ObjectID> added_objects;
  std::vector<ObjectID> removed_objects;
  std::vector<ObjectID> spilled_objects;
};

}  // namespace ray
