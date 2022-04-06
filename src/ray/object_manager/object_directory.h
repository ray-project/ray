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
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/gcs/gcs_client/gcs_client.h"
#include "ray/object_manager/common.h"

namespace ray {

/// Connection information for remote object managers.
struct RemoteConnectionInfo {
  RemoteConnectionInfo(const NodeID &id) : node_id(id) {}

  // Returns whether there is enough information to connect to the remote
  // object manager.
  bool Connected() const { return !ip.empty(); }

  NodeID node_id;
  std::string ip;
  uint16_t port;
};

/// Callback for object location notifications.
using OnLocationsFound = std::function<void(const ray::ObjectID &object_id,
                                            const std::unordered_set<ray::NodeID> &,
                                            const std::string &,
                                            const NodeID &,
                                            bool pending_creation,
                                            size_t object_size)>;

class IObjectDirectory {
 public:
  virtual ~IObjectDirectory() {}

  /// Lookup how to connect to a remote object manager.
  ///
  /// \param connection_info The connection information to fill out. This
  /// should be pre-populated with the requested node ID. If the directory
  /// has information about the requested node, then the rest of the fields
  /// in this struct will be populated accordingly.
  virtual void LookupRemoteConnectionInfo(
      RemoteConnectionInfo &connection_info) const = 0;

  /// Get information for all connected remote object managers.
  ///
  /// \return A vector of information for all connected remote object managers.
  virtual std::vector<RemoteConnectionInfo> LookupAllRemoteConnections() const = 0;

  /// Handle the removal of an object manager node. This updates the
  /// locations of all subscribed objects that have the removed node as a
  /// location, and fires the subscribed callbacks for those objects.
  ///
  /// \param node_id The object manager node that was removed.
  virtual void HandleNodeRemoved(const NodeID &node_id) = 0;

  /// Subscribe to be notified of locations (NodeID) of the given object.
  /// The callback will be invoked with the complete list of known locations
  /// whenever the set of locations changes. The callback will also be fired if
  /// the list of known locations is empty. The callback provided to this
  /// method may fire immediately, within the call to this method, if any other
  /// listener is subscribed to the same object: This occurs when location data
  /// for the object has already been obtained.
  ///
  /// \param callback_id The id associated with the specified callback. This is
  /// needed when UnsubscribeObjectLocations is called.
  /// \param object_id The required object's ObjectID.
  /// \param success_cb Invoked with non-empty list of node ids and object_id.
  /// \return Status of whether subscription succeeded.
  virtual ray::Status SubscribeObjectLocations(const UniqueID &callback_id,
                                               const ObjectID &object_id,
                                               const rpc::Address &owner_address,
                                               const OnLocationsFound &callback) = 0;

  /// Unsubscribe to object location notifications.
  ///
  /// \param callback_id The id associated with a callback. This was given
  /// at subscription time, and unsubscribes the corresponding callback from
  /// further notifications about the given object's location.
  /// \param object_id The object id invoked with Subscribe.
  /// \return Status of unsubscribing from object location notifications.
  virtual ray::Status UnsubscribeObjectLocations(const UniqueID &callback_id,
                                                 const ObjectID &object_id) = 0;

  /// Report objects added to this node's store to the object directory.
  ///
  /// \param object_id The object id that was put into the store.
  /// \param node_id The node id corresponding to this node.
  /// \param object_info Additional information about the object.
  virtual void ReportObjectAdded(const ObjectID &object_id,
                                 const NodeID &node_id,
                                 const ObjectInfo &object_info) = 0;

  /// Report objects removed from this node's store to the object directory.
  ///
  /// \param object_id The object id that was removed from the store.
  /// \param node_id The node id corresponding to this node.
  /// \param object_info Additional information about the object.
  virtual void ReportObjectRemoved(const ObjectID &object_id,
                                   const NodeID &node_id,
                                   const ObjectInfo &object_info) = 0;

  /// Record metrics.
  virtual void RecordMetrics(uint64_t duration_ms) = 0;

  /// Returns debug string for class.
  ///
  /// \return string.
  virtual std::string DebugString() const = 0;
};

}  // namespace ray
