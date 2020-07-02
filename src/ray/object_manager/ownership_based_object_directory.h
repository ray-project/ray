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

#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/object_manager/format/object_manager_generated.h"
#include "ray/object_manager/object_directory.h"

namespace ray {

/// Ray OwnershipBasedObjectDirectory declaration.
class OwnershipBasedObjectDirectory : public ObjectDirectoryInterface {
 public:
  /// Create an object directory.
  ///
  /// \param io_service The event loop to dispatch callbacks to. This should
  /// usually be the same event loop that the given gcs_client runs on.
  /// \param gcs_client A Ray GCS client to request object and client
  /// information from.
  OwnershipBasedObjectDirectory(boost::asio::io_service &io_service);

  virtual ~OwnershipBasedObjectDirectory() {}

  void LookupRemoteConnectionInfo(RemoteConnectionInfo &connection_info) const override;

  std::vector<RemoteConnectionInfo> LookupAllRemoteConnections() const override;

  ray::Status LookupLocations(const ObjectID &object_id,
                              const OnLocationsFound &callback) override;

  void HandleClientRemoved(const ClientID &client_id) override;

  ray::Status SubscribeObjectLocations(const UniqueID &callback_id,
                                       const ObjectID &object_id,
                                       const OnLocationsFound &callback) override;
  ray::Status UnsubscribeObjectLocations(const UniqueID &callback_id,
                                         const ObjectID &object_id) override;

  ray::Status ReportObjectAdded(
      const ObjectID &object_id, const ClientID &client_id,
      const object_manager::protocol::ObjectInfoT &object_info) override;
  ray::Status ReportObjectRemoved(
      const ObjectID &object_id, const ClientID &client_id,
      const object_manager::protocol::ObjectInfoT &object_info) override;

  std::string DebugString() const override;

  /// OwnershipBasedObjectDirectory should not be copied.
  RAY_DISALLOW_COPY_AND_ASSIGN(OwnershipBasedObjectDirectory);

 private:
  /// Callbacks associated with a call to GetLocations.
  struct LocationListenerState {
    /// The callback to invoke when object locations are found.
    std::unordered_map<UniqueID, OnLocationsFound> callbacks;
    /// The current set of known locations of this object.
    std::unordered_set<ClientID> current_object_locations;
    /// This flag will get set to true if received any notification of the object.
    /// It means current_object_locations is up-to-date with GCS. It
    /// should never go back to false once set to true. If this is true, and
    /// the current_object_locations is empty, then this means that the object
    /// does not exist on any nodes due to eviction or the object never getting created.
    bool subscribed;
  };

  /// Reference to the event loop.
  boost::asio::io_service &io_service_;
  /// Info about subscribers to object locations.
  std::unordered_map<ObjectID, LocationListenerState> listeners_;
};

}  // namespace ray
