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

#include "absl/container/flat_hash_map.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/gcs/gcs_client.h"
#include "ray/object_manager/object_directory.h"
#include "ray/pubsub/subscriber.h"
#include "ray/rpc/worker/core_worker_client.h"
#include "ray/util/sequencer.h"

namespace ray {

/// Ray OwnershipBasedObjectDirectory declaration.
class OwnershipBasedObjectDirectory : public IObjectDirectory {
 public:
  /// Create an ownership based object directory.
  ///
  /// \param io_service The event loop to dispatch callbacks to. This should
  /// usually be the same event loop that the given gcs_client runs on.
  /// \param gcs_client A Ray GCS client to request object and node
  /// information from.
  OwnershipBasedObjectDirectory(
      instrumented_io_context &io_service, std::shared_ptr<gcs::GcsClient> &gcs_client,
      pubsub::SubscriberInterface *object_location_subscriber,
      std::function<void(const ObjectID &, const rpc::ErrorType &)> mark_as_failed);

  virtual ~OwnershipBasedObjectDirectory() {}

  void LookupRemoteConnectionInfo(RemoteConnectionInfo &connection_info) const override;

  std::vector<RemoteConnectionInfo> LookupAllRemoteConnections() const override;

  void HandleNodeRemoved(const NodeID &node_id) override;

  ray::Status LookupLocations(const ObjectID &object_id,
                              const rpc::Address &owner_address,
                              const OnLocationsFound &callback) override;

  ray::Status SubscribeObjectLocations(const UniqueID &callback_id,
                                       const ObjectID &object_id,
                                       const rpc::Address &owner_address,
                                       const OnLocationsFound &callback) override;
  ray::Status UnsubscribeObjectLocations(const UniqueID &callback_id,
                                         const ObjectID &object_id) override;

  ray::Status ReportObjectAdded(const ObjectID &object_id, const NodeID &node_id,
                                const ObjectInfo &object_info) override;
  ray::Status ReportObjectRemoved(const ObjectID &object_id, const NodeID &node_id,
                                  const ObjectInfo &object_info) override;

  void RecordMetrics(uint64_t duration_ms) override;

  std::string DebugString() const override;

  /// OwnershipBasedObjectDirectory should not be copied.
  RAY_DISALLOW_COPY_AND_ASSIGN(OwnershipBasedObjectDirectory);

 private:
  /// Callbacks associated with a call to GetLocations.
  struct LocationListenerState {
    /// The callback to invoke when object locations are found.
    std::unordered_map<UniqueID, OnLocationsFound> callbacks;
    /// The current set of known locations of this object.
    std::unordered_set<NodeID> current_object_locations;
    /// The location where this object has been spilled, if any.
    std::string spilled_url = "";
    // The node id that spills the object to the disk.
    // It will be Nil if it uses a distributed external storage.
    NodeID spilled_node_id = NodeID::Nil();
    /// The size of the object.
    size_t object_size = 0;
    /// This flag will get set to true if received any notification of the object.
    /// It means current_object_locations is up-to-date with GCS. It
    /// should never go back to false once set to true. If this is true, and
    /// the current_object_locations is empty, then this means that the object
    /// does not exist on any nodes due to eviction or the object never getting created.
    bool subscribed;
    /// The address of the owner.
    rpc::Address owner_address;
  };

  /// Reference to the event loop.
  instrumented_io_context &io_service_;
  /// Reference to the gcs client.
  std::shared_ptr<gcs::GcsClient> gcs_client_;
  /// Info about subscribers to object locations.
  std::unordered_map<ObjectID, LocationListenerState> listeners_;
  /// The client call manager used to create the RPC clients.
  rpc::ClientCallManager client_call_manager_;
  /// The object location subscriber.
  pubsub::SubscriberInterface *object_location_subscriber_;
  /// The callback used to mark an object as failed.
  std::function<void(const ObjectID &, const rpc::ErrorType &)> mark_as_failed_;
  /// Cache of gRPC clients to workers (not necessarily running on this node).
  /// Also includes the number of inflight requests to each worker - when this
  /// reaches zero, the client will be deleted and a new one will need to be created
  /// for any subsequent requests.
  absl::flat_hash_map<WorkerID, std::shared_ptr<rpc::CoreWorkerClient>>
      worker_rpc_clients_;
  /// Used to order add/remove updates for a single ObjectID,
  /// so we don't lose updates at the directory.
  Sequencer<ObjectID> sequencer_;

  /// Get or create the rpc client in the worker_rpc_clients.
  std::shared_ptr<rpc::CoreWorkerClient> GetClient(const rpc::Address &owner_address);

  /// Internal callback function used by object location subscription.
  void ObjectLocationSubscriptionCallback(
      const rpc::WorkerObjectLocationsPubMessage &location_info,
      const ObjectID &object_id, bool location_lookup_failed);

  /// Metrics

  /// Number of object locations added to this object directory.
  uint64_t metrics_num_object_locations_added_;
  double metrics_num_object_locations_added_per_second_;

  /// Number of object locations removed from this object directory.
  uint64_t metrics_num_object_locations_removed_;
  double metrics_num_object_locations_removed_per_second_;

  /// Number of object location lookups.
  uint64_t metrics_num_object_location_lookups_;
  double metrics_num_object_location_lookups_per_second_;

  /// Number of object location updates.
  uint64_t metrics_num_object_location_updates_;
  double metrics_num_object_location_updates_per_second_;

  uint64_t cum_metrics_num_object_location_updates_;
};

}  // namespace ray
