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
#include <unordered_set>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/core_worker_rpc_client/core_worker_client_pool.h"
#include "ray/gcs_rpc_client/gcs_client.h"
#include "ray/object_manager/object_directory.h"
#include "ray/pubsub/subscriber_interface.h"
#include "ray/stats/metric.h"

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
      instrumented_io_context &io_service,
      gcs::GcsClient &gcs_client,
      pubsub::SubscriberInterface *object_location_subscriber,
      rpc::CoreWorkerClientPool *owner_client_pool,
      std::function<void(const ObjectID &, const rpc::ErrorType &)> mark_as_failed);

  void HandleNodeRemoved(const NodeID &node_id) override;

  void SubscribeObjectLocations(const UniqueID &callback_id,
                                const ObjectID &object_id,
                                const rpc::Address &owner_address,
                                const OnLocationsFound &callback) override;

  void UnsubscribeObjectLocations(const UniqueID &callback_id,
                                  const ObjectID &object_id) override;

  /// Report to the owner that the given object is added to the current node.
  /// This method guarantees ordering and batches requests.
  void ReportObjectAdded(const ObjectID &object_id,
                         const NodeID &node_id,
                         const ObjectInfo &object_info) override;

  /// Report to the owner that the given object is removed to the current node.
  /// This method guarantees ordering and batches requests.
  void ReportObjectRemoved(const ObjectID &object_id,
                           const NodeID &node_id,
                           const ObjectInfo &object_info) override;

  void ReportObjectSpilled(const ObjectID &object_id,
                           const NodeID &node_id,
                           const rpc::Address &owner_address,
                           const std::string &spilled_url,
                           const ObjectID &generator_id,
                           const bool spilled_to_local_storage) override;

  void RecordMetrics(uint64_t duration_ms) override;

  std::string DebugString() const override;

 private:
  friend class OwnershipBasedObjectDirectoryTest;

  /// Callbacks associated with a call to GetLocations.
  struct LocationListenerState {
    /// The callback to invoke when object locations are found.
    absl::flat_hash_map<UniqueID, OnLocationsFound> callbacks;
    /// The current set of known locations of this object.
    std::unordered_set<NodeID> current_object_locations;
    /// The location where this object has been spilled, if any.
    std::string spilled_url;
    // The node id that spills the object to the disk.
    // It will be Nil if it uses a distributed external storage.
    NodeID spilled_node_id = NodeID::Nil();
    bool pending_creation = true;
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
  gcs::GcsClient &gcs_client_;
  /// Info about subscribers to object locations.
  absl::flat_hash_map<ObjectID, LocationListenerState> listeners_;
  /// The object location subscriber.
  pubsub::SubscriberInterface *object_location_subscriber_;
  /// Client pool to owners.
  rpc::CoreWorkerClientPool *owner_client_pool_;
  /// The max batch size for ReportObjectAdded and ReportObjectRemoved.
  const int64_t kMaxObjectReportBatchSize;
  /// The callback used to mark an object as failed.
  std::function<void(const ObjectID &, const rpc::ErrorType &)> mark_as_failed_;

  /// A buffer for batch object location updates.
  /// owner id -> {(FIFO object queue (to avoid starvation), map for the latest update of
  /// objects)}. Since absl::flat_hash_map doesn't maintain the insertion order, we use a
  /// deque here to achieve FIFO.
  absl::flat_hash_map<WorkerID,
                      std::pair<std::deque<ObjectID>,
                                absl::flat_hash_map<ObjectID, rpc::ObjectLocationUpdate>>>
      location_buffers_;

  /// A set of in-flight UpdateObjectLocationBatch requests.
  absl::flat_hash_set<WorkerID> in_flight_requests_;

  /// Get or create the rpc client in the worker_rpc_clients.
  std::shared_ptr<rpc::CoreWorkerClientInterface> GetClient(
      const rpc::Address &owner_address);

  /// Internal callback function used by object location subscription.
  void ObjectLocationSubscriptionCallback(
      const rpc::WorkerObjectLocationsPubMessage &location_info,
      const ObjectID &object_id,
      bool location_lookup_failed);

  /// Send object location update batch from the location_buffers_.
  /// We only allow 1 in-flight request per owner for the batch request
  /// for backpressure. If there's already the backpressure, this method
  /// just buffers the location update and batches it in the next
  /// request.
  void SendObjectLocationUpdateBatchIfNeeded(const WorkerID &worker_id,
                                             const NodeID &node_id,
                                             const rpc::Address &owner_address);

  /// Metrics

  /// Number of object locations added to this object directory.
  uint64_t metrics_num_object_locations_added_ = 0;
  double metrics_num_object_locations_added_per_second_ = 0;

  /// Number of object locations removed from this object directory. = 0;
  uint64_t metrics_num_object_locations_removed_ = 0;
  double metrics_num_object_locations_removed_per_second_ = 0;

  /// Number of object location lookups. = 0;
  uint64_t metrics_num_object_location_lookups_ = 0;
  double metrics_num_object_location_lookups_per_second_ = 0;

  /// Number of object location updates.
  uint64_t metrics_num_object_location_updates_ = 0;
  double metrics_num_object_location_updates_per_second_ = 0;

  uint64_t cum_metrics_num_object_location_updates_ = 0;

  /// Ray metrics
  ray::stats::Gauge ray_metric_object_directory_location_subscriptions_{
      /*name=*/"object_directory_subscriptions",
      /*description=*/
      "Number of object location subscriptions. If this is high, the raylet is "
      "attempting "
      "to pull a lot of objects.",
      /*unit=*/"subscriptions"};

  ray::stats::Gauge ray_metric_object_directory_location_updates_{
      /*name=*/"object_directory_updates",
      /*description=*/
      "Number of object location updates per second. If this is high, the raylet is "
      "attempting to pull a lot of objects and/or the locations for objects are "
      "frequently "
      "changing (e.g. due to many object copies or evictions).",
      /*unit=*/"updates"};

  ray::stats::Gauge ray_metric_object_directory_location_lookups_{
      /*name=*/"object_directory_lookups",
      /*description=*/
      "Number of object location lookups per second. If this is high, the raylet is "
      "waiting on a lot of objects.",
      /*unit=*/"lookups"};

  ray::stats::Gauge ray_metric_object_directory_location_added_{
      /*name=*/"object_directory_added_locations",
      /*description=*/
      "Number of object locations added per second. If this is high, a lot of objects "
      "have been added on this node.",
      /*unit=*/"additions"};

  ray::stats::Gauge ray_metric_object_directory_location_removed_{
      /*name=*/"object_directory_removed_locations",
      /*description=*/
      "Number of object locations removed per second. If this is high, a lot of objects "
      "have been removed from this node.",
      /*unit=*/"removals"};

  friend class OwnershipBasedObjectDirectoryTest;
};

}  // namespace ray
