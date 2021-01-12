
#pragma once

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/bind.hpp>
#include <map>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/time/clock.h"
#include "ray/common/id.h"
#include "ray/common/ray_config.h"
#include "ray/common/status.h"
#include "ray/object_manager/common.h"
#include "ray/object_manager/format/object_manager_generated.h"
#include "ray/object_manager/notification/object_store_notification_manager_ipc.h"
#include "ray/object_manager/object_buffer_pool.h"
#include "ray/object_manager/object_directory.h"
#include "ray/object_manager/ownership_based_object_directory.h"
#include "ray/object_manager/plasma/store_runner.h"
#include "ray/rpc/object_manager/object_manager_client.h"
#include "ray/rpc/object_manager/object_manager_server.h"

namespace ray {

class PullManager {
 public:
  /// PullManager is responsible for managing the policy around when to send pull requests
  /// and to whom. Notably, it is _not_ responsible for controlling the object directory
  /// or any pubsub communications.
  ///
  /// \param self_node_id the current node
  /// \param object_is_local A callback which should return true if a given object is
  /// already on the local node. \param send_pull_request A callback which should send a
  /// pull request to the specified node.
  /// \param restore_spilled_object A callback which should
  /// retrieve an spilled object from the external store.
  PullManager(
      NodeID &self_node_id, const std::function<bool(const ObjectID &)> object_is_local,
      const std::function<void(const ObjectID &, const NodeID &)> send_pull_request,
      const RestoreSpilledObjectCallback restore_spilled_object,
      const std::function<double()> get_time, int pull_timeout_ms);

  /// Begin a new pull request for a bundle of objects.
  ///
  /// \param object_refs The bundle of objects that must be made local.
  /// \param objects_to_locate The objects whose new locations the caller
  /// should subscribe to, and call OnLocationChange for.
  /// \return A request ID that can be used to cancel the request.
  uint64_t Pull(const std::vector<rpc::ObjectReference> &object_ref_bundle,
                std::vector<rpc::ObjectReference> *objects_to_locate);

  /// Called when the available locations for a given object change.
  ///
  /// \param object_id The ID of the object which is now available in a new location.
  /// \param client_ids The new set of nodes that the object is available on. Not
  /// necessarily a super or subset of the previously available nodes.
  /// \param spilled_url The location of the object if it was spilled. If
  /// non-empty, the object may no longer be on any node.
  void OnLocationChange(const ObjectID &object_id,
                        const std::unordered_set<NodeID> &client_ids,
                        const std::string &spilled_url);

  /// Cancel an existing pull request.
  ///
  /// \param request_id The request ID returned by Pull that should be canceled.
  /// \return The objects for which the caller should stop subscribing to
  /// locations.
  std::vector<ObjectID> CancelPull(uint64_t request_id);

  /// Called when the retry timer fires. If this fires, the pull manager may try to pull
  /// existing objects from other nodes if necessary.
  void Tick();

  /// The number of ongoing object pulls.
  int NumActiveRequests() const;

 private:
  /// A helper structure for tracking information about each ongoing object pull.
  struct ObjectPullRequest {
    ObjectPullRequest(double first_retry_time)
        : client_locations(),
          spilled_url(),
          next_pull_time(first_retry_time),
          num_retries(0),
          bundle_request_ids() {}
    std::vector<NodeID> client_locations;
    std::string spilled_url;
    double next_pull_time;
    uint8_t num_retries;
    absl::flat_hash_set<uint64_t> bundle_request_ids;
  };

  /// Try to make an object local, by restoring the object from external
  /// storage or by fetching the object from one of its expected client
  /// locations. This does nothing if the object is not needed by any pull
  /// request or if it is already local. This also sets a timeout for when to
  /// make the next attempt to make the object local.
  void TryToMakeObjectLocal(const ObjectID &object_id);

  /// Try to Pull an object from one of its expected client locations. If there
  /// are more client locations to try after this attempt, then this method
  /// will try each of the other clients in succession.
  ///
  /// \return True if a pull request was sent, otherwise false.
  bool PullFromRandomLocation(const ObjectID &object_id);

  /// Update the request retry time for the given request.
  /// The retry timer is incremented exponentially, capped at 1024 * 10 seconds.
  ///
  /// \param request The request to update the retry time of.
  void UpdateRetryTimer(ObjectPullRequest &request);

  /// See the constructor's arguments.
  NodeID self_node_id_;
  const std::function<bool(const ObjectID &)> object_is_local_;
  const std::function<void(const ObjectID &, const NodeID &)> send_pull_request_;
  const RestoreSpilledObjectCallback restore_spilled_object_;
  const std::function<double()> get_time_;
  uint64_t pull_timeout_ms_;

  /// The next ID to assign to a bundle pull request, so that the caller can
  /// cancel. Start at 1 because 0 means null.
  uint64_t next_req_id_ = 1;

  std::unordered_map<uint64_t, std::vector<rpc::ObjectReference>> pull_request_bundles_;

  /// The objects that this object manager is currently trying to fetch from
  /// remote object managers.
  std::unordered_map<ObjectID, ObjectPullRequest> object_pull_requests_;

  /// Internally maintained random number generator.
  std::mt19937_64 gen_;
};
}  // namespace ray
