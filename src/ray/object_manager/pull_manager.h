
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

  /// Begin a new pull request if necessary.
  ///
  /// \param object_id The object id to pull.
  /// \param owner_address The owner of the object.
  ///
  /// \return True if a new pull request was necessary. If true, the caller should
  /// subscribe to new locations of the object, and call OnLocationChange when necessary.
  bool Pull(const ObjectID &object_id, const rpc::Address &owner_address);

  /// Called when the available locations for a given object change.
  ///
  /// \param object_id The ID of the object which is now available in a new location.
  /// \param client_ids The new set of nodes that the object is available on. Not
  /// necessarily a super or subset of the previously available nodes. \param spilled_url
  /// The location of the object if it was spilled. If non-empty, the object may no longer
  /// be on any node.
  void OnLocationChange(const ObjectID &object_id,
                        const std::unordered_set<NodeID> &client_ids,
                        const std::string &spilled_url);

  /// Cancel an existing pull request if necessary.
  ///
  /// \param object_id The object id that no longer needs to be pulled.
  ///
  /// \return True if a pull was cancelled. If there was no pending pull request for the
  /// object this method may return false.
  bool CancelPull(const ObjectID &object_id);

  /// Called when the retry timer fires. If this fires, the pull manager may try to pull
  /// existing objects from other nodes if necessary.
  void Tick();

  /// The number of ongoing object pulls.
  int NumActiveRequests() const;

 private:
  /// A helper structure for tracking information about each ongoing object pull.
  struct PullRequest {
    PullRequest(double first_retry_time)
        : client_locations(), next_pull_time(first_retry_time) {}
    std::vector<NodeID> client_locations;
    double next_pull_time;
  };

  /// See the constructor's arguments.
  NodeID self_node_id_;
  const std::function<bool(const ObjectID &)> object_is_local_;
  const std::function<void(const ObjectID &, const NodeID &)> send_pull_request_;
  const RestoreSpilledObjectCallback restore_spilled_object_;
  const std::function<double()> get_time_;
  int pull_timeout_ms_;

  /// The objects that this object manager is currently trying to fetch from
  /// remote object managers.
  std::unordered_map<ObjectID, PullRequest> pull_requests_;

  /// Internally maintained random number generator.
  std::mt19937_64 gen_;

  /// Try to Pull an object from one of its expected client locations. If there
  /// are more client locations to try after this attempt, then this method
  /// will try each of the other clients in succession, with a timeout between
  /// each attempt. If the object is received or if the Pull is Canceled before
  /// the timeout, then no more Pull requests for this object will be sent
  /// to other node managers until TryPull is called again.
  ///
  /// \param object_id The object's object id.
  /// \return Void.
  void TryPull(const ObjectID &object_id);
};
}  // namespace ray
