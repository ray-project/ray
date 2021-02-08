
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
      const std::function<double()> get_time, int pull_timeout_ms,
      size_t num_bytes_available, std::function<void()> object_store_full_callback);

  /// Add a new pull request for a bundle of objects. The objects in the
  /// request will get pulled once:
  /// 1. Their sizes are known.
  /// 2. Their total size, together with the total size of all requests
  /// preceding this one, is within the capacity of the local object store.
  ///
  /// \param object_refs The bundle of objects that must be made local.
  /// \param objects_to_locate The objects whose new locations the caller
  /// should subscribe to, and call OnLocationChange for.
  /// \return A request ID that can be used to cancel the request.
  uint64_t Pull(const std::vector<rpc::ObjectReference> &object_ref_bundle,
                std::vector<rpc::ObjectReference> *objects_to_locate);

  /// Update the pull requests that are currently being pulled, according to
  /// the current capacity. The PullManager will choose the objects to pull by
  /// taking the longest contiguous prefix of the request queue whose total
  /// size is less than the given capacity.
  ///
  /// \param num_bytes_available The number of bytes that are currently
  /// available to store objects pulled from another node.
  void UpdatePullsBasedOnAvailableMemory(size_t num_bytes_available);

  /// Called when the available locations for a given object change.
  ///
  /// \param object_id The ID of the object which is now available in a new location.
  /// \param client_ids The new set of nodes that the object is available on. Not
  /// necessarily a super or subset of the previously available nodes.
  /// \param spilled_url The location of the object if it was spilled. If
  /// non-empty, the object may no longer be on any node.
  /// \param spilled_node_id The node id of the object if it was spilled. If Nil, the
  /// object may no longer be on any node.
  void OnLocationChange(const ObjectID &object_id,
                        const std::unordered_set<NodeID> &client_ids,
                        const std::string &spilled_url, const NodeID &spilled_node_id,
                        size_t object_size);

  /// Cancel an existing pull request.
  ///
  /// \param request_id The request ID returned by Pull that should be canceled.
  /// \return The objects for which the caller should stop subscribing to
  /// locations.
  std::vector<ObjectID> CancelPull(uint64_t request_id);

  /// Called when the retry timer fires. If this fires, the pull manager may try to pull
  /// existing objects from other nodes if necessary.
  void Tick();

  /// Call to reset the retry timer for an object that is actively being
  /// pulled. This should be called for objects that were evicted but that may
  /// still be needed on this node.
  ///
  /// \param object_id The object ID to reset.
  void ResetRetryTimer(const ObjectID &object_id);

  /// The number of ongoing object pulls.
  int NumActiveRequests() const;

  std::string DebugString() const;

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
    NodeID spilled_node_id;
    double next_pull_time;
    uint8_t num_retries;
    bool object_size_set = false;
    size_t object_size = 0;
    // All bundle requests that haven't been canceled yet that require this
    // object. This includes bundle requests whose objects are not actively
    // being pulled.
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

  /// Activate the next pull request in the queue. This will start pulls for
  /// any objects in the request that are not already being pulled.
  bool ActivateNextPullBundleRequest(
      const std::map<uint64_t, std::vector<rpc::ObjectReference>>::iterator
          &next_request_it,
      std::vector<ObjectID> *objects_to_pull);

  /// Deactivate a pull request in the queue. This cancels any pull or restore
  /// operations for the object.
  void DeactivatePullBundleRequest(
      const std::map<uint64_t, std::vector<rpc::ObjectReference>>::iterator &request_it,
      std::unordered_set<ObjectID> *objects_to_cancel = nullptr);

  /// Trigger out-of-memory handling if the first request in the queue needs
  /// more space than the bytes available. This is needed to make room for the
  /// request.
  void TriggerOutOfMemoryHandlingIfNeeded();

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

  /// The currently active pull requests. Each request is a bundle of objects
  /// that must be made local. The key is the ID that was assigned to that
  /// request, which can be used by the caller to cancel the request.
  std::map<uint64_t, std::vector<rpc::ObjectReference>> pull_request_bundles_;

  /// The total number of bytes that we are currently pulling. This is the
  /// total size of the objects requested that we are actively pulling. To
  /// avoid starvation, this is always less than the available capacity in the
  /// local object store.
  size_t num_bytes_being_pulled_ = 0;

  /// The total number of bytes that is available to store objects that we are
  /// pulling.
  size_t num_bytes_available_;

  /// Triggered when the first request in the queue can't be pulled due to
  /// out-of-memory. This callback should try to make more bytes available.
  std::function<void()> object_store_full_callback_;

  /// The last time OOM was reported. Track this so we don't spam warnings when
  /// the object store is full.
  uint64_t last_oom_reported_ms_ = 0;

  /// A pointer to the highest request ID whose objects we are currently
  /// pulling. We always pull a contiguous prefix of the active pull requests.
  /// This means that all requests with a lower ID are either already canceled
  /// or their objects are also being pulled.
  uint64_t highest_req_id_being_pulled_ = 0;

  /// The objects that this object manager has been asked to fetch from remote
  /// object managers.
  std::unordered_map<ObjectID, ObjectPullRequest> object_pull_requests_;

  /// The objects that we are currently fetching. This is a subset of the
  /// objects that we have been asked to fetch. The total size of these objects
  /// is the number of bytes that we are currently pulling, and it must be less
  /// than the bytes available.
  absl::flat_hash_map<ObjectID, absl::flat_hash_set<uint64_t>>
      active_object_pull_requests_;

  /// Internally maintained random number generator.
  std::mt19937_64 gen_;

  friend class PullManagerTest;
  friend class PullManagerTestWithCapacity;
  friend class PullManagerWithAdmissionControlTest;
};
}  // namespace ray
