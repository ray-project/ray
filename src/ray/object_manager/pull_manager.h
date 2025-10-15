// Copyright 2020-2021 The Ray Authors.
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
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/time/clock.h"
#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/object_manager/common.h"
#include "ray/util/container_util.h"
#include "ray/util/counter_map.h"

namespace ray {

// Identifier for task metrics reporting, which is tuple of the task name
// (empty string if unknown), and is_retry bool.
using TaskMetricsKey = std::pair<std::string, bool>;

enum BundlePriority : uint8_t {
  /// Bundle requested by ray.get().
  GET_REQUEST,
  /// Bundle requested by ray.wait().
  WAIT_REQUEST,
  /// Bundle requested for fetching task arguments.
  TASK_ARGS,
};

// Not thread-safe except for IsObjectActive().
class PullManager {
 public:
  /// PullManager is responsible for managing the policy around when to send pull requests
  /// and to whom. Notably, it is _not_ responsible for controlling the object directory
  /// or any pubsub communications.
  ///
  /// \param self_node_id the current node
  /// \param object_is_local A callback which should return true if a given object is
  /// already on the local node.
  /// \param send_pull_request A callback which should send a
  /// pull request to the specified node.
  /// \param cancel_pull_request A callback which should
  /// cancel pulling an object.
  /// \param restore_spilled_object A callback which should
  /// retrieve an spilled object from the external store.
  PullManager(
      NodeID self_node_id,
      std::function<bool(const ObjectID &)> object_is_local,
      std::function<void(const ObjectID &, const NodeID &)> send_pull_request,
      std::function<void(const ObjectID &)> cancel_pull_request,
      std::function<void(const ObjectID &, rpc::ErrorType)> fail_pull_request,
      RestoreSpilledObjectCallback restore_spilled_object,
      std::function<double()> get_time_seconds,
      int pull_timeout_ms,
      int64_t num_bytes_available,
      std::function<std::unique_ptr<RayObject>(const ObjectID &object_id)> pin_object,
      std::function<std::string(const ObjectID &)> get_locally_spilled_object_url);

  /// Add a new pull request for a bundle of objects. The objects in the
  /// request will get pulled once:
  /// 1. Their sizes are known.
  /// 2. Their total size, together with the total size of all requests
  /// preceding this one, is within the capacity of the local object store.
  ///
  /// \param object_refs The bundle of objects that must be made local.
  /// \param prio The priority class of the bundle.
  /// \param task_key Task name and whether it is a retry.
  /// \param objects_to_locate The objects whose new locations the caller
  /// should subscribe to, and call OnLocationChange for.
  /// \return A request ID that can be used to cancel the request.
  uint64_t Pull(const std::vector<rpc::ObjectReference> &object_ref_bundle,
                BundlePriority prio,
                const TaskMetricsKey &task_key,
                std::vector<rpc::ObjectReference> *objects_to_locate);

  /// Update the pull requests that are currently being pulled, according to
  /// the current capacity. The PullManager will choose the objects to pull by
  /// taking as many as pullable requests whose total
  /// size is less than the given capacity.
  ///
  /// \param num_bytes_available The number of bytes that are currently
  /// available to store objects pulled from another node.
  void UpdatePullsBasedOnAvailableMemory(int64_t num_bytes_available);

  /// Called when the available locations for a given object change.
  ///
  /// \param object_id The ID of the object which is now available in a new location.
  /// \param client_ids The new set of nodes that the object is available on. Not
  /// necessarily a super or subset of the previously available nodes.
  /// \param spilled_url The location of the object if it was spilled. If
  /// non-empty, the object may no longer be on any node.
  /// \param spilled_node_id The node id of the object if it was spilled. If Nil, the
  /// object may no longer be on any node.
  /// \param pending_creation Whether this object is pending creation. This is
  /// used to time out objects that have had no locations for too long.
  /// \param object_size The size of the object. Used to compute how many
  /// objects we can safely pull.
  void OnLocationChange(const ObjectID &object_id,
                        const std::unordered_set<NodeID> &client_ids,
                        const std::string &spilled_url,
                        const NodeID &spilled_node_id,
                        bool pending_creation,
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

  /// Called when a new object appears locally. This gives a chance for the pull manager
  /// to pin the object as soon as it is available.
  void PinNewObjectIfNeeded(const ObjectID &object_id);

  /// Call to reset the retry timer for an object that is actively being
  /// pulled. This should be called for objects that were evicted but that may
  /// still be needed on this node.
  ///
  /// \param object_id The object ID to reset.
  void ResetRetryTimer(const ObjectID &object_id);

  /// The number of ongoing object pulls.
  int NumObjectPullRequests() const;

  /// Returns whether the object is actively being pulled.
  ///
  /// This method (and this method only) is thread-safe.
  bool IsObjectActive(const ObjectID &object_id) const;

  /// Check whether the pull request is currently active or waiting for object
  /// size information. If this returns false, then the pull request is most
  /// likely inactive due to lack of memory.
  bool PullRequestActiveOrWaitingForMetadata(uint64_t request_id) const;

  /// Whether we are have requests queued that are not currently active. This
  /// can happen when we are at capacity in the object store or temporarily, if
  /// there are object sizes missing.
  bool HasPullsQueued() const;

  /// Record the internal metrics.
  void RecordMetrics() const;

  std::string DebugString() const;

  /// Returns the number of bytes of quota remaining. When this is less than zero,
  /// we are OverQuota(). Visible for testing.
  int64_t RemainingQuota();

  void SetOutOfDisk(const ObjectID &object_id);

  int64_t NumInactivePulls(const TaskMetricsKey &task_key) const {
    return task_argument_bundles_.inactive_by_name.Get(task_key);
  }

 private:
  /// A helper structure for tracking information about each ongoing object pull.
  struct ObjectPullRequest {
    explicit ObjectPullRequest(double first_retry_time)
        : next_pull_time(first_retry_time) {}
    std::vector<NodeID> client_locations;
    std::string spilled_url;
    NodeID spilled_node_id;
    bool pending_object_creation = false;
    double next_pull_time;
    // The pull will timeout at this time if there are still no locations for
    // the object.
    double expiration_time_seconds = 0;
    int64_t activate_time_ms = 0;
    int64_t request_start_time_ms = absl::GetCurrentTimeNanos() / 1e3;
    uint8_t num_retries = 0;
    bool object_size_set = false;
    size_t object_size = 0;
    // All bundle requests that haven't been canceled yet that require this
    // object. This includes bundle requests whose objects are not actively
    // being pulled.
    absl::flat_hash_set<uint64_t> bundle_request_ids;

    // An object is pullable if we know the size and it's not pending
    // creation due to object reconstruction.
    bool IsPullable() const { return object_size_set && !pending_object_creation; }

    std::string DebugString() const {
      std::stringstream result;
      result << "ObjectPullRequest{";
      result << "locations: " << debug_string(client_locations);
      result << ", spilled url: " << spilled_url;
      result << ", spilled node id: " << spilled_node_id;
      result << ", pending creation: " << pending_object_creation;
      result << ", object size set: " << object_size_set;
      result << ", object size: " << object_size;
      result << ", num of retries: " << static_cast<uint64_t>(num_retries);
      result << "}";
      return result.str();
    }
  };

  /// A helper structure for tracking information about each ongoing bundle pull request.
  struct BundlePullRequest {
    BundlePullRequest(std::vector<ObjectID> requested_objects, TaskMetricsKey task_key)
        : objects_(std::move(requested_objects)), task_key_(std::move(task_key)) {}
    // All the objects that this bundle is trying to pull.
    std::vector<ObjectID> objects_;
    // All the objects that are pullable.
    absl::flat_hash_set<ObjectID> pullable_objects_;
    // The name of the task, if a task arg request, otherwise the empty string.
    TaskMetricsKey task_key_;

    void MarkObjectAsPullable(const ObjectID &object) {
      pullable_objects_.emplace(object);
    }

    void MarkObjectAsUnpullable(const ObjectID &object) {
      pullable_objects_.erase(object);
    }

    // A bundle is pullable if we know the sizes of all objects
    // and none of them is pending creation due to object reconstruction.
    bool IsPullable() const { return pullable_objects_.size() == objects_.size(); }
  };

  /// A helper structure for tracking all the bundle pull requests for a particular bundle
  /// priority.
  class BundlePullRequestQueue {
   public:
    // Key is the request id assigned to each bundle pull request.
    absl::flat_hash_map<uint64_t, BundlePullRequest> requests;
    // A bundle pull request can be in one of the three stats:
    // 1. active: the bundle is actively being pulled.
    // 2. inactive: the bundle is pullable but is not being pulled
    // because we are at capacity in the object store.
    // 3. unpullable: at least one object is not pullable (i.e. missing size or pending
    // creation).
    //
    // All requests in the queue that are not in one of the following sets are unpullable.
    // (i.e. requests - active_requests - inactive_requests == unpullable_requests).
    //
    // Allowed transitions between stats are:
    // 1. active -> inactive: when we need to deactivate the pull request to make room
    // for higher priority ones or the object being pulled is pending creation
    // due to object reconstruction.
    // 2. inactive -> active: when we have enough available object store memory to fullfil
    // the request.
    // 3. inactive -> unpullable: when the object is lost and pending creation due to
    // object reconstruction.
    // 4. unpullable -> inactive: when all objects have size and are not pending creation.
    //
    // Note: we use std::set here so requests are sorted by their request ids (i.e. the
    // order of pull).
    std::set<uint64_t> active_requests;
    std::set<uint64_t> inactive_requests;
    CounterMap<std::pair<std::string, bool>> inactive_by_name;

    bool Empty() const { return requests.empty(); }

    // Add a request to the queue, the request will start with inactive or unpullable
    // state. The caller must explicitly activate it if needed.
    void AddBundlePullRequest(uint64_t request_id, BundlePullRequest request) {
      requests.emplace(request_id, request);
      if (request.IsPullable()) {
        inactive_requests.emplace(request_id);
        inactive_by_name.Increment(request.task_key_);
        RAY_CHECK_EQ(inactive_requests.size(), inactive_by_name.Total());
      }
    }

    void ActivateBundlePullRequest(uint64_t request_id) {
      RAY_CHECK_EQ(inactive_requests.erase(request_id), 1u);
      active_requests.emplace(request_id);
      auto task_key = map_find_or_die(requests, request_id).task_key_;
      inactive_by_name.Decrement(task_key);
      RAY_CHECK_EQ(inactive_requests.size(), inactive_by_name.Total());
    }

    void DeactivateBundlePullRequest(uint64_t request_id) {
      RAY_CHECK_EQ(active_requests.erase(request_id), 1u);
      inactive_requests.emplace(request_id);
      auto task_key = map_find_or_die(requests, request_id).task_key_;
      inactive_by_name.Increment(task_key);
      RAY_CHECK_EQ(inactive_requests.size(), inactive_by_name.Total());
    }

    void MarkBundleAsPullable(uint64_t request_id) {
      RAY_CHECK(map_find_or_die(requests, request_id).IsPullable());
      RAY_CHECK_EQ(active_requests.count(request_id), 0u);
      inactive_requests.emplace(request_id);
      auto task_key = map_find_or_die(requests, request_id).task_key_;
      inactive_by_name.Increment(task_key);
      RAY_CHECK_EQ(inactive_requests.size(), inactive_by_name.Total());
    }

    void MarkBundleAsUnpullable(uint64_t request_id) {
      RAY_CHECK(!map_find_or_die(requests, request_id).IsPullable());
      // For a request to go from active to unpullable, it must be
      // deactivated first.
      RAY_CHECK_EQ(active_requests.count(request_id), 0u);
      auto it = inactive_requests.find(request_id);
      if (it != inactive_requests.end()) {
        inactive_requests.erase(it);
        auto task_key = map_find_or_die(requests, request_id).task_key_;
        inactive_by_name.Decrement(task_key);
        RAY_CHECK_EQ(inactive_requests.size(), inactive_by_name.Total());
      }
    }

    void RemoveBundlePullRequest(uint64_t request_id) {
      auto task_key = map_find_or_die(requests, request_id).task_key_;
      requests.erase(request_id);
      if (active_requests.find(request_id) != active_requests.end()) {
        active_requests.erase(request_id);
      }
      if (inactive_requests.find(request_id) != inactive_requests.end()) {
        inactive_requests.erase(request_id);
        inactive_by_name.Decrement(task_key);
      }
      RAY_CHECK_EQ(inactive_requests.size(), inactive_by_name.Total());
    }

    std::string DebugString() const {
      std::stringstream result;
      result << "BundlePullRequestQueue{";
      result << requests.size() << " total, ";
      result << active_requests.size() << " active, ";
      result << inactive_requests.size() << " inactive, ";
      result << (requests.size() - active_requests.size() - inactive_requests.size())
             << " unpullable}";
      return result.str();
    }
  };

  /// Try to make an object local, by restoring the object from external
  /// storage or by fetching the object from one of its expected client
  /// locations. This does nothing if the object is not needed by any pull
  /// request or if it is already local. This also sets a timeout for when to
  /// make the next attempt to make the object local.
  void TryToMakeObjectLocal(const ObjectID &object_id)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(active_objects_mu_);

  /// Returns whether the set of active pull requests exceeds the memory allowance
  /// for pulls. Note that exceeding the quota is allowed in certain situations,
  /// e.g., for get requests and to ensure at least one active request.
  bool OverQuota();

  /// Pin the object if possible. Only actively pulled objects should be pinned.
  bool TryPinObject(const ObjectID &object_id);

  /// Unpin the given object if pinned.
  void UnpinObject(const ObjectID &object_id);

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
  /// \param object_id The object id for the request.
  void UpdateRetryTimer(ObjectPullRequest &request, const ObjectID &object_id);

  /// Activate the next pull request in the queue. This will start pulls for
  /// any objects in the request that are not already being pulled.
  ///
  /// Returns whether the request was successfully activated. If this returns
  /// false, then there are no more requests in the queue that can be activated
  /// (because we have reached the end of the queue or because there is missing
  /// size information), or activating the request would exceed memory quota.
  ///
  /// Note that we allow exceeding the quota to maintain at least 1 active bundle.
  bool ActivateNextBundlePullRequest(BundlePullRequestQueue &bundles,
                                     bool respect_quota,
                                     std::vector<ObjectID> *objects_to_pull);

  /// Deactivate a pull request in the queue. This cancels any pull or restore
  /// operations for the object.
  void DeactivateBundlePullRequest(BundlePullRequestQueue &bundles,
                                   uint64_t request_id,
                                   std::unordered_set<ObjectID> *objects_to_cancel);

  /// Helper method that deactivates requests from the given queue until the pull
  /// memory usage is within quota.
  ///
  /// \param retain_min Don't deactivate if this would drop the total number of active
  ///                   bundles (in any queue) below this threshold.
  /// \param quota_margin Keep deactivating bundles until this amount of quota margin
  ///                     becomes available.
  void DeactivateUntilMarginAvailable(const std::string &debug_name,
                                      BundlePullRequestQueue &bundles,
                                      int retain_min,
                                      int64_t quota_margin,
                                      std::unordered_set<ObjectID> *objects_to_cancel);

  /// Return debug info about this bundle queue.
  std::string BundleInfo(const BundlePullRequestQueue &bundles) const;

  /// Return the incremental space required to pull the next bundle, if available.
  /// If the next bundle is not ready for pulling, 0L will be returned.
  int64_t NextRequestBundleSize(const BundlePullRequestQueue &bundles) const;

  const BundlePullRequestQueue &GetBundlePullRequestQueue(uint64_t request_id) const;
  BundlePullRequestQueue &GetBundlePullRequestQueue(uint64_t request_id);

  /// See the constructor's arguments.
  NodeID self_node_id_;
  const std::function<bool(const ObjectID &)> object_is_local_;
  const std::function<void(const ObjectID &, const NodeID &)> send_pull_request_;
  const std::function<void(const ObjectID &)> cancel_pull_request_;
  const RestoreSpilledObjectCallback restore_spilled_object_;
  const std::function<double()> get_time_seconds_;
  uint64_t pull_timeout_ms_;

  /// The next ID to assign to a bundle pull request, so that the caller can
  /// cancel. Start at 1 because 0 means null.
  uint64_t next_req_id_ = 1;

  /// The currently ongoing pull requests. Each request is a bundle of objects
  /// that must be made local. The key is the ID that was assigned to that
  /// request, which can be used by the caller to cancel the request.
  ///
  /// The pull requests are split into requests made by workers (`ray.get` or
  /// `ray.wait`) and arguments of queued tasks. This is so that we prioritize
  /// freeing resources held by workers over scheduling new tasks that may
  /// require those resources. If we try to pull arguments for a new task
  /// before handling a worker's request, we could deadlock.
  ///
  /// We only enable plasma fallback allocations for ray.get() requests, which
  /// also take precedence over ray.wait() requests.
  ///
  /// Bundle pull requests of `ray.get` and `ray.wait` requests made by workers.
  BundlePullRequestQueue get_request_bundles_;
  BundlePullRequestQueue wait_request_bundles_;
  /// Bundle pull requests of arguments of queued tasks.
  BundlePullRequestQueue task_argument_bundles_;

  /// The total number of bytes that we are currently pulling. This is the
  /// total size of the objects requested that we are actively pulling. To
  /// avoid starvation, this is always less than the available capacity in the
  /// local object store.
  int64_t num_bytes_being_pulled_ = 0;

  /// The total number of bytes that is available to store objects that we are
  /// pulling.
  int64_t num_bytes_available_;

  /// The number of currently active bundles.
  int64_t num_active_bundles_ = 0;

  /// Callback to pin plasma objects.
  std::function<std::unique_ptr<RayObject>(const ObjectID &object_ids)> pin_object_;

  /// The objects that this object manager has been asked to fetch from remote
  /// object managers.
  absl::flat_hash_map<ObjectID, ObjectPullRequest> object_pull_requests_;

  // Protects state that is shared by the threads used to receive object
  // chunks.
  mutable absl::Mutex active_objects_mu_;

  /// The objects that we are currently fetching. This is a subset of the
  /// objects that we have been asked to fetch. The total size of these objects
  /// is the number of bytes that we are currently pulling, and it must be less
  /// than the bytes available.
  absl::flat_hash_map<ObjectID, absl::flat_hash_set<uint64_t>>
      active_object_pull_requests_ ABSL_GUARDED_BY(active_objects_mu_);

  /// Tracks the objects we have pinned. Keys are subset of active_object_pull_requests_.
  /// We need to pin these objects so that parts of in-progress bundles aren't evicted
  /// due to self-induced memory pressure.
  absl::flat_hash_map<ObjectID, std::unique_ptr<RayObject>> pinned_objects_;

  /// The total size of pinned objects.
  int64_t pinned_objects_size_ = 0;

  // A callback to get the spilled object URL if the object is spilled locally.
  // It will return an empty string otherwise.
  std::function<std::string(const ObjectID &)> get_locally_spilled_object_url_;

  // A callback to fail a hung pull request.
  std::function<void(const ObjectID &, rpc::ErrorType)> fail_pull_request_;

  /// Internally maintained random number generator.
  std::mt19937_64 gen_;
  int64_t max_timeout_ = 0;
  ObjectID max_timeout_object_id_;
  int64_t num_tries_total_ = 0;
  int64_t num_retries_total_ = 0;
  int64_t num_succeeded_pins_total_ = 0;
  int64_t num_failed_pins_total_ = 0;

  friend class PullManagerTest;
  friend class PullManagerTestWithCapacity;
  friend class PullManagerWithAdmissionControlTest;
};
}  // namespace ray
