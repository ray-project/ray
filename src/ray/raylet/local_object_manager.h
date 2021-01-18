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

#include <google/protobuf/repeated_field.h>

#include <functional>

#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/gcs/accessor.h"
#include "ray/object_manager/common.h"
#include "ray/raylet/worker_pool.h"
#include "ray/rpc/worker/core_worker_client_pool.h"
#include "src/ray/protobuf/node_manager.pb.h"

namespace ray {

namespace raylet {

/// This class implements memory management for primary objects, objects that
/// have been freed, and objects that have been spilled.
class LocalObjectManager {
 public:
  LocalObjectManager(
      boost::asio::io_service &io_context, size_t free_objects_batch_size,
      int64_t free_objects_period_ms, IOWorkerPoolInterface &io_worker_pool,
      gcs::ObjectInfoAccessor &object_info_accessor,
      rpc::CoreWorkerClientPool &owner_client_pool, bool object_pinning_enabled,
      bool automatic_object_deletion_enabled, int max_io_workers,
      int64_t min_spilling_size,
      std::function<void(const std::vector<ObjectID> &)> on_objects_freed,
      std::function<bool(const ray::ObjectID &)> is_plasma_object_spillable)
      : free_objects_period_ms_(free_objects_period_ms),
        free_objects_batch_size_(free_objects_batch_size),
        io_worker_pool_(io_worker_pool),
        object_info_accessor_(object_info_accessor),
        owner_client_pool_(owner_client_pool),
        object_pinning_enabled_(object_pinning_enabled),
        automatic_object_deletion_enabled_(automatic_object_deletion_enabled),
        on_objects_freed_(on_objects_freed),
        last_free_objects_at_ms_(current_time_ms()),
        min_spilling_size_(min_spilling_size),
        num_active_workers_(0),
        max_active_workers_(max_io_workers),
        is_plasma_object_spillable_(is_plasma_object_spillable) {}

  /// Pin objects.
  ///
  /// \param object_ids The objects to be pinned.
  /// \param objects Pointers to the objects to be pinned. The pointer should
  /// be kept in scope until the object can be released.
  void PinObjects(const std::vector<ObjectID> &object_ids,
                  std::vector<std::unique_ptr<RayObject>> &&objects);

  /// Wait for the objects' owner to free the object.  The objects will be
  /// released when the owner at the given address fails or replies that the
  /// object can be evicted.
  ///
  /// \param owner_address The address of the owner of the objects.
  /// \param object_ids The objects to be freed.
  void WaitForObjectFree(const rpc::Address &owner_address,
                         const std::vector<ObjectID> &object_ids);

  /// Spill objects as much as possible as fast as possible up to the max throughput.
  ///
  /// \return True if spilling is in progress.
  void SpillObjectUptoMaxThroughput();

  /// Spill objects to external storage.
  ///
  /// \param objects_ids_to_spill The objects to be spilled.
  /// \param callback A callback to call once the objects have been spilled, or
  /// there is an error.
  void SpillObjects(const std::vector<ObjectID> &objects_ids,
                    std::function<void(const ray::Status &)> callback);

  /// Restore a spilled object from external storage back into local memory.
  ///
  /// \param object_id The ID of the object to restore.
  /// \param object_url The URL in external storage from which the object can be restored.
  /// \param callback A callback to call when the restoration is done. Status
  /// will contain the error during restoration, if any.
  void AsyncRestoreSpilledObject(const ObjectID &object_id, const std::string &object_url,
                                 std::function<void(const ray::Status &)> callback);

  /// Try to clear any objects that have been freed.
  void FlushFreeObjectsIfNeeded(int64_t now_ms);

  /// Judge if objects are deletable from pending_delete_queue and delete them if
  /// necessary.
  /// TODO(sang): We currently only use 1 IO worker per each call to this method because
  /// delete is a low priority tasks. But we can potentially support more workers to be
  /// used at once.
  ///
  /// \param max_batch_size Maximum number of objects that can be deleted by one
  /// invocation.
  void ProcessSpilledObjectsDeleteQueue(uint32_t max_batch_size);

  /// Return True if spilling is in progress.
  /// This is a narrow interface that is accessed by plasma store.
  /// We are using the narrow interface here because plasma store is running in a
  /// different thread, and we'd like to avoid making this component thread-safe,
  /// which is against the general raylet design.
  ///
  /// \return True if spilling is still in progress. False otherwise.
  bool IsSpillingInProgress();

  /// Populate object spilling stats.
  ///
  /// \param Output parameter.
  void FillObjectSpillingStats(rpc::GetNodeStatsReply *reply) const;

 private:
  FRIEND_TEST(LocalObjectManagerTest, TestSpillObjectsOfSize);
  FRIEND_TEST(LocalObjectManagerTest,
              TestSpillObjectsOfSizeNumBytesToSpillHigherThanMinBytesToSpill);
  FRIEND_TEST(LocalObjectManagerTest, TestSpillObjectNotEvictable);

  /// Asynchronously spill objects when space is needed.
  /// The callback tries to spill objects as much as num_bytes_to_spill and returns
  /// true if we could spill the corresponding bytes.
  /// NOTE(sang): If 0 is given, this method spills a single object.
  ///
  /// \param num_bytes_to_spill The total number of bytes to spill.
  /// \return True if it can spill num_bytes_to_spill. False otherwise.
  bool SpillObjectsOfSize(int64_t num_bytes_to_spill);

  /// Internal helper method for spilling objects.
  void SpillObjectsInternal(const std::vector<ObjectID> &objects_ids,
                            std::function<void(const ray::Status &)> callback);

  /// Release an object that has been freed by its owner.
  void ReleaseFreedObject(const ObjectID &object_id);

  /// Clear any freed objects. This will trigger the callback for freed
  /// objects.
  void FlushFreeObjects();

  /// Add objects' spilled URLs to the global object directory. Call the
  /// callback once all URLs have been added.
  void AddSpilledUrls(const std::vector<ObjectID> &object_ids,
                      const rpc::SpillObjectsReply &worker_reply,
                      std::function<void(const ray::Status &)> callback);

  /// Delete spilled objects stored in given urls.
  ///
  /// \param urls_to_delete List of urls to delete from external storages.
  void DeleteSpilledObjects(std::vector<std::string> &urls_to_delete);

  /// The period between attempts to eagerly evict objects from plasma.
  const int64_t free_objects_period_ms_;

  /// The number of freed objects to accumulate before flushing.
  const size_t free_objects_batch_size_;

  /// A worker pool, used for spilling and restoring objects.
  IOWorkerPoolInterface &io_worker_pool_;

  /// A GCS client, used to update locations for spilled objects.
  gcs::ObjectInfoAccessor &object_info_accessor_;

  /// Cache of gRPC clients to owners of objects pinned on
  /// this node.
  rpc::CoreWorkerClientPool &owner_client_pool_;

  /// Whether to enable pinning for plasma objects.
  bool object_pinning_enabled_;

  /// Whether to enable automatic deletion when refs are gone out of scope.
  bool automatic_object_deletion_enabled_;

  /// A callback to call when an object has been freed.
  std::function<void(const std::vector<ObjectID> &)> on_objects_freed_;

  // Objects that are pinned on this node.
  absl::flat_hash_map<ObjectID, std::unique_ptr<RayObject>> pinned_objects_;

  // Objects that were pinned on this node but that are being spilled.
  // These objects will be released once spilling is complete and the URL is
  // written to the object directory.
  absl::flat_hash_map<ObjectID, std::unique_ptr<RayObject>> objects_pending_spill_;

  /// Objects that were spilled on this node but that are being restored.
  /// The field is used to dedup the same restore request while restoration is in
  /// progress.
  absl::flat_hash_set<ObjectID> objects_pending_restore_;

  /// The time that we last sent a FreeObjects request to other nodes for
  /// objects that have gone out of scope in the application.
  uint64_t last_free_objects_at_ms_ = 0;

  /// Objects that are out of scope in the application and that should be freed
  /// from plasma. The cache is flushed when it reaches the
  /// free_objects_batch_size, or if objects have been in the cache for longer
  /// than the config's free_objects_period, whichever occurs first.
  std::vector<ObjectID> objects_to_free_;

  /// The total size of the objects that are currently being
  /// spilled from this node, in bytes.
  size_t num_bytes_pending_spill_;

  /// This class is accessed by both the raylet and plasma store threads. The
  /// mutex protects private members that relate to object spilling.
  mutable absl::Mutex mutex_;

  ///
  /// Fields below are used to delete spilled objects.
  ///

  /// A list of object id and url pairs that need to be deleted.
  /// We don't instantly delete objects when it goes out of scope from external storages
  /// because those objects could be still in progress of spilling.
  std::queue<ObjectID> spilled_object_pending_delete_;

  /// Mapping from object id to url_with_offsets. We cannot reuse pinned_objects_ because
  /// pinned_objects_ entries are deleted when spilling happens.
  absl::flat_hash_map<ObjectID, std::string> spilled_objects_url_;

  /// Base URL -> ref_count. It is used because there could be multiple objects
  /// within a single spilled file. We need to ref count to avoid deleting the file
  /// before all objects within that file are out of scope.
  absl::flat_hash_map<std::string, uint64_t> url_ref_count_;

  /// Minimum bytes to spill to a single IO spill worker.
  int64_t min_spilling_size_;

  /// The current number of active spill workers.
  int64_t num_active_workers_ GUARDED_BY(mutex_);

  /// The max number of active spill workers.
  const int64_t max_active_workers_;

  /// Callback to check if a plasma object is pinned in workers.
  /// Return true if unpinned, meaning we can safely spill the object. False otherwise.
  std::function<bool(const ray::ObjectID &)> is_plasma_object_spillable_;

  ///
  /// Stats
  ///

  /// The last time a spill operation finished.
  int64_t last_spill_finish_ns_ = 0;

  /// The total wall time in seconds spent in spilling.
  double spill_time_total_s_ = 0;

  /// The total number of bytes spilled.
  int64_t spilled_bytes_total_ = 0;

  /// The total number of objects spilled.
  int64_t spilled_objects_total_ = 0;

  /// The last time a restore operation finished.
  int64_t last_restore_finish_ns_ = 0;

  /// The total wall time in seconds spent in restoring.
  double restore_time_total_s_ = 0;

  /// The total number of bytes restored.
  int64_t restored_bytes_total_ = 0;

  /// The total number of objects restored.
  int64_t restored_objects_total_ = 0;

  /// The last time a spill log finished.
  int64_t last_spill_log_ns_ = 0;

  /// The last time a restore log finished.
  int64_t last_restore_log_ns_ = 0;
};

};  // namespace raylet

};  // namespace ray
