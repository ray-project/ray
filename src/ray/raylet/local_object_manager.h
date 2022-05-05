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
#include "ray/gcs/gcs_client/accessor.h"
#include "ray/object_manager/common.h"
#include "ray/object_manager/object_directory.h"
#include "ray/pubsub/subscriber.h"
#include "ray/raylet/worker_pool.h"
#include "ray/rpc/worker/core_worker_client_pool.h"
#include "ray/util/util.h"
#include "src/ray/protobuf/node_manager.pb.h"

namespace ray {

namespace raylet {

/// This class implements memory management for primary objects, objects that
/// have been freed, and objects that have been spilled.
class LocalObjectManager {
 public:
  LocalObjectManager(
      const NodeID &node_id,
      std::string self_node_address,
      int self_node_port,
      size_t free_objects_batch_size,
      int64_t free_objects_period_ms,
      IOWorkerPoolInterface &io_worker_pool,
      rpc::CoreWorkerClientPool &owner_client_pool,
      int max_io_workers,
      int64_t min_spilling_size,
      bool is_external_storage_type_fs,
      int64_t max_fused_object_count,
      std::function<void(const std::vector<ObjectID> &)> on_objects_freed,
      std::function<bool(const ray::ObjectID &)> is_plasma_object_spillable,
      pubsub::SubscriberInterface *core_worker_subscriber,
      IObjectDirectory *object_directory)
      : self_node_id_(node_id),
        self_node_address_(self_node_address),
        self_node_port_(self_node_port),
        free_objects_period_ms_(free_objects_period_ms),
        free_objects_batch_size_(free_objects_batch_size),
        io_worker_pool_(io_worker_pool),
        owner_client_pool_(owner_client_pool),
        on_objects_freed_(on_objects_freed),
        last_free_objects_at_ms_(current_time_ms()),
        min_spilling_size_(min_spilling_size),
        num_active_workers_(0),
        max_active_workers_(max_io_workers),
        is_plasma_object_spillable_(is_plasma_object_spillable),
        is_external_storage_type_fs_(is_external_storage_type_fs),
        max_fused_object_count_(max_fused_object_count),
        next_spill_error_log_bytes_(RayConfig::instance().verbose_spill_logs()),
        core_worker_subscriber_(core_worker_subscriber),
        object_directory_(object_directory) {}

  /// Pin objects.
  ///
  /// Also wait for the objects' owner to free the object.  The objects will be
  /// released when the owner at the given address fails or replies that the
  /// object can be evicted.
  ///
  /// \param object_ids The objects to be pinned.
  /// \param objects Pointers to the objects to be pinned. The pointer should
  /// be kept in scope until the object can be released.
  /// \param owner_address The owner of the objects to be pinned.
  void PinObjectsAndWaitForFree(const std::vector<ObjectID> &object_ids,
                                std::vector<std::unique_ptr<RayObject>> &&objects,
                                const rpc::Address &owner_address);

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
  /// Note: This is no-op if the same restoration request is in flight or the requested
  /// object wasn't spilled yet. The caller should ensure to retry object restoration in
  /// this case.
  ///
  /// \param object_id The ID of the object to restore.
  /// \param object_url The URL where the object is spilled.
  /// \param callback A callback to call when the restoration is done.
  /// Status will contain the error during restoration, if any.
  void AsyncRestoreSpilledObject(const ObjectID &object_id,
                                 int64_t object_size,
                                 const std::string &object_url,
                                 std::function<void(const ray::Status &)> callback);

  /// Clear any freed objects. This will trigger the callback for freed
  /// objects.
  void FlushFreeObjects();

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

  /// Record object spilling stats to metrics.
  void RecordMetrics() const;

  /// Return the spilled object URL if the object is spilled locally,
  /// or the empty string otherwise.
  /// If the external storage is cloud, this will always return an empty string.
  /// In that case, the URL is supposed to be obtained by the object directory.
  std::string GetLocalSpilledObjectURL(const ObjectID &object_id);

  /// Get the current pinned object store memory usage to help node scale down decisions.
  /// A node can only be safely drained when this function reports zero.
  int64_t GetPinnedBytes() const;

  std::string DebugString() const;

 private:
  FRIEND_TEST(LocalObjectManagerTest, TestSpillObjectsOfSizeZero);
  FRIEND_TEST(LocalObjectManagerTest, TestSpillUptoMaxFuseCount);
  FRIEND_TEST(LocalObjectManagerTest,
              TestSpillObjectsOfSizeNumBytesToSpillHigherThanMinBytesToSpill);
  FRIEND_TEST(LocalObjectManagerTest, TestSpillObjectNotEvictable);

  /// Asynchronously spill objects when space is needed. The callback tries to
  /// spill at least num_bytes_to_spill and returns true if we found objects to
  /// spill.
  /// If num_bytes_to_spill many objects cannot be found and there are other
  /// objects already being spilled, this will return false to give the
  /// currently spilling objects time to finish.
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

  /// Do operations that are needed after spilling objects such as
  /// 1. Unpin the pending spilling object.
  /// 2. Update the spilled URL to the owner.
  /// 3. Update the spilled URL to the local directory if it doesn't
  ///    use the external storages like S3.
  void OnObjectSpilled(const std::vector<ObjectID> &object_ids,
                       const rpc::SpillObjectsReply &worker_reply);

  /// Delete spilled objects stored in given urls.
  ///
  /// \param urls_to_delete List of urls to delete from external storages.
  void DeleteSpilledObjects(std::vector<std::string> &urls_to_delete);

  const NodeID self_node_id_;
  const std::string self_node_address_;
  const int self_node_port_;

  /// The period between attempts to eagerly evict objects from plasma.
  const int64_t free_objects_period_ms_;

  /// The number of freed objects to accumulate before flushing.
  const size_t free_objects_batch_size_;

  /// A worker pool, used for spilling and restoring objects.
  IOWorkerPoolInterface &io_worker_pool_;

  /// Cache of gRPC clients to owners of objects pinned on
  /// this node.
  rpc::CoreWorkerClientPool &owner_client_pool_;

  /// A callback to call when an object has been freed.
  std::function<void(const std::vector<ObjectID> &)> on_objects_freed_;

  /// Hashmap from local objects that we are waiting to free to a tuple of
  /// (their owner address, whether the object has been freed).
  /// All objects in this hashmap should also be in exactly one of the
  /// following maps:
  /// - pinned_objects_: objects pinned in shared memory
  /// - objects_pending_spill_: objects pinned and waiting for spill to complete
  /// - spilled_objects_url_: objects already spilled
  absl::flat_hash_map<ObjectID, std::pair<rpc::Address, bool>> local_objects_;

  // Objects that are pinned on this node.
  absl::flat_hash_map<ObjectID, std::unique_ptr<RayObject>> pinned_objects_;

  // Total size of objects pinned on this node.
  size_t pinned_objects_size_ = 0;

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
  size_t num_bytes_pending_spill_ = 0;

  /// The total size of the objects that are currently being
  /// restored from this node, in bytes. Note that this only includes objects
  /// that are being restored into the local object store. Objects restored on
  /// behalf of remote nodes will be read directly from disk to the network.
  size_t num_bytes_pending_restore_ = 0;

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

  /// This class is accessed by both the raylet and plasma store threads. The
  /// mutex protects private members that relate to object spilling.
  mutable absl::Mutex mutex_;

  /// The current number of active spill workers.
  int64_t num_active_workers_ GUARDED_BY(mutex_);

  /// The max number of active spill workers.
  const int64_t max_active_workers_;

  /// Callback to check if a plasma object is pinned in workers.
  /// Return true if unpinned, meaning we can safely spill the object. False otherwise.
  std::function<bool(const ray::ObjectID &)> is_plasma_object_spillable_;

  /// Used to decide spilling protocol.
  /// If it is "filesystem", it restores spilled objects only from an owner node.
  /// If it is not (meaning it is distributed backend), it always restores objects
  /// directly from the external storage.
  bool is_external_storage_type_fs_;

  /// Maximum number of objects that can be fused into a single file.
  int64_t max_fused_object_count_;

  /// The next total bytes for an error-level spill log, or zero to disable.
  /// This is doubled each time a message is logged.
  int64_t next_spill_error_log_bytes_;

  /// The raylet client to initiate the pubsub to core workers (owners).
  /// It is used to subscribe objects to evict.
  pubsub::SubscriberInterface *core_worker_subscriber_;

  /// The object directory interface to access object information.
  IObjectDirectory *object_directory_;

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

  friend class LocalObjectManagerTestWithMinSpillingSize;
  friend class LocalObjectManagerTest;
  friend class LocalObjectManagerFusedTest;
};

};  // namespace raylet

};  // namespace ray
