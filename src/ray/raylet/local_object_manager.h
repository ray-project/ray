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
#include "ray/raylet/worker_pool.h"
#include "ray/rpc/worker/core_worker_client_pool.h"

namespace ray {

namespace raylet {

/// This class implements memory management for primary objects, objects that
/// have been freed, and objects that have been spilled.
class LocalObjectManager {
 public:
  LocalObjectManager(size_t free_objects_batch_size, int64_t free_objects_period_ms,
                     IOWorkerPoolInterface &io_worker_pool,
                     gcs::ObjectInfoAccessor &object_info_accessor,
                     rpc::CoreWorkerClientPool &owner_client_pool,
                     std::function<void(const std::vector<ObjectID> &)> on_objects_freed)
      : free_objects_period_ms_(free_objects_period_ms),
        free_objects_batch_size_(free_objects_batch_size),
        io_worker_pool_(io_worker_pool),
        object_info_accessor_(object_info_accessor),
        owner_client_pool_(owner_client_pool),
        on_objects_freed_(on_objects_freed),
        last_free_objects_at_ms_(current_time_ms()) {}

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

  /// Spill objects to external storage.
  ///
  /// \param objects_ids_to_spill The objects to be spilled.
  /// \param callback A callback to call once the objects have been spilled, or
  /// there is anerror.
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

 private:
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

  /// A callback to call when an object has been freed.
  std::function<void(const std::vector<ObjectID> &)> on_objects_freed_;

  // Objects that are pinned on this node.
  absl::flat_hash_map<ObjectID, std::unique_ptr<RayObject>> pinned_objects_;

  /// The time that we last sent a FreeObjects request to other nodes for
  /// objects that have gone out of scope in the application.
  uint64_t last_free_objects_at_ms_ = 0;

  /// Objects that are out of scope in the application and that should be freed
  /// from plasma. The cache is flushed when it reaches the
  /// free_objects_batch_size, or if objects have been in the cache for longer
  /// than the config's free_objects_period, whichever occurs first.
  std::vector<ObjectID> objects_to_free_;
};

};  // namespace raylet

};  // namespace ray
