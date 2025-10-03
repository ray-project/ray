// Copyright 2019-2021 The Ray Authors.
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

#include <gtest/gtest_prod.h>

#include <memory>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/asio/asio_util.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/reference_counter.h"
#include "ray/raylet_ipc_client/raylet_ipc_client_interface.h"
#include "ray/rpc/utils.h"

namespace ray {
namespace core {

struct MemoryStoreStats {
  int32_t num_in_plasma = 0;
  int32_t num_local_objects = 0;
  int64_t num_local_objects_bytes = 0;
};

class GetRequest;

/// The class provides implementations for local process memory store.
/// An example usage for this is to retrieve the returned objects from direct
/// actor call (see task_receiver.cc).
class CoreWorkerMemoryStore {
 public:
  /// Create a memory store.
  ///
  /// \param[in] io_context Posts async callbacks to this context.
  /// \param[in] counter If not null, this enables ref counting for local objects,
  ///            and the `remove_after_get` flag for Get() will be ignored.
  /// \param[in] raylet_ipc_client If not null, used to notify tasks blocked / unblocked.
  explicit CoreWorkerMemoryStore(
      instrumented_io_context &io_context,
      ReferenceCounter *counter = nullptr,
      std::shared_ptr<ipc::RayletIpcClientInterface> raylet_ipc_client = nullptr,
      std::function<Status()> check_signals = nullptr,
      std::function<void(const RayObject &)> unhandled_exception_handler = nullptr,
      std::function<std::shared_ptr<RayObject>(const RayObject &object,
                                               const ObjectID &object_id)>
          object_allocator = nullptr);
  ~CoreWorkerMemoryStore() = default;

  /// Put an object with specified ID into object store. If there are pending GetAsync
  /// requests, the callbacks are posted onto the io_context.
  ///
  /// \param[in] object The ray object.
  /// \param[in] object_id Object ID specified by user.
  void Put(const RayObject &object, const ObjectID &object_id);

  /// Get a list of objects from the object store.
  ///
  /// \param[in] object_ids IDs of the objects to get. Duplicates are not allowed.
  /// \param[in] num_objects Number of objects that should appear.
  /// \param[in] timeout_ms Timeout in milliseconds, wait infinitely if it's negative.
  /// \param[in] ctx The current worker context.
  /// \param[in] remove_after_get When to remove the objects from store after `Get`
  /// finishes. This has no effect if ref counting is enabled.
  /// \param[out] results Result list of objects data.
  /// \return Status.
  Status Get(const std::vector<ObjectID> &object_ids,
             int num_objects,
             int64_t timeout_ms,
             const WorkerContext &ctx,
             bool remove_after_get,
             std::vector<std::shared_ptr<RayObject>> *results);

  /// Convenience wrapper around Get() that stores results in a given result map.
  Status Get(const absl::flat_hash_set<ObjectID> &object_ids,
             int64_t timeout_ms,
             const WorkerContext &ctx,
             absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> *results,
             bool *got_exception);

  /// Waits for a number of objects to be ready from the list of object_ids given.
  /// \return A pair of sets of object IDs. The first set contains the object IDs that
  /// are ready in the core worker memory store (capped to num_objects), and the second
  /// set contains the object IDs are ready in the plasma object store (not capped).
  Status Wait(const absl::flat_hash_set<ObjectID> &object_ids,
              int num_objects,
              int64_t timeout_ms,
              const WorkerContext &ctx,
              absl::flat_hash_set<ObjectID> *ready,
              absl::flat_hash_set<ObjectID> *plasma_object_ids);

  /// Get an object if it exists.
  ///
  /// \param[in] object_id The object id to get.
  /// \return Pointer to the object if it exists, otherwise nullptr.
  std::shared_ptr<RayObject> GetIfExists(const ObjectID &object_id);

  /// Asynchronously get an object from the object store. The object will not be removed
  /// from storage after GetAsync (TODO(ekl): integrate this with object GC).
  ///
  /// \param[in] object_id The object id to get.
  /// \param[in] callback The callback to run with the reference to the retrieved
  ///            object value once available.
  void GetAsync(const ObjectID &object_id,
                std::function<void(std::shared_ptr<RayObject>)> callback);

  /// Delete a list of objects from the object store.
  /// NOTE(swang): Objects that contain IsInPlasmaError will not be
  /// deleted from the in-memory store. Instead, any future Get
  /// calls should check with plasma to see whether the object has
  /// been deleted.
  ///
  /// \param[in] object_ids IDs of the objects to delete.
  /// \param[out] plasma_ids_to_delete This will be extended to
  /// include the IDs of the plasma objects to delete, based on the
  /// in-memory objects that contained InPlasmaError.
  void Delete(const absl::flat_hash_set<ObjectID> &object_ids,
              absl::flat_hash_set<ObjectID> *plasma_ids_to_delete);

  /// Delete a list of objects from the object store.
  ///
  /// \param[in] object_ids IDs of the objects to delete.
  void Delete(const std::vector<ObjectID> &object_ids);

  /// Check whether this store contains the object.
  ///
  /// \param[in] object_id The object to check.
  /// \param[out] in_plasma Set to true if the object was spilled to plasma.
  /// Will only be true if the store contains the object.
  /// \return Whether the store has the object.
  bool Contains(const ObjectID &object_id, bool *in_plasma);

  /// Returns the number of objects in this store.
  ///
  /// \return Count of objects in the store.
  int Size() {
    absl::MutexLock lock(&mu_);
    return objects_.size();
  }

  /// Returns stats data of memory usage.
  ///
  /// \return number of local objects and used memory size.
  MemoryStoreStats GetMemoryStoreStatisticalData();

  /// Returns the memory usage of this store.
  ///
  /// \return Total size of objects in the store.
  uint64_t UsedMemory();

  /// Raise any unhandled errors that have not been accessed within a timeout.
  /// This is used to surface unhandled task errors in interactive consoles.
  /// In those settings, errors may never be garbage collected and hence we
  /// never trigger the deletion hook for task errors that prints them.
  void NotifyUnhandledErrors();

  /// Record CoreWorker heap memory related metrics.
  void RecordMetrics();

 private:
  FRIEND_TEST(TestMemoryStore, TestMemoryStoreStats);

  /// See the public version of `Get` for meaning of the other arguments.
  /// \param[in] abort_if_any_object_is_exception Whether we should abort if any object
  /// resources. is an exception.
  /// \param[in] at_most_num_objects Whether this function will return *at most*
  /// num_objects even if more are ready. We will still stop waiting when we have
  /// num_objects.
  Status GetImpl(const std::vector<ObjectID> &object_ids,
                 int num_objects,
                 int64_t timeout_ms,
                 const WorkerContext &ctx,
                 bool remove_after_get,
                 std::vector<std::shared_ptr<RayObject>> *results,
                 bool abort_if_any_object_is_exception,
                 bool at_most_num_objects);

  /// Called when an object is deleted from the store.
  void OnDelete(std::shared_ptr<RayObject> obj);

  /// Emplace the given object entry to the in-memory-store and update stats properly.
  void EmplaceObjectAndUpdateStats(const ObjectID &object_id,
                                   std::shared_ptr<RayObject> &object_entry)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Erase the object of the object id from the in memory store and update stats
  /// properly.
  void EraseObjectAndUpdateStats(const ObjectID &object_id)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  instrumented_io_context &io_context_;

  /// If enabled, holds a reference to local worker ref counter. TODO(ekl) make this
  /// mandatory once Java is supported.
  ReferenceCounter *ref_counter_;

  // If set, this will be used to notify worker blocked / unblocked on get calls.
  std::shared_ptr<ipc::RayletIpcClientInterface> raylet_ipc_client_;

  /// Protects the data structures below.
  mutable absl::Mutex mu_;

  /// Map from object ID to `RayObject`.
  /// NOTE: This map should be modified by EmplaceObjectAndUpdateStats and
  /// EraseObjectAndUpdateStats.
  absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> objects_ ABSL_GUARDED_BY(mu_);

  /// Map from object ID to its get requests.
  absl::flat_hash_map<ObjectID, std::vector<std::shared_ptr<GetRequest>>>
      object_get_requests_ ABSL_GUARDED_BY(mu_);

  /// Map from object ID to its async get requests.
  absl::flat_hash_map<ObjectID,
                      std::vector<std::function<void(std::shared_ptr<RayObject>)>>>
      object_async_get_requests_ ABSL_GUARDED_BY(mu_);

  /// Function passed in to be called to check for signals (e.g., Ctrl-C).
  std::function<Status()> check_signals_;

  /// Function called to report unhandled exceptions.
  std::function<void(const RayObject &)> unhandled_exception_handler_;

  ///
  /// Below information is stats.
  ///
  /// Number of objects in the plasma store for this memory store.
  int32_t num_in_plasma_ ABSL_GUARDED_BY(mu_) = 0;
  /// Number of objects that don't exist in the plasma store.
  int32_t num_local_objects_ ABSL_GUARDED_BY(mu_) = 0;
  /// Number of bytes used by this memory store on heap, including both
  /// placeholder values for objects in plasma and inlined small returned
  /// objects from task.
  int64_t num_local_objects_bytes_ ABSL_GUARDED_BY(mu_) = 0;

  /// This lambda is used to allow language frontend to allocate the objects
  /// in the memory store.
  std::function<std::shared_ptr<RayObject>(const RayObject &object,
                                           const ObjectID &object_id)>
      object_allocator_;
};

}  // namespace core
}  // namespace ray
