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

#ifndef RAY_CORE_WORKER_PLASMA_STORE_PROVIDER_H
#define RAY_CORE_WORKER_PLASMA_STORE_PROVIDER_H

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "plasma/client.h"
#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/context.h"
#include "ray/raylet/raylet_client.h"

namespace ray {

/// The class provides implementations for accessing plasma store, which includes both
/// local and remote stores. Local access goes is done via a
/// CoreWorkerLocalPlasmaStoreProvider and remote access goes through the raylet.
/// See `CoreWorkerStoreProvider` for the semantics of public methods.
class CoreWorkerPlasmaStoreProvider {
 public:
  CoreWorkerPlasmaStoreProvider(
      const std::string &store_socket,
      const std::shared_ptr<raylet::RayletClient> raylet_client,
      std::function<Status()> check_signals, bool evict_if_full,
      std::function<void()> on_store_full = nullptr,
      std::function<std::string()> get_current_call_site = nullptr);

  ~CoreWorkerPlasmaStoreProvider();

  Status SetClientOptions(std::string name, int64_t limit_bytes);

  /// Create and seal an object.
  ///
  /// NOTE: The caller must subsequently call Release() to release the first reference to
  /// the created object. Until then, the object is pinned and cannot be evicted.
  ///
  /// \param[in] object The object to create.
  /// \param[in] object_id The ID of the object.
  /// \param[out] object_exists Optional. Returns whether an object with the
  /// same ID already exists. If this is true, then the Put does not write any
  /// object data.
  Status Put(const RayObject &object, const ObjectID &object_id, bool *object_exists);

  /// Create an object in plasma and return a mutable buffer to it. The buffer should be
  /// subsequently written to and then sealed using Seal().
  ///
  /// \param[in] metadata The metadata of the object.
  /// \param[in] data_size The size of the object.
  /// \param[in] object_id The ID of the object.
  /// \param[out] data The mutable object buffer in plasma that can be written to.
  Status Create(const std::shared_ptr<Buffer> &metadata, const size_t data_size,
                const ObjectID &object_id, std::shared_ptr<Buffer> *data);

  /// Seal an object buffer created with Create().
  ///
  /// NOTE: The caller must subsequently call Release() to release the first reference to
  /// the created object. Until then, the object is pinned and cannot be evicted.
  ///
  /// \param[in] object_id The ID of the object. This can be used as an
  /// argument to Get to retrieve the object data.
  Status Seal(const ObjectID &object_id);

  /// Release the first reference to the object created by Put() or Create(). This should
  /// be called exactly once per object and until it is called, the object is pinned and
  /// cannot be evicted.
  ///
  /// \param[in] object_id The ID of the object. This can be used as an
  /// argument to Get to retrieve the object data.
  Status Release(const ObjectID &object_id);

  Status Get(const absl::flat_hash_set<ObjectID> &object_ids, int64_t timeout_ms,
             const WorkerContext &ctx,
             absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> *results,
             bool *got_exception);

  Status Contains(const ObjectID &object_id, bool *has_object);

  Status Wait(const absl::flat_hash_set<ObjectID> &object_ids, int num_objects,
              int64_t timeout_ms, const WorkerContext &ctx,
              absl::flat_hash_set<ObjectID> *ready);

  Status Delete(const absl::flat_hash_set<ObjectID> &object_ids, bool local_only,
                bool delete_creating_tasks);

  /// Lists objects in used (pinned) by the current client.
  ///
  /// \return Output mapping of used object ids to (size, callsite).
  absl::flat_hash_map<ObjectID, std::pair<int64_t, std::string>> UsedObjectsList() const;

  std::string MemoryUsageString();

 private:
  /// Ask the raylet to fetch a set of objects and then attempt to get them
  /// from the local plasma store. Successfully fetched objects will be removed
  /// from the input set of remaining IDs and added to the results map.
  ///
  /// \param[in/out] remaining IDs of the remaining objects to get.
  /// \param[in] batch_ids IDs of the objects to get.
  /// \param[in] timeout_ms Timeout in milliseconds.
  /// \param[in] fetch_only Whether the raylet should only fetch or also attempt to
  /// reconstruct objects.
  /// \param[in] in_direct_call_task Whether the current task is direct call.
  /// \param[in] task_id The current TaskID.
  /// \param[out] results Map of objects to write results into. This method will only
  /// add to this map, not clear or remove from it, so the caller can pass in a non-empty
  /// map.
  /// \param[out] got_exception Set to true if any of the fetched objects contained an
  /// exception.
  /// \return Status.
  Status FetchAndGetFromPlasmaStore(
      absl::flat_hash_set<ObjectID> &remaining, const std::vector<ObjectID> &batch_ids,
      int64_t timeout_ms, bool fetch_only, bool in_direct_call_task,
      const TaskID &task_id,
      absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> *results,
      bool *got_exception);

  /// Print a warning if we've attempted too many times, but some objects are still
  /// unavailable. Only the keys in the 'remaining' map are used.
  ///
  /// \param[in] num_attemps The number of attempted times.
  /// \param[in] remaining The remaining objects.
  static void WarnIfAttemptedTooManyTimes(int num_attempts,
                                          const absl::flat_hash_set<ObjectID> &remaining);

  const std::shared_ptr<raylet::RayletClient> raylet_client_;
  plasma::PlasmaClient store_client_;
  std::mutex store_client_mutex_;
  std::function<Status()> check_signals_;
  const bool evict_if_full_;
  std::function<void()> on_store_full_;
  std::function<std::string()> get_current_call_site_;

  // Active buffers tracker. This must be allocated as a separate structure since its
  // lifetime can exceed that of the store provider due to callback references.
  struct BufferTracker {
    // Guards the active buffers map. This mutex may be acquired during PlasmaBuffer
    // destruction.
    mutable absl::Mutex active_buffers_mutex_;
    // Mapping of live object buffers to their creation call site. Destroyed buffers are
    // automatically removed from this list via destructor callback. The map key uniquely
    // identifies a buffer. It should not be a shared ptr since that would keep the Buffer
    // alive forever (i.e., this is a weak ref map).
    absl::flat_hash_map<std::pair<ObjectID, PlasmaBuffer *>, std::string> active_buffers_
        GUARDED_BY(active_buffers_mutex_);
  };

  // Pointer to the shared buffer tracker.
  std::shared_ptr<BufferTracker> buffer_tracker_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_PLASMA_STORE_PROVIDER_H
