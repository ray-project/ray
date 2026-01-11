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
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/common/status_or.h"
#include "ray/core_worker/context.h"
#include "ray/object_manager/plasma/client.h"
#include "ray/raylet_ipc_client/raylet_ipc_client_interface.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {
namespace core {

class TrackedBuffer;

// Active buffers tracker. This must be allocated as a separate structure since its
// lifetime can exceed that of the store provider due to TrackedBuffer.
class BufferTracker {
 public:
  // Track an object.
  void Record(const ObjectID &object_id,
              TrackedBuffer *buffer,
              const std::string &call_site);
  // Release an object from tracking.
  void Release(const ObjectID &object_id, TrackedBuffer *buffer);
  // List tracked objects.
  absl::flat_hash_map<ObjectID, std::pair<int64_t, std::string>> UsedObjects() const;

 private:
  // Guards the active buffers map. This mutex may be acquired during TrackedBuffer
  // destruction.
  mutable absl::Mutex active_buffers_mutex_;
  // Mapping of live object buffers to their creation call site. Destroyed buffers are
  // automatically removed from this list via destructor. The map key uniquely
  // identifies a buffer. It should not be a shared ptr since that would keep the Buffer
  // alive forever (i.e., this is a weak ref map).
  absl::flat_hash_map<std::pair<ObjectID, TrackedBuffer *>, std::string> active_buffers_
      ABSL_GUARDED_BY(active_buffers_mutex_);
};

/// This can be used to hold the reference to a buffer.
class TrackedBuffer : public Buffer {
 public:
  TrackedBuffer(std::shared_ptr<Buffer> buffer,
                const std::shared_ptr<BufferTracker> &tracker,
                const ObjectID &object_id)
      : buffer_(buffer), tracker_(tracker), object_id_(object_id) {}

  uint8_t *Data() const override { return buffer_->Data(); }

  size_t Size() const override { return buffer_->Size(); }

  bool OwnsData() const override { return true; }

  bool IsPlasmaBuffer() const override { return true; }

  ~TrackedBuffer() { tracker_->Release(object_id_, this); }

 private:
  /// shared_ptr to a buffer which can potentially hold a reference
  /// for the object (when it's a SharedMemoryBuffer).
  std::shared_ptr<Buffer> buffer_;
  std::shared_ptr<BufferTracker> tracker_;
  ObjectID object_id_;
};

/// The class provides implementations for accessing plasma store, which includes both
/// local and remote stores. Local access goes is done via a
/// CoreWorkerLocalPlasmaStoreProvider and remote access goes through the raylet.
/// See `CoreWorkerStoreProvider` for the semantics of public methods.
class CoreWorkerPlasmaStoreProvider {
 public:
  CoreWorkerPlasmaStoreProvider(
      const std::string &store_socket,
      const std::shared_ptr<ipc::RayletIpcClientInterface> raylet_ipc_client,
      std::function<Status()> check_signals,
      bool warmup,
      std::shared_ptr<plasma::PlasmaClientInterface> store_client,
      int64_t fetch_batch_size,
      std::function<std::string()> get_current_call_site = nullptr);

  ~CoreWorkerPlasmaStoreProvider();

  /// Create and seal an object.
  ///
  /// NOTE: The caller must subsequently call Release() to release the first reference to
  /// the created object. Until then, the object is pinned and cannot be evicted.
  ///
  /// \param[in] object The object to create.
  /// \param[in] object_id The ID of the object.
  /// \param[in] owner_address The address of the object's owner.
  /// \param[out] object_exists Optional. Returns whether an object with the
  /// same ID already exists. If this is true, then the Put does not write any
  /// object data.
  Status Put(const RayObject &object,
             const ObjectID &object_id,
             const rpc::Address &owner_address,
             bool *object_exists);

  /// Create an object in plasma and return a mutable buffer to it. The buffer should be
  /// subsequently written to and then sealed using Seal().
  ///
  /// \param[in] metadata The metadata of the object.
  /// \param[in] data_size The size of the object.
  /// \param[in] object_id The ID of the object.
  /// \param[in] owner_address The address of the object's owner.
  /// \param[out] data The mutable object buffer in plasma that can be written to.
  Status Create(const std::shared_ptr<Buffer> &metadata,
                const size_t data_size,
                const ObjectID &object_id,
                const rpc::Address &owner_address,
                std::shared_ptr<Buffer> *data,
                bool created_by_worker,
                bool is_mutable = false);

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

  /// Fetches data from the local plasma store. If an object is not available in the
  /// local plasma store, then the raylet will trigger a pull request to copy an object
  /// into the local plasma store from another node.
  ///
  /// \param[in] object_ids objects to fetch if they are not already in local plasma.
  /// \param[in] owner_addresses owner addresses of the objects.
  /// \param[in] timeout_ms if the timeout elapses, the request will be canceled.
  /// \param[out] results objects fetched from plasma. This is only valid if the function
  ///
  /// \return Status::IOError if there's an error communicating with the raylet.
  /// \return Status::TimedOut if timeout_ms was reached before all object_ids could be
  /// fetched.
  /// \return Status::Interrupted if a SIGINT signal was received.
  /// \return Status::IntentionalSystemExit if a SIGTERM signal was was received.
  /// \return Status::UnexpectedSystemExit if any other signal was received.
  /// \return Status::OK otherwise.
  Status Get(const std::vector<ObjectID> &object_ids,
             const std::vector<rpc::Address> &owner_addresses,
             int64_t timeout_ms,
             absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> *results);

  /// Get objects directly from the local plasma store, without waiting for the
  /// objects to be fetched from another node. This should only be used
  /// internally, never by user code.
  ///
  /// \param[in] ids The IDs of the objects to get.
  /// \param[out] results The results will be stored here. A nullptr will be
  /// added for objects that were not in the local store.
  /// \return Status OK if the request to the local object store was
  /// successful.
  Status GetIfLocal(const std::vector<ObjectID> &ids,
                    absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> *results);

  /// Get an experimental mutable object. The object must be
  /// local to this node.
  ///
  /// \param[in] object_id The object to get.
  /// \param[out] mutable_object Metadata needed to read and write the mutable
  /// object. Keeping the returned pointer in scope will pin the object in the
  /// object store.
  ///
  /// \return Status OK if we got the mutable object. Can fail if the object
  /// was not local or is not mutable.
  Status GetExperimentalMutableObject(
      const ObjectID &object_id, std::unique_ptr<plasma::MutableObject> *mutable_object);

  Status Contains(const ObjectID &object_id, bool *has_object);

  Status Wait(const std::vector<ObjectID> &object_ids,
              const std::vector<rpc::Address> &owner_addresses,
              int num_objects,
              int64_t timeout_ms,
              const WorkerContext &ctx,
              absl::flat_hash_set<ObjectID> *ready);

  Status Delete(const absl::flat_hash_set<ObjectID> &object_ids, bool local_only);

  /// Lists objects in used (pinned) by the current client.
  ///
  /// \return Output mapping of used object ids to (size, callsite).
  absl::flat_hash_map<ObjectID, std::pair<int64_t, std::string>> UsedObjectsList() const;

  StatusOr<std::string> GetMemoryUsage();

  std::shared_ptr<plasma::PlasmaClientInterface> &store_client() { return store_client_; }

 private:
  /// Ask the plasma store to return object objects within the timeout.
  /// Successfully fetched objects will be removed from the input set of remaining IDs and
  /// added to the results map.
  ///
  /// \param[in/out] remaining_object_id_to_idx map of object IDs to their indices left to
  /// get. \param[in] ids IDs of the objects to get. \param[in] timeout_ms Timeout in
  /// milliseconds. \param[out] results Map of objects to write results into. This method
  /// will only add to this map, not clear or remove from it, so the caller can pass in a
  /// non-empty map. \param[out] got_exception Set to true if any of the fetched objects
  /// contained an exception. \return Status::IOError if there is an error in
  /// communicating with the raylet or the plasma store. \return Status::OK if successful.
  Status GetObjectsFromPlasmaStore(
      absl::flat_hash_map<ObjectID, int64_t> &remaining_object_id_to_idx,
      const std::vector<ObjectID> &ids,
      int64_t timeout_ms,
      absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> *results,
      bool *got_exception);

  /// Print a warning if we've attempted the fetch for too long and some
  /// objects are still unavailable.
  static void WarnIfFetchHanging(
      int64_t fetch_start_time_ms,
      const absl::flat_hash_map<ObjectID, int64_t> &remaining_object_id_to_idx);

  /// Put something in the plasma store so that subsequent plasma store accesses
  /// will be faster. Currently the first access is always slow, and we don't
  /// want the user to experience this.
  /// \return status
  Status WarmupStore();

  const std::shared_ptr<ipc::RayletIpcClientInterface> raylet_ipc_client_;
  std::shared_ptr<plasma::PlasmaClientInterface> store_client_;
  std::function<Status()> check_signals_;
  std::function<std::string()> get_current_call_site_;
  uint32_t object_store_full_delay_ms_;
  // Pointer to the shared buffer tracker.
  std::shared_ptr<BufferTracker> buffer_tracker_;
  int64_t fetch_batch_size_ = 0;
  std::atomic<int64_t> get_request_counter_;
};

}  // namespace core
}  // namespace ray
