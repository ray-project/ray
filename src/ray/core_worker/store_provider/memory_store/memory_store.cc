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

#include <condition_variable>

#include "ray/common/ray_config.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"

namespace ray {
namespace core {

// Notify the user about an unhandled error after this amount of time. This only
// applies to interactive console (e.g., IPython), see:
// https://github.com/ray-project/ray/issues/14485 for more info.
const int64_t kUnhandledErrorGracePeriodNanos = static_cast<int64_t>(5e9);

// Only scan at most this many items for unhandled errors, to avoid slowdowns
// when there are too many local objects.
const int kMaxUnhandledErrorScanItems = 1000;

/// A class that represents a `Get` request.
class GetRequest {
 public:
  GetRequest(absl::flat_hash_set<ObjectID> object_ids,
             size_t num_objects,
             bool remove_after_get,
             bool abort_if_any_object_is_exception);

  const absl::flat_hash_set<ObjectID> &ObjectIds() const;

  /// Wait until all requested objects are available, or timeout happens.
  ///
  /// \param timeout_ms The maximum time in milliseconds to wait for.
  /// \return Whether all requested objects are available.
  bool Wait(int64_t timeout_ms);
  /// Set the object content for the specific object id.
  void Set(const ObjectID &object_id, std::shared_ptr<RayObject> buffer);
  /// Get the object content for the specific object id.
  std::shared_ptr<RayObject> Get(const ObjectID &object_id) const;
  /// Whether this is a `get` request.
  bool ShouldRemoveObjects() const;

 private:
  /// Wait until all requested objects are available.
  void Wait();

  /// The object IDs involved in this request.
  const absl::flat_hash_set<ObjectID> object_ids_;
  /// The object information for the objects in this request.
  absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> objects_;
  /// Number of objects required.
  const size_t num_objects_;

  // Whether the requested objects should be removed from store
  // after `get` returns.
  const bool remove_after_get_;
  // Whether we should abort the waiting if any object is an exception.
  const bool abort_if_any_object_is_exception_;
  // Whether all the requested objects are available.
  bool is_ready_;
  mutable std::mutex mutex_;
  std::condition_variable cv_;
};

GetRequest::GetRequest(absl::flat_hash_set<ObjectID> object_ids,
                       size_t num_objects,
                       bool remove_after_get,
                       bool abort_if_any_object_is_exception_)
    : object_ids_(std::move(object_ids)),
      num_objects_(num_objects),
      remove_after_get_(remove_after_get),
      abort_if_any_object_is_exception_(abort_if_any_object_is_exception_),
      is_ready_(false) {
  RAY_CHECK(num_objects_ <= object_ids_.size());
}

const absl::flat_hash_set<ObjectID> &GetRequest::ObjectIds() const { return object_ids_; }

bool GetRequest::ShouldRemoveObjects() const { return remove_after_get_; }

bool GetRequest::Wait(int64_t timeout_ms) {
  RAY_CHECK(timeout_ms >= 0 || timeout_ms == -1);
  if (timeout_ms == -1) {
    // Wait forever until all objects are ready.
    Wait();
    return true;
  }

  // Wait until all objects are ready, or the timeout expires.
  std::unique_lock<std::mutex> lock(mutex_);
  auto remaining_timeout_ms = timeout_ms;
  auto timeout_timestamp = current_time_ms() + timeout_ms;
  while (!is_ready_) {
    auto status = cv_.wait_for(lock, std::chrono::milliseconds(remaining_timeout_ms));
    auto current_timestamp = current_time_ms();
    remaining_timeout_ms =
        current_timestamp < timeout_timestamp ? timeout_timestamp - current_timestamp : 0;
    if (status == std::cv_status::timeout || remaining_timeout_ms <= 0) {
      return false;
    }
  }
  return true;
}

void GetRequest::Wait() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (!is_ready_) {
    cv_.wait(lock);
  }
}

void GetRequest::Set(const ObjectID &object_id, std::shared_ptr<RayObject> object) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (is_ready_) {
    return;  // We have already hit the number of objects to return limit.
  }
  object->SetAccessed();
  objects_.emplace(object_id, object);
  if (objects_.size() == num_objects_ ||
      (abort_if_any_object_is_exception_ && object->IsException() &&
       !object->IsInPlasmaError())) {
    is_ready_ = true;
    cv_.notify_all();
  }
}

std::shared_ptr<RayObject> GetRequest::Get(const ObjectID &object_id) const {
  std::unique_lock<std::mutex> lock(mutex_);
  auto iter = objects_.find(object_id);
  if (iter != objects_.end()) {
    iter->second->SetAccessed();
    return iter->second;
  }

  return nullptr;
}

CoreWorkerMemoryStore::CoreWorkerMemoryStore(
    std::shared_ptr<ReferenceCounter> counter,
    std::shared_ptr<raylet::RayletClient> raylet_client,
    std::function<Status()> check_signals,
    std::function<void(const RayObject &)> unhandled_exception_handler,
    std::function<std::shared_ptr<ray::RayObject>(
        const ray::RayObject &object, const ObjectID &object_id)> object_allocator)
    : ref_counter_(std::move(counter)),
      raylet_client_(raylet_client),
      check_signals_(check_signals),
      unhandled_exception_handler_(unhandled_exception_handler),
      object_allocator_(std::move(object_allocator)) {}

void CoreWorkerMemoryStore::GetAsync(
    const ObjectID &object_id, std::function<void(std::shared_ptr<RayObject>)> callback) {
  std::shared_ptr<RayObject> ptr;
  {
    absl::MutexLock lock(&mu_);
    auto iter = objects_.find(object_id);
    if (iter != objects_.end()) {
      ptr = iter->second;
    } else {
      object_async_get_requests_[object_id].push_back(callback);
    }
    if (ptr != nullptr) {
      ptr->SetAccessed();
    }
  }
  // It's important for performance to run the callback outside the lock.
  if (ptr != nullptr) {
    callback(ptr);
  }
}

std::shared_ptr<RayObject> CoreWorkerMemoryStore::GetIfExists(const ObjectID &object_id) {
  std::shared_ptr<RayObject> ptr;
  {
    absl::MutexLock lock(&mu_);
    auto iter = objects_.find(object_id);
    if (iter != objects_.end()) {
      ptr = iter->second;
    }
    if (ptr != nullptr) {
      ptr->SetAccessed();
    }
  }
  return ptr;
}

bool CoreWorkerMemoryStore::Put(const RayObject &object, const ObjectID &object_id) {
  std::vector<std::function<void(std::shared_ptr<RayObject>)>> async_callbacks;
  RAY_LOG(DEBUG) << "Putting object into memory store. objectid is " << object_id;
  std::shared_ptr<RayObject> object_entry = nullptr;
  if (object_allocator_ != nullptr) {
    object_entry = object_allocator_(object, object_id);
  } else {
    object_entry = std::make_shared<RayObject>(
        object.GetData(), object.GetMetadata(), object.GetNestedRefs(), true);
  }

  // TODO(edoakes): we should instead return a flag to the caller to put the object in
  // plasma.
  {
    absl::MutexLock lock(&mu_);

    auto iter = objects_.find(object_id);
    if (iter != objects_.end()) {
      return true;  // Object already exists in the store, which is fine.
    }

    auto async_callback_it = object_async_get_requests_.find(object_id);
    if (async_callback_it != object_async_get_requests_.end()) {
      auto &callbacks = async_callback_it->second;
      async_callbacks = std::move(callbacks);
      object_async_get_requests_.erase(async_callback_it);
    }

    bool should_add_entry = true;
    auto object_request_iter = object_get_requests_.find(object_id);
    if (object_request_iter != object_get_requests_.end()) {
      auto &get_requests = object_request_iter->second;
      for (auto &get_request : get_requests) {
        get_request->Set(object_id, object_entry);
        // If ref counting is enabled, override the removal behaviour.
        if (get_request->ShouldRemoveObjects() && ref_counter_ == nullptr) {
          should_add_entry = false;
        }
      }
    }
    // Don't put it in the store, since we won't get a callback for deletion.
    if (ref_counter_ != nullptr && !ref_counter_->HasReference(object_id)) {
      should_add_entry = false;
    }

    if (should_add_entry) {
      // If there is no existing get request, then add the `RayObject` to map.
      EmplaceObjectAndUpdateStats(object_id, object_entry);
    } else {
      // It is equivalent to the object being added and immediately deleted from the
      // store.
      OnDelete(object_entry);
    }

    if (!async_callbacks.empty()) {
      object_entry->SetAccessed();
    }
  }

  // It's important for performance to run the callbacks outside the lock.
  for (const auto &cb : async_callbacks) {
    cb(object_entry);
  }

  return true;
}

Status CoreWorkerMemoryStore::Get(const std::vector<ObjectID> &object_ids,
                                  int num_objects,
                                  int64_t timeout_ms,
                                  const WorkerContext &ctx,
                                  bool remove_after_get,
                                  std::vector<std::shared_ptr<RayObject>> *results) {
  return GetImpl(object_ids,
                 num_objects,
                 timeout_ms,
                 ctx,
                 remove_after_get,
                 results,
                 /*abort_if_any_object_is_exception=*/true);
}

Status CoreWorkerMemoryStore::GetImpl(const std::vector<ObjectID> &object_ids,
                                      int num_objects,
                                      int64_t timeout_ms,
                                      const WorkerContext &ctx,
                                      bool remove_after_get,
                                      std::vector<std::shared_ptr<RayObject>> *results,
                                      bool abort_if_any_object_is_exception) {
  (*results).resize(object_ids.size(), nullptr);

  std::shared_ptr<GetRequest> get_request;
  int count = 0;

  {
    absl::flat_hash_set<ObjectID> remaining_ids;
    absl::flat_hash_set<ObjectID> ids_to_remove;

    absl::MutexLock lock(&mu_);
    for (size_t i = 0; i < object_ids.size() && count < num_objects; i++) {
      const auto &object_id = object_ids[i];
      auto iter = objects_.find(object_id);
      if (iter != objects_.end()) {
        iter->second->SetAccessed();
        (*results)[i] = iter->second;
        if (remove_after_get) {
          // Note that we cannot remove the object_id from `objects_` now,
          // because `object_ids` might have duplicate ids.
          ids_to_remove.insert(object_id);
        }
        count += 1;
      } else {
        remaining_ids.insert(object_id);
      }
    }
    RAY_CHECK(count <= num_objects);

    // Clean up the objects if ref counting is off.
    if (ref_counter_ == nullptr) {
      for (const auto &object_id : ids_to_remove) {
        EraseObjectAndUpdateStats(object_id);
      }
    }

    // Return if all the objects are obtained.
    if (remaining_ids.empty() || count >= num_objects) {
      return Status::OK();
    }

    size_t required_objects = num_objects - (object_ids.size() - remaining_ids.size());

    // Otherwise, create a GetRequest to track remaining objects.
    get_request = std::make_shared<GetRequest>(std::move(remaining_ids),
                                               required_objects,
                                               remove_after_get,
                                               abort_if_any_object_is_exception);
    for (const auto &object_id : get_request->ObjectIds()) {
      object_get_requests_[object_id].push_back(get_request);
    }
  }

  // Only send block/unblock IPCs for non-actor tasks on the main thread.
  bool should_notify_raylet =
      (raylet_client_ != nullptr && ctx.ShouldReleaseResourcesOnBlockingCalls());

  // Wait for remaining objects (or timeout).
  if (should_notify_raylet) {
    RAY_CHECK_OK(raylet_client_->NotifyDirectCallTaskBlocked(/*release_resources=*/true));
  }

  bool done = false;
  bool timed_out = false;
  Status signal_status = Status::OK();
  int64_t remaining_timeout = timeout_ms;
  int64_t iteration_timeout =
      std::min(timeout_ms, RayConfig::instance().get_timeout_milliseconds());
  if (timeout_ms == -1) {
    iteration_timeout = RayConfig::instance().get_timeout_milliseconds();
  }

  // Repeatedly call Wait() on a shorter timeout so we can check for signals between
  // calls. If timeout_ms == -1, this should run forever until all objects are
  // ready or a signal is received. Else it should run repeatedly until that timeout
  // is reached.
  while (!timed_out && signal_status.ok() &&
         !(done = get_request->Wait(iteration_timeout))) {
    if (check_signals_) {
      signal_status = check_signals_();
    }

    if (remaining_timeout >= 0) {
      remaining_timeout -= iteration_timeout;
      iteration_timeout = std::min(remaining_timeout, iteration_timeout);
      timed_out = remaining_timeout <= 0;
    }
  }

  if (should_notify_raylet) {
    RAY_CHECK_OK(raylet_client_->NotifyDirectCallTaskUnblocked());
  }

  {
    absl::MutexLock lock(&mu_);
    // Populate results.
    for (size_t i = 0; i < object_ids.size(); i++) {
      const auto &object_id = object_ids[i];
      if ((*results)[i] == nullptr) {
        (*results)[i] = get_request->Get(object_id);
      }
    }

    // Remove get request.
    for (const auto &object_id : get_request->ObjectIds()) {
      auto object_request_iter = object_get_requests_.find(object_id);
      if (object_request_iter != object_get_requests_.end()) {
        auto &get_requests = object_request_iter->second;
        // Erase get_request from the vector.
        auto it = std::find(get_requests.begin(), get_requests.end(), get_request);
        if (it != get_requests.end()) {
          get_requests.erase(it);
          // If the vector is empty, remove the object ID from the map.
          if (get_requests.empty()) {
            object_get_requests_.erase(object_request_iter);
          }
        }
      }
    }
  }

  if (!signal_status.ok()) {
    return signal_status;
  } else if (done) {
    return Status::OK();
  } else {
    return Status::TimedOut("Get timed out: some object(s) not ready.");
  }
}

Status CoreWorkerMemoryStore::Get(
    const absl::flat_hash_set<ObjectID> &object_ids,
    int64_t timeout_ms,
    const WorkerContext &ctx,
    absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> *results,
    bool *got_exception) {
  const std::vector<ObjectID> id_vector(object_ids.begin(), object_ids.end());
  std::vector<std::shared_ptr<RayObject>> result_objects;
  RAY_RETURN_NOT_OK(Get(id_vector,
                        id_vector.size(),
                        timeout_ms,
                        ctx,
                        /*remove_after_get=*/false,
                        &result_objects));

  for (size_t i = 0; i < id_vector.size(); i++) {
    if (result_objects[i] != nullptr) {
      (*results)[id_vector[i]] = result_objects[i];
      if (result_objects[i]->IsException() && !result_objects[i]->IsInPlasmaError()) {
        // Can return early if an object value contains an exception.
        // InPlasmaError does not count as an exception because then the object
        // value should then be found in plasma.
        *got_exception = true;
      }
    }
  }
  return Status::OK();
}

Status CoreWorkerMemoryStore::Wait(const absl::flat_hash_set<ObjectID> &object_ids,
                                   int num_objects,
                                   int64_t timeout_ms,
                                   const WorkerContext &ctx,
                                   absl::flat_hash_set<ObjectID> *ready) {
  std::vector<ObjectID> id_vector(object_ids.begin(), object_ids.end());
  std::vector<std::shared_ptr<RayObject>> result_objects;
  RAY_CHECK(object_ids.size() == id_vector.size());
  auto status = GetImpl(id_vector,
                        num_objects,
                        timeout_ms,
                        ctx,
                        false,
                        &result_objects,
                        /*abort_if_any_object_is_exception=*/false);
  // Ignore TimedOut statuses since we return ready objects explicitly.
  if (!status.IsTimedOut()) {
    RAY_RETURN_NOT_OK(status);
  }

  for (size_t i = 0; i < id_vector.size(); i++) {
    if (result_objects[i] != nullptr) {
      ready->insert(id_vector[i]);
    }
  }

  return Status::OK();
}

void CoreWorkerMemoryStore::Delete(const absl::flat_hash_set<ObjectID> &object_ids,
                                   absl::flat_hash_set<ObjectID> *plasma_ids_to_delete) {
  absl::MutexLock lock(&mu_);
  for (const auto &object_id : object_ids) {
    auto it = objects_.find(object_id);
    if (it != objects_.end()) {
      if (it->second->IsInPlasmaError()) {
        plasma_ids_to_delete->insert(object_id);
      } else {
        OnDelete(it->second);
        EraseObjectAndUpdateStats(object_id);
      }
    }
  }
}

void CoreWorkerMemoryStore::Delete(const std::vector<ObjectID> &object_ids) {
  absl::MutexLock lock(&mu_);
  for (const auto &object_id : object_ids) {
    auto it = objects_.find(object_id);
    if (it != objects_.end()) {
      OnDelete(it->second);
      EraseObjectAndUpdateStats(object_id);
    }
  }
}

bool CoreWorkerMemoryStore::Contains(const ObjectID &object_id, bool *in_plasma) {
  absl::MutexLock lock(&mu_);
  auto it = objects_.find(object_id);
  if (it != objects_.end()) {
    if (it->second->IsInPlasmaError()) {
      *in_plasma = true;
    }
    return true;
  }
  return false;
}

inline bool IsUnhandledError(const std::shared_ptr<RayObject> &obj) {
  rpc::ErrorType error_type;
  // TODO(ekl) note that this doesn't warn on errors that are stored in plasma.
  return obj->IsException(&error_type) &&
         // Only warn on task failures (avoid actor died, for example).
         (error_type == rpc::ErrorType::WORKER_DIED ||
          error_type == rpc::ErrorType::TASK_EXECUTION_EXCEPTION) &&
         !obj->WasAccessed();
}

void CoreWorkerMemoryStore::OnDelete(std::shared_ptr<RayObject> obj) {
  if (IsUnhandledError(obj) && unhandled_exception_handler_ != nullptr) {
    unhandled_exception_handler_(*obj);
  }
}

void CoreWorkerMemoryStore::NotifyUnhandledErrors() {
  absl::MutexLock lock(&mu_);
  int64_t threshold = absl::GetCurrentTimeNanos() - kUnhandledErrorGracePeriodNanos;
  auto it = objects_.begin();
  int count = 0;
  while (it != objects_.end() && count < kMaxUnhandledErrorScanItems) {
    const auto &obj = it->second;
    if (IsUnhandledError(obj) && obj->CreationTimeNanos() < threshold &&
        unhandled_exception_handler_ != nullptr) {
      obj->SetAccessed();
      unhandled_exception_handler_(*obj);
    }
    it++;
    count++;
  }
}

inline void CoreWorkerMemoryStore::EraseObjectAndUpdateStats(const ObjectID &object_id) {
  auto it = objects_.find(object_id);
  if (it == objects_.end()) {
    return;
  }

  if (it->second->IsInPlasmaError()) {
    num_in_plasma_ -= 1;
  } else {
    num_local_objects_ -= 1;
    num_local_objects_bytes_ -= it->second->GetSize();
  }
  RAY_CHECK(num_in_plasma_ >= 0 && num_local_objects_ >= 0 &&
            num_local_objects_bytes_ >= 0);
  objects_.erase(it);
}

inline void CoreWorkerMemoryStore::EmplaceObjectAndUpdateStats(
    const ObjectID &object_id, std::shared_ptr<RayObject> &object_entry) {
  auto inserted = objects_.emplace(object_id, object_entry).second;
  if (inserted) {
    if (object_entry->IsInPlasmaError()) {
      num_in_plasma_ += 1;
    } else {
      num_local_objects_ += 1;
      num_local_objects_bytes_ += object_entry->GetSize();
    }
  }
  RAY_CHECK(num_in_plasma_ >= 0 && num_local_objects_ >= 0 &&
            num_local_objects_bytes_ >= 0);
}

MemoryStoreStats CoreWorkerMemoryStore::GetMemoryStoreStatisticalData() {
  absl::MutexLock lock(&mu_);
  MemoryStoreStats item;
  item.num_in_plasma = num_in_plasma_;
  item.num_local_objects = num_local_objects_;
  item.num_local_objects_bytes = num_local_objects_bytes_;
  return item;
}

void CoreWorkerMemoryStore::RecordMetrics() {
  absl::MutexLock lock(&mu_);
  ray::stats::STATS_object_store_memory.Record(
      num_local_objects_bytes_,
      {{ray::stats::LocationKey, ray::stats::kObjectLocWorkerHeap}});
}

}  // namespace core
}  // namespace ray
