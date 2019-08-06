#include <condition_variable>
#include "ray/common/ray_config.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/object_interface.h"
#include "ray/core_worker/store_provider/memory_store_provider.h"

namespace ray {

/// A class that represents a `Get` request.
class GetRequest {
 public:
  GetRequest(std::unordered_set<ObjectID> object_ids, bool remove_after_get);

  const std::unordered_set<ObjectID> &ObjectIds() const;

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
  std::unordered_set<ObjectID> object_ids_;
  /// The object information for the objects in this request.
  std::unordered_map<ObjectID, std::shared_ptr<RayObject>> objects_;

  // Whether the requested objects should be removed from store
  // after `get` returns.
  const bool remove_after_get_;
  // Whether all the requested objects are available.
  bool is_ready_;
  mutable std::mutex mutex_;
  std::condition_variable cv_;
};

GetRequest::GetRequest(std::unordered_set<ObjectID> object_ids, bool remove_after_get)
    : object_ids_(std::move(object_ids)), remove_after_get_(remove_after_get) {}

const std::unordered_set<ObjectID> &GetRequest::ObjectIds() const { return object_ids_; }

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
  while (!is_ready_) {
    auto status = cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms));
    if (status == std::cv_status::timeout) {
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
  objects_.emplace(object_id, object);
  if (objects_.size() == object_ids_.size()) {
    is_ready_ = true;
    cv_.notify_all();
  }
}

std::shared_ptr<RayObject> GetRequest::Get(const ObjectID &object_id) const {
  std::unique_lock<std::mutex> lock(mutex_);
  auto iter = objects_.find(object_id);
  if (iter != objects_.end()) {
    return iter->second;
  }

  return nullptr;
}

CoreWorkerMemoryStore::CoreWorkerMemoryStore() {}

Status CoreWorkerMemoryStore::Put(const ObjectID &object_id, const RayObject &object) {
  std::unique_lock<std::mutex> lock(lock_);
  auto iter = objects_.find(object_id);
  if (iter != objects_.end()) {
    return Status::KeyError("object already exists");
  }

  auto object_entry =
      std::make_shared<RayObject>(object.GetData(), object.GetMetadata(), true);

  bool should_add_entry = true;
  auto object_request_iter = object_get_requests_.find(object_id);
  if (object_request_iter != object_get_requests_.end()) {
    auto &get_requests = object_request_iter->second;
    for (auto &get_request : get_requests) {
      get_request->Set(object_id, object_entry);
      if (get_request->ShouldRemoveObjects()) {
        should_add_entry = false;
      }
    }
  }

  if (should_add_entry) {
    // If there is no existing get request, then add the `RayObject` to map.
    objects_.emplace(object_id, object_entry);
  }
  return Status::OK();
}

Status CoreWorkerMemoryStore::Get(const std::vector<ObjectID> &object_ids,
                                  int64_t timeout_ms, bool remove_after_get,
                                  std::vector<std::shared_ptr<RayObject>> *results) {
  (*results).resize(object_ids.size(), nullptr);

  std::shared_ptr<GetRequest> get_request;

  {
    std::unordered_set<ObjectID> remaining_ids;
    std::unordered_set<ObjectID> ids_to_remove;

    std::unique_lock<std::mutex> lock(lock_);
    // Check for existing objects and see if this get request can be fullfilled.
    for (int i = 0; i < object_ids.size(); i++) {
      const auto &object_id = object_ids[i];
      auto iter = objects_.find(object_id);
      if (iter != objects_.end()) {
        (*results)[i] = iter->second;
        if (remove_after_get) {
          // Note that we cannot remove the object_id from `objects_` now,
          // because `object_ids` might have duplicate ids.
          ids_to_remove.insert(object_id);
        }
      } else {
        remaining_ids.insert(object_id);
      }
    }

    for (const auto &object_id : ids_to_remove) {
      objects_.erase(object_id);
    }

    // Return if all the objects are obtained.
    if (remaining_ids.empty()) {
      return Status::OK();
    }

    // Otherwise, create a GetRequest to track remaining objects.
    get_request =
        std::make_shared<GetRequest>(std::move(remaining_ids), remove_after_get);
    for (const auto &object_id : get_request->ObjectIds()) {
      object_get_requests_[object_id].push_back(get_request);
    }
  }

  // Wait for remaining objects (or timeout).
  get_request->Wait(timeout_ms);

  {
    std::unique_lock<std::mutex> lock(lock_);
    // Populate results.
    for (int i = 0; i < object_ids.size(); i++) {
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

  return Status::OK();
}

void CoreWorkerMemoryStore::Delete(const std::vector<ObjectID> &object_ids) {
  std::unique_lock<std::mutex> lock(lock_);
  for (const auto &object_id : object_ids) {
    objects_.erase(object_id);
  }
}

}  // namespace ray
