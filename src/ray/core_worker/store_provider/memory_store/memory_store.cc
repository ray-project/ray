#include "ray/core_worker/store_provider/memory_store_provider.h"
#include "ray/common/ray_config.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/object_interface.h"
#include <condition_variable>

namespace ray {

/// A RayObject class that automatically releases the backing object
/// when it goes out of scope. This is returned by Get.
class ReferencedRayObject : public RayObject {
 public:
  ~ReferencedRayObject();

  ReferencedRayObject(
      std::shared_ptr<CoreWorkerMemoryStore> provider,
      const ObjectID &object_id,
      std::shared_ptr<RayObject> object)
      : RayObject(object->GetData(), object->GetMetadata(), /* should_copy=*/ true),
        provider_(provider),
        object_id_(object_id) {}

 private:
  std::shared_ptr<CoreWorkerMemoryStore> provider_;
  const ObjectID object_id_;
};

ReferencedRayObject::~ReferencedRayObject() { provider_->Release(object_id_); }


ObjectEntry::ObjectEntry(
    const ObjectID &object_id, const RayObject &object)
    : object_id_(object_id),
      object_(std::make_shared<RayObject>(
          object.GetData(), object.GetMetadata(), /* copy_data */ true)),
      refcnt_(0) {}

std::shared_ptr<ReferencedRayObject> ObjectEntry::CreateReferencedObject(
    std::shared_ptr<CoreWorkerMemoryStore> provider) {
  // Get a shared_ptr reference RayObject, which will automatically
  // release the refcnt when it destructs, so we increase the refcnt here.       
  IncreaseRefcnt();
  provider->cache_.Remove(object_id_);
  return std::make_shared<ReferencedRayObject>(
      provider, object_id_, object_);
}

/// A class that represents a `Get` or `Wait` reuquest.
class GetOrWaitRequest {
 public:
  GetOrWaitRequest(const std::vector<ObjectID> &object_ids, bool is_get);

  const std::vector<ObjectID> &ObjectIds() const;

  /// Wait until all requested objects are available, or timeout happens.
  bool Wait(int64_t timeout_ms);
  /// Set the object content for the specific object id.
  void Set(const ObjectID &object_id, std::shared_ptr<RayObject> buffer);
  /// Get the object content for the specific object id.
  std::shared_ptr<RayObject> Get(const ObjectID &object_id) const;
  /// Whether this is a `get` request.
  bool IsGetRequest() const;

 private: 
  /// Wait until all requested objects are available.
  void Wait();

  /// The object IDs involved in this request. This is used in the reply.
  const std::vector<ObjectID> object_ids_;
  /// The object information for the objects in this request.
  std::unordered_map<ObjectID, std::shared_ptr<RayObject>> objects_;

  // Whether this request is a `get` request.
  const bool is_get_;
  // Whether all the requested objects are available.
  bool is_ready_;
  mutable std::mutex mutex_;
  std::condition_variable cv_;
};

GetOrWaitRequest::GetOrWaitRequest(
    const std::vector<ObjectID> &object_ids, bool is_get)
  : object_ids_(object_ids), is_get_(is_get) {}

const std::vector<ObjectID> &GetOrWaitRequest::ObjectIds() const {
  return object_ids_;
}

bool GetOrWaitRequest::IsGetRequest() const {
  return is_get_;
}

bool GetOrWaitRequest::Wait(int64_t timeout_ms) {
  if (timeout_ms < 0) {
    // Wait forever until the object is ready.
    Wait();
    return true;
  }

  // Wait until the object is ready, or the timeout expires.
  std::unique_lock<std::mutex> lock(mutex_);
  while (!is_ready_) {
    auto status = cv_.wait_for(lock,
        std::chrono::milliseconds(timeout_ms));
    if (status == std::cv_status::timeout) {
      return false;
    }
  }
  return true;
}

void GetOrWaitRequest::Wait() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (!is_ready_) {
    cv_.wait(lock);
  }
}

void GetOrWaitRequest::Set(const ObjectID &object_id, std::shared_ptr<RayObject> object) {
  std::unique_lock<std::mutex> lock(mutex_);
  objects_.emplace(object_id, object);
  if (objects_.size() == object_ids_.size()) {
    is_ready_ = true;
    cv_.notify_all();
  }
}

std::shared_ptr<RayObject> GetOrWaitRequest::Get(const ObjectID &object_id) const
{
  std::unique_lock<std::mutex> lock(mutex_);
  auto iter = objects_.find(object_id);
  if (iter != objects_.end()) {
    return iter->second;
  }

  return nullptr;
}

void EvictionCache::Add(const ObjectID& key, uint64_t size) {
  auto it = item_map_.find(key);
  RAY_CHECK(it == item_map_.end());
  // Note that it is important to use a list so the iterators stay valid.
  item_list_.emplace_front(key, size);
  item_map_.emplace(key, item_list_.begin());
}

void EvictionCache::Remove(const ObjectID& key) {
  auto it = item_map_.find(key);
  if (it == item_map_.end()) {
    return;
  }

  item_list_.erase(it->second);
  item_map_.erase(it);
}

uint64_t EvictionCache::ChooseObjectsToEvict(uint64_t num_bytes_required,
                                       std::vector<ObjectID>* objects_to_evict) {
  uint64_t bytes_evicted = 0;
  auto it = item_list_.end();
  while (bytes_evicted < num_bytes_required && it != item_list_.begin()) {
    it--;
    objects_to_evict->push_back(it->first);
    bytes_evicted += it->second;
  }
  return bytes_evicted;
}


CoreWorkerMemoryStore::CoreWorkerMemoryStore(uint64_t max_size)
    : max_size_(max_size), total_size_(0) {}

Status CoreWorkerMemoryStore::Put(const RayObject &object, const ObjectID &object_id) {
  std::unique_lock<std::mutex> lock(lock_);
  auto iter = objects_.find(object_id);
  if (iter != objects_.end()) {
    return Status::KeyError("object already exists");
  }

  // Check if adding this object is allowed.
  if (object.GetSize() + total_size_ > max_size_) {
    std::vector<ObjectID> objects_to_evict;
    auto evicted_bytes = cache_.ChooseObjectsToEvict(
        object.GetSize(), &objects_to_evict);
    for (const auto &id : objects_to_evict) {
      RAY_UNUSED(DeleteObjectImpl(id));
    }
    
    if (evicted_bytes < object.GetSize()) {
      return Status::OutOfMemory("out of memory");
    }
  }

  // add to eviction cache.
  cache_.Add(object_id, object.GetSize());
  total_size_ += object.GetSize();

  auto entry = std::unique_ptr<ObjectEntry>(
      new ObjectEntry(object_id, object));
  objects_.emplace(object_id, std::move(entry));

  auto object_request_iter = object_get_requests_.find(object_id);
  if (object_request_iter != object_get_requests_.end()) {
    auto& get_requests = object_request_iter->second;
    for (auto &get_req : get_requests) {
      auto& object_entry = objects_[object_id];
      auto object_buffer = get_req->IsGetRequest() ?
          object_entry->CreateReferencedObject(shared_from_this()) :
          object_entry->GetObject();
      get_req->Set(object_id, object_buffer);
    }
  }
  return Status::OK();
}

Status CoreWorkerMemoryStore::GetOrWait(
    const std::vector<ObjectID> &object_ids, int64_t timeout_ms, 
    std::vector<std::shared_ptr<RayObject>> *results, bool is_get) {
  (*results).resize(object_ids.size(), nullptr);
  std::vector<ObjectID> remaining_ids;

  std::shared_ptr<GetOrWaitRequest> get_request;

  {
    std::unique_lock<std::mutex> lock(lock_);
    // Check for existing objects and see if this get request can be fullfilled.
    for (int i = 0; i < object_ids.size(); i++) {
      const auto &object_id = object_ids[i];
      auto iter = objects_.find(object_id);
      if (iter != objects_.end()) {
        auto object_buffer = is_get ?
            iter->second->CreateReferencedObject(shared_from_this()) :
            iter->second->GetObject();
        (*results)[i] = object_buffer;
      } else {
        remaining_ids.emplace_back(object_id);
      }
    }

    // Return if all the objects are obtained.
    if (remaining_ids.empty()) {
      return Status::OK();
    }

    // Otherwise, create a GetOrWaitRequest to track remaining objects.
    get_request = std::make_shared<GetOrWaitRequest>(remaining_ids, is_get);
    for (const auto &object_id : remaining_ids) {
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

    // Remove get rquest.
    for (const auto &object_id : get_request->ObjectIds()) {
      auto object_request_iter = object_get_requests_.find(object_id);
      if (object_request_iter != object_get_requests_.end()) {
        auto& get_requests = object_request_iter->second;
        // Erase get_req from the vector.
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

Status CoreWorkerMemoryStore::Get(
    const std::vector<ObjectID> &object_ids, int64_t timeout_ms, 
    std::vector<std::shared_ptr<RayObject>> *results) {
  return GetOrWait(object_ids, timeout_ms, results, /* is_get */ true);
}

Status CoreWorkerMemoryStore::Wait(const std::vector<ObjectID> &object_ids,
                                                 int num_objects, int64_t timeout_ms,
                                                 std::vector<bool> *results) {
  if (num_objects != object_ids.size()) {
    return Status::Invalid("num_objects should equal to number of items in object_ids");
  }

  (*results).resize(object_ids.size(), false);

  std::vector<std::shared_ptr<RayObject>> result_objects;
  auto status = GetOrWait(object_ids, timeout_ms, &result_objects, /* is_get */ false);
  if (status.ok()) {
    RAY_CHECK(result_objects.size() == object_ids.size());
    for (int i = 0; i < object_ids.size(); i++) {
      (*results)[i] = (result_objects[i] != nullptr);
    }
  }

  return Status::OK();
}

void CoreWorkerMemoryStore::Release(const ObjectID &object_id) {
  std::unique_lock<std::mutex> lock(lock_);
  auto iter = objects_.find(object_id);
  if (iter != objects_.end()) {
    if (iter->second->DecreaseRefcnt() == 0) {
      // TODO(zhijunfu): do we want to preserve it here?
      cache_.Remove(object_id);
      total_size_ -= iter->second->GetObject()->GetSize();
      objects_.erase(iter);
    }
  }
}

void CoreWorkerMemoryStore::Delete(const std::vector<ObjectID> &object_ids) {
  std::unique_lock<std::mutex> lock(lock_);
  for (const auto &object_id : object_ids) {
    RAY_UNUSED(DeleteObjectImpl(object_id));
  }
}

Status CoreWorkerMemoryStore::DeleteObjectImpl(const ObjectID &object_id) {
  // Note that this function doesn't take a lock.
  auto iter = objects_.find(object_id);
  if (iter != objects_.end()) {
    if (iter->second->Refcnt() == 0) {
      // TODO(zhijunfu): do we want to directly delete here, or just mark a flag
      // and actually do the deletion when refcnt drops to 0? It's ok just to delete
      // as the data validity is ensured by shared_ptr.
      cache_.Remove(object_id);
      total_size_ -= iter->second->GetObject()->GetSize();
      objects_.erase(iter);
      return Status::OK();
    }
    return Status::Invalid("reference count is not zero");
  }
  return Status::Invalid("object not exists");
}

}  // namespace ray
