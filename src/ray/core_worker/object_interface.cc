#include <algorithm>

#include "ray/common/ray_config.h"
#include "ray/core_worker/store_provider/store_provider.h"
#include "ray/core_worker/object_interface.h"

namespace ray {

void GetObjectIdsPerTransport(
    const std::vector<ObjectID> &object_ids,
    EnumUnorderedMap<TaskTransportType, std::unordered_set<ObjectID>> *results) {

  for (const auto &object_id : object_ids) {
    if (object_id.IsReturnObject()) {
      auto type = static_cast<TaskTransportType>(object_id.GetTransportType());
      (*results)[type].insert(object_id);
    } else {
      // For non-return objects, treat them the same as return objects
      // that use raylet transport.
      (*results)[TaskTransportType::RAYLET].insert(object_id);
    }
  }
}

CoreWorkerObjectInterface::CoreWorkerObjectInterface(
    WorkerContext &worker_context, std::unique_ptr<RayletClient> &raylet_client,
    CoreWorkerStoreProviderLayer &store_provider_layer,
    CoreWorkerTaskSubmitterLayer &task_submitter_layer)
    : worker_context_(worker_context),
      raylet_client_(raylet_client),
      store_provider_layer_(store_provider_layer),
      task_submitter_layer_(task_submitter_layer) {
}

Status CoreWorkerObjectInterface::Put(const RayObject &object, ObjectID *object_id) {
  ObjectID put_id = ObjectID::ForPut(worker_context_.GetCurrentTaskID(),
                                     worker_context_.GetNextPutIndex(),
                                     /*transport_type=*/0);
  *object_id = put_id;
  return Put(object, put_id);
}

Status CoreWorkerObjectInterface::Put(const RayObject &object,
                                      const ObjectID &object_id) {
  // `ray.put` always writes objects to plasma.                              
  return store_provider_layer_.Put(StoreProviderType::PLASMA, object, object_id);
}

Status CoreWorkerObjectInterface::Get(const std::vector<ObjectID> &ids,
                                      int64_t timeout_ms,
                                      std::vector<std::shared_ptr<RayObject>> *results) {
  (*results).resize(ids.size(), nullptr);
  // TODO(zhijunfu): should determine transport based on object id, and then
  // find the store provider that the transport is using.
  
  // There can be a few cases here:
  // - for task return objects, find the store provider type for an object from
  //   its transport, and then try to get from the corresponding store provider;
  // - for other objects, try to get from plasma store.
  EnumUnorderedMap<TaskTransportType, std::unordered_set<ObjectID>> object_ids_per_transport;
  GetObjectIdsPerTransport(ids, &object_ids_per_transport);

  std::unordered_map<ObjectID, std::shared_ptr<RayObject>> objects;
  auto current_timeout_ms = timeout_ms;

  for (const auto &entry : object_ids_per_transport) {
    auto start_time = current_time_ms();    
    auto store_provider_type =
        task_submitter_layer_.GetStoreProviderTypeForReturnObject(entry.first);
    RAY_RETURN_NOT_OK(Get(store_provider_type, entry.first, entry.second, current_timeout_ms, &objects));
    int64_t duration = current_time_ms() - start_time;
    current_timeout_ms =
        (current_timeout_ms == -1) ? current_timeout_ms
                         : std::max(static_cast<int64_t>(0), current_timeout_ms - duration);
  }

  for (size_t i = 0; i < ids.size(); i++) {
    (*results)[i] = objects[ids[i]];
  }

  return Status::OK();
}

bool CoreWorkerObjectInterface::ShouldWaitObjects(TaskTransportType transport_type,
    const std::vector<ObjectID> &object_ids) {

  for (const auto &object_id : object_ids) {
    bool should_wait = task_submitter_layer_.ShouldWaitTask(transport_type, object_id.TaskId());
    if (should_wait) {
      return true;
    }
  }

  return false;
}

Status CoreWorkerObjectInterface::Get(
    StoreProviderType type, TaskTransportType transport_type,
    const std::unordered_set<ObjectID> &object_ids,
    int64_t timeout_ms,
    std::unordered_map<ObjectID, std::shared_ptr<RayObject>> *results) {
  if (object_ids.empty()) {
    return Status::OK();
  }

  if (type == StoreProviderType::PLASMA) {
    std::vector<ObjectID> unready_ids(object_ids.begin(), object_ids.end());
    std::vector<std::shared_ptr<RayObject>> result_objects;
    RAY_RETURN_NOT_OK(store_provider_layer_.Get(type, unready_ids, timeout_ms, &result_objects));
    for (size_t i = 0; i < unready_ids.size(); i++) {
      (*results).emplace(unready_ids[i], result_objects[i]);
    }
    return Status::OK();
  }

  std::unordered_set<ObjectID> unready(object_ids);

  int num_attempts = 0;
  bool should_break = false;
  int64_t remaining_timeout = timeout_ms;
  // Repeat until we get all objects.
  while (!unready.empty() && !should_break) {
    std::vector<ObjectID> unready_ids;
    for (const auto &entry : unready) {
      unready_ids.push_back(entry);
    }

    // Check whether we should wait for objects to be created/reconstructed,
    // or just fetch from store.
    bool should_wait = ShouldWaitObjects(transport_type, unready_ids);

    // Get the objects from the object store, and parse the result.
    int64_t get_timeout = RayConfig::instance().get_timeout_milliseconds();
    if (!should_wait) {
      get_timeout = 0;
      remaining_timeout = 0;
      should_break = true;
    } else if (remaining_timeout >= 0) {
      get_timeout = std::min(remaining_timeout, get_timeout);
      remaining_timeout -= get_timeout;
      should_break = remaining_timeout <= 0;
    }

    std::vector<std::shared_ptr<RayObject>> result_objects;
    RAY_RETURN_NOT_OK(store_provider_layer_.Get(type, unready_ids, get_timeout, &result_objects));      

    for (size_t i = 0; i < result_objects.size(); i++) {
      if (result_objects[i] != nullptr) {
        const auto &object_id = unready_ids[i];
        (*results).emplace(object_id, result_objects[i]);
        unready.erase(object_id);
        if (result_objects[i]->IsException()) {
          should_break = true;
        }
      }
    }

    num_attempts += 1;
    CoreWorkerStoreProvider::WarnIfAttemptedTooManyTimes(num_attempts, unready);
  
    if (!should_wait && !unready.empty()) {
      // If the tasks that created these objects have already finished, but we are still
      // not able to get some of the objects from store, it's likely that these objects
      // have been evicted from store, so them as unreconstructable.
      std::string meta = std::to_string(static_cast<int>(rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE));
      auto metadata = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
      auto meta_buffer = std::make_shared<LocalMemoryBuffer>(metadata, meta.size(), true);
      auto object = std::make_shared<RayObject>(nullptr, meta_buffer);
      for (const auto &entry : unready) {
        (*results).emplace(entry, object);
      }
    }
  }

  return Status::OK();
}  

Status CoreWorkerObjectInterface::Wait(const std::vector<ObjectID> &object_ids,
                                       int num_objects, int64_t timeout_ms,
                                       std::vector<bool> *results) {
  // TODO: if this is plasma, call the raylet client's wait.
  // otherwise, use a loop to invoke 
  return store_provider_layer_.Wait(StoreProviderType::PLASMA,
      object_ids, num_objects, timeout_ms,
      results);
}

Status CoreWorkerObjectInterface::Delete(const std::vector<ObjectID> &object_ids,
                                         bool local_only, bool delete_creating_tasks) {
  return store_provider_layer_.Delete(StoreProviderType::PLASMA, object_ids, local_only,
                                      delete_creating_tasks);
}

}  // namespace ray
