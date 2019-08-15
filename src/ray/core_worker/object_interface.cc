#include <algorithm>

#include "ray/common/ray_config.h"
#include "ray/core_worker/object_interface.h"
#include "ray/core_worker/store_provider/local_plasma_provider.h"
#include "ray/core_worker/store_provider/memory_store_provider.h"
#include "ray/core_worker/store_provider/plasma_store_provider.h"

namespace ray {

void ObjectIdsByStoreProvider(
    const std::vector<ObjectID> &object_ids,
    EnumUnorderedMap<StoreProviderType, std::unordered_set<ObjectID>> *results) {
  // There can be a few cases here:
  // - for task return objects, determine the store provider type for an object
  //   from its transport type;
  // - for other objects, just use plasma store provider.
  for (const auto &object_id : object_ids) {
    // By default use `PLASMA` store provider for all the objects.
    auto type = StoreProviderType::PLASMA;
    // Use `MEMORY` store provider for task returns objects from direct actor call.
    if (object_id.IsReturnObject() &&
        object_id.GetTransportType() ==
            static_cast<int>(TaskTransportType::DIRECT_ACTOR)) {
      type = StoreProviderType::MEMORY;
    }

    (*results)[type].insert(object_id);
  }
}

CoreWorkerObjectInterface::CoreWorkerObjectInterface(
    WorkerContext &worker_context, std::unique_ptr<RayletClient> &raylet_client,
    const std::string &store_socket)
    : worker_context_(worker_context),
      raylet_client_(raylet_client),
      store_socket_(store_socket),
      memory_store_(std::make_shared<CoreWorkerMemoryStore>()) {
  AddStoreProvider(StoreProviderType::LOCAL_PLASMA);
  AddStoreProvider(StoreProviderType::PLASMA);
  AddStoreProvider(StoreProviderType::MEMORY);
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
  return store_providers_[StoreProviderType::PLASMA]->Put(object, object_id);
}

Status CoreWorkerObjectInterface::Get(const std::vector<ObjectID> &ids,
                                      int64_t timeout_ms,
                                      std::vector<std::shared_ptr<RayObject>> *results) {
  (*results).resize(ids.size(), nullptr);

  EnumUnorderedMap<StoreProviderType, std::unordered_set<ObjectID>>
      object_ids_per_store_provider;
  GetObjectIdsPerStoreProvider(ids, &object_ids_per_store_provider);

  std::unordered_map<ObjectID, std::shared_ptr<RayObject>> objects;
  auto current_timeout_ms = timeout_ms;

  // Note that if one store provider uses up the timeout, we will still try the others
  // with a timeout of 0.
  for (const auto &entry : object_ids_per_store_provider) {
    auto start_time = current_time_ms();
    RAY_RETURN_NOT_OK(Get(entry.first, entry.second, current_timeout_ms, &objects));
    if (current_timeout_ms > 0) {
      int64_t duration = current_time_ms() - start_time;
      current_timeout_ms =
          std::max(static_cast<int64_t>(0), current_timeout_ms - duration);
    }
  }

  for (size_t i = 0; i < ids.size(); i++) {
    (*results)[i] = objects[ids[i]];
  }

  return Status::OK();
}

Status CoreWorkerObjectInterface::Get(
    StoreProviderType type, const std::unordered_set<ObjectID> &object_ids,
    int64_t timeout_ms,
    std::unordered_map<ObjectID, std::shared_ptr<RayObject>> *results) {
  std::vector<ObjectID> ids(object_ids.begin(), object_ids.end());
  if (!ids.empty()) {
    std::vector<std::shared_ptr<RayObject>> objects;
    RAY_RETURN_NOT_OK(store_providers_[type]->Get(
        ids, timeout_ms, worker_context_.GetCurrentTaskID(), &objects));
    RAY_CHECK(ids.size() == objects.size());
    for (size_t i = 0; i < objects.size(); i++) {
      if (objects[i] != nullptr) {
        (*results).emplace(ids[i], objects[i]);
      }
    }
  }

  return Status::OK();
}

Status CoreWorkerObjectInterface::Wait(const std::vector<ObjectID> &ids, int num_objects,
                                       int64_t timeout_ms, std::vector<bool> *results) {
  (*results).resize(ids.size(), false);

  if (num_objects <= 0 || num_objects > ids.size()) {
    return Status::Invalid("num_objects value is not valid");
  }

  EnumUnorderedMap<StoreProviderType, std::unordered_set<ObjectID>>
      object_ids_per_store_provider;
  GetObjectIdsPerStoreProvider(ids, &object_ids_per_store_provider);

  std::unordered_map<ObjectID, int> object_counts;
  for (const auto &entry : ids) {
    auto iter = object_counts.find(entry);
    if (iter == object_counts.end()) {
      object_counts.emplace(entry, 1);
    } else {
      iter->second++;
    }
  }

  auto wait_from_store_providers = [&ids, &object_ids_per_store_provider, &num_objects,
                                    &object_counts, results, this](int64_t timeout) {
    auto current_timeout_ms = timeout;
    for (const auto &entry : object_ids_per_store_provider) {
      std::unordered_set<ObjectID> objects;
      auto start_time = current_time_ms();
      int required_objects = std::min(static_cast<int>(entry.second.size()), num_objects);
      RAY_RETURN_NOT_OK(Wait(entry.first, entry.second, required_objects,
                             current_timeout_ms, &objects));
      if (current_timeout_ms > 0) {
        int64_t duration = current_time_ms() - start_time;
        current_timeout_ms =
            std::max(static_cast<int64_t>(0), current_timeout_ms - duration);
      }
      for (const auto &entry : objects) {
        num_objects -= object_counts[entry];
      }

      for (size_t i = 0; i < ids.size(); i++) {
        if (objects.count(ids[i]) > 0) {
          (*results)[i] = true;
        }
      }

      if (num_objects <= 0) {
        break;
      }
    }

    return Status::OK();
  };

  // Wait from all the store providers with timeout set to 0. This is to avoid the case
  // where we might use up the entire timeout on trying to get objects from one store
  // provider before even trying another (which might have all of the objects available).
  RAY_RETURN_NOT_OK(wait_from_store_providers(0));

  if (num_objects > 0) {
    // Wait from all the store providers with the specified timeout
    // if the required number of objects haven't been ready yet.
    RAY_RETURN_NOT_OK(wait_from_store_providers(timeout_ms));
  }

  return Status::OK();
}

Status CoreWorkerObjectInterface::Wait(StoreProviderType type,
                                       const std::unordered_set<ObjectID> &object_ids,
                                       int num_objects, int64_t timeout_ms,
                                       std::unordered_set<ObjectID> *results) {
  std::vector<ObjectID> ids(object_ids.begin(), object_ids.end());
  if (!ids.empty()) {
    std::vector<bool> objects;
    RAY_RETURN_NOT_OK(store_providers_[type]->Wait(
        ids, num_objects, timeout_ms, worker_context_.GetCurrentTaskID(), &objects));
    RAY_CHECK(ids.size() == objects.size());
    for (size_t i = 0; i < objects.size(); i++) {
      if (objects[i]) {
        (*results).insert(ids[i]);
      }
    }
  }

  return Status::OK();
}

Status CoreWorkerObjectInterface::Delete(const std::vector<ObjectID> &object_ids,
                                         bool local_only, bool delete_creating_tasks) {
  EnumUnorderedMap<StoreProviderType, std::unordered_set<ObjectID>>
      object_ids_per_store_provider;
  GetObjectIdsPerStoreProvider(object_ids, &object_ids_per_store_provider);

  for (const auto &entry : object_ids_per_store_provider) {
    auto type = entry.first;
    bool is_plasma = (type == StoreProviderType::PLASMA);

    std::vector<ObjectID> ids(entry.second.begin(), entry.second.end());
    RAY_RETURN_NOT_OK(store_providers_[type]->Delete(
        ids, is_plasma ? local_only : false, is_plasma ? delete_creating_tasks : false));
  }

  return Status::OK();
}

void CoreWorkerObjectInterface::AddStoreProvider(StoreProviderType type) {
  store_providers_.emplace(type, CreateStoreProvider(type));
}

std::unique_ptr<CoreWorkerStoreProvider> CoreWorkerObjectInterface::CreateStoreProvider(
    StoreProviderType type) const {
  switch (type) {
  case StoreProviderType::LOCAL_PLASMA:
    return std::unique_ptr<CoreWorkerStoreProvider>(
        new CoreWorkerLocalPlasmaStoreProvider(store_socket_));
  case StoreProviderType::PLASMA:
    return std::unique_ptr<CoreWorkerStoreProvider>(
        new CoreWorkerPlasmaStoreProvider(store_socket_, raylet_client_));
  case StoreProviderType::MEMORY:
    return std::unique_ptr<CoreWorkerStoreProvider>(
        new CoreWorkerMemoryStoreProvider(memory_store_));
    break;
  default:
    RAY_LOG(FATAL) << "unknown store provider type " << static_cast<int>(type);
    return nullptr;
  }
}

}  // namespace ray
