#include <algorithm>

#include "ray/common/ray_config.h"
#include "ray/core_worker/object_interface.h"
#include "ray/core_worker/store_provider/local_plasma_provider.h"
#include "ray/core_worker/store_provider/memory_store_provider.h"
#include "ray/core_worker/store_provider/plasma_store_provider.h"

namespace ray {

// Group object ids according the the corresponding store providers.
void GroupObjectIdsByStoreProvider(
    const std::vector<ObjectID> &object_ids,
    EnumUnorderedMap<StoreProviderType, std::unordered_set<ObjectID>> *results) {
  // There are two cases:
  // - for task return objects from direct actor call, use memory store provider;
  // - all the others use plasma store provider.
  for (const auto &object_id : object_ids) {
    auto type = StoreProviderType::PLASMA;
    // For raylet transport we always use plasma store provider, for direct actor call
    // there are a few cases:
    // - objects manually added to store by `ray.put`: for these objects they always use
    //   plasma store provider;
    // - task arguments: these objects are passed by value, and are not put into store;
    // - task return objects: these are put into memory store of the task submitter
    //   and are only used locally.
    // Thus we need to check whether this object is a task return object in additional
    // to whether it's from direct actor call before we can choose memory store provider.
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

  // Divide the object ids by store provider type. For each store provider,
  // maintain an unordered_set which does proper de-duplication, thus the
  // store provider could simply assume its object ids don't have duplicates.
  EnumUnorderedMap<StoreProviderType, std::unordered_set<ObjectID>>
      object_ids_per_store_provider;
  GroupObjectIdsByStoreProvider(ids, &object_ids_per_store_provider);

  std::unordered_map<ObjectID, std::shared_ptr<RayObject>> objects;
  auto remaining_timeout_ms = timeout_ms;

  // Re-order the list so that we always get from plasma store provider first,
  // since it uses a loop of `FetchOrReconstruct` and plasma `Get`, it's not
  // desirable if other store providers use up the timeout and leaves no time
  // for plasma provider to reconstruct the objects as necessary.
  std::list<
      std::pair<StoreProviderType, std::reference_wrapper<std::unordered_set<ObjectID>>>>
      ids_per_provider;
  for (auto &entry : object_ids_per_store_provider) {
    auto list_entry = std::make_pair(entry.first, std::ref(entry.second));
    if (entry.first == StoreProviderType::PLASMA) {
      ids_per_provider.emplace_front(list_entry);
    } else {
      ids_per_provider.emplace_back(list_entry);
    }
  }

  // Note that if one store provider uses up the timeout, we will still try the others
  // with a timeout of 0.
  for (const auto &entry : object_ids_per_store_provider) {
    auto start_time = current_time_ms();
    RAY_RETURN_NOT_OK(
        GetFromStoreProvider(entry.first, entry.second, remaining_timeout_ms, &objects));
    if (remaining_timeout_ms > 0) {
      int64_t duration = current_time_ms() - start_time;
      remaining_timeout_ms =
          std::max(static_cast<int64_t>(0), remaining_timeout_ms - duration);
    }
  }

  // Loop through `ids` and fill each entry for the `results` vector,
  // this ensures that entries `results` have exactly the same order as
  // they are in `ids`. When there are duplicate object ids, all the entries
  // for the same id are filled in.
  for (size_t i = 0; i < ids.size(); i++) {
    (*results)[i] = objects[ids[i]];
  }

  return Status::OK();
}

Status CoreWorkerObjectInterface::GetFromStoreProvider(
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
      (*results).emplace(ids[i], objects[i]);
    }
  }

  return Status::OK();
}

Status CoreWorkerObjectInterface::Wait(const std::vector<ObjectID> &ids, int num_objects,
                                       int64_t timeout_ms, std::vector<bool> *results) {
  (*results).resize(ids.size(), false);

  if (num_objects <= 0 || num_objects > static_cast<int>(ids.size())) {
    return Status::Invalid("num_objects value is not valid");
  }

  EnumUnorderedMap<StoreProviderType, std::unordered_set<ObjectID>>
      object_ids_per_store_provider;
  GroupObjectIdsByStoreProvider(ids, &object_ids_per_store_provider);

  // Wait from all the store providers with timeout set to 0. This is to avoid the case
  // where we might use up the entire timeout on trying to get objects from one store
  // provider before even trying another (which might have all of the objects available).
  RAY_RETURN_NOT_OK(WaitFromMultipleStoreProviders(ids, object_ids_per_store_provider,
                                                   /* timeout_ms= */ 0, &num_objects,
                                                   results));

  if (num_objects > 0) {
    // Wait from all the store providers with the specified timeout
    // if the required number of objects haven't been ready yet.
    RAY_RETURN_NOT_OK(WaitFromMultipleStoreProviders(ids, object_ids_per_store_provider,
                                                     /* timeout_ms= */ timeout_ms,
                                                     &num_objects, results));
  }

  return Status::OK();
}

Status CoreWorkerObjectInterface::WaitFromMultipleStoreProviders(
    const std::vector<ObjectID> &ids,
    const EnumUnorderedMap<StoreProviderType, std::unordered_set<ObjectID>>
        &ids_per_provider,
    int64_t timeout_ms, int *num_objects, std::vector<bool> *results) {
  std::unordered_map<ObjectID, int> object_counts;
  for (const auto &entry : ids) {
    auto iter = object_counts.find(entry);
    if (iter == object_counts.end()) {
      object_counts.emplace(entry, 1);
    } else {
      iter->second++;
    }
  }

  auto remaining_timeout_ms = timeout_ms;
  for (const auto &entry : ids_per_provider) {
    std::unordered_set<ObjectID> objects;
    auto start_time = current_time_ms();
    int required_objects = std::min(static_cast<int>(entry.second.size()), *num_objects);
    RAY_RETURN_NOT_OK(WaitFromStoreProvider(entry.first, entry.second, required_objects,
                                            remaining_timeout_ms, &objects));
    if (remaining_timeout_ms > 0) {
      int64_t duration = current_time_ms() - start_time;
      remaining_timeout_ms =
          std::max(static_cast<int64_t>(0), remaining_timeout_ms - duration);
    }
    for (const auto &entry : objects) {
      *num_objects -= object_counts[entry];
    }

    for (size_t i = 0; i < ids.size(); i++) {
      if (objects.count(ids[i]) > 0) {
        (*results)[i] = true;
      }
    }

    if (*num_objects <= 0) {
      break;
    }
  }

  return Status::OK();
};

Status CoreWorkerObjectInterface::WaitFromStoreProvider(
    StoreProviderType type, const std::unordered_set<ObjectID> &object_ids,
    int num_objects, int64_t timeout_ms, std::unordered_set<ObjectID> *results) {
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
  GroupObjectIdsByStoreProvider(object_ids, &object_ids_per_store_provider);

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
