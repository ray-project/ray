#include <algorithm>

#include "ray/common/ray_config.h"
#include "ray/core_worker/object_interface.h"
#include "ray/core_worker/store_provider/memory_store_provider.h"
#include "ray/core_worker/store_provider/plasma_store_provider.h"

namespace ray {

// Group object ids according the the corresponding store providers.
void CoreWorkerObjectInterface::GroupObjectIdsByStoreProvider(
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
    if (use_memory_store_ && object_id.IsReturnObject() &&
        object_id.GetTransportType() ==
            static_cast<uint8_t>(TaskTransportType::DIRECT_ACTOR)) {
      type = StoreProviderType::MEMORY;
    }

    (*results)[type].insert(object_id);
  }
}

CoreWorkerObjectInterface::CoreWorkerObjectInterface(
    WorkerContext &worker_context, std::unique_ptr<RayletClient> &raylet_client,
    const std::string &store_socket, bool use_memory_store)
    : worker_context_(worker_context),
      raylet_client_(raylet_client),
      store_socket_(store_socket),
      use_memory_store_(use_memory_store),
      memory_store_(std::make_shared<CoreWorkerMemoryStore>()) {
  AddStoreProvider(StoreProviderType::PLASMA);
  AddStoreProvider(StoreProviderType::MEMORY);
}

Status CoreWorkerObjectInterface::SetClientOptions(std::string name,
                                                   int64_t limit_bytes) {
  // Currently only the Plasma store supports client options.
  return store_providers_[StoreProviderType::PLASMA]->SetClientOptions(name, limit_bytes);
}

Status CoreWorkerObjectInterface::Put(const RayObject &object, ObjectID *object_id) {
  ObjectID put_id = ObjectID::ForPut(worker_context_.GetCurrentTaskID(),
                                     worker_context_.GetNextPutIndex(),
                                     static_cast<uint8_t>(TaskTransportType::RAYLET));
  *object_id = put_id;
  return Put(object, put_id);
}

Status CoreWorkerObjectInterface::Put(const RayObject &object,
                                      const ObjectID &object_id) {
  RAY_CHECK(object_id.GetTransportType() ==
            static_cast<uint8_t>(TaskTransportType::RAYLET))
      << "Invalid transport type flag in object ID: " << object_id.GetTransportType();
  return store_providers_[StoreProviderType::PLASMA]->Put(object, object_id);
}

Status CoreWorkerObjectInterface::Create(const std::shared_ptr<Buffer> &metadata,
                                         const size_t data_size,
                                         const ObjectID &object_id,
                                         std::shared_ptr<Buffer> *data) {
  return store_providers_[StoreProviderType::PLASMA]->Create(metadata, data_size,
                                                             object_id, data);
}

Status CoreWorkerObjectInterface::Seal(const ObjectID &object_id) {
  return store_providers_[StoreProviderType::PLASMA]->Seal(object_id);
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

  std::unordered_map<ObjectID, std::shared_ptr<RayObject>> result_map;
  auto remaining_timeout_ms = timeout_ms;
  bool got_exception = false;

  // Re-order the list so that we always get from plasma store provider first,
  // since it uses a loop of `FetchOrReconstruct` and plasma `Get`, it's not
  // desirable if other store providers use up the timeout and leaves no time
  // for plasma provider to reconstruct the objects as necessary.
  std::list<std::pair<StoreProviderType,
                      std::reference_wrapper<const std::unordered_set<ObjectID>>>>
      ids_per_provider;
  for (const auto &entry : object_ids_per_store_provider) {
    auto list_entry = std::make_pair(entry.first, std::ref(entry.second));
    if (entry.first == StoreProviderType::PLASMA) {
      ids_per_provider.emplace_front(list_entry);
    } else {
      ids_per_provider.emplace_back(list_entry);
    }
  }

  // Note that if one store provider uses up the timeout, we will still try the others
  // with a timeout of 0.
  for (const auto &entry : ids_per_provider) {
    auto start_time = current_time_ms();
    RAY_RETURN_NOT_OK(store_providers_[entry.first]->Get(
        entry.second, remaining_timeout_ms, worker_context_.GetCurrentTaskID(),
        &result_map, &got_exception));
    if (got_exception) {
      break;
    }
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
    if (result_map.find(ids[i]) != result_map.end()) {
      (*results)[i] = result_map[ids[i]];
    }
  }

  return Status::OK();
}

Status CoreWorkerObjectInterface::Contains(const ObjectID &object_id, bool *has_object) {
  // Currently only the Plasma store supports Contains().
  return store_providers_[StoreProviderType::PLASMA]->Contains(object_id, has_object);
}

Status CoreWorkerObjectInterface::Wait(const std::vector<ObjectID> &ids, int num_objects,
                                       int64_t timeout_ms, std::vector<bool> *results) {
  (*results).resize(ids.size(), false);

  if (num_objects <= 0 || num_objects > static_cast<int>(ids.size())) {
    return Status::Invalid(
        "Number of objects to wait for must be between 1 and the number of ids.");
  }

  EnumUnorderedMap<StoreProviderType, std::unordered_set<ObjectID>>
      object_ids_per_store_provider;
  GroupObjectIdsByStoreProvider(ids, &object_ids_per_store_provider);

  size_t total_count = 0;
  for (const auto &entry : object_ids_per_store_provider) {
    total_count += entry.second.size();
  }
  if (total_count != ids.size()) {
    return Status::Invalid("Duplicate object IDs not supported in wait.");
  }

  // TODO(edoakes): this logic is not ideal, and will have to be addressed
  // before we enable direct actor calls in the Python code. If we are waiting
  // on a list of objects mixed between multiple store providers, we could
  // easily end up in the situation where we're blocked waiting on one store
  // provider while another actually has enough objects ready to fulfill
  // 'num_objects'. This is partially addressed by trying them all once with
  // a timeout of 0, but that does not address the situation where objects
  // become available on the second store provider while waiting on the first.

  std::unordered_set<ObjectID> ready;
  // Wait from all the store providers with timeout set to 0. This is to avoid the case
  // where we might use up the entire timeout on trying to get objects from one store
  // provider before even trying another (which might have all of the objects available).
  RAY_RETURN_NOT_OK(WaitFromMultipleStoreProviders(object_ids_per_store_provider,
                                                   /*timeout_ms=*/0, &num_objects,
                                                   &ready));

  if (num_objects > 0) {
    // Wait from all the store providers with the specified timeout
    // if the required number of objects haven't been ready yet.
    RAY_RETURN_NOT_OK(WaitFromMultipleStoreProviders(object_ids_per_store_provider,
                                                     /*timeout_ms=*/timeout_ms,
                                                     &num_objects, &ready));
  }

  for (size_t i = 0; i < ids.size(); i++) {
    if (ready.find(ids[i]) != ready.end()) {
      (*results)[i] = true;
    }
  }

  return Status::OK();
}

Status CoreWorkerObjectInterface::WaitFromMultipleStoreProviders(
    EnumUnorderedMap<StoreProviderType, std::unordered_set<ObjectID>> &ids_per_provider,
    int64_t timeout_ms, int *num_objects, std::unordered_set<ObjectID> *ready) {
  int64_t remaining_timeout_ms = timeout_ms;
  for (auto &provider_entry : ids_per_provider) {
    if (*num_objects <= 0) {
      break;
    }
    int64_t start_time = current_time_ms();
    int required_objects =
        std::min(static_cast<int>(provider_entry.second.size()), *num_objects);
    std::unordered_set<ObjectID> provider_ready;
    RAY_RETURN_NOT_OK(store_providers_[provider_entry.first]->Wait(
        provider_entry.second, required_objects, remaining_timeout_ms,
        worker_context_.GetCurrentTaskID(), &provider_ready));

    // Update num_objects and remove the ready objects from the list so they don't get
    // double-counted.
    *num_objects -= provider_ready.size();
    for (const ObjectID &ready_id : provider_ready) {
      ready->insert(ready_id);
      provider_entry.second.erase(ready_id);
    }

    if (remaining_timeout_ms > 0) {
      int64_t duration = current_time_ms() - start_time;
      remaining_timeout_ms =
          std::max(static_cast<int64_t>(0), remaining_timeout_ms - duration);
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

std::string CoreWorkerObjectInterface::MemoryUsageString() {
  // Currently only the Plasma store returns a debug string.
  return store_providers_[StoreProviderType::PLASMA]->MemoryUsageString();
}

void CoreWorkerObjectInterface::AddStoreProvider(StoreProviderType type) {
  store_providers_.emplace(type, CreateStoreProvider(type));
}

std::unique_ptr<CoreWorkerStoreProvider> CoreWorkerObjectInterface::CreateStoreProvider(
    StoreProviderType type) const {
  switch (type) {
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
