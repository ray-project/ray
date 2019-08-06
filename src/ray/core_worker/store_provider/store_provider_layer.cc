#include "ray/core_worker/store_provider/local_plasma_provider.h"
#include "ray/core_worker/store_provider/plasma_store_provider.h"
#include "ray/common/ray_config.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"


namespace ray {

CoreWorkerStoreProviderLayer::CoreWorkerStoreProviderLayer(
    const WorkerContext &worker_context,
    const std::string &store_socket,
    std::unique_ptr<RayletClient> &raylet_client)
    : worker_context_(worker_context),
      store_socket_(store_socket),
      raylet_client_(raylet_client) {
  AddStoreProvider(StoreProviderType::LOCAL_PLASMA);
  AddStoreProvider(StoreProviderType::PLASMA);
}

Status CoreWorkerStoreProviderLayer::Put(StoreProviderType type, 
                                      const RayObject &object,
                                      const ObjectID &object_id) {
  return store_providers_[type]->Put(object, object_id);
}

Status CoreWorkerStoreProviderLayer::Get(StoreProviderType type,
                                      const std::vector<ObjectID> &ids,
                                      int64_t timeout_ms,
                                      std::vector<std::shared_ptr<RayObject>> *results) {
  // plasma is special that it already handles the task status itself.                                        
  if (type == StoreProviderType::PLASMA) {
    return store_providers_[type]->Get(ids, timeout_ms, results);
  }


  (*results).resize(ids.size(), nullptr);

  const TaskID &task_id = worker_context_.GetCurrentTaskID();

  std::unordered_map<ObjectID, int> unready;
  for (size_t i = 0; i < ids.size(); i++) {
    unready.insert({ids[i], i});
  }

  int num_attempts = 0;
  bool should_break = false;
  int64_t remaining_timeout = timeout_ms;
  // Repeat until we get all objects.
  while (!unready.empty() && !should_break) {
    std::vector<ObjectID> unready_ids;
    for (const auto &entry : unready) {
      unready_ids.push_back(entry.first);
    }

    // TODO(zhijunfu): can call `fetchOrReconstruct` in batches as an optimization.
    bool done = ObjectsDone(unready_ids);

    // Get the objects from the object store, and parse the result.
    int64_t get_timeout;
    if (remaining_timeout >= 0) {
      get_timeout =
          std::min(remaining_timeout, RayConfig::instance().get_timeout_milliseconds());
      remaining_timeout -= get_timeout;
      should_break = remaining_timeout <= 0;
    } else {
      get_timeout = RayConfig::instance().get_timeout_milliseconds();
    }

    std::vector<std::shared_ptr<RayObject>> result_objects;
    RAY_RETURN_NOT_OK(
      store_providers_[type]->Get(unready_ids, get_timeout, &result_objects));

    for (size_t i = 0; i < result_objects.size(); i++) {
      if (result_objects[i] != nullptr) {
        const auto &object_id = unready_ids[i];
        (*results)[unready[object_id]] = result_objects[i];
        unready.erase(object_id);
        if (IsException(*result_objects[i])) {
          should_break = true;
        }
      }
    }

    num_attempts += 1;
    WarnIfAttemptedTooManyTimes(num_attempts, unready);
    if (done) {
      break;
    }
  }
  return Status::OK();
}

Status CoreWorkerStoreProviderLayer::Wait(StoreProviderType type,
                                       const std::vector<ObjectID> &object_ids,
                                       int num_objects, 
                                       int64_t timeout_ms,
                                       std::vector<bool> *results) {
  // TODO(zhijunfu): for other types other than PLASMA, we need to use a loop
  // like in `Get`.
  return store_providers_[type]->Wait(object_ids, num_objects, timeout_ms, results);
}

Status CoreWorkerStoreProviderLayer::Delete(StoreProviderType type,
                                         const std::vector<ObjectID> &object_ids,
                                         bool local_only,
                                         bool delete_creating_tasks) {
  return store_providers_[type]->Delete(object_ids, local_only, delete_creating_tasks);
}

void CoreWorkerStoreProviderLayer::AddStoreProvider(StoreProviderType type) {
  store_providers_.emplace(type, CreateStoreProvider(type));
}

std::unique_ptr<CoreWorkerStoreProvider> CoreWorkerStoreProviderLayer::CreateStoreProvider(
    StoreProviderType type) const {
  switch (type) {
  case StoreProviderType::LOCAL_PLASMA:
    return std::unique_ptr<CoreWorkerStoreProvider>(
        new CoreWorkerLocalPlasmaStoreProvider(store_socket_));
    break;
  case StoreProviderType::PLASMA:
    return std::unique_ptr<CoreWorkerStoreProvider>(
        new CoreWorkerPlasmaStoreProvider(worker_context_, store_socket_, raylet_client_));
    break;
  default:
    RAY_LOG(FATAL) << "unknown store provider type " << static_cast<int>(type);
    break;
  }
}

bool CoreWorkerStoreProviderLayer::ObjectsDone(const std::vector<ObjectID> &object_ids) {

  for (const auto &object_id : object_ids) {
    // TODO(zhijunfu): implement this.
    // FIXME.
    auto type = GetTransportType(object_id);
    bool is_task_done = task_submitter_layer_.IsTaskdone(type, object_id.TaskId());
    if (!is_task_done) {
      return false;
    }
  }

  return true;
}


bool CoreWorkerPlasmaStoreProvider::IsException(const RayObject &object) {
  // TODO (kfstorm): metadata should be structured.
  const std::string metadata(reinterpret_cast<const char *>(object.GetMetadata()->Data()),
                             object.GetMetadata()->Size());
  const auto error_type_descriptor = ray::rpc::ErrorType_descriptor();
  for (int i = 0; i < error_type_descriptor->value_count(); i++) {
    const auto error_type_number = error_type_descriptor->value(i)->number();
    if (metadata == std::to_string(error_type_number)) {
      return true;
    }
  }
  return false;
}

void CoreWorkerPlasmaStoreProvider::WarnIfAttemptedTooManyTimes(
    int num_attempts, const std::unordered_map<ObjectID, int> &unready) {
  if (num_attempts % RayConfig::instance().object_store_get_warn_per_num_attempts() ==
      0) {
    std::ostringstream oss;
    size_t printed = 0;
    for (auto &entry : unready) {
      if (printed >=
          RayConfig::instance().object_store_get_max_ids_to_print_in_warning()) {
        break;
      }
      if (printed > 0) {
        oss << ", ";
      }
      oss << entry.first.Hex();
    }
    if (printed < unready.size()) {
      oss << ", etc";
    }
    RAY_LOG(WARNING)
        << "Attempted " << num_attempts << " times to reconstruct objects, but "
        << "some objects are still unavailable. If this message continues to print,"
        << " it may indicate that object's creating task is hanging, or something wrong"
        << " happened in raylet backend. " << unready.size()
        << " object(s) pending: " << oss.str() << ".";
  }
}




}  // namespace ray
