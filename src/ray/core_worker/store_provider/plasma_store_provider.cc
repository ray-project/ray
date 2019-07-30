#include "ray/core_worker/store_provider/plasma_store_provider.h"
#include "ray/common/ray_config.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/object_interface.h"
#include "ray/protobuf/gcs.pb.h"

namespace ray {

CoreWorkerPlasmaStoreProvider::CoreWorkerPlasmaStoreProvider(
    const std::string &store_socket, std::unique_ptr<RayletClient> &raylet_client)
    : local_store_provider_(store_socket), raylet_client_(raylet_client) {}

Status CoreWorkerPlasmaStoreProvider::Put(const RayObject &object,
                                          const ObjectID &object_id) {
  return local_store_provider_.Put(object, object_id);
}

Status CoreWorkerPlasmaStoreProvider::Create(const std::shared_ptr<Buffer> &metadata,
		                               const size_t data_size,
                                               const ObjectID &object_id,
					       std::shared_ptr<Buffer> *data) {
  return local_store_provider_.Create(metadata, data_size, object_id, data);
}

Status CoreWorkerPlasmaStoreProvider::Seal(const ObjectID &object_id) {
  return local_store_provider_.Seal(object_id);
}

Status CoreWorkerPlasmaStoreProvider::Get(
    const std::vector<ObjectID> &ids, int64_t timeout_ms, const TaskID &task_id,
    std::vector<std::shared_ptr<RayObject>> *results) {
  int64_t batch_size = RayConfig::instance().worker_fetch_request_size();
  (*results).resize(ids.size(), nullptr);
  std::unordered_map<ObjectID, std::vector<int>> unready;

  // First, attempt to fetch all of the required objects without reconstructing.
  for (int64_t start = 0; start < ids.size(); start += batch_size) {
    int64_t end = std::max(start + batch_size, int64_t(ids.size()));
    const std::vector<ObjectID> ids_slice(ids.cbegin()+start, ids.cbegin()+end);
    RAY_CHECK_OK(raylet_client_->FetchOrReconstruct(ids_slice, /*fetch_only=*/true, task_id));
    std::vector<std::shared_ptr<RayObject>> results_slice;
    RAY_RETURN_NOT_OK(local_store_provider_.Get(ids_slice, 0, task_id, &results_slice));

    // Iterate through the results from the local store, adding them to the unready
    // map if they weren't successfully fetched from the local store (are nullptr).
    // Keeps track of the locations of the unready object IDs in the original list
    // (accounting for duplicates).
    for (size_t i = 0; i < ids_slice.size(); i++) {
      if (results_slice[i] != nullptr) {
        (*results)[start+i] = results_slice[i];
        continue;
      }
      auto it = unready.find(ids_slice[i]);
      if (it == unready.end()) {
        std::vector<int> v;
        v.push_back(start+i);
        unready.insert({ids[i], v});
      } else {
        it->second.push_back(start+i);
      }
    }
  }

  // If all objects were fetched already, return.
  if (unready.empty()) {
    return Status::OK();
  }

  // If not all objects were successfully fetched, repeatedly call FetchOrReconstruct and 
  // Get from the local object store in batches. This loop will run indefinitely until the
  // objects are all fetched if timeout is -1.
  int num_attempts = 0;
  bool should_break = false;
  int64_t remaining_timeout = timeout_ms;
  while (!unready.empty() && !should_break) {
    std::vector<ObjectID> unready_ids;
    for (const auto &entry : unready) {
      if (unready_ids.size() == batch_size) {
        break;
      }
      unready_ids.push_back(entry.first);
    }

    RAY_CHECK_OK(raylet_client_->FetchOrReconstruct(unready_ids, /*fetch_only=*/false, task_id));

    int64_t timeout = std::max(
		    RayConfig::instance().get_timeout_milliseconds(), int64_t(0.01 * unready.size()));
    if (remaining_timeout >= 0) {
      timeout = std::min(remaining_timeout, timeout);
      remaining_timeout -= timeout;
      should_break = remaining_timeout <= 0;
    }

    std::vector<std::shared_ptr<RayObject>> result_objects;
    RAY_RETURN_NOT_OK(local_store_provider_.Get(unready_ids, timeout, task_id, &result_objects));

    // Add successfully retrieved objects to the result list and remove them from unready.
    uint64_t successes = 0;
    for (size_t i = 0; i < result_objects.size(); i++) {
      if (result_objects[i] != nullptr) {
        successes++;
        const auto &object_id = unready_ids[i];
        for (int idx : unready[object_id]) {
          (*results)[idx] = result_objects[i];
	}
        unready.erase(object_id);
        if (IsException(*result_objects[i])) {
          should_break = true;
        }
      }
    }

    if (successes < unready_ids.size()) {
      num_attempts += 1;
    }
    WarnIfAttemptedTooManyTimes(num_attempts, unready);
  }

  // Notify unblocked because we blocked when calling FetchOrReconstruct with fetch_only=false.
  return raylet_client_->NotifyUnblocked(task_id);
}

Status CoreWorkerPlasmaStoreProvider::Wait(const std::vector<ObjectID> &object_ids,
                                           int num_objects, int64_t timeout_ms,
                                           const TaskID &task_id,
                                           std::vector<bool> *results) {
  WaitResultPair result_pair;
  auto status = raylet_client_->Wait(object_ids, num_objects, timeout_ms, /*wait_local=*/false, task_id,
                                     &result_pair);
  std::unordered_set<ObjectID> ready_ids;
  for (const auto &entry : result_pair.first) {
    ready_ids.insert(entry);
  }

  // TODO(zhijunfu): change RayletClient::Wait() to return a bit set, so that we don't
  // need
  // to do this translation.
  (*results).resize(object_ids.size());
  for (size_t i = 0; i < object_ids.size(); i++) {
    (*results)[i] = ready_ids.count(object_ids[i]) > 0;
  }

  return status;
}

Status CoreWorkerPlasmaStoreProvider::Free(const std::vector<ObjectID> &object_ids,
                                             bool local_only,
                                             bool delete_creating_tasks) {
  return raylet_client_->FreeObjects(object_ids, local_only, delete_creating_tasks);
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
    int num_attempts, const std::unordered_map<ObjectID, std::vector<int>> &unready) {
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
