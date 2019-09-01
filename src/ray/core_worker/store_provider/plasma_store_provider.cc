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

Status CoreWorkerPlasmaStoreProvider::Get(
    const std::vector<ObjectID> &ids, int64_t timeout_ms, const TaskID &task_id,
    std::vector<std::shared_ptr<RayObject>> *results) {
  int64_t batch_size = RayConfig::instance().worker_fetch_request_size();
  (*results).resize(ids.size(), nullptr);
  std::unordered_map<ObjectID, int> remaining;

  // First, attempt to fetch all of the required objects without reconstructing.
  for (int64_t start = 0; start < int64_t(ids.size()); start += batch_size) {
    int64_t end = std::min(start + batch_size, int64_t(ids.size()));
    const std::vector<ObjectID> ids_slice(ids.cbegin() + start, ids.cbegin() + end);
    RAY_CHECK_OK(
        raylet_client_->FetchOrReconstruct(ids_slice, /*fetch_only=*/true, task_id));
    std::vector<std::shared_ptr<RayObject>> results_slice;
    RAY_RETURN_NOT_OK(local_store_provider_.Get(ids_slice, 0, task_id, &results_slice));

    // Iterate through the results from the local store, adding them to the remaining
    // map if they weren't successfully fetched from the local store (are nullptr).
    // Keeps track of the locations of the remaining object IDs in the original list.
    for (size_t i = 0; i < ids_slice.size(); i++) {
      if (results_slice[i] != nullptr) {
        (*results)[start + i] = results_slice[i];
        // Terminate early on exception because it'll be raised by the worker anyways.
        if (IsException(*results_slice[i])) {
          return Status::OK();
        }
      } else {
        remaining.insert({ids_slice[i], start + i});
      }
    }
  }

  // If all objects were fetched already, return.
  if (remaining.empty()) {
    return Status::OK();
  }

  // If not all objects were successfully fetched, repeatedly call FetchOrReconstruct and
  // Get from the local object store in batches. This loop will run indefinitely until the
  // objects are all fetched if timeout is -1.
  int unsuccessful_attempts = 0;
  bool should_break = false;
  int64_t remaining_timeout = timeout_ms;
  while (!remaining.empty() && !should_break) {
    std::vector<ObjectID> batch_ids;
    for (const auto &entry : remaining) {
      if (int64_t(batch_ids.size()) == batch_size) {
        break;
      }
      batch_ids.push_back(entry.first);
    }

    RAY_CHECK_OK(
        raylet_client_->FetchOrReconstruct(batch_ids, /*fetch_only=*/false, task_id));

    int64_t batch_timeout = std::max(RayConfig::instance().get_timeout_milliseconds(),
                                     int64_t(10 * batch_ids.size()));
    if (remaining_timeout >= 0) {
      batch_timeout = std::min(remaining_timeout, batch_timeout);
      remaining_timeout -= batch_timeout;
      should_break = remaining_timeout <= 0;
    }

    std::vector<std::shared_ptr<RayObject>> batch_results;
    RAY_RETURN_NOT_OK(
        local_store_provider_.Get(batch_ids, batch_timeout, task_id, &batch_results));

    // Add successfully retrieved objects to the result list and remove them from
    // remaining.
    uint64_t successes = 0;
    for (size_t i = 0; i < batch_results.size(); i++) {
      if (batch_results[i] != nullptr) {
        successes++;
        const auto &object_id = batch_ids[i];
        (*results)[remaining[object_id]] = batch_results[i];
        remaining.erase(object_id);
        if (IsException(*batch_results[i])) {
          should_break = true;
        }
      }
    }

    if (successes < batch_ids.size()) {
      unsuccessful_attempts++;
      WarnIfAttemptedTooManyTimes(unsuccessful_attempts, remaining);
    }
  }

  // Notify unblocked because we blocked when calling FetchOrReconstruct with
  // fetch_only=false.
  return raylet_client_->NotifyUnblocked(task_id);
}

Status CoreWorkerPlasmaStoreProvider::Wait(const std::vector<ObjectID> &object_ids,
                                           int num_objects, int64_t timeout_ms,
                                           const TaskID &task_id,
                                           std::vector<bool> *results) {
  WaitResultPair result_pair;
  auto status = raylet_client_->Wait(object_ids, num_objects, timeout_ms, false, task_id,
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

Status CoreWorkerPlasmaStoreProvider::Delete(const std::vector<ObjectID> &object_ids,
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
    int num_attempts, const std::unordered_map<ObjectID, int> &remaining) {
  if (num_attempts % RayConfig::instance().object_store_get_warn_per_num_attempts() ==
      0) {
    std::ostringstream oss;
    size_t printed = 0;
    for (auto &entry : remaining) {
      if (printed >=
          RayConfig::instance().object_store_get_max_ids_to_print_in_warning()) {
        break;
      }
      if (printed > 0) {
        oss << ", ";
      }
      oss << entry.first.Hex();
    }
    if (printed < remaining.size()) {
      oss << ", etc";
    }
    RAY_LOG(WARNING)
        << "Attempted " << num_attempts << " times to reconstruct objects, but "
        << "some objects are still unavailable. If this message continues to print,"
        << " it may indicate that object's creating task is hanging, or something wrong"
        << " happened in raylet backend. " << remaining.size()
        << " object(s) pending: " << oss.str() << ".";
  }
}

}  // namespace ray
