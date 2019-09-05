#include "ray/core_worker/store_provider/plasma_store_provider.h"
#include "ray/common/ray_config.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/object_interface.h"
#include "ray/protobuf/gcs.pb.h"

namespace ray {

CoreWorkerPlasmaStoreProvider::CoreWorkerPlasmaStoreProvider(
    const std::string &store_socket, std::unique_ptr<RayletClient> &raylet_client)
    : raylet_client_(raylet_client) {
  RAY_ARROW_CHECK_OK(store_client_.Connect(store_socket));
}

Status CoreWorkerPlasmaStoreProvider::Put(const RayObject &object,
                                          const ObjectID &object_id) {
  auto plasma_id = object_id.ToPlasmaId();
  auto data = object.GetData();
  auto metadata = object.GetMetadata();
  std::shared_ptr<arrow::Buffer> out_buffer;
  {
    std::unique_lock<std::mutex> guard(store_client_mutex_);
    arrow::Status status = store_client_.Create(
        plasma_id, data ? data->Size() : 0, metadata ? metadata->Data() : nullptr,
        metadata ? metadata->Size() : 0, &out_buffer);
    if (plasma::IsPlasmaObjectExists(status)) {
      // TODO(hchen): Should we propagate this error out of `ObjectInterface::put`?
      RAY_LOG(WARNING) << "Trying to put an object that already existed in plasma: "
                       << object_id << ".";
      return Status::OK();
    }
    RAY_ARROW_RETURN_NOT_OK(status);
  }

  if (data != nullptr) {
    memcpy(out_buffer->mutable_data(), data->Data(), data->Size());
  }

  {
    std::unique_lock<std::mutex> guard(store_client_mutex_);
    RAY_ARROW_RETURN_NOT_OK(store_client_.Seal(plasma_id));
    RAY_ARROW_RETURN_NOT_OK(store_client_.Release(plasma_id));
  }
  return Status::OK();
}

Status CoreWorkerPlasmaStoreProvider::FetchAndGetFromPlasmaStore(
    std::unordered_set<ObjectID> &remaining, const std::vector<ObjectID> &batch_ids,
    int64_t timeout_ms, bool fetch_only, const TaskID &task_id,
    std::unordered_map<ObjectID, std::shared_ptr<RayObject>> *results,
    bool *got_exception) {
  RAY_CHECK_OK(raylet_client_->FetchOrReconstruct(batch_ids, fetch_only, task_id));

  std::vector<plasma::ObjectID> plasma_batch_ids;
  plasma_batch_ids.reserve(batch_ids.size());
  for (size_t i = 0; i < batch_ids.size(); i++) {
    plasma_batch_ids.push_back(batch_ids[i].ToPlasmaId());
  }
  std::vector<plasma::ObjectBuffer> plasma_results;
  {
    std::unique_lock<std::mutex> guard(store_client_mutex_);
    RAY_ARROW_RETURN_NOT_OK(
        store_client_.Get(plasma_batch_ids, timeout_ms, &plasma_results));
  }

  // Add successfully retrieved objects to the result map and remove them from
  // the set of IDs to get.
  for (size_t i = 0; i < plasma_results.size(); i++) {
    if (plasma_results[i].data != nullptr || plasma_results[i].metadata != nullptr) {
      const auto &object_id = batch_ids[i];
      const auto result_object = std::make_shared<RayObject>(
          std::make_shared<PlasmaBuffer>(plasma_results[i].data),
          std::make_shared<PlasmaBuffer>(plasma_results[i].metadata));
      (*results)[object_id] = result_object;
      remaining.erase(object_id);
      if (IsException(*result_object)) {
        *got_exception = true;
      }
    }
  }

  return Status::OK();
}

Status CoreWorkerPlasmaStoreProvider::Get(
    const std::unordered_set<ObjectID> &object_ids, int64_t timeout_ms,
    const TaskID &task_id,
    std::unordered_map<ObjectID, std::shared_ptr<RayObject>> *results) {
  int64_t batch_size = RayConfig::instance().worker_fetch_request_size();
  bool got_exception = false;
  std::vector<ObjectID> batch_ids;
  std::unordered_set<ObjectID> remaining(object_ids.begin(), object_ids.end());

  // First, attempt to fetch all of the required objects once without reconstructing.
  std::vector<ObjectID> id_vector(object_ids.begin(), object_ids.end());
  int64_t total_size = static_cast<int64_t>(object_ids.size());
  for (int64_t start = 0; start < total_size; start += batch_size) {
    batch_ids.clear();
    for (int64_t i = start; i < batch_size && i < total_size; i++) {
      batch_ids.push_back(id_vector[start + i]);
    }
    RAY_RETURN_NOT_OK(FetchAndGetFromPlasmaStore(remaining, batch_ids, /*timeout_ms=*/0,
                                                 /*fetch_only=*/true, task_id, results,
                                                 &got_exception));
  }

  // If all objects were fetched already, return.
  if (remaining.empty() || got_exception) {
    return Status::OK();
  }

  // If not all objects were successfully fetched, repeatedly call FetchOrReconstruct and
  // Get from the local object store in batches. This loop will run indefinitely until the
  // objects are all fetched if timeout is -1.
  int unsuccessful_attempts = 0;
  bool should_break = false;
  int64_t remaining_timeout = timeout_ms;
  while (!remaining.empty() && !should_break) {
    batch_ids.clear();
    for (const auto &id : remaining) {
      if (int64_t(batch_ids.size()) == batch_size) {
        break;
      }
      batch_ids.push_back(id);
    }

    int64_t batch_timeout = std::max(RayConfig::instance().get_timeout_milliseconds(),
                                     int64_t(10 * batch_ids.size()));
    if (remaining_timeout >= 0) {
      batch_timeout = std::min(remaining_timeout, batch_timeout);
      remaining_timeout -= batch_timeout;
      should_break = remaining_timeout <= 0;
    }

    size_t previous_size = remaining.size();
    RAY_RETURN_NOT_OK(FetchAndGetFromPlasmaStore(remaining, batch_ids, batch_timeout,
                                                 /*fetch_only=*/false, task_id, results,
                                                 &got_exception));
    should_break = should_break || got_exception;

    if ((previous_size - remaining.size()) < batch_ids.size()) {
      unsuccessful_attempts++;
      WarnIfAttemptedTooManyTimes(unsuccessful_attempts, remaining);
    }
  }

  // Notify unblocked because we blocked when calling FetchOrReconstruct with
  // fetch_only=false.
  return raylet_client_->NotifyUnblocked(task_id);
}

Status CoreWorkerPlasmaStoreProvider::Wait(const std::unordered_set<ObjectID> &object_ids,
                                           int num_objects, int64_t timeout_ms,
                                           const TaskID &task_id,
                                           std::unordered_set<ObjectID> *ready) {
  WaitResultPair result_pair;
  std::vector<ObjectID> id_vector(object_ids.begin(), object_ids.end());
  RAY_RETURN_NOT_OK(raylet_client_->Wait(id_vector, num_objects, timeout_ms, false,
                                         task_id, &result_pair));

  for (const auto &entry : result_pair.first) {
    ready->insert(entry);
  }

  return Status::OK();
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
    int num_attempts, const std::unordered_set<ObjectID> &remaining) {
  if (num_attempts % RayConfig::instance().object_store_get_warn_per_num_attempts() ==
      0) {
    std::ostringstream oss;
    size_t printed = 0;
    for (auto &id : remaining) {
      if (printed >=
          RayConfig::instance().object_store_get_max_ids_to_print_in_warning()) {
        break;
      }
      if (printed > 0) {
        oss << ", ";
      }
      oss << id.Hex();
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
