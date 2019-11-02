#include "ray/core_worker/store_provider/plasma_store_provider.h"
#include "ray/common/ray_config.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/protobuf/gcs.pb.h"

namespace ray {

CoreWorkerPlasmaStoreProvider::CoreWorkerPlasmaStoreProvider(
    const std::string &store_socket, const std::unique_ptr<RayletClient> &raylet_client,
    std::function<Status()> check_signals)
    : raylet_client_(raylet_client) {
  check_signals_ = check_signals;
  RAY_ARROW_CHECK_OK(store_client_.Connect(store_socket));
}

CoreWorkerPlasmaStoreProvider::~CoreWorkerPlasmaStoreProvider() {
  RAY_IGNORE_EXPR(store_client_.Disconnect());
}

Status CoreWorkerPlasmaStoreProvider::SetClientOptions(std::string name,
                                                       int64_t limit_bytes) {
  std::lock_guard<std::mutex> guard(store_client_mutex_);
  RAY_ARROW_RETURN_NOT_OK(store_client_.SetClientOptions(name, limit_bytes));
  return Status::OK();
}

Status CoreWorkerPlasmaStoreProvider::Put(const RayObject &object,
                                          const ObjectID &object_id) {
  std::shared_ptr<Buffer> data;
  RAY_RETURN_NOT_OK(Create(object.GetMetadata(),
                           object.HasData() ? object.GetData()->Size() : 0, object_id,
                           &data));
  // data could be a nullptr if the ObjectID already existed, but this does
  // not throw an error.
  if (data != nullptr) {
    if (object.HasData()) {
      memcpy(data->Data(), object.GetData()->Data(), object.GetData()->Size());
    }
    RAY_RETURN_NOT_OK(Seal(object_id));
  }
  return Status::OK();
}

Status CoreWorkerPlasmaStoreProvider::Create(const std::shared_ptr<Buffer> &metadata,
                                             const size_t data_size,
                                             const ObjectID &object_id,
                                             std::shared_ptr<Buffer> *data) {
  auto plasma_id = object_id.ToPlasmaId();
  std::shared_ptr<arrow::Buffer> arrow_buffer;
  {
    std::lock_guard<std::mutex> guard(store_client_mutex_);
    arrow::Status status =
        store_client_.Create(plasma_id, data_size, metadata ? metadata->Data() : nullptr,
                             metadata ? metadata->Size() : 0, &arrow_buffer);
    if (plasma::IsPlasmaObjectExists(status)) {
      RAY_LOG(WARNING) << "Trying to put an object that already existed in plasma: "
                       << object_id << ".";
      return Status::OK();
    }
    if (plasma::IsPlasmaStoreFull(status)) {
      std::ostringstream message;
      message << "Failed to put object " << object_id << " in object store because it "
              << "is full. Object size is " << data_size << " bytes.";
      return Status::ObjectStoreFull(message.str());
    }
    RAY_ARROW_RETURN_NOT_OK(status);
  }
  *data = std::make_shared<PlasmaBuffer>(PlasmaBuffer(arrow_buffer));
  return Status::OK();
}

Status CoreWorkerPlasmaStoreProvider::Seal(const ObjectID &object_id) {
  auto plasma_id = object_id.ToPlasmaId();
  {
    std::lock_guard<std::mutex> guard(store_client_mutex_);
    RAY_ARROW_RETURN_NOT_OK(store_client_.Seal(plasma_id));
    RAY_ARROW_RETURN_NOT_OK(store_client_.Release(plasma_id));
  }
  return Status::OK();
}

Status CoreWorkerPlasmaStoreProvider::FetchAndGetFromPlasmaStore(
    absl::flat_hash_set<ObjectID> &remaining, const std::vector<ObjectID> &batch_ids,
    int64_t timeout_ms, bool fetch_only, const TaskID &task_id,
    absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> *results,
    bool *got_exception) {
  RAY_RETURN_NOT_OK(raylet_client_->FetchOrReconstruct(batch_ids, fetch_only, task_id));

  std::vector<plasma::ObjectID> plasma_batch_ids;
  plasma_batch_ids.reserve(batch_ids.size());
  for (size_t i = 0; i < batch_ids.size(); i++) {
    plasma_batch_ids.push_back(batch_ids[i].ToPlasmaId());
  }
  std::vector<plasma::ObjectBuffer> plasma_results;
  {
    std::lock_guard<std::mutex> guard(store_client_mutex_);
    RAY_ARROW_RETURN_NOT_OK(
        store_client_.Get(plasma_batch_ids, timeout_ms, &plasma_results));
  }

  // Add successfully retrieved objects to the result map and remove them from
  // the set of IDs to get.
  for (size_t i = 0; i < plasma_results.size(); i++) {
    if (plasma_results[i].data != nullptr || plasma_results[i].metadata != nullptr) {
      const auto &object_id = batch_ids[i];
      std::shared_ptr<PlasmaBuffer> data = nullptr;
      std::shared_ptr<PlasmaBuffer> metadata = nullptr;
      if (plasma_results[i].data && plasma_results[i].data->size()) {
        data = std::make_shared<PlasmaBuffer>(plasma_results[i].data);
      }
      if (plasma_results[i].metadata && plasma_results[i].metadata->size()) {
        metadata = std::make_shared<PlasmaBuffer>(plasma_results[i].metadata);
      }
      const auto result_object = std::make_shared<RayObject>(data, metadata);
      (*results)[object_id] = result_object;
      remaining.erase(object_id);
      if (result_object->IsException()) {
        *got_exception = true;
      }
    }
  }

  return Status::OK();
}

Status CoreWorkerPlasmaStoreProvider::Get(
    const absl::flat_hash_set<ObjectID> &object_ids, int64_t timeout_ms,
    const TaskID &task_id,
    absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> *results,
    bool *got_exception) {
  int64_t batch_size = RayConfig::instance().worker_fetch_request_size();
  std::vector<ObjectID> batch_ids;
  absl::flat_hash_set<ObjectID> remaining(object_ids.begin(), object_ids.end());

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
                                                 got_exception));
  }

  // If all objects were fetched already, return.
  if (remaining.empty() || *got_exception) {
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
                                                 got_exception));
    should_break = should_break || *got_exception;

    if ((previous_size - remaining.size()) < batch_ids.size()) {
      unsuccessful_attempts++;
      WarnIfAttemptedTooManyTimes(unsuccessful_attempts, remaining);
    }
    if (check_signals_) {
      Status status = check_signals_();
      if (!status.ok()) {
        // TODO(edoakes): in this case which status should we return?
        RAY_RETURN_NOT_OK(raylet_client_->NotifyUnblocked(task_id));
        return status;
      }
    }
  }

  // Notify unblocked because we blocked when calling FetchOrReconstruct with
  // fetch_only=false.
  return raylet_client_->NotifyUnblocked(task_id);
}

Status CoreWorkerPlasmaStoreProvider::Contains(const ObjectID &object_id,
                                               bool *has_object) {
  std::lock_guard<std::mutex> guard(store_client_mutex_);
  RAY_ARROW_RETURN_NOT_OK(store_client_.Contains(object_id.ToPlasmaId(), has_object));
  return Status::OK();
}

Status CoreWorkerPlasmaStoreProvider::Wait(
    const absl::flat_hash_set<ObjectID> &object_ids, int num_objects, int64_t timeout_ms,
    const TaskID &task_id, absl::flat_hash_set<ObjectID> *ready) {
  std::vector<ObjectID> id_vector(object_ids.begin(), object_ids.end());

  bool should_break = false;
  int64_t remaining_timeout = timeout_ms;
  while (!should_break) {
    WaitResultPair result_pair;
    int64_t call_timeout = RayConfig::instance().get_timeout_milliseconds();
    if (remaining_timeout >= 0) {
      call_timeout = std::min(remaining_timeout, call_timeout);
      remaining_timeout -= call_timeout;
      should_break = remaining_timeout <= 0;
    }

    RAY_RETURN_NOT_OK(raylet_client_->Wait(id_vector, num_objects, call_timeout, false,
                                           task_id, &result_pair));

    if (result_pair.first.size() >= static_cast<size_t>(num_objects)) {
      should_break = true;
    }
    for (const auto &entry : result_pair.first) {
      ready->insert(entry);
    }
    if (check_signals_) {
      RAY_RETURN_NOT_OK(check_signals_());
    }
  }
  return Status::OK();
}

Status CoreWorkerPlasmaStoreProvider::Delete(
    const absl::flat_hash_set<ObjectID> &object_ids, bool local_only,
    bool delete_creating_tasks) {
  std::vector<ObjectID> object_id_vector(object_ids.begin(), object_ids.end());
  return raylet_client_->FreeObjects(object_id_vector, local_only, delete_creating_tasks);
}

std::string CoreWorkerPlasmaStoreProvider::MemoryUsageString() {
  std::lock_guard<std::mutex> guard(store_client_mutex_);
  return store_client_.DebugString();
}

void CoreWorkerPlasmaStoreProvider::WarnIfAttemptedTooManyTimes(
    int num_attempts, const absl::flat_hash_set<ObjectID> &remaining) {
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
