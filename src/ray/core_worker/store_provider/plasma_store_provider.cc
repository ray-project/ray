#include "ray/core_worker/store_provider/plasma_store_provider.h"
#include "ray/common/ray_config.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/object_interface.h"

namespace ray {

CoreWorkerPlasmaStoreProvider::CoreWorkerPlasmaStoreProvider(
    const std::string &store_socket, std::unique_ptr<RayletClient> &raylet_client)
    : raylet_client_(raylet_client) {
  auto status = store_client_.Connect(store_socket);
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Connecting plasma store failed when trying to construct"
                   << " core worker: " << status.message();
    throw std::runtime_error(status.message());
  }
}

Status CoreWorkerPlasmaStoreProvider::Put(const RayObject &object,
                                          const ObjectID &object_id) {
  auto plasma_id = object_id.ToPlasmaId();
  auto data = object.GetData();
  auto metadata = object.GetMetadata();
  std::shared_ptr<arrow::Buffer> out_buffer;
  {
    std::unique_lock<std::mutex> guard(store_client_mutex_);
    RAY_ARROW_RETURN_NOT_OK(store_client_.Create(
        plasma_id, data->Size(), metadata ? metadata->Data() : nullptr,
        metadata ? metadata->Size() : 0, &out_buffer));
  }

  memcpy(out_buffer->mutable_data(), data->Data(), data->Size());

  {
    std::unique_lock<std::mutex> guard(store_client_mutex_);
    RAY_ARROW_RETURN_NOT_OK(store_client_.Seal(plasma_id));
    RAY_ARROW_RETURN_NOT_OK(store_client_.Release(plasma_id));
  }
  return Status::OK();
}

Status CoreWorkerPlasmaStoreProvider::Get(
    const std::vector<ObjectID> &ids, int64_t timeout_ms, const TaskID &task_id,
    std::vector<std::shared_ptr<RayObject>> *results) {
  (*results).resize(ids.size(), nullptr);

  bool was_blocked = false;

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

    // For the initial fetch, we only fetch the objects, do not reconstruct them.
    bool fetch_only = num_attempts == 0;
    if (!fetch_only) {
      // If fetch_only is false, this worker will be blocked.
      was_blocked = true;
    }

    // TODO(zhijunfu): can call `fetchOrReconstruct` in batches as an optimization.
    RAY_CHECK_OK(raylet_client_->FetchOrReconstruct(unready_ids, fetch_only, task_id));

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

    std::vector<plasma::ObjectID> plasma_ids;
    for (const auto &id : unready_ids) {
      plasma_ids.push_back(id.ToPlasmaId());
    }

    std::vector<plasma::ObjectBuffer> object_buffers;
    {
      std::unique_lock<std::mutex> guard(store_client_mutex_);
      auto status = store_client_.Get(plasma_ids, get_timeout, &object_buffers);
    }

    for (size_t i = 0; i < object_buffers.size(); i++) {
      if (object_buffers[i].data != nullptr) {
        const auto &object_id = unready_ids[i];
        (*results)[unready[object_id]] = std::make_shared<RayObject>(
            std::make_shared<PlasmaBuffer>(object_buffers[i].data),
            std::make_shared<PlasmaBuffer>(object_buffers[i].metadata));
        unready.erase(object_id);
      }
    }

    num_attempts += 1;
    // TODO(zhijunfu): log a message if attempted too many times.
  }

  if (was_blocked) {
    RAY_CHECK_OK(raylet_client_->NotifyUnblocked(task_id));
  }

  return Status::OK();
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

}  // namespace ray
