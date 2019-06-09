#include "object_interface.h"
#include "context.h"
#include "core_worker.h"
#include "ray/common/ray_config.h"

namespace ray {

CoreWorkerObjectInterface::CoreWorkerObjectInterface(CoreWorker &core_worker)
    : core_worker_(core_worker) {}

Status CoreWorkerObjectInterface::Put(const Buffer &buffer, ObjectID *object_id) {
  ObjectID put_id = ObjectID::ForPut(core_worker_.worker_context_.GetCurrentTaskID(),
                                     core_worker_.worker_context_.GetNextPutIndex());
  *object_id = put_id;

  auto plasma_id = put_id.ToPlasmaId();
  std::shared_ptr<arrow::Buffer> data;
  RAY_ARROW_RETURN_NOT_OK(
      core_worker_.store_client_.Create(plasma_id, buffer.Size(), nullptr, 0, &data));
  memcpy(data->mutable_data(), buffer.Data(), buffer.Size());
  RAY_ARROW_RETURN_NOT_OK(core_worker_.store_client_.Seal(plasma_id));
  RAY_ARROW_RETURN_NOT_OK(core_worker_.store_client_.Release(plasma_id));
  return Status::OK();
}

Status CoreWorkerObjectInterface::Get(const std::vector<ObjectID> &ids,
                                      int64_t timeout_ms,
                                      std::vector<std::shared_ptr<Buffer>> *results) {
  (*results).resize(ids.size(), nullptr);

  bool was_blocked = false;

  std::unordered_map<ObjectID, int> unready;
  for (int i = 0; i < ids.size(); i++) {
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

    // TODO: can call `fetchOrReconstruct` in batches as an optimization.
    RAY_CHECK_OK(core_worker_.raylet_client_->FetchOrReconstruct(
        unready_ids, fetch_only, core_worker_.worker_context_.GetCurrentTaskID()));

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
    auto status =
        core_worker_.store_client_.Get(plasma_ids, get_timeout, &object_buffers);

    for (int i = 0; i < object_buffers.size(); i++) {
      if (object_buffers[i].data != nullptr) {
        const auto &object_id = unready_ids[i];
        (*results)[unready[object_id]] =
            std::make_shared<PlasmaBuffer>(object_buffers[i].data);
        unready.erase(object_id);
      }
    }

    num_attempts += 1;
    // TODO: log a message if attempted too many times.
  }

  if (was_blocked) {
    RAY_CHECK_OK(core_worker_.raylet_client_->NotifyUnblocked(
        core_worker_.worker_context_.GetCurrentTaskID()));
  }

  return Status::OK();
}

Status CoreWorkerObjectInterface::Wait(const std::vector<ObjectID> &object_ids,
                                       int num_objects, int64_t timeout_ms,
                                       std::vector<bool> *results) {
  WaitResultPair result_pair;
  auto status = core_worker_.raylet_client_->Wait(
      object_ids, num_objects, timeout_ms, false,
      core_worker_.worker_context_.GetCurrentTaskID(), &result_pair);
  std::unordered_set<ObjectID> ready_ids;
  for (const auto &entry : result_pair.first) {
    ready_ids.insert(entry);
  }

  // TODO: change RayletClient::Wait() to return a bit set, so that we don't need
  // to do this translation.
  (*results).resize(object_ids.size());
  for (int i = 0; i < object_ids.size(); i++) {
    (*results)[i] = ready_ids.count(object_ids[i]) > 0;
  }

  return status;
}

Status CoreWorkerObjectInterface::Delete(const std::vector<ObjectID> &object_ids,
                                         bool local_only, bool delete_creating_tasks) {
  return core_worker_.raylet_client_->FreeObjects(object_ids, local_only,
                                                  delete_creating_tasks);
}

}  // namespace ray
