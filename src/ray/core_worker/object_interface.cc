#include "object_interface.h"

namespace ray {

CoreWorkerObjectInterface::CoreWorkerObjectInterface(CoreWorker &core_worker)
  : core_worker_(core_worker) {

  RAY_ARROW_CHECK_OK(core_worker_.store_client_.Connect(core_worker_.store_socket_name_));
}

Status CoreWorkerObjectInterface::Put(const Buffer &buffer, const ObjectID *object_id) {
  auto &context = core_worker_.GetContext();
  ObjectID put_id = ObjectID::for_put(context.GetCurrentTaskID(),
      context.NextPutIndex());
  *object_id = put_id;

  std::shared_ptr<arrow::Buffer> data;
  ARROW_RETURN_NOT_OK(core_worker_.store_client_.Create(put_id, buffer.Size(), nullptr, 0, &data));
  memcpy(data.mutable_data(), buffer.Data(), buffer.Size());
  ARROW_RETURN_NOT_OK(core_worker_.store_client_.Seal(put_id);
  ARROW_RETURN_NOT_OK(core_worker_.store_client_.Release(put_id));
  
  return Status::OK();
}

Status CoreWorkerObjectInterface::Get(const std::vector<ObjectID> &ids,
                                      int64_t timeout_ms, std::vector<Buffer> *results) {
  auto &context = core_worker_.GetContext();
  bool was_blocked = false;

  std::unordered_map<ObjectID, int> unready;
  for (int i = 0; i < ids.size(); i++) {
    unready.insert(ids.get(i), i);
  }

  int num_attempts = 0;
  bool should_break = false;
  int64_t remaining_timeout = timeout_ms;
  // Repeat until we get all objects.
  while (!unready.emtpy() && !should_break) {
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
    context.raylet_client_.FetchOrReconstruct(unready_ids, fetch_only, context.GetCurrentTaskID());

    // Get the objects from the object store, and parse the result.
    int64_t get_timeout = (remaining_timeout < 0 || remaining_timeout > GET_TIMEOUT_MS) ?
        GET_TIMEOUT_MS : remaining_timeout;
    should_break = (get_timeout == remaining_timeout);
  
    if (remaining_timeout >= GET_TIMEOUT_MS) {
      remaining_timeout -= GET_TIMEOUT_MS;
    }

    std::vector<ObjectBuffer> object_buffers;
    auto status = core_worker_.store_client_.Get(unready_ids, get_timeout, &object_buffers);
  
    for (int i = 0; i < object_buffers.size(); i++) {
      if (object_buffers[i].data != nullptr) {
        const auto &object_id = unready_ids[i];
        (*results)[i] = object_buffers[i].data;
        unready.erase(object_id);
      }
    }

    num_attempts += 1;
    // TODO: log a message if attempted too many times.
  }

  if (was_blocked) {
    context.raylet_client_.NotifyUnblocked(context.GetCurrentTaskID());
  }

  return Status::OK();
}

Status CoreWorkerObjectInterface::Wait(const std::vector<ObjectID> &object_ids,
                                       int num_objects, int64_t timeout_ms,
                                       std::vector<bool> *results) {
  auto &context = core_worker_.GetContext();
  WaitResultPair result_pair;
  auto status = context.raylet_client_.Wait(object_ids, num_objects, timeout_ms,
      context.GetCurrentTaskID(), &result_pair);
  std::unordered_set<ObjectID> ready_ids;
  for (const auto &entry : result_pair.first) {
    ready_ids.insert(entry);
  }

  (*results).resize(object_ids.size());
  for (int i = 0; i < object_ids.size(); i++) {
    (*results)[i] = ready_ids.contains(object_is[i]);
  }

  return status;
}

Status CoreWorkerObjectInterface::Delete(const std::vector<ObjectID> &object_ids,
                                         bool local_only, bool delete_creating_tasks) {
  auto &context = core_worker_.GetContext();
  return context.raylet_client_.FreeObjects(object_ids, local_only, delete_creating_tasks);
}

}  // namespace ray
