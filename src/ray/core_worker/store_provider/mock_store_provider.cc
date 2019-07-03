#include "ray/core_worker/store_provider/mock_store_provider.h"
#include "ray/core_worker/transport/mock_transport.h"

namespace ray {

CoreWorkerMockStoreProvider::CoreWorkerMockStoreProvider() {}

CoreWorkerMockStoreProvider &CoreWorkerMockStoreProvider::Instance() { return instance_; }

Status CoreWorkerMockStoreProvider::Put(const RayObject &object,
                                        const ObjectID &object_id) {
  std::lock_guard<std::mutex> guard(mutex_);
  pool_.emplace(object_id, object);
  CoreWorkerMockTaskSubmitterReceiver::Instance().OnObjectPut(object_id);
  return Status::OK();
}

Status CoreWorkerMockStoreProvider::Get(
    const std::vector<ObjectID> &ids, int64_t timeout_ms, const TaskID &task_id,
    std::vector<std::shared_ptr<RayObject>> *results) {
  (*results).resize(ids.size(), nullptr);

  std::vector<bool> temp_results;
  RAY_CHECK_OK(Wait(ids, ids.size(), timeout_ms, task_id, &temp_results));

  {
    std::lock_guard<std::mutex> guard(mutex_);
    for (size_t i = 0; i < ids.size(); i++) {
      auto it = pool_.find(ids[i]);
      if (it != pool_.end()) {
        (*results)[i] = std::make_shared<RayObject>(it->second);
      }
    }
  }

  return Status::OK();
}

Status CoreWorkerMockStoreProvider::Wait(const std::vector<ObjectID> &object_ids,
                                         int num_objects, int64_t timeout_ms,
                                         const TaskID &task_id,
                                         std::vector<bool> *results) {
  (*results).resize(object_ids.size(), false);

  size_t ready = 0;
  int num_attempts = 0;
  bool should_break = false;
  int64_t remaining_timeout = timeout_ms;
  // Repeat until we get all objects.
  while (ready < object_ids.size() && !should_break) {
    if (num_attempts > 0) {
      int64_t get_timeout;
      if (remaining_timeout >= 0) {
        get_timeout = std::min(remaining_timeout, get_check_interval_ms_);
        remaining_timeout -= get_timeout;
        should_break = remaining_timeout <= 0;
      } else {
        get_timeout = get_check_interval_ms_;
      }
      usleep(get_timeout * 1000);
    }

    ready = 0;
    {
      std::lock_guard<std::mutex> guard(mutex_);
      for (size_t i = 0; i < object_ids.size(); i++) {
        if (pool_.find(object_ids[i]) != pool_.end()) {
          (*results)[i] = true;
          ready++;
        }
      }
    }

    num_attempts += 1;
  }

  return Status::OK();
}

Status CoreWorkerMockStoreProvider::Delete(const std::vector<ObjectID> &object_ids,
                                           bool local_only, bool delete_creating_tasks) {
  std::lock_guard<std::mutex> guard(mutex_);
  for (size_t i = 0; i < object_ids.size(); i++) {
    pool_.erase(object_ids[i]);
  }
}

bool CoreWorkerMockStoreProvider::IsObjectReady(const ObjectID &object_id) {
  std::lock_guard<std::mutex> guard(mutex_);
  return pool_.find(object_id) != pool_.end();
}

}  // namespace ray
