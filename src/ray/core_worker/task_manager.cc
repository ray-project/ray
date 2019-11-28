#include "ray/core_worker/task_manager.h"

namespace ray {

void TaskManager::AddPendingTask(const TaskSpecification &spec) {
  RAY_LOG(DEBUG) << "Adding pending task " << spec.TaskId();
  absl::MutexLock lock(&mu_);
  RAY_CHECK(pending_tasks_.emplace(spec.TaskId(), spec.NumReturns()).second);
}

void TaskManager::CompletePendingTask(const TaskID &task_id,
                                      const rpc::PushTaskReply &reply) {
  RAY_LOG(DEBUG) << "Completing task " << task_id;
  {
    absl::MutexLock lock(&mu_);
    auto it = pending_tasks_.find(task_id);
    RAY_CHECK(it != pending_tasks_.end())
        << "Tried to complete task that was not pending " << task_id;
    pending_tasks_.erase(it);
  }

  for (int i = 0; i < reply.return_objects_size(); i++) {
    const auto &return_object = reply.return_objects(i);
    ObjectID object_id = ObjectID::FromBinary(return_object.object_id());

    if (return_object.in_plasma()) {
      // Mark it as in plasma with a dummy object.
      RAY_CHECK_OK(
          in_memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA), object_id));
    } else {
      std::shared_ptr<LocalMemoryBuffer> data_buffer;
      if (return_object.data().size() > 0) {
        data_buffer = std::make_shared<LocalMemoryBuffer>(
            const_cast<uint8_t *>(
                reinterpret_cast<const uint8_t *>(return_object.data().data())),
            return_object.data().size());
      }
      std::shared_ptr<LocalMemoryBuffer> metadata_buffer;
      if (return_object.metadata().size() > 0) {
        metadata_buffer = std::make_shared<LocalMemoryBuffer>(
            const_cast<uint8_t *>(
                reinterpret_cast<const uint8_t *>(return_object.metadata().data())),
            return_object.metadata().size());
      }
      RAY_CHECK_OK(
          in_memory_store_->Put(RayObject(data_buffer, metadata_buffer), object_id));
    }
  }
}

void TaskManager::FailPendingTask(const TaskID &task_id, rpc::ErrorType error_type) {
  RAY_LOG(DEBUG) << "Failing task " << task_id;
  int64_t num_returns;
  {
    absl::MutexLock lock(&mu_);
    auto it = pending_tasks_.find(task_id);
    RAY_CHECK(it != pending_tasks_.end())
        << "Tried to complete task that was not pending " << task_id;
    num_returns = it->second;
    pending_tasks_.erase(it);
  }

  RAY_LOG(DEBUG) << "Treat task as failed. task_id: " << task_id
                 << ", error_type: " << ErrorType_Name(error_type);
  for (int i = 0; i < num_returns; i++) {
    const auto object_id = ObjectID::ForTaskReturn(
        task_id, /*index=*/i + 1,
        /*transport_type=*/static_cast<int>(TaskTransportType::DIRECT));
    RAY_CHECK_OK(in_memory_store_->Put(RayObject(error_type), object_id));
  }
}

}  // namespace ray
