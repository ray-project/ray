#include "ray/core_worker/store_provider/local_plasma_provider.h"
#include "ray/common/ray_config.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/object_interface.h"

namespace ray {

CoreWorkerLocalPlasmaStoreProvider::CoreWorkerLocalPlasmaStoreProvider(
    const std::string &store_socket) {
  RAY_ARROW_CHECK_OK(store_client_.Connect(store_socket));
}

Status CoreWorkerLocalPlasmaStoreProvider::Put(const RayObject &object,
                                               const ObjectID &object_id) {
  auto plasma_id = object_id.ToPlasmaId();
  auto data = object.GetData();
  auto metadata = object.GetMetadata();
  std::shared_ptr<arrow::Buffer> out_buffer;
  {
    std::unique_lock<std::mutex> guard(store_client_mutex_);
    RAY_ARROW_RETURN_NOT_OK(store_client_.Create(
        plasma_id, data ? data->Size() : 0, metadata ? metadata->Data() : nullptr,
        metadata ? metadata->Size() : 0, &out_buffer));
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

Status CoreWorkerLocalPlasmaStoreProvider::Get(
    const std::vector<ObjectID> &object_ids, int64_t timeout_ms, const TaskID &task_id,
    std::vector<std::shared_ptr<RayObject>> *results) {
  std::vector<plasma::ObjectID> plasma_ids;
  plasma_ids.reserve(object_ids.size());
  for (const auto &object_id : object_ids) {
    plasma_ids.push_back(object_id.ToPlasmaId());
  }

  std::vector<plasma::ObjectBuffer> object_buffers;
  {
    std::unique_lock<std::mutex> guard(store_client_mutex_);
    RAY_ARROW_RETURN_NOT_OK(store_client_.Get(plasma_ids, timeout_ms, &object_buffers));
  }

  (*results).resize(object_ids.size(), nullptr);
  for (size_t i = 0; i < object_buffers.size(); i++) {
    if (object_buffers[i].data != nullptr || object_buffers[i].metadata != nullptr) {
      (*results)[i] = std::make_shared<RayObject>(
          std::make_shared<PlasmaBuffer>(object_buffers[i].data),
          std::make_shared<PlasmaBuffer>(object_buffers[i].metadata));
    }
  }

  return Status::OK();
}

Status CoreWorkerLocalPlasmaStoreProvider::Wait(const std::vector<ObjectID> &object_ids,
                                                int num_objects, int64_t timeout_ms,
                                                const TaskID &task_id,
                                                std::vector<bool> *results) {
  if (num_objects != object_ids.size()) {
    return Status::Invalid("num_objects should equal to number of items in object_ids");
  }

  std::vector<std::shared_ptr<RayObject>> objects;
  RAY_RETURN_NOT_OK(Get(object_ids, timeout_ms, task_id, &objects));

  (*results).resize(object_ids.size());
  for (size_t i = 0; i < object_ids.size(); i++) {
    (*results)[i] = objects[i]->GetData() != nullptr;
  }

  return Status::OK();
}

Status CoreWorkerLocalPlasmaStoreProvider::Delete(const std::vector<ObjectID> &object_ids,
                                                  bool local_only,
                                                  bool delete_creating_tasks) {
  std::vector<plasma::ObjectID> plasma_ids;
  plasma_ids.reserve(object_ids.size());
  for (const auto &object_id : object_ids) {
    plasma_ids.push_back(object_id.ToPlasmaId());
  }

  std::unique_lock<std::mutex> guard(store_client_mutex_);
  RAY_ARROW_RETURN_NOT_OK(store_client_.Delete(plasma_ids));
  return Status::OK();
}

}  // namespace ray
