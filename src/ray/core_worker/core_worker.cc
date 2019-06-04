#include "core_worker.h"
#include "context.h"

namespace ray {

CoreWorker::CoreWorker(const enum WorkerType worker_type, const enum WorkerLanguage language,
                       const std::string &store_socket, const std::string &raylet_socket,
                       DriverID driver_id)
    : worker_type_(worker_type),
      language_(language),
      worker_context_(worker_type, driver_id),
      store_socket_(store_socket),
      raylet_socket_(raylet_socket),
      is_initialized_(false),
      task_interface_(*this),
      object_interface_(*this),
      task_execution_interface_(*this) {}

Status CoreWorker::Connect() {
  // connect to plasma.
  RAY_ARROW_RETURN_NOT_OK(store_client_.Connect(store_socket_));

  // connect to raylet.
  ::Language language = (language_ == ray::WorkerLanguage::JAVA) ?
      (::Language::JAVA) : (::Language::PYTHON);

  // TODO: currently RayletClient would crash in its constructor if it cannot
  // connect to Raylet after a number of retries, this needs to be changed
  // so that the worker (java/python .etc) can retrieve and handle the error
  // instead of crashing.
  raylet_client_ = std::unique_ptr<RayletClient>(
      new RayletClient(raylet_socket_, worker_context_.GetWorkerID(),
                       (worker_type_ == ray::WorkerType::WORKER),
                       worker_context_.GetCurrentDriverID(), language));
  is_initialized_ = true;
  return Status::OK();
}

}  // namespace ray
