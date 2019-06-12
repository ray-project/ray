#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/context.h"

namespace ray {

CoreWorker::CoreWorker(const enum WorkerType worker_type,
                       const enum WorkerLanguage language,
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
      task_execution_interface_(*this) {
  switch (language_) {
  case ray::WorkerLanguage::JAVA:
    task_language_ = ::Language::JAVA;
    break;
  case ray::WorkerLanguage::PYTHON:
    task_language_ = ::Language::PYTHON;
    break;
  default:
    RAY_LOG(FATAL) << "Unsupported worker language: " << static_cast<int>(language_);
    break;
  }
}

Status CoreWorker::Connect() {
  // connect to plasma.
  RAY_ARROW_RETURN_NOT_OK(store_client_.Connect(store_socket_));

  // connect to raylet.
  // TODO: currently RayletClient would crash in its constructor if it cannot
  // connect to Raylet after a number of retries, this needs to be changed
  // so that the worker (java/python .etc) can retrieve and handle the error
  // instead of crashing.
  raylet_client_ = std::unique_ptr<RayletClient>(
      new RayletClient(raylet_socket_, worker_context_.GetWorkerID(),
                       (worker_type_ == ray::WorkerType::WORKER),
                       worker_context_.GetCurrentDriverID(), task_language_));
  is_initialized_ = true;
  return Status::OK();
}

}  // namespace ray
