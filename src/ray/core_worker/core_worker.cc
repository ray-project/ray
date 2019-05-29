#include "core_worker.h"
#include "context.h"

namespace ray {

CoreWorker::CoreWorker(
  const enum WorkerType worker_type,
  const enum Language language,
  const std::string &store_socket,
  const std::string &raylet_socket,
  DriverID driver_id)
  : worker_type_(worker_type),
    language_(language),
    task_interface_(*this),
    object_interface_(*this),
    task_execution_interface_(*this),
    store_socket_(store_socket),
    driver_id_(driver_id) {
    
    ::Language lang = ::Language::PYTHON;
    if (language == Language::JAVA) {
      lang = ::Language::JAVA;
    }
  
    auto &context = GetContext();
    raylet_client_ = std::unique_ptr<RayletClient>(new RayletClient(
        raylet_socket, context.worker_id, (worker_type == WorkerType::WORKER),
        context.current_driver_id, lang));
}

WorkerContext& CoreWorker::GetPerThreadContext(const enum WorkerType worker_type, DriverID driver_id) {
  if (context_ == nullptr) {
    context_ = std::unique_ptr<WorkerContext>(new WorkerContext(worker_type, driver_id));
  }

  return *context_;
}

}  // namespace ray