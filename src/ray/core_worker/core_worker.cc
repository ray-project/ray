#include "core_worker.h"
#include "context.h"

namespace ray {

thread_local std::unique_ptr<WorkerContext> CoreWorker::context_ = nullptr;

CoreWorker::CoreWorker(
  const enum WorkerType worker_type,
  const enum Language language,
  const std::string &store_socket,
  const std::string &raylet_socket,
  DriverID driver_id)
  : worker_type_(worker_type),
    language_(language),
    store_socket_(store_socket),
    driver_id_(driver_id),
    task_interface_(*this),
    object_interface_(*this),
    task_execution_interface_(*this) {
  
    ::Language lang = ::Language::PYTHON;
    if (language == Language::JAVA) {
      lang = ::Language::JAVA;
    }
  
    auto &context = GetContext();
  
    ClientID worker_id;
    if (worker_type == WorkerType::DRIVER) {
      // TODO: this is a hack. Need a consistent approach.
      worker_id = ClientID::from_binary(driver_id.binary());
    } else {
      worker_id = ClientID::from_random();
    }

    raylet_client_ = std::unique_ptr<RayletClient>(new RayletClient(
        raylet_socket, worker_id, (worker_type == WorkerType::WORKER),
        context.current_driver_id, lang));
}

WorkerContext& CoreWorker::GetPerThreadContext(const enum WorkerType worker_type, DriverID driver_id) {
  if (context_ == nullptr) {
    context_ = std::unique_ptr<WorkerContext>(new WorkerContext(worker_type, driver_id));
  }

  return *context_;
}

}  // namespace ray