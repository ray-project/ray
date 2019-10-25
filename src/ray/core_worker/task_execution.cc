#include "ray/core_worker/task_execution.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/core_worker/transport/raylet_transport.h"

namespace ray {

CoreWorkerTaskExecutionInterface::CoreWorkerTaskExecutionInterface(
    CoreWorker &core_worker)
    : core_worker_(core_worker) {
}

void CoreWorkerTaskExecutionInterface::Run() {
  core_worker_.idle_profile_event_.reset(
      new worker::ProfileEvent(core_worker_.profiler_, "worker_idle"));
  core_worker_.main_service_->run();
}

void CoreWorkerTaskExecutionInterface::Stop() {
  // Stop main IO service.
  std::shared_ptr<boost::asio::io_service> main_service = core_worker_.main_service_;
  // Delay the execution of io_service::stop() to avoid deadlock if
  // CoreWorkerTaskExecutionInterface::Stop is called inside a task.
  core_worker_.idle_profile_event_.reset();
  core_worker_.main_service_->post([main_service]() { main_service->stop(); });
}

}  // namespace ray
