#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/context.h"

namespace ray {

CoreWorker::CoreWorker(
    const WorkerType worker_type, const Language language,
    const std::string &store_socket, const std::string &raylet_socket,
    const JobID &job_id,
    const CoreWorkerTaskExecutionInterface::TaskExecutor &execution_callback)
    : worker_type_(worker_type),
      language_(language),
      raylet_socket_(raylet_socket),
      worker_context_(worker_type, job_id),
      task_interface_(worker_context_, raylet_client_),
      object_interface_(worker_context_, raylet_client_, store_socket) {
  int rpc_server_port = 0;
  if (worker_type_ == ray::WorkerType::WORKER) {
    RAY_CHECK(execution_callback != nullptr);
    task_execution_interface_ = std::unique_ptr<CoreWorkerTaskExecutionInterface>(
        new CoreWorkerTaskExecutionInterface(worker_context_, raylet_client_,
                                             object_interface_, execution_callback));
    rpc_server_port = task_execution_interface_->worker_server_.GetPort();
  }
  // TODO(zhijunfu): currently RayletClient would crash in its constructor if it cannot
  // connect to Raylet after a number of retries, this can be changed later
  // so that the worker (java/python .etc) can retrieve and handle the error
  // instead of crashing.
  raylet_client_ = std::unique_ptr<RayletClient>(new RayletClient(
      raylet_socket_, ClientID::FromBinary(worker_context_.GetWorkerID().Binary()),
      (worker_type_ == ray::WorkerType::WORKER), worker_context_.GetCurrentJobID(),
      language_, rpc_server_port));
}

}  // namespace ray
