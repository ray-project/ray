#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/context.h"

namespace ray {

CoreWorker::CoreWorker(
    const WorkerType worker_type, const Language language,
    const std::string &store_socket, const std::string &raylet_socket,
    const JobID &job_id, const gcs::GcsClientOptions &gcs_options,
    const CoreWorkerTaskExecutionInterface::TaskExecutor &execution_callback)
    : worker_type_(worker_type),
      language_(language),
      raylet_socket_(raylet_socket),
      worker_context_(worker_type, job_id),
      io_work_(io_service_) {
  // Initialize gcs client
  gcs_client_ =
      std::unique_ptr<gcs::RedisGcsClient>(new gcs::RedisGcsClient(gcs_options));
  RAY_CHECK_OK(gcs_client_->Connect(io_service_));

  object_interface_ = std::unique_ptr<CoreWorkerObjectInterface>(
      new CoreWorkerObjectInterface(worker_context_, raylet_client_, store_socket));
  task_interface_ = std::unique_ptr<CoreWorkerTaskInterface>(new CoreWorkerTaskInterface(
      worker_context_, raylet_client_, *object_interface_, io_service_, *gcs_client_));

  int rpc_server_port = 0;
  if (worker_type_ == WorkerType::WORKER) {
    RAY_CHECK(execution_callback != nullptr);
    task_execution_interface_ = std::unique_ptr<CoreWorkerTaskExecutionInterface>(
        new CoreWorkerTaskExecutionInterface(worker_context_, raylet_client_,
                                             *object_interface_, execution_callback));
    rpc_server_port = task_execution_interface_->worker_server_.GetPort();
  }
  // TODO(zhijunfu): currently RayletClient would crash in its constructor if it cannot
  // connect to Raylet after a number of retries, this can be changed later
  // so that the worker (java/python .etc) can retrieve and handle the error
  // instead of crashing.
  raylet_client_ = std::unique_ptr<RayletClient>(new RayletClient(
      raylet_socket_, WorkerID::FromBinary(worker_context_.GetWorkerID().Binary()),
      (worker_type_ == ray::WorkerType::WORKER), worker_context_.GetCurrentJobID(),
      language_, rpc_server_port));

  io_thread_ = std::thread(&CoreWorker::StartIOService, this);
}

CoreWorker::~CoreWorker() {
  gcs_client_->Disconnect();
  io_service_.stop();
  io_thread_.join();
}

void CoreWorker::StartIOService() { io_service_.run(); }

}  // namespace ray
