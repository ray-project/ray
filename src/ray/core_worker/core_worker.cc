#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/context.h"

namespace ray {

CoreWorker::CoreWorker(const enum WorkerType worker_type,
                       const enum WorkerLanguage language,
                       const std::string &store_socket, const std::string &raylet_socket,
                       const JobID &job_id)
    : worker_type_(worker_type),
      language_(language),
      store_socket_(store_socket),
      raylet_socket_(raylet_socket),
      worker_context_(worker_type, job_id),
      io_work_(io_service_) {
  auto status = store_client_.Connect(store_socket_);
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Connecting plasma store failed when trying to construct"
                   << " core worker: " << status.message();
    throw std::runtime_error(status.message());
  }

  // NOTE(zhijunfu): For non-driver worker, the initialization of
  // raylet client is delayed to when `TaskExecutionInterface::Run()`
  // is called, as it is until that time the port for the worker
  // rpc server can be determined, and this information needs to be
  // included when worker registers to raylet.
  if (worker_type_ == WorkerType::DRIVER) {
    InitializeRayletClient(0);
  }

  std::vector<std::pair<std::string, int>> server_list;
  server_list.emplace_back("127.0.0.1", 6379);
  gcs::ClientOption option;
  option.server_list_ = server_list;
  option.command_type_ = gcs::CommandType::kRegular;
  gcs::ClientInfo info{ gcs::ClientInfo::ClientType::kClientTypeWorker,
      ClientID::FromBinary(worker_context_.GetWorkerID().Binary()) };
  gcs_client_ = std::unique_ptr<gcs::GcsClient>(new gcs::GcsClient(
      option, info, io_service_));

  RAY_CHECK_OK(gcs_client_->Connect());

  object_interface_ = std::unique_ptr<CoreWorkerObjectInterface>(
      new CoreWorkerObjectInterface(*this));
  task_interface_ = std::unique_ptr<CoreWorkerTaskInterface>(
      new CoreWorkerTaskInterface(*this));      
  task_execution_interface_ = std::unique_ptr<CoreWorkerTaskExecutionInterface>(
      new CoreWorkerTaskExecutionInterface(*this));

  io_thread_ = std::thread(&CoreWorker::RunIOService, this);
}

 CoreWorker::~CoreWorker() {
  gcs_client_->Disconnect();
  io_service_.stop();
  io_thread_.join();  
}

void CoreWorker::RunIOService() { io_service_.run(); }

void CoreWorker::InitializeRayletClient(int server_port) {
  if (raylet_client_ == nullptr) {
    // TODO(zhijunfu): currently RayletClient would crash in its constructor if it cannot
    // connect to Raylet after a number of retries, this can be changed later
    // so that the worker (java/python .etc) can retrieve and handle the error
    // instead of crashing.
    raylet_client_ = std::unique_ptr<RayletClient>(new RayletClient(
        raylet_socket_, ClientID::FromBinary(worker_context_.GetWorkerID().Binary()),
        (worker_type_ == ray::WorkerType::WORKER), worker_context_.GetCurrentJobID(),
        ToTaskLanguage(language_), server_port));
  }
}

::Language CoreWorker::ToTaskLanguage(WorkerLanguage language) {
  switch (language) {
  case ray::WorkerLanguage::JAVA:
    return ::Language::JAVA;
    break;
  case ray::WorkerLanguage::PYTHON:
    return ::Language::PYTHON;
    break;
  default:
    RAY_LOG(FATAL) << "invalid language specified: " << static_cast<int>(language);
    break;
  }
}

}  // namespace ray
