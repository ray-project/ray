#include <boost/asio/signal_set.hpp>

#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"

namespace ray {

CoreWorker::CoreWorker(
    const WorkerType worker_type, const Language language,
    const std::string &store_socket, const std::string &raylet_socket,
    const JobID &job_id, const gcs::GcsClientOptions &gcs_options,
    const std::string &log_dir, const std::string &node_ip_address,
    const CoreWorkerTaskExecutionInterface::TaskExecutor &execution_callback,
    bool use_memory_store)
    : worker_type_(worker_type),
      language_(language),
      raylet_socket_(raylet_socket),
      log_dir_(log_dir),
      worker_context_(worker_type, job_id),
      io_work_(io_service_) {
  // Initialize logging if log_dir is passed. Otherwise, it must be initialized
  // and cleaned up by the caller.
  if (log_dir_ != "") {
    std::stringstream app_name;
    app_name << LanguageString(language_) << "-" << WorkerTypeString(worker_type_) << "-"
             << worker_context_.GetWorkerID();
    RayLog::StartRayLog(app_name.str(), RayLogLevel::INFO, log_dir_);
    RayLog::InstallFailureSignalHandler();
  }

  boost::asio::signal_set sigint(io_service_, SIGINT);
  sigint.async_wait(
      [](const boost::system::error_code &error, int signal_number) -> void {
        if (!error) {
          RAY_LOG(WARNING) << "Got SIGINT " << signal_number << ", ignoring it.";
        }
      });

  boost::asio::signal_set sigterm(io_service_, SIGTERM);
  sigterm.async_wait(
      [this](const boost::system::error_code &error, int signal_number) -> void {
        if (!error) {
          RAY_LOG(WARNING) << "Got SIGTERM " << signal_number << ", shutting down.";
          io_service_.stop();
        }
      });

  // Initialize gcs client.
  gcs_client_ =
      std::unique_ptr<gcs::RedisGcsClient>(new gcs::RedisGcsClient(gcs_options));
  RAY_CHECK_OK(gcs_client_->Connect(io_service_));

  // Initialize profiler.
  profiler_ = std::make_shared<worker::Profiler>(worker_context_, node_ip_address,
                                                 io_service_, gcs_client_);

  object_interface_ =
      std::unique_ptr<CoreWorkerObjectInterface>(new CoreWorkerObjectInterface(
          worker_context_, raylet_client_, store_socket, use_memory_store));
  task_interface_ = std::unique_ptr<CoreWorkerTaskInterface>(new CoreWorkerTaskInterface(
      worker_context_, raylet_client_, *object_interface_, io_service_, *gcs_client_));

  // Initialize task execution.
  int rpc_server_port = 0;
  if (worker_type_ == WorkerType::WORKER) {
    // TODO(edoakes): Remove this check once Python core worker migration is complete.
    if (language != Language::PYTHON || execution_callback != nullptr) {
      RAY_CHECK(execution_callback != nullptr);
      task_execution_interface_ = std::unique_ptr<CoreWorkerTaskExecutionInterface>(
          new CoreWorkerTaskExecutionInterface(worker_context_, raylet_client_,
                                               *object_interface_, execution_callback));
      rpc_server_port = task_execution_interface_->worker_server_.GetPort();
    }
  }

  // Initialize raylet client.
  // TODO(zhijunfu): currently RayletClient would crash in its constructor if it cannot
  // connect to Raylet after a number of retries, this can be changed later
  // so that the worker (java/python .etc) can retrieve and handle the error
  // instead of crashing.
  raylet_client_ = std::unique_ptr<RayletClient>(new RayletClient(
      raylet_socket_, WorkerID::FromBinary(worker_context_.GetWorkerID().Binary()),
      (worker_type_ == ray::WorkerType::WORKER), worker_context_.GetCurrentJobID(),
      language_, rpc_server_port));

  io_thread_ = std::thread(&CoreWorker::StartIOService, this);

  // Create an entry for the driver task in the task table. This task is
  // added immediately with status RUNNING. This allows us to push errors
  // related to this driver task back to the driver. For example, if the
  // driver creates an object that is later evicted, we should notify the
  // user that we're unable to reconstruct the object, since we cannot
  // rerun the driver.
  if (worker_type_ == WorkerType::DRIVER) {
    TaskSpecBuilder builder;
    std::vector<std::string> empty_descriptor;
    std::unordered_map<std::string, double> empty_resources;
    const TaskID task_id = TaskID::ForDriverTask(worker_context_.GetCurrentJobID());
    builder.SetCommonTaskSpec(task_id, language_, empty_descriptor,
                              worker_context_.GetCurrentJobID(),
                              TaskID::ComputeDriverTaskId(worker_context_.GetWorkerID()),
                              0, 0, empty_resources, empty_resources);

    std::shared_ptr<gcs::TaskTableData> data = std::make_shared<gcs::TaskTableData>();
    data->mutable_task()->mutable_task_spec()->CopyFrom(builder.Build().GetMessage());
    RAY_CHECK_OK(gcs_client_->raylet_task_table().Add(job_id, task_id, data, nullptr));
    worker_context_.SetCurrentTaskId(task_id);
  }
}

CoreWorker::~CoreWorker() {
  io_service_.stop();
  io_thread_.join();
  if (task_execution_interface_) {
    task_execution_interface_->Stop();
  }
  if (log_dir_ != "") {
    RayLog::ShutDownRayLog();
  }
}

void CoreWorker::Disconnect() {
  if (gcs_client_) {
    gcs_client_->Disconnect();
  }
  if (raylet_client_) {
    RAY_IGNORE_EXPR(raylet_client_->Disconnect());
  }
}

void CoreWorker::StartIOService() { io_service_.run(); }

std::unique_ptr<worker::ProfileEvent> CoreWorker::CreateProfileEvent(
    const std::string &event_type) {
  return std::unique_ptr<worker::ProfileEvent>(
      new worker::ProfileEvent(profiler_, event_type));
}

}  // namespace ray
