#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/context.h"

namespace ray {

CoreWorker::CoreWorker(const enum WorkerType worker_type, const ::Language language,
                       const std::string &store_socket, const std::string &raylet_socket,
                       const JobID &job_id)
    : worker_type_(worker_type),
      language_(language),
      worker_context_(std::make_shared<WorkerContext>(worker_type, job_id)) {
  // TODO(zhijunfu): currently RayletClient would crash in its constructor
  // if it cannot connect to Raylet after a number of retries, this needs
  // to be changed so that the worker (java/python .etc) can retrieve and
  // handle the error instead of crashing.
  // TODO(qwang): JobId parameter can be removed once we embed jobId in driverId.
  auto raylet_client = std::make_shared<RayletClient>(
      raylet_socket, ClientID::FromBinary(worker_context_->GetWorkerID().Binary()),
      (worker_type_ == ray::WorkerType::WORKER), worker_context_->GetCurrentJobID(),
      language_);
  task_interface_ = std::make_shared<CoreWorkerTaskInterface>(
      worker_context_, std::make_shared<CoreWorkerRayletTaskSubmitter>(raylet_client));
  object_interface_ = std::make_shared<CoreWorkerObjectInterface>(
      worker_context_,
      std::make_shared<CoreWorkerPlasmaStoreProvider>(raylet_client, store_socket));
  task_execution_interface_ = std::make_shared<CoreWorkerTaskExecutionInterface>(
      worker_context_, object_interface_,
      std::make_shared<CoreWorkerRayletTaskReceiver>(raylet_client));
}

CoreWorker::CoreWorker(
    const ::Language language, std::shared_ptr<WorkerContext> worker_context,
    std::shared_ptr<CoreWorkerTaskInterface> task_interface,
    std::shared_ptr<CoreWorkerObjectInterface> object_interface,
    std::shared_ptr<CoreWorkerTaskExecutionInterface> task_execution_interface)
    : worker_type_(worker_context->GetWorkerType()),
      language_(language),
      worker_context_(worker_context),
      task_interface_(task_interface),
      object_interface_(object_interface),
      task_execution_interface_(task_execution_interface) {}
}  // namespace ray
