#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/context.h"

namespace ray {

CoreWorker::CoreWorker(const enum WorkerType worker_type, const ::Language language,
                       const std::string &store_socket, const std::string &raylet_socket,
                       const JobID &job_id)
    : worker_type_(worker_type),
      language_(language),
      raylet_socket_(raylet_socket),
      worker_context_(worker_type, job_id),
      // TODO(zhijunfu): currently RayletClient would crash in its constructor
      // if it cannot connect to Raylet after a number of retries, this needs
      // to be changed so that the worker (java/python .etc) can retrieve and
      // handle the error instead of crashing.
      raylet_client_(raylet_socket_,
                     ClientID::FromBinary(worker_context_.GetWorkerID().Binary()),
                     (worker_type_ == ray::WorkerType::WORKER),
                     worker_context_.GetCurrentJobID(), language_),
      task_interface_(worker_context_, raylet_client_),
      object_interface_(worker_context_, raylet_client_, store_socket),
      task_execution_interface_(worker_context_, raylet_client_, object_interface_) {}
}  // namespace ray
