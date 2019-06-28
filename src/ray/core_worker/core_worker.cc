#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/context.h"

namespace ray {

CoreWorker::CoreWorker(const enum WorkerType worker_type,
                       const enum WorkerLanguage language,
                       const std::string &store_socket, const std::string &raylet_socket,
                       const JobID &job_id)
    : worker_type_(worker_type),
      language_(language),
      raylet_socket_(raylet_socket),
      worker_context_(worker_type, job_id),
      raylet_client_(raylet_socket_,
                     ClientID::FromBinary(worker_context_.GetWorkerID().Binary()),
                     (worker_type_ == ray::WorkerType::WORKER),
                     worker_context_.GetCurrentJobID(), ToTaskLanguage(language_)),
      task_interface_(*this),
      object_interface_(*this, store_socket),
      task_execution_interface_(*this) {
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
