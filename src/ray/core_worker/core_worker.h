#ifndef RAY_CORE_WORKER_CORE_WORKER_H
#define RAY_CORE_WORKER_CORE_WORKER_H

#include "ray/common/buffer.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/object_interface.h"
#include "ray/core_worker/task_execution.h"
#include "ray/core_worker/task_interface.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/raylet/raylet_client.h"

namespace ray {

/// The root class that contains all the core and language-independent functionalities
/// of the worker. This class is supposed to be used to implement app-language (Java,
/// Python, etc) workers.
class CoreWorker {
 public:
  /// Construct a CoreWorker instance.
  ///
  /// \param[in] worker_type Type of this worker.
  /// \param[in] langauge Language of this worker.
  ///
  /// NOTE(zhijunfu): the constructor would throw if a failure happens.
  CoreWorker(const WorkerType worker_type, const Language language,
             const std::string &store_socket, const std::string &raylet_socket,
             const JobID &job_id, const gcs::GcsClientOptions &gcs_options,
             const CoreWorkerTaskExecutionInterface::TaskExecutor &execution_callback);

  ~CoreWorker();

  /// Type of this worker.
  WorkerType GetWorkerType() const { return worker_type_; }

  /// Language of this worker.
  Language GetLanguage() const { return language_; }

  /// Return the `CoreWorkerTaskInterface` that contains the methods related to task
  /// submisson.
  CoreWorkerTaskInterface &Tasks() { return *task_interface_; }

  /// Return the `CoreWorkerObjectInterface` that contains methods related to object
  /// store.
  CoreWorkerObjectInterface &Objects() { return *object_interface_; }

  /// Return the `CoreWorkerTaskExecutionInterface` that contains methods related to
  /// task execution.
  CoreWorkerTaskExecutionInterface &Execution() {
    RAY_CHECK(task_execution_interface_ != nullptr);
    return *task_execution_interface_;
  }

 private:
  void StartIOService();

  /// Type of this worker.
  const WorkerType worker_type_;

  /// Language of this worker.
  const Language language_;

  /// raylet socket name.
  const std::string raylet_socket_;

  /// Worker context.
  WorkerContext worker_context_;

  /// event loop where the IO events are handled. e.g. async GCS operations.
  boost::asio::io_service io_service_;

  /// keeps io_service_ alive.
  boost::asio::io_service::work io_work_;

  /// The thread to handle IO events.
  std::thread io_thread_;

  /// Raylet client.
  std::unique_ptr<RayletClient> raylet_client_;

  /// GCS client.
  std::unique_ptr<gcs::RedisGcsClient> gcs_client_;

  /// The `CoreWorkerTaskInterface` instance.
  std::unique_ptr<CoreWorkerTaskInterface> task_interface_;

  /// The `CoreWorkerObjectInterface` instance.
  std::unique_ptr<CoreWorkerObjectInterface> object_interface_;

  /// The `CoreWorkerTaskExecutionInterface` instance.
  /// This is only available if it's not a driver.
  std::unique_ptr<CoreWorkerTaskExecutionInterface> task_execution_interface_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_CORE_WORKER_H
