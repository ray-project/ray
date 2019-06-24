#ifndef RAY_CORE_WORKER_CORE_WORKER_H
#define RAY_CORE_WORKER_CORE_WORKER_H

#include "ray/common/buffer.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/object_interface.h"
#include "ray/core_worker/task_execution.h"
#include "ray/core_worker/task_interface.h"
#include "ray/raylet/raylet_client.h"
#include "ray/rpc/worker_server.h"

namespace ray {

/// The root class that contains all the core and language-independent functionalities
/// of the worker. This class is supposed to be used to implement app-language (Java,
/// Python, etc) workers.
class CoreWorker : public rpc::WorkerServiceHandler {
 public:
  /// Construct a CoreWorker instance.
  ///
  /// \param[in] worker_type Type of this worker.
  /// \param[in] langauge Language of this worker.
  ///
  /// NOTE(zhijunfu): the constructor would throw if a failure happens.
  CoreWorker(const WorkerType worker_type, const WorkerLanguage language,
             const std::string &store_socket, const std::string &raylet_socket,
             DriverID driver_id = DriverID::Nil());

  /// Type of this worker.
  enum WorkerType WorkerType() const { return worker_type_; }

  /// Language of this worker.
  enum WorkerLanguage Language() const { return language_; }

  /// Return the `CoreWorkerTaskInterface` that contains the methods related to task
  /// submisson.
  CoreWorkerTaskInterface &Tasks() { return task_interface_; }

  /// Return the `CoreWorkerObjectInterface` that contains methods related to object
  /// store.
  CoreWorkerObjectInterface &Objects() { return object_interface_; }

  /// Return the `CoreWorkerTaskExecutionInterface` that contains methods related to
  /// task execution.
  CoreWorkerTaskExecutionInterface &Execution() { return task_execution_interface_; }

  /// Handle a `PushTask` request.
  /// The implementation can handle this request asynchronously. When hanling is done, the
  /// `done_callback` should be called.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] done_callback The callback to be called when the request is done.
  void HandlePushTask(const rpc::PushTaskRequest &request,
                      rpc::PushTaskReply *reply,
                      rpc::RequestDoneCallback done_callback) override { /* TODO */ }

 private:
  /// Translate from WorkLanguage to Language type (required by raylet client).
  ///
  /// \param[in] language Language for a task.
  /// \return Translated task language.
  ::Language ToTaskLanguage(WorkerLanguage language);

  /// Type of this worker.
  const enum WorkerType worker_type_;

  /// Language of this worker.
  const enum WorkerLanguage language_;

  /// Plasma store socket name.
  const std::string store_socket_;

  /// raylet socket name.
  const std::string raylet_socket_;

  /// Worker context.
  WorkerContext worker_context_;

  /// Plasma store client.
  plasma::PlasmaClient store_client_;

  /// Mutex to protect store_client_.
  std::mutex store_client_mutex_;

  /// Raylet client.
  std::unique_ptr<RayletClient> raylet_client_;

  /// Main IO service to execute tasks.
  boost::asio::io_service main_service_;

  /// To ensure the IO service will not stop when there are no tasks
  /// to process.
  boost::asio::io_service::work main_work_;

  /// The RPC server for this worker.
  rpc::WorkerServer worker_server_;

  /// The `CoreWorkerTaskInterface` instance.
  CoreWorkerTaskInterface task_interface_;

  /// The `CoreWorkerObjectInterface` instance.
  CoreWorkerObjectInterface object_interface_;

  /// The `CoreWorkerTaskExecutionInterface` instance.
  CoreWorkerTaskExecutionInterface task_execution_interface_;

  friend class CoreWorkerTaskInterface;
  friend class CoreWorkerObjectInterface;
  friend class CoreWorkerTaskExecutionInterface;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_CORE_WORKER_H
