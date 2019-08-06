#ifndef RAY_CORE_WORKER_RAYLET_TRANSPORT_H
#define RAY_CORE_WORKER_RAYLET_TRANSPORT_H

#include <list>

#include "ray/core_worker/object_interface.h"
#include "ray/core_worker/transport/transport.h"
#include "ray/rpc/raylet/raylet_client.h"
#include "ray/rpc/worker/worker_server.h"

namespace ray {

using rpc::RayletClient;

/// In raylet task submitter and receiver, a task is submitted to raylet, and possibly
/// gets forwarded to another raylet on which node the task should be executed, and
/// then a worker on that node gets this task and starts executing it.

class CoreWorkerRayletTaskSubmitter : public CoreWorkerTaskSubmitter {
 public:
  CoreWorkerRayletTaskSubmitter(std::unique_ptr<RayletClient> &raylet_client);

  /// Submit a task for execution to raylet.
  ///
  /// \param[in] task The task spec to submit.
  /// \return Status.
  virtual Status SubmitTask(const TaskSpecification &task_spec) override;

  /// Check if a task has finished.
  ///
  /// \param[in] task_id The ID of the task.
  /// \return If the task has finished.
  bool IsTaskDone(const TaskID &task_id) override;

 private:
  /// Raylet client.
  std::unique_ptr<RayletClient> &raylet_client_;
};

class CoreWorkerRayletTaskReceiver : public CoreWorkerTaskReceiver,
                                     public rpc::WorkerTaskHandler {
 public:
  CoreWorkerRayletTaskReceiver(std::unique_ptr<RayletClient> &raylet_client,
                               CoreWorkerStoreProviderLayer &store_provider_layer,
                               boost::asio::io_service &io_service,
                               const TaskHandler &task_handler);

  /// Handle a `AssignTask` request.
  /// The implementation can handle this request asynchronously. When hanling is done, the
  /// `send_reply_callback` should be called.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] send_reply_callback The callback to be called when the request is done.
  void HandleAssignTask(const rpc::AssignTaskRequest &request,
                        rpc::AssignTaskReply *reply,
                        rpc::SendReplyCallback send_reply_callback) override;

  /// Return the underlying rpc service.
  rpc::GrpcService &GetRpcService() override;
 private:
  /// Raylet client.
  std::unique_ptr<RayletClient> &raylet_client_;
  // Reference to `CoreWorkerStoreProviderLayer`.
  CoreWorkerStoreProviderLayer &store_provider_layer_;
  /// The rpc service for `WorkerTaskService`.
  rpc::WorkerTaskGrpcService task_service_;
  /// The callback function to process a task.
  TaskHandler task_handler_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_RAYLET_TRANSPORT_H
