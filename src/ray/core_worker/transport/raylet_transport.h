#ifndef RAY_CORE_WORKER_RAYLET_TRANSPORT_H
#define RAY_CORE_WORKER_RAYLET_TRANSPORT_H

#include <list>

#include "ray/core_worker/object_interface.h"
#include "ray/raylet/raylet_client.h"
#include "ray/rpc/worker/worker_server.h"

namespace ray {

class CoreWorkerRayletTaskReceiver : public rpc::WorkerTaskHandler {
 public:
  using TaskHandler = std::function<Status(
      const TaskSpecification &task_spec, const ResourceMappingType &resource_ids,
      std::vector<std::shared_ptr<RayObject>> *results)>;

  CoreWorkerRayletTaskReceiver(WorkerContext &worker_context,
                               std::unique_ptr<RayletClient> &raylet_client,
                               CoreWorkerObjectInterface &object_interface,
                               boost::asio::io_service &io_service,
                               rpc::GrpcServer &server, const TaskHandler &task_handler,
                               std::function<void(int64_t)> on_wait_complete);

  /// Handle a `AssignTask` request.
  /// The implementation can handle this request asynchronously. When handling is done,
  /// the `send_reply_callback` should be called.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] send_reply_callback The callback to be called when the request is done.
  void HandleAssignTask(const rpc::AssignTaskRequest &request,
                        rpc::AssignTaskReply *reply,
                        rpc::SendReplyCallback send_reply_callback) override;

  // TODO(ekl) this shouldn't be in the raylet task receiver
  void HandleDirectActorCallArgWaitComplete(
      const rpc::DirectActorCallArgWaitCompleteRequest &request,
      rpc::DirectActorCallArgWaitCompleteReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

 private:
  // Worker context.
  WorkerContext &worker_context_;
  /// Raylet client.
  std::unique_ptr<RayletClient> &raylet_client_;
  // Object interface.
  CoreWorkerObjectInterface &object_interface_;
  /// The rpc service for `WorkerTaskService`.
  rpc::WorkerTaskGrpcService task_service_;
  /// The callback function to process a task.
  TaskHandler task_handler_;
  /// The callback to process arg wait complete.
  std::function<void(int64_t)> on_wait_complete_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_RAYLET_TRANSPORT_H
