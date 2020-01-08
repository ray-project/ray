#ifndef RAY_GCS_TASK_INFO_HANDLER_IMPL_H
#define RAY_GCS_TASK_INFO_HANDLER_IMPL_H

#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace rpc {

/// This implementation class of `TaskInfoHandler`.
class DefaultTaskInfoHandler : public rpc::TaskInfoHandler {
 public:
  explicit DefaultTaskInfoHandler(gcs::RedisGcsClient &gcs_client)
      : gcs_client_(gcs_client) {}

  void HandleAddTask(const AddTaskRequest &request, AddTaskReply *reply,
                     SendReplyCallback send_reply_callback) override;

  void HandleGetTask(const GetTaskRequest &request, GetTaskReply *reply,
                     SendReplyCallback send_reply_callback) override;

  void HandleDeleteTasks(const DeleteTasksRequest &request, DeleteTasksReply *reply,
                         SendReplyCallback send_reply_callback) override;

  void HandleAddTaskLease(const AddTaskLeaseRequest &request, AddTaskLeaseReply *reply,
                          SendReplyCallback send_reply_callback) override;

  void HandleAttemptTaskReconstruction(const AttemptTaskReconstructionRequest &request,
                                       AttemptTaskReconstructionReply *reply,
                                       SendReplyCallback send_reply_callback) override;

 private:
  gcs::RedisGcsClient &gcs_client_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_GCS_TASK_INFO_HANDLER_IMPL_H
