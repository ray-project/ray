#ifndef RAY_CORE_WORKER_RAYLET_TRANSPORT_H
#define RAY_CORE_WORKER_RAYLET_TRANSPORT_H

#include <list>

#include "ray/common/ray_object.h"
#include "ray/core_worker/reference_count.h"
#include "ray/raylet/raylet_client.h"
#include "ray/rpc/worker/core_worker_server.h"

namespace ray {

class CoreWorkerRayletTaskReceiver {
 public:
  using TaskHandler =
      std::function<Status(const TaskSpecification &task_spec,
                           const std::shared_ptr<ResourceMappingType> &resource_ids,
                           std::vector<std::shared_ptr<RayObject>> *return_objects,
                           ReferenceCounter::ReferenceTableProto *borrower_refs)>;

  CoreWorkerRayletTaskReceiver(const WorkerID &worker_id,
                               std::shared_ptr<raylet::RayletClient> &raylet_client,
                               const TaskHandler &task_handler,
                               const std::function<void(bool)> &exit_handler);

  /// Handle a `AssignTask` request.
  /// The implementation can handle this request asynchronously. When handling is done,
  /// the `send_reply_callback` should be called.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] send_reply_callback The callback to be called when the request is done.
  void HandleAssignTask(const rpc::AssignTaskRequest &request,
                        rpc::AssignTaskReply *reply,
                        rpc::SendReplyCallback send_reply_callback);

 private:
  // WorkerID of this worker.
  WorkerID worker_id_;
  /// Reference to the core worker's raylet client. This is a pointer ref so that it
  /// can be initialized by core worker after this class is constructed.
  std::shared_ptr<raylet::RayletClient> &raylet_client_;
  /// The callback function to process a task.
  TaskHandler task_handler_;
  /// The callback function to exit the worker.
  std::function<void(bool)> exit_handler_;
  /// The callback to process arg wait complete.
  std::function<void(int64_t)> on_wait_complete_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_RAYLET_TRANSPORT_H
