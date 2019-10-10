#ifndef RAY_CORE_WORKER_RAYLET_TRANSPORT_H
#define RAY_CORE_WORKER_RAYLET_TRANSPORT_H

#include <list>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "ray/common/task/task.h"
#include "ray/core_worker/object_interface.h"
#include "ray/core_worker/transport/transport.h"
#include "ray/raylet/raylet_client.h"
#include "ray/rpc/worker/worker_server.h"

namespace ray {

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
  Status SubmitTask(const TaskSpecification &task_spec) override;
  Status SubmitTaskBatch(const std::vector<TaskSpecification> &tasks) override;

 private:
  /// Raylet client.
  std::unique_ptr<RayletClient> &raylet_client_;
};

class CoreWorkerRayletTaskReceiver : public CoreWorkerTaskReceiver,
                                     public rpc::WorkerTaskHandler {
 public:
  CoreWorkerRayletTaskReceiver(WorkerContext &worker_context,
                               std::unique_ptr<RayletClient> &raylet_client,
                               CoreWorkerObjectInterface &object_interface,
                               boost::asio::io_service &rpc_io_service,
                               boost::asio::io_service &main_io_service,
                               rpc::GrpcServer &server, const TaskHandler &task_handler);

  void HandleAssignTask(const rpc::AssignTaskRequest &request,
                        rpc::AssignTaskReply *reply,
                        rpc::SendReplyCallback send_reply_callback) override;

  void HandleStealTasks(const rpc::StealTasksRequest &request,
                        rpc::StealTasksReply *reply,
                        rpc::SendReplyCallback send_reply_callback) override;

 private:
  void ProcessAssignedTasks(rpc::SendReplyCallback send_reply_callback);

  Status HandleAssignTask0(const rpc::AssignTaskRequest &request,
                           const TaskSpecification &task_spec);

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
  /// Event loop to run tasks on.
  boost::asio::io_service &task_main_io_service_;

  /// Mutex to protect the assigned task queues below.
  std::mutex mu_;
  /// List of tasks to execute next.
  std::list<TaskSpecification> assigned_tasks_ GUARDED_BY(mu_);
  /// Number of tasks assigned in total in the last assign call.
  int num_assigned_ GUARDED_BY(mu_);
  /// Number of tasks stolen of those assigned.
  int num_stolen_ GUARDED_BY(mu_);
  /// RPC request being worked on.
  rpc::AssignTaskRequest assigned_req_ GUARDED_BY(mu_);
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_RAYLET_TRANSPORT_H
