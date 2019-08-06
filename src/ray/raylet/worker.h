#ifndef RAY_RAYLET_WORKER_H
#define RAY_RAYLET_WORKER_H

#include <memory>

#include "ray/common/id.h"
#include "ray/common/task/scheduling_resources.h"
#include "ray/common/task/task.h"
#include "ray/common/task/task_common.h"
#include "ray/protobuf/common.pb.h"
#include "ray/rpc/worker/worker_client.h"
#include "src/ray/protobuf/gcs.pb.h"
#include "src/ray/protobuf/raylet.pb.h"
#include "src/ray/rpc/server_call.h"

namespace ray {

namespace raylet {

/// Worker class encapsulates the implementation details of a worker. A worker
/// is the execution container around a unit of Ray work, such as a task or an
/// actor. Ray units of work execute in the context of a Worker.
class Worker {
 public:
  /// A constructor that initializes a worker object.
  Worker(const WorkerID &worker_id, pid_t pid, const Language &language, int port,
         rpc::ClientCallManager &client_call_manager, bool is_worker = true);
  /// A destructor responsible for freeing all worker state.
  ~Worker() {}
  void MarkAsBeingKilled();
  bool IsBeingKilled() const;
  bool IsWorker() const;
  void MarkBlocked();
  void MarkUnblocked();
  bool IsBlocked() const;
  /// Return the worker's ID.
  WorkerID WorkerId() const;
  /// Return the worker's PID.
  pid_t Pid() const;
  Language GetLanguage() const;
  const WorkerID &GetWorkerId() const;
  int Port() const;
  void AssignTaskId(const TaskID &task_id);
  const TaskID &GetAssignedTaskId() const;
  bool AddBlockedTaskId(const TaskID &task_id);
  bool RemoveBlockedTaskId(const TaskID &task_id);
  const std::unordered_set<TaskID> &GetBlockedTaskIds() const;
  void AssignJobId(const JobID &job_id);
  const JobID &GetAssignedJobId() const;
  void AssignActorId(const ActorID &actor_id);
  const ActorID &GetActorId() const;

  const ResourceIdSet &GetLifetimeResourceIds() const;
  void SetLifetimeResourceIds(ResourceIdSet &resource_ids);
  void ResetLifetimeResourceIds();

  const ResourceIdSet &GetTaskResourceIds() const;
  void SetTaskResourceIds(ResourceIdSet &resource_ids);
  void ResetTaskResourceIds();
  ResourceIdSet ReleaseTaskCpuResources();
  void AcquireTaskCpuResources(const ResourceIdSet &cpu_resources);

  int TickHeartbeatTimer() { return ++num_missed_heartbeats_; }
  void ClearHeartbeat() { num_missed_heartbeats_ = 0; }

  /// When receiving a `GetTask` request from worker, this function should be called to
  /// pass the reply and callback of the request to the worker. Later, when we actually
  /// assign a task to the worker, the reply will be filled and the callback will be
  /// called.
  void SetGetTaskReplyAndCallback(rpc::GetTaskReply *reply,
                                  const rpc::SendReplyCallback &&send_reply_callback);

  bool UsePush() const;
  void AssignTask(const Task &task, const ResourceIdSet &resource_id_set);

 private:
  /// The worker's ID.
  WorkerID worker_id_;
  /// The worker's PID.
  pid_t pid_;
  /// The worker port.
  int port_;
  /// The language type of this worker.
  Language language_;
  /// The worker's currently assigned task.
  TaskID assigned_task_id_;
  /// Job ID for the worker's current assigned task.
  JobID assigned_job_id_;
  /// The worker's actor ID. If this is nil, then the worker is not an actor.
  ActorID actor_id_;
  /// Whether the worker is blocked. Workers become blocked in a `ray.get`, if
  /// they require a data dependency while executing a task.
  bool blocked_;
  /// The specific resource IDs that this worker owns for its lifetime. This is
  /// only used for actors.
  ResourceIdSet lifetime_resource_ids_;
  /// The specific resource IDs that this worker currently owns for the duration
  // of a task.
  ResourceIdSet task_resource_ids_;
  std::unordered_set<TaskID> blocked_task_ids_;
  /// How many heartbeats have been missed for this worker.
  int num_missed_heartbeats_;
  /// Indicates we have sent kill signal to the worker if it's true. We cannot treat the
  /// worker process as really dead until we lost the heartbeats from the worker.
  bool is_being_killed_;
  /// The `ClientCallManager` object that is shared by `WorkerTaskClient` from all
  /// workers.
  rpc::ClientCallManager &client_call_manager_;
  /// Indicates whether this is a worker or a driver.
  bool is_worker_;
  /// The rpc client to send tasks to this worker.
  std::unique_ptr<rpc::WorkerTaskClient> rpc_client_;
  /// Reply of the `GetTask` request.
  rpc::GetTaskReply *reply_ = nullptr;
  /// Callback of the `GetTask` request.
  rpc::SendReplyCallback send_reply_callback_ = nullptr;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_WORKER_H
