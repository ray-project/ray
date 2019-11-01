#ifndef RAY_RAYLET_WORKER_H
#define RAY_RAYLET_WORKER_H

#include <memory>

#include "ray/common/client_connection.h"
#include "ray/common/id.h"
#include "ray/common/task/scheduling_resources.h"
#include "ray/common/task/task.h"
#include "ray/common/task/task_common.h"
#include "ray/rpc/worker/direct_actor_client.h"
#include "ray/rpc/worker/worker_client.h"

namespace ray {

namespace raylet {

/// Worker class encapsulates the implementation details of a worker. A worker
/// is the execution container around a unit of Ray work, such as a task or an
/// actor. Ray units of work execute in the context of a Worker.
class Worker {
 public:
  /// A constructor that initializes a worker object.
  Worker(const WorkerID &worker_id, pid_t pid, const Language &language, int port,
         std::shared_ptr<LocalClientConnection> connection,
         rpc::ClientCallManager &client_call_manager);
  /// A destructor responsible for freeing all worker state.
  ~Worker() {}
  void MarkDead();
  bool IsDead() const;
  void MarkBlocked();
  void MarkUnblocked();
  bool IsBlocked() const;
  /// Return the worker's ID.
  WorkerID WorkerId() const;
  /// Return the worker's PID.
  pid_t Pid() const;
  Language GetLanguage() const;
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
  void MarkDetachedActor();
  bool IsDetachedActor() const;
  const std::shared_ptr<LocalClientConnection> Connection() const;

  const ResourceIdSet &GetLifetimeResourceIds() const;
  void SetLifetimeResourceIds(ResourceIdSet &resource_ids);
  void ResetLifetimeResourceIds();

  const ResourceIdSet &GetTaskResourceIds() const;
  void SetTaskResourceIds(ResourceIdSet &resource_ids);
  void ResetTaskResourceIds();
  ResourceIdSet ReleaseTaskCpuResources();
  void AcquireTaskCpuResources(const ResourceIdSet &cpu_resources);

  const std::unordered_set<ObjectID> &GetActiveObjectIds() const;
  void SetActiveObjectIds(const std::unordered_set<ObjectID> &&object_ids);

  void AssignTask(const Task &task, const ResourceIdSet &resource_id_set,
                  const std::function<void(Status)> finish_assign_callback);
  void DirectActorCallArgWaitComplete(int64_t tag);

 private:
  /// The worker's ID.
  WorkerID worker_id_;
  /// The worker's PID.
  pid_t pid_;
  /// The language type of this worker.
  Language language_;
  /// Port that this worker listens on.
  /// If port <= 0, this indicates that the worker will not listen to a port.
  int port_;
  /// Connection state of a worker.
  std::shared_ptr<LocalClientConnection> connection_;
  /// The worker's currently assigned task.
  TaskID assigned_task_id_;
  /// Job ID for the worker's current assigned task.
  JobID assigned_job_id_;
  /// The worker's actor ID. If this is nil, then the worker is not an actor.
  ActorID actor_id_;
  /// Whether the worker is dead.
  bool dead_;
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
  /// The set of object IDs that are currently in use on the worker.
  std::unordered_set<ObjectID> active_object_ids_;
  /// The `ClientCallManager` object that is shared by `WorkerTaskClient` from all
  /// workers.
  rpc::ClientCallManager &client_call_manager_;
  /// The rpc client to send tasks to this worker.
  std::unique_ptr<rpc::WorkerTaskClient> rpc_client_;
  /// Whether the worker is detached. This is applies when the worker is actor.
  /// Detached actor means the actor's creator can exit without killing this actor.
  bool is_detached_actor_;
  /// The rpc client to send tasks to the direct actor service.
  std::unique_ptr<rpc::DirectActorClient> direct_rpc_client_;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_WORKER_H
