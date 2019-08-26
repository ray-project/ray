#ifndef RAY_CORE_WORKER_CONTEXT_H
#define RAY_CORE_WORKER_CONTEXT_H

#include "ray/common/task/task_spec.h"
#include "ray/core_worker/common.h"

namespace ray {

struct WorkerThreadContext;

class WorkerContext {
 public:
  WorkerContext(WorkerType worker_type, const JobID &job_id);

  const WorkerType GetWorkerType() const;

  const WorkerID &GetWorkerID() const;

  const JobID &GetCurrentJobID() const;

  const TaskID &GetCurrentTaskID() const;

  void SetCurrentTask(const TaskSpecification &task_spec);

  std::shared_ptr<const TaskSpecification> GetCurrentTask() const;

  const ActorID &GetCurrentActorID() const;

  int GetNextTaskIndex();

  int GetNextPutIndex();

 private:
  /// Type of the worker.
  const WorkerType worker_type_;

  /// ID for this worker.
  const WorkerID worker_id_;

  /// Job ID for this worker.
  JobID current_job_id_;

  /// ID of current actor.
  ActorID current_actor_id_;

 private:
  static WorkerThreadContext &GetThreadContext();

  /// Per-thread worker context.
  static thread_local std::unique_ptr<WorkerThreadContext> thread_context_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_CONTEXT_H
