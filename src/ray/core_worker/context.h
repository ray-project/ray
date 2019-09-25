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

  // TODO(edoakes): remove this once Python core worker uses the task interfaces.
  void SetCurrentJobId(const JobID &job_id);

  // TODO(edoakes): remove this once Python core worker uses the task interfaces.
  void SetCurrentTaskId(const TaskID &task_id);

  void SetCurrentTask(const TaskSpecification &task_spec);

  std::shared_ptr<const TaskSpecification> GetCurrentTask() const;

  const ActorID &GetCurrentActorID() const;

  bool CurrentActorUseDirectCall() const;

  int GetNextTaskIndex();

  int GetNextPutIndex();

 private:
  const WorkerType worker_type_;
  const WorkerID worker_id_;
  JobID current_job_id_;
  ActorID current_actor_id_;
  bool current_actor_use_direct_call_;

 private:
  static WorkerThreadContext &GetThreadContext();

  /// Per-thread worker context.
  static thread_local std::unique_ptr<WorkerThreadContext> thread_context_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_CONTEXT_H
