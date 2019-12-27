#ifndef RAY_CORE_WORKER_CONTEXT_H
#define RAY_CORE_WORKER_CONTEXT_H

#include <boost/thread.hpp>

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

  void ResetCurrentTask(const TaskSpecification &task_spec);

  std::shared_ptr<const TaskSpecification> GetCurrentTask() const;

  const ActorID &GetCurrentActorID() const;

  /// Returns whether the current thread is the main worker thread.
  bool CurrentThreadIsMain() const;

  /// Returns whether we should Block/Unblock through the raylet on Get/Wait.
  /// This only applies to direct task calls.
  bool ShouldReleaseResourcesOnBlockingCalls() const;

  /// Returns whether we are in a direct call actor.
  bool CurrentActorIsDirectCall() const;

  /// Returns whether we are in a direct call task. This encompasses both direct
  /// actor and normal tasks.
  bool CurrentTaskIsDirectCall() const;

  int CurrentActorMaxConcurrency() const;

  bool CurrentActorIsAsync() const;

  int GetNextTaskIndex();

  int GetNextPutIndex();

 private:
  const WorkerType worker_type_;
  const WorkerID worker_id_;
  JobID current_job_id_;
  ActorID current_actor_id_;
  bool current_actor_is_direct_call_ = false;
  bool current_task_is_direct_call_ = false;
  int current_actor_max_concurrency_ = 1;
  bool current_actor_is_asyncio_ = false;

  /// The id of the (main) thread that constructed this worker context.
  boost::thread::id main_thread_id_;

 private:
  static WorkerThreadContext &GetThreadContext(bool for_main_thread = false);

  /// Per-thread worker context.
  static thread_local std::unique_ptr<WorkerThreadContext> thread_context_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_CONTEXT_H
