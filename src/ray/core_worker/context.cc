
#include "ray/core_worker/context.h"

namespace ray {

/// per-thread context for core worker.
struct WorkerThreadContext {
  WorkerThreadContext()
      : current_task_id_(TaskID::ForFakeTask()), task_index_(0), put_index_(0) {}

  int GetNextTaskIndex() { return ++task_index_; }

  int GetNextPutIndex() { return ++put_index_; }

  const TaskID &GetCurrentTaskID() const { return current_task_id_; }

  std::shared_ptr<const TaskSpecification> GetCurrentTask() const {
    return current_task_;
  }

  void SetCurrentTaskId(const TaskID &task_id) {
    current_task_id_ = task_id;
    task_index_ = 0;
    put_index_ = 0;
  }

  void SetCurrentTask(const TaskSpecification &task_spec) {
    SetCurrentTaskId(task_spec.TaskId());
    current_task_ = std::make_shared<const TaskSpecification>(task_spec);
  }

 private:
  /// The task ID for current task.
  TaskID current_task_id_;

  /// The current task.
  std::shared_ptr<const TaskSpecification> current_task_;

  /// Number of tasks that have been submitted from current task.
  int task_index_;

  /// Number of objects that have been put from current task.
  int put_index_;
};

thread_local std::unique_ptr<WorkerThreadContext> WorkerContext::thread_context_ =
    nullptr;

WorkerContext::WorkerContext(WorkerType worker_type, const JobID &job_id)
    : worker_type_(worker_type),
      worker_id_(worker_type_ == WorkerType::DRIVER ? ComputeDriverIdFromJob(job_id)
                                                    : WorkerID::FromRandom()),
      current_job_id_(worker_type_ == WorkerType::DRIVER ? job_id : JobID::Nil()),
      current_actor_id_(ActorID::Nil()) {
  // For worker main thread which initializes the WorkerContext,
  // set task_id according to whether current worker is a driver.
  // (For other threads it's set to random ID via GetThreadContext).
  GetThreadContext().SetCurrentTaskId((worker_type_ == WorkerType::DRIVER)
                                          ? TaskID::ForDriverTask(job_id)
                                          : TaskID::Nil());
}

const WorkerType WorkerContext::GetWorkerType() const { return worker_type_; }

const WorkerID &WorkerContext::GetWorkerID() const { return worker_id_; }

int WorkerContext::GetNextTaskIndex() { return GetThreadContext().GetNextTaskIndex(); }

int WorkerContext::GetNextPutIndex() { return GetThreadContext().GetNextPutIndex(); }

const JobID &WorkerContext::GetCurrentJobID() const { return current_job_id_; }

const TaskID &WorkerContext::GetCurrentTaskID() const {
  return GetThreadContext().GetCurrentTaskID();
}

// TODO(edoakes): remove this once Python core worker uses the task interfaces.
void WorkerContext::SetCurrentJobId(const JobID &job_id) { current_job_id_ = job_id; }

// TODO(edoakes): remove this once Python core worker uses the task interfaces.
void WorkerContext::SetCurrentTaskId(const TaskID &task_id) {
  GetThreadContext().SetCurrentTaskId(task_id);
}

void WorkerContext::SetCurrentTask(const TaskSpecification &task_spec) {
  SetCurrentJobId(task_spec.JobId());
  GetThreadContext().SetCurrentTask(task_spec);
  if (task_spec.IsActorCreationTask()) {
    RAY_CHECK(current_actor_id_.IsNil());
    current_actor_id_ = task_spec.ActorCreationId();
    current_actor_use_direct_call_ = task_spec.IsDirectCall();
  }
  if (task_spec.IsActorTask()) {
    RAY_CHECK(current_actor_id_ == task_spec.ActorId());
  }
}
std::shared_ptr<const TaskSpecification> WorkerContext::GetCurrentTask() const {
  return GetThreadContext().GetCurrentTask();
}

const ActorID &WorkerContext::GetCurrentActorID() const { return current_actor_id_; }

bool WorkerContext::CurrentActorUseDirectCall() const {
  return current_actor_use_direct_call_;
}

WorkerThreadContext &WorkerContext::GetThreadContext() {
  if (thread_context_ == nullptr) {
    thread_context_ = std::unique_ptr<WorkerThreadContext>(new WorkerThreadContext());
  }

  return *thread_context_;
}

}  // namespace ray
