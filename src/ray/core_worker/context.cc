// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

  void SetCurrentTaskId(const TaskID &task_id) { current_task_id_ = task_id; }

  void SetCurrentTask(const TaskSpecification &task_spec) {
    RAY_CHECK(task_index_ == 0);
    RAY_CHECK(put_index_ == 0);
    SetCurrentTaskId(task_spec.TaskId());
    current_task_ = std::make_shared<const TaskSpecification>(task_spec);
  }

  void ResetCurrentTask(const TaskSpecification &task_spec) {
    SetCurrentTaskId(TaskID::Nil());
    task_index_ = 0;
    put_index_ = 0;
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

WorkerContext::WorkerContext(WorkerType worker_type, const WorkerID &worker_id,
                             const JobID &job_id)
    : worker_type_(worker_type),
      worker_id_(worker_id),
      current_job_id_(worker_type_ == WorkerType::DRIVER ? job_id : JobID::Nil()),
      current_actor_id_(ActorID::Nil()),
      main_thread_id_(boost::this_thread::get_id()) {
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

void WorkerContext::SetCurrentJobId(const JobID &job_id) { current_job_id_ = job_id; }

void WorkerContext::SetCurrentTaskId(const TaskID &task_id) {
  GetThreadContext().SetCurrentTaskId(task_id);
}

void WorkerContext::SetCurrentTask(const TaskSpecification &task_spec) {
  GetThreadContext().SetCurrentTask(task_spec);
  if (task_spec.IsNormalTask()) {
    RAY_CHECK(current_job_id_.IsNil());
    SetCurrentJobId(task_spec.JobId());
    current_task_is_direct_call_ = true;
  } else if (task_spec.IsActorCreationTask()) {
    RAY_CHECK(current_job_id_.IsNil());
    SetCurrentJobId(task_spec.JobId());
    RAY_CHECK(current_actor_id_.IsNil());
    current_actor_id_ = task_spec.ActorCreationId();
    current_actor_is_direct_call_ = true;
    current_actor_max_concurrency_ = task_spec.MaxActorConcurrency();
    current_actor_is_asyncio_ = task_spec.IsAsyncioActor();
  } else if (task_spec.IsActorTask()) {
    RAY_CHECK(current_job_id_ == task_spec.JobId());
    RAY_CHECK(current_actor_id_ == task_spec.ActorId());
  } else {
    RAY_CHECK(false);
  }
}

void WorkerContext::ResetCurrentTask(const TaskSpecification &task_spec) {
  GetThreadContext().ResetCurrentTask(task_spec);
  if (task_spec.IsNormalTask()) {
    SetCurrentJobId(JobID::Nil());
  }
}

std::shared_ptr<const TaskSpecification> WorkerContext::GetCurrentTask() const {
  return GetThreadContext().GetCurrentTask();
}

const ActorID &WorkerContext::GetCurrentActorID() const { return current_actor_id_; }

bool WorkerContext::CurrentThreadIsMain() const {
  return boost::this_thread::get_id() == main_thread_id_;
}

bool WorkerContext::ShouldReleaseResourcesOnBlockingCalls() const {
  // Check if we need to release resources when we block:
  //  - Driver doesn't acquire resources and thus doesn't need to release.
  //  - We only support lifetime resources for direct actors, which can be
  //    acquired when the actor is created, per call resources are not supported,
  //    thus we don't need to release resources for direct actor call.
  return worker_type_ != WorkerType::DRIVER && !CurrentActorIsDirectCall() &&
         CurrentThreadIsMain();
}

// TODO(edoakes): simplify these checks now that we only support direct call mode.
bool WorkerContext::CurrentActorIsDirectCall() const {
  return current_actor_is_direct_call_;
}

bool WorkerContext::CurrentTaskIsDirectCall() const {
  return current_task_is_direct_call_ || current_actor_is_direct_call_;
}

int WorkerContext::CurrentActorMaxConcurrency() const {
  return current_actor_max_concurrency_;
}

bool WorkerContext::CurrentActorIsAsync() const { return current_actor_is_asyncio_; }

WorkerThreadContext &WorkerContext::GetThreadContext() {
  if (thread_context_ == nullptr) {
    thread_context_ = std::unique_ptr<WorkerThreadContext>(new WorkerThreadContext());
  }

  return *thread_context_;
}

}  // namespace ray
