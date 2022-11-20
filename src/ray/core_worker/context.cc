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

#include <google/protobuf/util/json_util.h>

#include "ray/common/runtime_env_common.h"

namespace ray {
namespace core {

WorkerContext::WorkerContext(WorkerType worker_type,
                             const WorkerID &worker_id,
                             const JobID &job_id)
    : worker_type_(worker_type),
      worker_id_(worker_id),
      current_job_id_(job_id),
      current_actor_id_(ActorID::Nil()),
      current_actor_placement_group_id_(PlacementGroupID::Nil()),
      placement_group_capture_child_tasks_(false),
      extra_context_(std::make_unique<WorkerExtraContext>(job_id)),
      main_thread_id_(boost::this_thread::get_id()),
      mutex_() {
  // For worker main thread which initializes the WorkerContext,
  // set task_id according to whether current worker is a driver.
  // (For other threads it's set to random ID via GetThreadContext).
  extra_context_->SetCurrentTaskId((worker_type_ == WorkerType::DRIVER)
                                       ? TaskID::ForDriverTask(job_id)
                                       : TaskID::Nil(),
                                   /*attempt_number=*/0);
}

const WorkerType WorkerContext::GetWorkerType() const { return worker_type_; }

const WorkerID &WorkerContext::GetWorkerID() const { return worker_id_; }

uint64_t WorkerContext::GetNextTaskIndex() {
  absl::WriterMutexLock lock(&mutex_);
  return extra_context_->GetNextTaskIndex();
}

uint64_t WorkerContext::GetTaskIndex() {
  absl::ReaderMutexLock lock(&mutex_);
  return extra_context_->GetTaskIndex();
}

ObjectIDIndexType WorkerContext::GetNextPutIndex() {
  absl::WriterMutexLock lock(&mutex_);
  return extra_context_->GetNextPutIndex();
}

int64_t WorkerContext::GetTaskDepth() const {
  auto task_spec = GetCurrentTask();
  if (task_spec) {
    return task_spec->GetDepth();
  }
  return 0;
}

const JobID &WorkerContext::GetCurrentJobID() const { return current_job_id_; }

const TaskID &WorkerContext::GetCurrentTaskID() const {
  absl::ReaderMutexLock lock(&mutex_);
  return extra_context_->GetCurrentTaskID();
}

const TaskID &WorkerContext::GetCurrentInternalTaskId() const {
  absl::ReaderMutexLock lock(&mutex_);
  return extra_context_->GetCurrentInternalTaskId();
}

const PlacementGroupID &WorkerContext::GetCurrentPlacementGroupId() const {
  absl::ReaderMutexLock lock(&mutex_);
  // If the worker is an actor, we should return the actor's placement group id.
  if (current_actor_id_ != ActorID::Nil()) {
    return current_actor_placement_group_id_;
  } else {
    return extra_context_->GetCurrentPlacementGroupId();
  }
}

bool WorkerContext::ShouldCaptureChildTasksInPlacementGroup() const {
  absl::ReaderMutexLock lock(&mutex_);
  // If the worker is an actor, we should return the actor's placement group id.
  if (current_actor_id_ != ActorID::Nil()) {
    return placement_group_capture_child_tasks_;
  } else {
    return extra_context_->PlacementGroupCaptureChildTasks();
  }
}

const std::shared_ptr<rpc::RuntimeEnvInfo> WorkerContext::GetCurrentRuntimeEnvInfo()
    const {
  absl::ReaderMutexLock lock(&mutex_);
  return runtime_env_info_;
}

const std::string &WorkerContext::GetCurrentSerializedRuntimeEnv() const {
  absl::ReaderMutexLock lock(&mutex_);
  return runtime_env_info_->serialized_runtime_env();
}

std::shared_ptr<json> WorkerContext::GetCurrentRuntimeEnv() const {
  absl::ReaderMutexLock lock(&mutex_);
  return runtime_env_;
}

void WorkerContext::SetCurrentTaskId(const TaskID &task_id, uint64_t attempt_number) {
  absl::WriterMutexLock lock(&mutex_);
  extra_context_->SetCurrentTaskId(task_id, attempt_number);
}

void WorkerContext::SetCurrentActorId(const ActorID &actor_id) LOCKS_EXCLUDED(mutex_) {
  absl::WriterMutexLock lock(&mutex_);
  if (!current_actor_id_.IsNil()) {
    RAY_CHECK(current_actor_id_ == actor_id);
    return;
  }
  current_actor_id_ = actor_id;
}

void WorkerContext::SetCurrentTask(const TaskSpecification &task_spec) {
  absl::WriterMutexLock lock(&mutex_);
  extra_context_->SetCurrentTask(task_spec);
  RAY_CHECK(current_job_id_ == task_spec.JobId());
  if (task_spec.IsNormalTask()) {
    current_task_is_direct_call_ = true;
  } else if (task_spec.IsActorCreationTask()) {
    if (!current_actor_id_.IsNil()) {
      RAY_CHECK(current_actor_id_ == task_spec.ActorCreationId());
    }
    current_actor_id_ = task_spec.ActorCreationId();
    current_actor_is_direct_call_ = true;
    current_actor_max_concurrency_ = task_spec.MaxActorConcurrency();
    current_actor_is_asyncio_ = task_spec.IsAsyncioActor();
    is_detached_actor_ = task_spec.IsDetachedActor();
    current_actor_placement_group_id_ = task_spec.PlacementGroupBundleId().first;
    placement_group_capture_child_tasks_ = task_spec.PlacementGroupCaptureChildTasks();
  } else if (task_spec.IsActorTask()) {
    RAY_CHECK(current_actor_id_ == task_spec.ActorId());
  } else {
    RAY_CHECK(false);
  }
  if (task_spec.IsNormalTask() || task_spec.IsActorCreationTask()) {
    // TODO(architkulkarni): Once workers are cached by runtime env, we should
    // only set runtime_env_ once and then RAY_CHECK that we
    // never see a new one.
    runtime_env_info_.reset(new rpc::RuntimeEnvInfo());
    *runtime_env_info_ = task_spec.RuntimeEnvInfo();
    if (!IsRuntimeEnvEmpty(runtime_env_info_->serialized_runtime_env())) {
      runtime_env_.reset(new json());
      *runtime_env_ = json::parse(runtime_env_info_->serialized_runtime_env());
    }
  }
}

void WorkerContext::ResetCurrentTask() {
  absl::WriterMutexLock lock(&mutex_);
  extra_context_->ResetCurrentTask();
}

std::shared_ptr<const TaskSpecification> WorkerContext::GetCurrentTask() const {
  absl::ReaderMutexLock lock(&mutex_);
  return extra_context_->GetCurrentTask();
}

const ActorID &WorkerContext::GetCurrentActorID() const {
  absl::ReaderMutexLock lock(&mutex_);
  return current_actor_id_;
}

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
  absl::ReaderMutexLock lock(&mutex_);
  return current_actor_is_direct_call_;
}

bool WorkerContext::CurrentTaskIsDirectCall() const {
  absl::ReaderMutexLock lock(&mutex_);
  return current_task_is_direct_call_ || current_actor_is_direct_call_;
}

int WorkerContext::CurrentActorMaxConcurrency() const {
  absl::ReaderMutexLock lock(&mutex_);
  return current_actor_max_concurrency_;
}

bool WorkerContext::CurrentActorIsAsync() const {
  absl::ReaderMutexLock lock(&mutex_);
  return current_actor_is_asyncio_;
}

bool WorkerContext::CurrentActorDetached() const {
  absl::ReaderMutexLock lock(&mutex_);
  return is_detached_actor_;
}
}  // namespace core
}  // namespace ray
