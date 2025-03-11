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

#pragma once

#include <boost/thread.hpp>
#include <memory>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "nlohmann/json.hpp"
#include "ray/common/task/task_spec.h"
#include "ray/core_worker/common.h"

namespace ray::core {

struct WorkerThreadContext;

class WorkerContext {
 public:
  WorkerContext(WorkerType worker_type, const WorkerID &worker_id, const JobID &job_id);

  // Return the generator return ID.
  ///
  /// By default, it deduces a generator return ID from a current task
  /// from the context. However, it also supports manual specification of
  /// put index and task id to support `AllocateDynamicReturnId`.
  /// See the docstring of AllocateDynamicReturnId for more details.
  ///
  /// The caller should either not specify both task_id AND put_index
  /// or specify both at the same time. Otherwise it will panic.
  ///
  /// \param[in] task_id The task id of the dynamically generated return ID.
  /// If Nil() is specified, it will deduce the Task ID from the current
  /// worker context.
  /// \param[in] put_index The equivalent of the return value of
  /// WorkerContext::GetNextPutIndex.
  /// If std::nullopt is specified, it will deduce the put index from the
  /// current worker context.
  ObjectID GetGeneratorReturnId(const TaskID &task_id,
                                std::optional<ObjectIDIndexType> put_index);

  WorkerType GetWorkerType() const;

  const WorkerID &GetWorkerID() const;

  JobID GetCurrentJobID() const ABSL_LOCKS_EXCLUDED(mutex_);
  rpc::JobConfig GetCurrentJobConfig() const ABSL_LOCKS_EXCLUDED(mutex_);

  const TaskID &GetCurrentTaskID() const;

  TaskID GetMainThreadOrActorCreationTaskID() const;

  const PlacementGroupID &GetCurrentPlacementGroupId() const ABSL_LOCKS_EXCLUDED(mutex_);

  bool ShouldCaptureChildTasksInPlacementGroup() const ABSL_LOCKS_EXCLUDED(mutex_);

  std::shared_ptr<rpc::RuntimeEnvInfo> GetCurrentRuntimeEnvInfo() const
      ABSL_LOCKS_EXCLUDED(mutex_);

  const std::string &GetCurrentSerializedRuntimeEnv() const ABSL_LOCKS_EXCLUDED(mutex_);

  std::shared_ptr<nlohmann::json> GetCurrentRuntimeEnv() const
      ABSL_LOCKS_EXCLUDED(mutex_);

  // Initialize worker's job_id and job_config if they haven't already.
  // Note a worker's job config can't be changed after initialization.
  void MaybeInitializeJobInfo(const JobID &job_id, const rpc::JobConfig &job_config)
      ABSL_LOCKS_EXCLUDED(mutex_);

  // TODO(edoakes): remove this once Python core worker uses the task interfaces.
  void SetCurrentTaskId(const TaskID &task_id, uint64_t attempt_number);

  const TaskID &GetCurrentInternalTaskId() const;

  void SetCurrentActorId(const ActorID &actor_id) ABSL_LOCKS_EXCLUDED(mutex_);

  void SetTaskDepth(int64_t depth) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void SetCurrentTask(const TaskSpecification &task_spec) ABSL_LOCKS_EXCLUDED(mutex_);

  void ResetCurrentTask();

  /// NOTE: This method can't be used in fiber/async actor context.
  std::shared_ptr<const TaskSpecification> GetCurrentTask() const;

  const ActorID &GetCurrentActorID() const ABSL_LOCKS_EXCLUDED(mutex_);

  const ActorID &GetRootDetachedActorID() const ABSL_LOCKS_EXCLUDED(mutex_);

  /// Returns whether the current thread is the main worker thread.
  bool CurrentThreadIsMain() const;

  /// Returns whether we should Block/Unblock through the raylet on Get/Wait.
  /// This only applies to direct task calls.
  bool ShouldReleaseResourcesOnBlockingCalls() const;

  /// Returns whether we are in a direct call actor.
  bool CurrentActorIsDirectCall() const ABSL_LOCKS_EXCLUDED(mutex_);

  /// Returns whether we are in a direct call task. This encompasses both direct
  /// actor and normal tasks.
  bool CurrentTaskIsDirectCall() const ABSL_LOCKS_EXCLUDED(mutex_);

  int CurrentActorMaxConcurrency() const ABSL_LOCKS_EXCLUDED(mutex_);

  bool CurrentActorIsAsync() const ABSL_LOCKS_EXCLUDED(mutex_);

  /// Set a flag to indicate that the current actor should exit, it'll be checked
  /// periodically and the actor will exit if the flag is set.
  void SetCurrentActorShouldExit() ABSL_LOCKS_EXCLUDED(mutex_);

  /// Get the flag to indicate that the current actor should exit.
  bool GetCurrentActorShouldExit() const ABSL_LOCKS_EXCLUDED(mutex_);

  bool CurrentActorDetached() const ABSL_LOCKS_EXCLUDED(mutex_);

  uint64_t GetNextTaskIndex();

  uint64_t GetTaskIndex() const;

  // Returns the next put object index; used to calculate ObjectIDs for puts.
  ObjectIDIndexType GetNextPutIndex();

  int64_t GetTaskDepth() const;

 protected:
  // allow unit test to set.
  bool current_actor_is_direct_call_ = false;
  bool current_task_is_direct_call_ = false;

 private:
  const WorkerType worker_type_;
  const WorkerID worker_id_;

  // a worker's job information might be lazily initialized.
  JobID current_job_id_ ABSL_GUARDED_BY(mutex_);
  std::optional<rpc::JobConfig> job_config_ ABSL_GUARDED_BY(mutex_);

  int64_t task_depth_ ABSL_GUARDED_BY(mutex_) = 0;
  ActorID current_actor_id_ ABSL_GUARDED_BY(mutex_);
  int current_actor_max_concurrency_ ABSL_GUARDED_BY(mutex_) = 1;
  bool current_actor_is_asyncio_ ABSL_GUARDED_BY(mutex_) = false;
  bool current_actor_should_exit_ ABSL_GUARDED_BY(mutex_) = false;
  bool is_detached_actor_ ABSL_GUARDED_BY(mutex_) = false;
  // The placement group id that the current actor belongs to.
  PlacementGroupID current_actor_placement_group_id_ ABSL_GUARDED_BY(mutex_);
  // Whether or not we should implicitly capture parent's placement group.
  bool placement_group_capture_child_tasks_ ABSL_GUARDED_BY(mutex_);
  // The runtime env for the current actor or task.
  // For one worker context, it should have exactly one serialized runtime env; cache the
  // parsed json and string for reuse.
  std::string serialized_runtime_env_ ABSL_GUARDED_BY(mutex_);
  std::shared_ptr<nlohmann::json> runtime_env_ ABSL_GUARDED_BY(mutex_);
  // The runtime env info.
  // For one worker context, it should be assigned only once because Ray currently doesn't
  // reuse worker to run tasks or actors with different runtime envs.
  std::shared_ptr<rpc::RuntimeEnvInfo> runtime_env_info_ ABSL_GUARDED_BY(mutex_);
  /// The id of the (main) thread that constructed this worker context.
  const boost::thread::id main_thread_id_;
  /// The currently executing main thread's task id. It's the actor creation task id
  /// for concurrent actor, or the main thread's task id for other cases.
  /// Used merely for observability purposes to track task hierarchy.
  TaskID main_thread_or_actor_creation_task_id_ ABSL_GUARDED_BY(mutex_);
  /// If the current task or actor is originated from a detached actor,
  /// this contains that actor's id otherwise it's nil.
  ActorID root_detached_actor_id_ ABSL_GUARDED_BY(mutex_);
  // To protect access to mutable members;
  mutable absl::Mutex mutex_;

 private:
  /// NOTE: This method can't be used in fiber/async actor context.
  WorkerThreadContext &GetThreadContext() const;

  /// Per-thread worker context.
  static thread_local std::unique_ptr<WorkerThreadContext> thread_context_;
};

}  // namespace ray::core
