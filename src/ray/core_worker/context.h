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

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "nlohmann/json.hpp"
#include "ray/common/task/task_spec.h"
#include "ray/core_worker/common.h"
using json = nlohmann::json;
namespace ray {
namespace core {

struct WorkerThreadContext;

class WorkerContext {
 public:
  WorkerContext(WorkerType worker_type, const WorkerID &worker_id, const JobID &job_id);

  const WorkerType GetWorkerType() const;

  const WorkerID &GetWorkerID() const;

  JobID GetCurrentJobID() const LOCKS_EXCLUDED(mutex_);
  rpc::JobConfig GetCurrentJobConfig() const LOCKS_EXCLUDED(mutex_);

  const TaskID &GetCurrentTaskID() const;

  const PlacementGroupID &GetCurrentPlacementGroupId() const LOCKS_EXCLUDED(mutex_);

  bool ShouldCaptureChildTasksInPlacementGroup() const LOCKS_EXCLUDED(mutex_);

  const std::shared_ptr<rpc::RuntimeEnvInfo> GetCurrentRuntimeEnvInfo() const
      LOCKS_EXCLUDED(mutex_);

  const std::string &GetCurrentSerializedRuntimeEnv() const LOCKS_EXCLUDED(mutex_);

  std::shared_ptr<json> GetCurrentRuntimeEnv() const LOCKS_EXCLUDED(mutex_);

  // Initialize worker's job_id and job_config if they haven't already.
  // Note a worker's job config can't be changed after initialization.
  void MaybeInitializeJobInfo(const JobID &job_id, const rpc::JobConfig &job_config)
      LOCKS_EXCLUDED(mutex_);

  // TODO(edoakes): remove this once Python core worker uses the task interfaces.
  void SetCurrentTaskId(const TaskID &task_id, uint64_t attempt_number);

  const TaskID &GetCurrentInternalTaskId() const;

  void SetCurrentActorId(const ActorID &actor_id) LOCKS_EXCLUDED(mutex_);

  void SetTaskDepth(int64_t depth) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void SetCurrentTask(const TaskSpecification &task_spec) LOCKS_EXCLUDED(mutex_);

  void ResetCurrentTask();

  std::shared_ptr<const TaskSpecification> GetCurrentTask() const;

  const ActorID &GetCurrentActorID() const LOCKS_EXCLUDED(mutex_);

  /// Returns whether the current thread is the main worker thread.
  bool CurrentThreadIsMain() const;

  /// Returns whether we should Block/Unblock through the raylet on Get/Wait.
  /// This only applies to direct task calls.
  bool ShouldReleaseResourcesOnBlockingCalls() const;

  /// Returns whether we are in a direct call actor.
  bool CurrentActorIsDirectCall() const LOCKS_EXCLUDED(mutex_);

  /// Returns whether we are in a direct call task. This encompasses both direct
  /// actor and normal tasks.
  bool CurrentTaskIsDirectCall() const LOCKS_EXCLUDED(mutex_);

  int CurrentActorMaxConcurrency() const LOCKS_EXCLUDED(mutex_);

  bool CurrentActorIsAsync() const LOCKS_EXCLUDED(mutex_);

  bool CurrentActorDetached() const LOCKS_EXCLUDED(mutex_);

  uint64_t GetNextTaskIndex();

  uint64_t GetTaskIndex();

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

  // a worker's job infomation might be lazily initialized.
  JobID current_job_id_ GUARDED_BY(mutex_);
  std::optional<rpc::JobConfig> job_config_ GUARDED_BY(mutex_);

  int64_t task_depth_ GUARDED_BY(mutex_) = 0;
  ActorID current_actor_id_ GUARDED_BY(mutex_);
  int current_actor_max_concurrency_ GUARDED_BY(mutex_) = 1;
  bool current_actor_is_asyncio_ GUARDED_BY(mutex_) = false;
  bool is_detached_actor_ GUARDED_BY(mutex_) = false;
  // The placement group id that the current actor belongs to.
  PlacementGroupID current_actor_placement_group_id_ GUARDED_BY(mutex_);
  // Whether or not we should implicitly capture parent's placement group.
  bool placement_group_capture_child_tasks_ GUARDED_BY(mutex_);
  // The runtime env for the current actor or task.
  std::shared_ptr<json> runtime_env_ GUARDED_BY(mutex_);
  // The runtime env info.
  std::shared_ptr<rpc::RuntimeEnvInfo> runtime_env_info_ GUARDED_BY(mutex_);
  /// The id of the (main) thread that constructed this worker context.
  const boost::thread::id main_thread_id_;
  // To protect access to mutable members;
  mutable absl::Mutex mutex_;

 private:
  WorkerThreadContext &GetThreadContext() const;

  /// Per-thread worker context.
  static thread_local std::unique_ptr<WorkerThreadContext> thread_context_;
};

}  // namespace core
}  // namespace ray
