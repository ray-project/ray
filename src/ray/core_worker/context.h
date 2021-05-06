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

#include "ray/common/task/task_spec.h"
#include "ray/core_worker/common.h"

namespace ray {

struct WorkerThreadContext;

class WorkerContext {
 public:
  WorkerContext(WorkerType worker_type, const WorkerID &worker_id, const JobID &job_id);

  const WorkerType GetWorkerType() const;

  const WorkerID &GetWorkerID() const;

  const JobID &GetCurrentJobID() const;

  const TaskID &GetCurrentTaskID() const;

  const PlacementGroupID &GetCurrentPlacementGroupId() const;

  bool ShouldCaptureChildTasksInPlacementGroup() const;

  const std::string &GetCurrentSerializedRuntimeEnv() const;

  const std::unordered_map<std::string, std::string>
      &GetCurrentOverrideEnvironmentVariables() const;

  // TODO(edoakes): remove this once Python core worker uses the task interfaces.
  void SetCurrentTaskId(const TaskID &task_id);

  void SetCurrentTask(const TaskSpecification &task_spec);

  void ResetCurrentTask();

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

  bool CurrentActorDetached() const;

  uint64_t GetNextTaskIndex();

  // Returns the next put object index; used to calculate ObjectIDs for puts.
  ObjectIDIndexType GetNextPutIndex();

 protected:
  // allow unit test to set.
  bool current_actor_is_direct_call_ = false;
  bool current_task_is_direct_call_ = false;

 private:
  const WorkerType worker_type_;
  const WorkerID worker_id_;
  JobID current_job_id_;
  ActorID current_actor_id_;
  int current_actor_max_concurrency_ = 1;
  bool current_actor_is_asyncio_ = false;
  bool is_detached_actor_ = false;
  // The placement group id that the current actor belongs to.
  PlacementGroupID current_actor_placement_group_id_;
  // Whether or not we should implicitly capture parent's placement group.
  bool placement_group_capture_child_tasks_;
  // The runtime env for the current actor or task.
  std::string serialized_runtime_env_;
  // The environment variable overrides for the current actor or task.
  std::unordered_map<std::string, std::string> override_environment_variables_;
  /// The id of the (main) thread that constructed this worker context.
  boost::thread::id main_thread_id_;

 private:
  static WorkerThreadContext &GetThreadContext();

  /// Per-thread worker context.
  static thread_local std::unique_ptr<WorkerThreadContext> thread_context_;
};

}  // namespace ray
