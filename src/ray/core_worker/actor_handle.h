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

#include <gtest/gtest_prod.h>

#include "ray/common/id.h"
#include "ray/common/task/task_util.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/context.h"
#include "ray/gcs/redis_gcs_client.h"
#include "src/ray/protobuf/core_worker.pb.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {

class ActorHandle {
 public:
  ActorHandle(ray::rpc::ActorHandle inner)
      : inner_(inner), actor_cursor_(ObjectID::FromBinary(inner_.actor_cursor())) {}

  // Constructs a new ActorHandle as part of the actor creation process.
  ActorHandle(const ActorID &actor_id, const TaskID &owner_id,
              const rpc::Address &owner_address, const JobID &job_id,
              const ObjectID &initial_cursor, const Language actor_language,
              const ray::FunctionDescriptor &actor_creation_task_function_descriptor,
              const std::string &extension_data, int64_t max_task_retries);

  /// Constructs an ActorHandle from a serialized string.
  ActorHandle(const std::string &serialized);

  /// Constructs an ActorHandle from a gcs::ActorTableData message.
  ActorHandle(const gcs::ActorTableData &actor_table_data);

  ActorID GetActorID() const { return ActorID::FromBinary(inner_.actor_id()); };

  TaskID GetOwnerId() const { return TaskID::FromBinary(inner_.owner_id()); }

  rpc::Address GetOwnerAddress() const { return inner_.owner_address(); }

  /// ID of the job that created the actor (it is possible that the handle
  /// exists on a job with a different job ID).
  JobID CreationJobID() const { return JobID::FromBinary(inner_.creation_job_id()); };

  Language ActorLanguage() const { return inner_.actor_language(); };

  ray::FunctionDescriptor ActorCreationTaskFunctionDescriptor() const {
    return ray::FunctionDescriptorBuilder::FromProto(
        inner_.actor_creation_task_function_descriptor());
  };

  std::string ExtensionData() const { return inner_.extension_data(); }

  /// Set the actor task spec fields.
  ///
  /// \param[in] builder Task spec builder.
  /// \param[in] new_cursor Actor dummy object. This is legacy code needed for
  /// raylet-based actor restart.
  void SetActorTaskSpec(TaskSpecBuilder &builder, const ObjectID new_cursor);

  /// Reset the actor task spec fields of an existing task so that the task can
  /// be re-executed.
  ///
  /// \param[in] spec An existing task spec that has executed on the actor
  /// before.
  /// \param[in] new_cursor Actor dummy object. This is legacy code needed for
  /// raylet-based actor restart.
  void SetResubmittedActorTaskSpec(TaskSpecification &spec, const ObjectID new_cursor);

  void Serialize(std::string *output);

  int64_t MaxTaskRetries() const { return inner_.max_task_retries(); }

 private:
  // Protobuf-defined persistent state of the actor handle.
  const ray::rpc::ActorHandle inner_;

  /// The unique id of the dummy object returned by the previous task.
  /// TODO: This can be removed once we schedule actor tasks by task counter
  /// only.
  // TODO: Save this state in the core worker.
  ObjectID actor_cursor_ GUARDED_BY(mutex_);
  // Number of tasks that have been submitted on this handle.
  uint64_t task_counter_ GUARDED_BY(mutex_) = 0;

  /// Mutex to protect fields in the actor handle.
  mutable absl::Mutex mutex_;

  FRIEND_TEST(ZeroNodeTest, TestActorHandle);
};

}  // namespace ray
