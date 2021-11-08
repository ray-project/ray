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

#include <map>
#include <utility>

#include "absl/types/optional.h"
#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"

namespace ray {
namespace core {

/**
 * IActorSubmitQueue is responsible for queuing per actor requests on the client.
 * TODO(scv119): add more details.
 */
class IActorSubmitQueue {
 public:
  virtual ~IActorSubmitQueue() = default;
  virtual bool Emplace(uint64_t sequence_no, TaskSpecification spec) = 0;
  virtual bool Contains(uint64_t sequence_no) const = 0;
  virtual const std::pair<TaskSpecification, bool> &Get(uint64_t sequence_no) const = 0;
  virtual void MarkDependencyFailed(uint64_t sequence_no) = 0;
  virtual void MarkDependencyResolved(uint64_t sequence_no) = 0;
  virtual std::vector<TaskID> ClearAllTasks() = 0;
  virtual absl::optional<std::pair<TaskSpecification, bool>> PopNextTaskToSend() = 0;
  virtual std::map<uint64_t, TaskSpecification> PopAllOutOfOrderCompletedTasks() = 0;
  virtual void OnClientConnected() = 0;
  virtual uint64_t GetSequenceNumber(const TaskSpecification &task_spec) const = 0;
  virtual void MarkTaskCompleted(uint64_t sequence_no, TaskSpecification task_spec) = 0;
};
}  // namespace core
}  // namespace ray