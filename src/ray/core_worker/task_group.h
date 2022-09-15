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

#include "absl/synchronization/mutex.h"
#include "ray/common/grpc_util.h"
#include "ray/common/id.h"
#include "ray/common/task/task.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray {
namespace core {

class TaskGroup {};

// This class is thread-safe.
class TaskGroupManager {
  // TODO: add enabled config flag
 public:
  void FillTaskGroupInfo(std::vector<TaskGroup> *);
  void AddPendingTask(const TaskSpecification &spec);

 private:
  absl::Mutex mu_;

  std::deque<TaskGroup> groups_ GUARDED_BY(mu_);

  bool has_current_task_group GUARDED_BY(mu_) = false;
};

}  // namespace core
}  // namespace ray
