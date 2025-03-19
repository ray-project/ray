// Copyright 2025 The Ray Authors.
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

#include "ray/common/task/task_util.h"

#include <atomic>
#include <cstdint>

#include "absl/strings/str_format.h"

namespace ray {

std::string GetTaskAttemptId(const TaskID &task_id) {
  static std::atomic<uint64_t> global_attempt_id = 0;  // Global attempt id.
  return absl::StrFormat("%s_%u", task_id.Hex(), global_attempt_id++);
}

}  // namespace ray
