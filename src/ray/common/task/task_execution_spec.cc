// Copyright 2019-2020 The Ray Authors.
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

#include "ray/common/task/task_execution_spec.h"

#include <sstream>

namespace ray {

size_t TaskExecutionSpecification::NumForwards() const {
  return message_->num_forwards();
}

void TaskExecutionSpecification::IncrementNumForwards() {
  message_->set_num_forwards(message_->num_forwards() + 1);
}

std::string TaskExecutionSpecification::DebugString() const {
  std::ostringstream stream;
  stream << "num_forwards=" << message_->num_forwards();
  return stream.str();
}

}  // namespace ray
