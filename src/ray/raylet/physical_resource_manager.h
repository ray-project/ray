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

#include <filesystem>
#include <optional>
#include <string>
#include <utility>

#include "ray/util/logging.h"

namespace ray {

namespace raylet {

class PhysicalResourceManager {
 public:
  bool HasResourceCapacityForTask(const TaskSpecification &task_spec) const;

  std::optional<std::filesystem::space_info> FileSystemSpace() const;

 private:
  bool OverFileSystemCapacity() const;

 private:
  const std::string path_;
  const double available_threshold_;
};

}  // namespace raylet
}  // namespace ray
