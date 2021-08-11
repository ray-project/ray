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

#include "ray/core_worker/common.h"

namespace ray {
namespace core {

std::string WorkerTypeString(WorkerType type) {
  // TODO(suquark): Use proto3 utils to get the string.
  if (type == WorkerType::DRIVER) {
    return "driver";
  } else if (type == WorkerType::WORKER) {
    return "worker";
  } else if (type == WorkerType::SPILL_WORKER) {
    return "spill_worker";
  } else if (type == WorkerType::RESTORE_WORKER) {
    return "restore_worker";
  } else if (type == WorkerType::UTIL_WORKER) {
    return "util_worker";
  }
  RAY_CHECK(false);
  return "";
}

std::string LanguageString(Language language) {
  if (language == Language::PYTHON) {
    return "python";
  } else if (language == Language::JAVA) {
    return "java";
  } else if (language == Language::CPP) {
    return "cpp";
  }
  RAY_CHECK(false);
  return "";
}

std::string GenerateCachedActorName(const std::string &ns,
                                    const std::string &actor_name) {
  return ns + "-" + actor_name;
}

}  // namespace core
}  // namespace ray
