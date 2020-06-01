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

std::string WorkerTypeString(WorkerType type) {
  if (type == WorkerType::DRIVER) {
    return "driver";
  } else if (type == WorkerType::WORKER) {
    return "worker";
  }
  RAY_CHECK(false);
  return "";
}

std::string LanguageString(Language language) {
  if (language == Language::PYTHON) {
    return "python";
  } else if (language == Language::JAVA) {
    return "java";
  }
  RAY_CHECK(false);
  return "";
}

}  // namespace ray
