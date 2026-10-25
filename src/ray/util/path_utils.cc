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

#include <cstdint>
#include <string>

#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

#include "absl/strings/str_format.h"
#include "ray/util/path_utils.h"

int64_t GetProcessId() {
#ifdef _WIN32
  return static_cast<int64_t>(_getpid());
#else
  return static_cast<int64_t>(getpid());
#endif
}

namespace ray {

std::string GetLogFilepathFromDirectory(const std::string &log_dir,
                                        const std::string &app_name) {
  if (log_dir.empty()) {
    return "";
  }
  return JoinPaths(log_dir, absl::StrFormat("%s_%d.log", app_name, GetProcessId()));
}

std::string GetErrLogFilepathFromDirectory(const std::string &log_dir,
                                           const std::string &app_name) {
  if (log_dir.empty()) {
    return "";
  }
  return JoinPaths(log_dir, absl::StrFormat("%s_%d.err", app_name, GetProcessId()));
}
}  // namespace ray
