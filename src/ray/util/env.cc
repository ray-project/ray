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

#include "ray/util/env.h"

#include <cstdlib>
#include <string>

#include "absl/strings/ascii.h"
#include "ray/util/logging.h"

namespace ray {

void SetEnv(const std::string &name, const std::string &value) {
#ifdef _WIN32
  std::string env = name + "=" + value;
  int ret = _putenv(env.c_str());
#else
  int ret = setenv(name.c_str(), value.c_str(), 1);
#endif
  RAY_CHECK_EQ(ret, 0) << "Failed to set env var " << name << " " << value;
}

void UnsetEnv(const std::string &name) {
#ifdef _WIN32
  // Use _putenv on Windows with an empty value to unset
  std::string env = name + "=";
  int ret = _putenv(env.c_str());
#else
  int ret = unsetenv(name.c_str());
#endif
  RAY_CHECK_EQ(ret, 0) << "Failed to unset env var " << name;
}

bool IsEnvTrue(const std::string &name) {
  const char *val = ::getenv(name.data());
  if (val == nullptr) {
    return false;
  }
  const std::string lower_case_val = absl::AsciiStrToLower(val);
  return lower_case_val == "true" || lower_case_val == "1";
}

}  // namespace ray
