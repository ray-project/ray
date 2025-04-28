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

// Create a scoped environment variable, which is set env at construction and unset and
// recover to old value at destruction.

#pragma once

#include <cstring>
#include <optional>
#include <string>

namespace ray {

class ScopedEnvSetter {
 public:
  ScopedEnvSetter(const char *env_name, const char *value);
  ~ScopedEnvSetter();

  ScopedEnvSetter(const ScopedEnvSetter &) = delete;
  ScopedEnvSetter &operator=(const ScopedEnvSetter &) = delete;

 private:
  std::string env_name_;
  std::optional<std::string> old_value_;
};

}  // namespace ray
