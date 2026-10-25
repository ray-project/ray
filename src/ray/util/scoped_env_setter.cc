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

#include "ray/util/scoped_env_setter.h"

#include <cstdlib>

#include "ray/util/env.h"

namespace ray {

ScopedEnvSetter::ScopedEnvSetter(const char *env_name, const char *value)
    : env_name_(env_name) {
  const char *val = ::getenv(env_name);
  if (val != nullptr) {
    old_value_ = val;
  }
  SetEnv(env_name, value);
}

ScopedEnvSetter::~ScopedEnvSetter() {
  UnsetEnv(env_name_.c_str());
  if (old_value_.has_value()) {
    SetEnv(env_name_, *old_value_);
  }
}

}  // namespace ray
