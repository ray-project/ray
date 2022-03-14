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
#include <string>

namespace ray {

// Return whether a string representation of a runtime env represents an empty
// runtime env.  It could either be "" (from the default string value in protobuf),
// or "{}" (from serializing an empty Python dict or a JSON file.)
bool IsRuntimeEnvEmpty(const std::string &serialized_runtime_env);

// Return whether a string representation of a runtime env info represents an empty
// runtime env info.  It could either be "" (from the default string value in protobuf),
// or "{}" (from serializing an empty Python dict or a JSON file.)
bool IsRuntimeEnvInfoEmpty(const std::string &serialized_runtime_env_info);

}  // namespace ray
