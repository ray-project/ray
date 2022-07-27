// Copyright 2022 The Ray Authors.
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

#include <google/protobuf/util/json_util.h>
#include <ray/api/runtime_env.h>
#include <ray/util/logging.h>

#include "src/ray/protobuf/runtime_env_common.pb.h"

namespace ray {

void RuntimeEnv::SetJsonStr(const std::string &name, const std::string &json_str) {
  try {
    json value_j = json::parse(json_str);
    fields_[name] = value_j;
  } catch (std::exception &e) {
    throw ray::internal::RayRuntimeEnvException("Failed to set the field " + name +
                                                " by json string: " + e.what());
  }
}

std::string RuntimeEnv::GetJsonStr(const std::string &name) const {
  if (!Contains(name)) {
    throw ray::internal::RayRuntimeEnvException("The field " + name + " not found.");
  }
  auto j = fields_[name].get<json>();
  return j.dump();
}

bool RuntimeEnv::Contains(const std::string &name) const {
  return fields_.contains(name);
}

bool RuntimeEnv::Remove(const std::string &name) {
  if (Contains(name)) {
    fields_.erase(name);
    return true;
  }
  return false;
}

bool RuntimeEnv::Empty() const { return fields_.empty(); }

std::string RuntimeEnv::Serialize() const { return fields_.dump(); }

std::string RuntimeEnv::SerializeToRuntimeEnvInfo() const {
  rpc::RuntimeEnvInfo runtime_env_info;
  runtime_env_info.set_serialized_runtime_env(Serialize());
  std::string serialized_runtime_env_info;
  RAY_CHECK(google::protobuf::util::MessageToJsonString(runtime_env_info,
                                                        &serialized_runtime_env_info)
                .ok());
  return serialized_runtime_env_info;
}

RuntimeEnv RuntimeEnv::Deserialize(const std::string &serialized_runtime_env) {
  RuntimeEnv runtime_env;
  try {
    runtime_env.fields_ = json::parse(serialized_runtime_env);
  } catch (std::exception &e) {
    throw ray::internal::RayRuntimeEnvException("Failed to deserialize runtime env " +
                                                serialized_runtime_env + ": " + e.what());
  }
  return runtime_env;
}

}  // namespace ray
