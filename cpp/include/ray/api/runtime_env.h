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

#pragma once
#include <ray/api/ray_exception.h>

#include <string>

#include "nlohmann/json.hpp"

using json = nlohmann::json;

namespace ray {

/// This class provides interfaces of setting runtime environments for job/actor/task.
class RuntimeEnv {
 public:
  /// Set a runtime env field by name and Object.
  /// \param[in] name The runtime env plugin name.
  /// \param[in] value An object with primitive data type or jsonable type of
  /// nlohmann/json.
  template <typename T>
  void Set(const std::string &name, const T &value);

  /// Get the object of a runtime env field.
  /// \param[in] name The runtime env plugin name.
  template <typename T>
  T Get(const std::string &name) const;

  /// Set a runtime env field by name and json string.
  /// \param[in] name The runtime env plugin name.
  /// \param[in] json_str A json string represents the runtime env field.
  void SetJsonStr(const std::string &name, const std::string &json_str);

  /// Get the json string of a runtime env field.
  /// \param[in] name The runtime env plugin name.
  std::string GetJsonStr(const std::string &name) const;

  /// Whether a field is contained.
  /// \param[in] name The runtime env plugin name.
  bool Contains(const std::string &name) const;

  /// Remove a field by name.
  /// \param[in] name The runtime env plugin name.
  /// \return true if remove an existing field, otherwise false.
  bool Remove(const std::string &name);

  /// Whether the runtime env is empty.
  bool Empty() const;

  /// Serialize the runtime env to string.
  std::string Serialize() const;

  /// Serialize the runtime env to RuntimeEnvInfo.
  std::string SerializeToRuntimeEnvInfo() const;

  /// Deserialize the runtime env from string.
  /// \return The deserialized RuntimeEnv instance.
  static RuntimeEnv Deserialize(const std::string &serialized_runtime_env);

 private:
  json fields_;
};

// --------- inline implementation ------------

template <typename T>
inline void RuntimeEnv::Set(const std::string &name, const T &value) {
  try {
    json value_j = value;
    fields_[name] = value_j;
  } catch (std::exception &e) {
    throw ray::internal::RayRuntimeEnvException("Failed to set the field " + name + ": " +
                                                e.what());
  }
}

template <typename T>
inline T RuntimeEnv::Get(const std::string &name) const {
  if (!Contains(name)) {
    throw ray::internal::RayRuntimeEnvException("The field " + name + " not found.");
  }
  try {
    return fields_[name].get<T>();
  } catch (std::exception &e) {
    throw ray::internal::RayRuntimeEnvException("Failed to get the field " + name + ": " +
                                                e.what());
  }
}

}  // namespace ray
