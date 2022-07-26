#pragma once
#include <string>

#include "nlohmann/json.hpp"

using json = nlohmann::json;

namespace ray {

class RuntimeEnv {
 public:
  template <typename T>
  void Set(std::string name, T value) {
    json value_j = value;
    j_[name] = value_j;
  }

  void SetJsonStr(std::string name, std::string json_str);

  template <typename T>
  T Get(std::string name) {
    return j_[name].get<T>();
  }

  std::string GetJsonStr(std::string name);

  void Remove(std::string name);

  std::string Serialize();

  std::string SerializeToRuntimeEnvInfo();

  static RuntimeEnv Deserialize(const std::string &serialized_runtime_env);

 private:
  json j_;
};
}  // namespace ray