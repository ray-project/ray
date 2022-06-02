#pragma once
#include "nlohmann/json.hpp"

using json = nlohmann::json;

class RuntimeEnv {
  public:
    json j_;
    template <typename T>
    void Set(std::string name, T typed_runtime_env) {
      json typed_runtime_env_j = typed_runtime_env;
      j_[name] = typed_runtime_env_j;
    }

    template <typename T>
    T Get(std::string name) {
      return j_[name].get<T>();
    }

    void Remove(std::string name) {
      j_.erase(name);
    }

    std::string Serialize() {
      return j_.dump();
    }

    static RuntimeEnv Deserialize(const std::string &serialized_runtime_env) {
      RuntimeEnv runtime_env;
      runtime_env.j_ = json::parse(serialized_runtime_env);
      return runtime_env;
    }
};