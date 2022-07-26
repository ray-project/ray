#include <ray/api/runtime_env.h>

namespace ray {

void RuntimeEnv::SetJsonStr(std::string name, std::string json_str) {
  json value_j = json::parse(json_str);
  j_[name] = value_j;
}

std::string RuntimeEnv::GetJsonStr(std::string name) {
  auto j = j_[name].get<json>();
  return j.dump();
}

void RuntimeEnv::Remove(std::string name) { j_.erase(name); }

std::string RuntimeEnv::Serialize() { return j_.dump(); }

RuntimeEnv RuntimeEnv::Deserialize(const std::string &serialized_runtime_env) {
  RuntimeEnv runtime_env;
  runtime_env.j_ = json::parse(serialized_runtime_env);
  return runtime_env;
}

}  // namespace ray