#include <iostream>
#include "pip.h"
#include "runtime_env.h"


int main(int argc, char **argv) {
  RuntimeEnv runtime_env;
  // Set pip
  std::vector<std::string> packages = {"requests"};
  Pip pip(packages, true);
  runtime_env.Set("pip", pip);
  // Set working_dir
  std::string working_dir = "https://path/to/working_dir.zip";
  runtime_env.Set("working_dir", working_dir);

  // Serialize
  auto serialized_runtime_env = runtime_env.Serialize();
  std::cout << "serialized_runtime_env " << serialized_runtime_env << std::endl;

  // Deserialize
  auto runtime_env_2 = RuntimeEnv::Deserialize(serialized_runtime_env);

  auto pip2 = runtime_env_2.Get<Pip>("pip");
  assert(pip2.packages == pip.packages);
  assert(pip2.pip_check == pip.pip_check);

  auto working_dir2 = runtime_env_2.Get<std::string>("working_dir");
  assert(working_dir2 == working_dir);

  std::cout << "Finished!" << std::endl;;

  return 0;
}
