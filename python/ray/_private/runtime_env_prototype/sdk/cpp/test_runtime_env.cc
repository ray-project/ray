
#include "nlohmann/json.hpp"
#include "pip.h"
#include "runtime_env.h"
#include <iostream>

using json = nlohmann::json;

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

  
// std::map<std::string, int> c_map { {"one", 1}, {"two", 2}, {"three", 3} };
// json j_map(c_map);
// printf("%s\n", j_map.dump().c_str());
// // {"one": 1, "three": 3, "two": 2 }

// std::unordered_map<const char*, double> c_umap { {"one", 1.2}, {"two", 2.3}, {"three", 3.4} };
// json j_umap(c_umap);
// printf("%s\n", j_umap.dump().c_str());
// // {"one": 1.2, "two": 2.3, "three": 3.4}

// std::multimap<std::string, bool> c_mmap { {"one", true}, {"two", true}, {"three", false}, {"three", true} };
// json j_mmap(c_mmap); // only one entry for key "three" is used
// printf("%s\n", j_mmap.dump().c_str());
// // maybe {"one": true, "two": true, "three": true}

// std::unordered_multimap<std::string, bool> c_ummap { {"one", true}, {"two", true}, {"three", false}, {"three", true} };
// json j_ummap(c_ummap); // only one entry for key "three" is used
// printf("%s\n", j_ummap.dump().c_str());
// // maybe {"one": true, "two": true, "three": true}

// std::map<std::string, std::map<std::string, int>> c_nestmap { {"one", c_map}, {"two", c_map}, {"three", c_map} };
// json j_nestmap(c_nestmap);
// printf("%s\n", j_nestmap.dump().c_str());

// std::vector<int> c_vector {1, 2, 3, 4};
// std::map<std::string, std::vector<int>> c_mapvector { {"one", c_vector}, {"two", c_vector}, {"three", c_vector} };
// json j_mapvector(c_mapvector);
// printf("%s\n", j_mapvector.dump().c_str());
  return 0;
}
