#pragma once
#include <string>
#include <unordered_map>

namespace ray {
namespace api {

class FunctionHelper {
 public:
  uintptr_t GetBaseAddress(std::string lib_name);

  static std::shared_ptr<FunctionHelper> GetInstance();

 private:
  std::unordered_map<std::string, uintptr_t> loaded_library_;
  static std::shared_ptr<FunctionHelper> function_helper_;
  uintptr_t LoadLibrary(std::string lib_name);

};
}  // namespace api
}  // namespace ray