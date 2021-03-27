#pragma once
#include <string>
#include <unordered_map>

namespace ray {
namespace api {

class FunctionHelper {
 public:
  uintptr_t GetBaseAddress(std::string lib_name);

  static FunctionHelper &GetInstance() {
    static FunctionHelper functionHelper;
    return functionHelper;
  }

 private:
  std::unordered_map<std::string, uintptr_t> loaded_library_;
  uintptr_t LoadLibrary(std::string lib_name);
};
}  // namespace api
}  // namespace ray