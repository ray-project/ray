#pragma once

#include <boost/dll.hpp>
#include <memory>
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

  std::shared_ptr<boost::dll::shared_library> LoadDll(const std::string &lib_name);

 private:
  FunctionHelper() = default;
  ~FunctionHelper() = default;
  FunctionHelper(FunctionHelper const &) = delete;
  FunctionHelper(FunctionHelper &&) = delete;

  uintptr_t LoadLibrary(std::string lib_name);

  std::unordered_map<std::string, uintptr_t> loaded_library_;
  std::unordered_map<std::string, std::shared_ptr<boost::dll::shared_library>> libraries_;
};
}  // namespace api
}  // namespace ray