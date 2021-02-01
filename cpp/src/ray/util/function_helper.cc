
#include "function_helper.h"
#include <dlfcn.h>
#include <stdio.h>
#include <string.h>
#include <memory>
#include "address_helper.h"
#include "ray/core.h"

namespace ray {
namespace api {

uintptr_t base_addr = 0;

static const uintptr_t BaseAddressForHandle(void *handle) {
  /// TODO(Guyang Song): Implement a cross-platform function.
  return (uintptr_t)((NULL == handle) ? NULL : (void *)*(size_t const *)(handle));
}

uintptr_t FunctionHelper::LoadLibrary(std::string lib_name) {
  /// Generate base address from library.
  RAY_LOG(INFO) << "Start load library " << lib_name;
  void *handle = dlopen(lib_name.c_str(), RTLD_LAZY);
  uintptr_t base_addr = BaseAddressForHandle(handle);
  RAY_CHECK(base_addr > 0);
  RAY_LOG(INFO) << "Loaded library " << lib_name << " to base address " << base_addr;
  loaded_library_.emplace(lib_name, base_addr);
  return base_addr;
}

uintptr_t FunctionHelper::GetBaseAddress(std::string lib_name) {
  auto got = loaded_library_.find(lib_name);
  if (got == loaded_library_.end()) {
    return LoadLibrary(lib_name);
  }
  return got->second;
}

}  // namespace api
}  // namespace ray