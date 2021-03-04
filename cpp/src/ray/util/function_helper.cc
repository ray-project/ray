
#include <ray/api/util/function_helper.h>

#include <dlfcn.h>
#include <stdio.h>
#include <string.h>

#include <memory>

#include "address_helper.h"
#include "ray/core.h"

namespace ray {
namespace api {

static const uintptr_t BaseAddressForHandle(void *handle) {
  /// TODO(Guyang Song): Implement a cross-platform function.
  return (uintptr_t)((NULL == handle) ? NULL : (void *)*(size_t const *)(handle));
}

uintptr_t FunctionHelper::LoadLibrary(std::string lib_name) {
  RAY_LOG(INFO) << "Start load library " << lib_name;
  uintptr_t base_addr = 0;
  std::shared_ptr<boost::dll::shared_library> lib = nullptr;
  try {
    lib = std::make_shared<boost::dll::shared_library>(
        lib_name, boost::dll::load_mode::type::rtld_lazy);
    /// Generate base address from library.
    base_addr = BaseAddressForHandle(lib->native());
  } catch (std::exception &e) {
    RAY_LOG(WARNING) << "Load library failed, lib_name: " << lib_name
                     << ", failed reason: " << e.what();
  } catch (...) {
    RAY_LOG(WARNING) << "Load library failed, lib_name: " << lib_name
                     << ", unknown failed reason.";
  }

  RAY_CHECK(base_addr > 0);
  RAY_LOG(INFO) << "Loaded library " << lib_name << " to base address " << base_addr;
  loaded_library_.emplace(lib_name, base_addr);
  libraries_.emplace(lib_name, std::move(lib));

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