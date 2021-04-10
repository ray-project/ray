
#include "function_helper.h"
#include <dlfcn.h>
#include <stdio.h>
#include <string.h>
#include <boost/filesystem.hpp>
#include <memory>
#include "address_helper.h"
#include "ray/core.h"

namespace ray {
namespace api {

static const uintptr_t BaseAddressForHandle(void *handle) {
  /// TODO(Guyang Song): Implement a cross-platform function.
  return (uintptr_t)((NULL == handle) ? NULL : (void *)*(size_t const *)(handle));
}

std::shared_ptr<boost::dll::shared_library> FunctionHelper::LoadDll(
    const std::string &lib_name) {
  RAY_LOG(INFO) << "Start load library " << lib_name;

  auto it = libraries_.find(lib_name);
  if (it != libraries_.end()) {
    return it->second;
  }

  auto absolute_path = boost::filesystem::absolute(lib_name);
  if (!boost::filesystem::exists(absolute_path)) {
    RAY_LOG(WARNING) << lib_name << " dll not found, absolute_path: " << absolute_path
                     << ", current path" << boost::filesystem::current_path();
    return nullptr;
  }

  std::shared_ptr<boost::dll::shared_library> lib = nullptr;
  try {
    lib = std::make_shared<boost::dll::shared_library>(
        lib_name, boost::dll::load_mode::type::rtld_lazy);
  } catch (std::exception &e) {
    RAY_LOG(WARNING) << "Load library failed, lib_name: " << lib_name
                     << ", failed reason: " << e.what();
  } catch (...) {
    RAY_LOG(WARNING) << "Load library failed, lib_name: " << lib_name
                     << ", unknown failed reason.";
  }

  if (lib == nullptr) {
    return nullptr;
  }

  RAY_LOG(INFO) << "Loaded library: " << lib_name << " successfully.";
  RAY_CHECK(libraries_.emplace(lib_name, lib).second);
  return lib;
}

std::function<msgpack::sbuffer(const std::string &,
                               const std::vector<std::shared_ptr<::ray::RayObject>> &)>
FunctionHelper::GetExecuteFunction(const std::string &lib_name) {
  auto it = funcs_.find(lib_name);
  if (it != funcs_.end()) {
    return it->second;
  }

  auto lib = LoadDll(lib_name);
  if (lib == nullptr) {
    return nullptr;
  }

  try {
    auto execute_func = boost::dll::import_alias<msgpack::sbuffer(
        const std::string &, const std::vector<std::shared_ptr<::ray::RayObject>> &)>(
        *lib, "TaskExecutionHandler");
    funcs_.emplace(lib_name, execute_func);
    return execute_func;
  } catch (std::exception &e) {
    RAY_LOG(WARNING) << "Get execute function failed, lib_name: " << lib_name
                     << ", failed reason: " << e.what();
  } catch (...) {
    RAY_LOG(WARNING) << "Get execute function, lib_name: " << lib_name
                     << ", unknown failed reason.";
  }

  RAY_LOG(WARNING) << "Can't get execute function, lib_name: " << lib_name;
  return nullptr;
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