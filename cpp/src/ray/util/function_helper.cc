
#include "function_helper.h"
#include <boost/filesystem.hpp>
#include <memory>
#include "ray/util/logging.h"

namespace ray {
namespace api {

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

std::string FunctionHelper::OutputAllRemoteFunctionNames(
    const boost::dll::shared_library &lib) {
  auto get_names_func =
      boost::dll::import_alias<std::vector<std::string>()>(lib, "GetRemoteFunctionNames");
  std::string names_str;
  auto names = get_names_func();
  for (const auto &name : names) {
    names_str.append(name).append(", ");
  }
  names_str.pop_back();
  names_str.pop_back();
  return names_str;
}

std::function<msgpack::sbuffer(const std::string &, const std::vector<msgpack::sbuffer> &,
                               msgpack::sbuffer *)>
FunctionHelper::GetEntryFunction(const std::string &lib_name) {
  auto it = funcs_.find(lib_name);
  if (it != funcs_.end()) {
    return it->second;
  }

  auto lib = LoadDll(lib_name);
  if (lib == nullptr) {
    return nullptr;
  }

  try {
    auto entry_func = boost::dll::import_alias<msgpack::sbuffer(
        const std::string &, const std::vector<msgpack::sbuffer> &, msgpack::sbuffer *)>(
        *lib, "TaskExecutionHandler");
    RAY_LOG(INFO) << "The lib name: " << lib_name
                  << ", all remote functions: " << OutputAllRemoteFunctionNames(*lib);
    funcs_.emplace(lib_name, entry_func);
    return entry_func;
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

}  // namespace api
}  // namespace ray