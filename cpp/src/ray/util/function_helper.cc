
#include "function_helper.h"
#include <boost/filesystem.hpp>
#include <boost/range/iterator_range.hpp>
#include <memory>
#include "ray/util/logging.h"

namespace ray {
namespace api {

void FunctionHelper::LoadDll(const boost::filesystem::path &lib_path) {
  RAY_LOG(INFO) << "Start load library " << lib_path;

  auto it = libraries_.find(lib_path.string());
  if (it != libraries_.end()) {
    return;
  }

  if (!boost::filesystem::exists(lib_path)) {
    RAY_LOG(WARNING) << lib_path << " dynamic library not found.";
    return;
  }

  std::shared_ptr<boost::dll::shared_library> lib = nullptr;
  try {
    lib = std::make_shared<boost::dll::shared_library>(
        lib_path.string(), boost::dll::load_mode::type::rtld_lazy);
  } catch (std::exception &e) {
    RAY_LOG(WARNING) << "Load library failed, lib_path: " << lib_path
                     << ", failed reason: " << e.what();
    return;
  } catch (...) {
    RAY_LOG(WARNING) << "Load library failed, lib_path: " << lib_path
                     << ", unknown failed reason.";
    return;
  }

  RAY_LOG(INFO) << "Loaded library: " << lib_path << " successfully.";
  RAY_CHECK(libraries_.emplace(lib_path.string(), lib).second);

  try {
    auto entry_func = boost::dll::import_alias<msgpack::sbuffer(
        const void *, const std::string &, const std::vector<msgpack::sbuffer> &,
        msgpack::sbuffer *)>(*lib, "TaskExecutionHandler");
    auto function_names = LoadAllRemoteFunctions(lib_path.string(), *lib);
    if (function_names.empty()) {
      RAY_LOG(WARNING) << "No remote functions in library " << lib_path
                       << ", maybe it's not a dynamic library of Ray application.";
      lib->unload();
      return;
    }
    RAY_LOG(INFO) << "The lib path: " << lib_path
                  << ", all remote functions: " << function_names;
    entry_funcs_.emplace(lib_path.string(), entry_func);
    return;
  } catch (std::exception &e) {
    RAY_LOG(WARNING) << "Get execute function failed, lib_path: " << lib_path
                     << ", failed reason: " << e.what();
  } catch (...) {
    RAY_LOG(WARNING) << "Get execute function failed, lib_path: " << lib_path
                     << ", unknown failed reason.";
  }
  return;
}

std::string FunctionHelper::LoadAllRemoteFunctions(
    const std::string lib_path, const boost::dll::shared_library &lib) {
  static const std::string internal_function_name = "GetRemoteFunctions";
  if (!lib.has(internal_function_name)) {
    RAY_LOG(WARNING) << "Internal function '" << internal_function_name
                     << "' not found in " << lib_path;
    return "";
  }
  auto get_remote_func = boost::dll::import_alias<
      std::pair<const RemoteFunctionMap_t &, const RemoteMemberFunctionMap_t &>()>(
      lib, internal_function_name);
  std::string names_str;
  auto function_maps = get_remote_func();
  for (const auto &pair : function_maps.first) {
    names_str.append(pair.first).append(", ");
  }
  for (const auto &pair : function_maps.second) {
    names_str.append(pair.first).append(", ");
  }
  if (!names_str.empty()) {
    names_str.pop_back();
    names_str.pop_back();
    remote_funcs_.emplace(lib_path, function_maps);
  }
  return names_str;
}

void ParseDynamicLibrary(boost::filesystem::path path,
                         std::list<boost::filesystem::path> &dynamic_libraries) {
#if defined(_WIN32)
  static const std::unordered_set<std::string> dynamic_library_extension = {".dll"};
#elif __APPLE__
  static const std::unordered_set<std::string> dynamic_library_extension = {".dylib",
                                                                            ".so"};
#else
  static const std::unordered_set<std::string> dynamic_library_extension = {".so"};
#endif
  auto absolute_path = boost::filesystem::absolute(path);
  auto extension = boost::filesystem::extension(absolute_path);
  if (dynamic_library_extension.find(extension) != dynamic_library_extension.end()) {
    RAY_LOG(INFO) << absolute_path << " dynamic library found.";
    dynamic_libraries.emplace_back(absolute_path);
  }
}

void FunctionHelper::LoadFunctionsFromPaths(const std::list<std::string> paths) {
  std::list<boost::filesystem::path> dynamic_libraries;
  for (auto path : paths) {
    if (boost::filesystem::is_directory(path)) {
      for (auto &entry :
           boost::make_iterator_range(boost::filesystem::directory_iterator(path), {})) {
        ParseDynamicLibrary(entry, dynamic_libraries);
      }
    } else if (boost::filesystem::exists(path)) {
      ParseDynamicLibrary(path, dynamic_libraries);
    } else {
      RAY_LOG(WARNING) << path << " dynamic library not found.";
    }
  }
  // // TODO(guyang.sgy): support multiple dynamic libraries.
  // // We support only one dynamic library now.
  // RAY_CHECK(dynamic_libraries.size() == 1);
  for (auto lib : dynamic_libraries) {
    LoadDll(lib);
  }
}

std::pair<EntryFuntion, const void *> FunctionHelper::GetExecutableFunctions(
    const std::string &function_name, bool is_member_function) {
  for (auto &entry : remote_funcs_) {
    if (!is_member_function) {
      auto it = entry.second.first.find(function_name);
      if (it == entry.second.first.end()) {
        continue;
      }
      const void *func_ptr = static_cast<const void *>(&it->second);
      auto entry_it = entry_funcs_.find(entry.first);
      if (entry_it == entry_funcs_.end()) {
        continue;
      }
      auto entry_function = entry_it->second;
      return std::make_pair(entry_function, func_ptr);
    } else {
      auto it = entry.second.second.find(function_name);
      if (it == entry.second.second.end()) {
        continue;
      }
      const void *func_ptr = static_cast<const void *>(&it->second);
      auto entry_it = entry_funcs_.find(entry.first);
      if (entry_it == entry_funcs_.end()) {
        continue;
      }
      auto entry_function = entry_it->second;
      return std::make_pair(entry_function, func_ptr);
    }
  }
  // return std::make_pair(nullptr, nullptr);

  throw RayException("Executable functions not found, the function name " +
                     function_name);
}

}  // namespace api
}  // namespace ray