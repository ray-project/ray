
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

  RAY_CHECK(boost::filesystem::exists(lib_path))
      << lib_path << " dynamic library not found.";

  std::shared_ptr<boost::dll::shared_library> lib = nullptr;
  try {
    lib = std::make_shared<boost::dll::shared_library>(
        lib_path.string(), boost::dll::load_mode::type::rtld_lazy);
  } catch (std::exception &e) {
    RAY_LOG(FATAL) << "Load library failed, lib_path: " << lib_path
                   << ", failed reason: " << e.what();
    return;
  } catch (...) {
    RAY_LOG(FATAL) << "Load library failed, lib_path: " << lib_path
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

void FindDynamicLibrary(boost::filesystem::path path,
                        std::list<boost::filesystem::path> &dynamic_libraries) {
#if defined(_WIN32)
  static const std::unordered_set<std::string> dynamic_library_extension = {".dll"};
#elif __APPLE__
  static const std::unordered_set<std::string> dynamic_library_extension = {".dylib",
                                                                            ".so"};
#else
  static const std::unordered_set<std::string> dynamic_library_extension = {".so"};
#endif
  auto extension = boost::filesystem::extension(path);
  if (dynamic_library_extension.find(extension) != dynamic_library_extension.end()) {
    RAY_LOG(INFO) << path << " dynamic library found.";
    dynamic_libraries.emplace_back(path);
  }
}

void FunctionHelper::LoadFunctionsFromPaths(const std::vector<std::string> paths) {
  std::list<boost::filesystem::path> dynamic_libraries;
  // Lookup dynamic libraries from paths.
  for (auto path : paths) {
    if (boost::filesystem::is_directory(path)) {
      for (auto &entry :
           boost::make_iterator_range(boost::filesystem::directory_iterator(path), {})) {
        FindDynamicLibrary(entry, dynamic_libraries);
      }
    } else if (boost::filesystem::exists(path)) {
      FindDynamicLibrary(path, dynamic_libraries);
    } else {
      RAY_LOG(FATAL) << path << " dynamic library not found.";
    }
  }

  // Try to load all found libraries.
  for (auto lib : dynamic_libraries) {
    LoadDll(lib);
  }
}

std::pair<EntryFuntion, const RemoteFunction *> FunctionHelper::GetExecutableFunctions(
    const std::string &function_name) {
  for (auto &entry : remote_funcs_) {
    auto pair = LookupExecutableFunctions<RemoteFunction>(function_name, entry.first,
                                                          entry.second.first);
    if (!pair.first) {
      continue;
    } else {
      return pair;
    }
  }

  throw RayException("Executable functions not found, the function name " +
                     function_name);
}

// Return a pair which contains a executable entry function and a remote member function
// pointer.
std::pair<EntryFuntion, const RemoteMemberFunction *>
FunctionHelper::GetExecutableMemberFunctions(const std::string &function_name) {
  for (auto &entry : remote_funcs_) {
    auto pair = LookupExecutableFunctions<RemoteMemberFunction>(
        function_name, entry.first, entry.second.second);
    if (!pair.first) {
      continue;
    } else {
      return pair;
    }
  }

  throw RayException("Executable member functions not found, the function name " +
                     function_name);
}

}  // namespace api
}  // namespace ray