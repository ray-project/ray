// Copyright 2020-2021 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "function_helper.h"

#include <boost/filesystem.hpp>
#include <boost/range/iterator_range.hpp>
#include <memory>

#include "ray/util/logging.h"

namespace ray {
namespace internal {

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
        const std::string &, const ArgsBufferList &, msgpack::sbuffer *)>(
        *lib, "TaskExecutionHandler");
    auto function_names = LoadAllRemoteFunctions(lib_path.string(), *lib, entry_func);
    if (function_names.empty()) {
      RAY_LOG(WARNING) << "No remote functions in library " << lib_path
                       << ", maybe it's not a dynamic library of Ray application.";
      lib->unload();
      return;
    }
    RAY_LOG(INFO) << "The lib path: " << lib_path
                  << ", all remote functions: " << function_names;
    return;
  } catch (std::exception &e) {
    RAY_LOG(WARNING) << "Get execute function failed, lib_path: " << lib_path
                     << ", failed reason: " << e.what();
    lib->unload();
  } catch (...) {
    RAY_LOG(WARNING) << "Get execute function failed, lib_path: " << lib_path
                     << ", unknown failed reason.";
    lib->unload();
  }
  return;
}

std::string FunctionHelper::LoadAllRemoteFunctions(const std::string lib_path,
                                                   const boost::dll::shared_library &lib,
                                                   const EntryFuntion &entry_function) {
  static const std::string internal_function_name = "GetRemoteFunctions";
  if (!lib.has(internal_function_name)) {
    RAY_LOG(WARNING) << "Internal function '" << internal_function_name
                     << "' not found in " << lib_path;
    return "";
  }
  // Both default worker and user dynamic library static link libray_api.so which has a
  // singleton class RayRuntimeHolder, the user dynamic library will get a new un-init
  // instance of RayRuntimeHolder, so we need to init the RayRuntimeHolder singleton when
  // loading the user dynamic library to make sure the new instance valid.
  auto init_func =
      boost::dll::import_alias<void(std::shared_ptr<RayRuntime>)>(lib, "InitRayRuntime");
  init_func(RayRuntimeHolder::Instance().Runtime());

  auto get_remote_func = boost::dll::import_alias<
      std::pair<const RemoteFunctionMap_t &, const RemoteMemberFunctionMap_t &>()>(
      lib, internal_function_name);
  std::string names_str;
  auto function_maps = get_remote_func();
  for (const auto &pair : function_maps.first) {
    names_str.append(pair.first).append(", ");
    remote_funcs_.emplace(pair.first, entry_function);
  }
  for (const auto &pair : function_maps.second) {
    names_str.append(pair.first).append(", ");
    remote_member_funcs_.emplace(pair.first, entry_function);
  }
  if (!names_str.empty()) {
    names_str.pop_back();
    names_str.pop_back();
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

void FunctionHelper::LoadFunctionsFromPaths(const std::vector<std::string> &paths) {
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

const EntryFuntion &FunctionHelper::GetExecutableFunctions(
    const std::string &function_name) {
  auto it = remote_funcs_.find(function_name);
  if (it == remote_funcs_.end()) {
    throw RayFunctionNotFound("Executable function not found, the function name " +
                              function_name);
  } else {
    return it->second;
  }
}

const EntryFuntion &FunctionHelper::GetExecutableMemberFunctions(
    const std::string &function_name) {
  auto it = remote_member_funcs_.find(function_name);
  if (it == remote_member_funcs_.end()) {
    throw RayFunctionNotFound("Executable member function not found, the function name " +
                              function_name);
  } else {
    return it->second;
  }
}

}  // namespace internal
}  // namespace ray