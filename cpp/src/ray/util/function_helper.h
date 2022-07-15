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

#pragma once

#include <ray/api/common_types.h>
#include <ray/api/ray_runtime_holder.h>

#include <boost/dll.hpp>
#include <memory>
#include <msgpack.hpp>
#include <string>
#include <unordered_map>

using namespace ::ray::internal;

namespace ray {
namespace internal {

using EntryFuntion = std::function<msgpack::sbuffer(
    const std::string &, const ArgsBufferList &, msgpack::sbuffer *)>;

class FunctionHelper {
 public:
  static FunctionHelper &GetInstance() {
    // We use `new` here because we don't want to destruct this instance forever.
    // If we do destruct, the shared libraries will be unloaded. And Maybe the unloading
    // will bring some errors which hard to debug.
    static auto *instance = new FunctionHelper();
    return *instance;
  }

  void LoadDll(const boost::filesystem::path &lib_path);
  void LoadFunctionsFromPaths(const std::vector<std::string> &paths);
  const EntryFuntion &GetExecutableFunctions(const std::string &function_name);
  const EntryFuntion &GetExecutableMemberFunctions(const std::string &function_name);

 private:
  FunctionHelper() = default;
  ~FunctionHelper() = default;
  FunctionHelper(FunctionHelper const &) = delete;
  FunctionHelper(FunctionHelper &&) = delete;
  std::string LoadAllRemoteFunctions(const std::string lib_path,
                                     const boost::dll::shared_library &lib,
                                     const EntryFuntion &entry_function);
  std::unordered_map<std::string, std::shared_ptr<boost::dll::shared_library>> libraries_;
  // Map from remote function name to executable entry function.
  std::unordered_map<std::string, EntryFuntion> remote_funcs_;
  // Map from remote member function name to executable entry function.
  std::unordered_map<std::string, EntryFuntion> remote_member_funcs_;
};
}  // namespace internal
}  // namespace ray