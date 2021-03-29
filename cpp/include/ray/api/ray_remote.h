// Copyright 2017 The Ray Authors.
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

#include <ray/api/function_manager.h>
#include <ray/api/remote_function_name.h>

namespace ray {

namespace internal {
template <typename T, typename... U>
inline static int RegisterRemoteFunctions(const T &t, U... u) {
  int index = 0;
  (void)std::initializer_list<int>{
      (ray::internal::FunctionManager::Instance().RegisterRemoteFunction(
           t[index++].data(), u),
       0)...};
  return 0;
}

#define CONCATENATE_DIRECT(s1, s2) s1##s2
#define CONCATENATE(s1, s2) CONCATENATE_DIRECT(s1, s2)
#ifdef _MSC_VER
#define ANONYMOUS_VARIABLE(str) CONCATENATE(str, __COUNTER__)
#else
#define ANONYMOUS_VARIABLE(str) CONCATENATE(str, __LINE__)
#endif
}  // namespace internal

namespace api {

#define RAY_REMOTE(...)                                                                  \
  static auto ANONYMOUS_VARIABLE(var) = ray::internal::RegisterRemoteFunctions(          \
      std::array<absl::string_view, GET_ARG_COUNT(__VA_ARGS__)>{                         \
          MARCO_EXPAND(MACRO_CONCAT(CON_STR, GET_ARG_COUNT(__VA_ARGS__))(__VA_ARGS__))}, \
      __VA_ARGS__);

}  // namespace api
}  // namespace ray