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

namespace ray {
namespace api {

inline static msgpack::sbuffer TaskExecutionHandler(const char *data, std::size_t size) {
  msgpack::sbuffer result;
  do {
    try {
      auto p = Serializer::Deserialize<std::tuple<std::string>>(data, size);
      auto &func_name = std::get<0>(p);
      auto pair = internal::FunctionManager::Instance().GetFunction(func_name);
      if (!pair.first) {
        result = internal::PackReturnValue(internal::ErrorCode::FAIL,
                                           "unknown function: " + func_name, 0);
        break;
      }

      result = (*pair.second)(data, size);
    } catch (const std::exception &ex) {
      result = internal::PackReturnValue(internal::ErrorCode::FAIL, ex.what());
    }
  } while (0);

  return result;
}

#define RAY_REGISTER(f)                 \
  static auto ANONYMOUS_VARIABLE(var) = \
      ray::internal::FunctionManager::Instance().RegisterRemoteFunction(#f, f);

#define CONCATENATE_DIRECT(s1, s2) s1##s2
#define CONCATENATE(s1, s2) CONCATENATE_DIRECT(s1, s2)
#ifdef _MSC_VER
#define ANONYMOUS_VARIABLE(str) CONCATENATE(str, __COUNTER__)
#else
#define ANONYMOUS_VARIABLE(str) CONCATENATE(str, __LINE__)
#endif
}  // namespace api
}  // namespace ray