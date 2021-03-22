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

#include <ray/api/serializer.h>
#include "absl/utility/utility.h"

#include <boost/callable_traits.hpp>
#include <functional>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>

namespace ray {
namespace internal {

template <class, class>
struct AddType;

template <class First, class... Second>
struct AddType<First, std::tuple<Second...>> {
  using type = std::tuple<First, Second...>;
};

/// Add a type to a tuple: AddType_t<int, std::tuple<double>> equal std::tuple<int,
/// double>.
template <class First, class Second>
using AddType_t = typename AddType<First, Second>::type;

enum ErrorCode {
  OK = 0,
  FAIL = 1,
};

struct VoidResponse {
  int error_code;
  std::string error_msg;

  MSGPACK_DEFINE(error_code, error_msg);
};

template <typename T>
struct Response {
  int error_code;
  std::string error_msg;
  T data;

  MSGPACK_DEFINE(error_code, error_msg, data);
};

template <typename T>
inline static msgpack::sbuffer PackReturnValue(int error_code, std::string error_msg,
                                               T result) {
  return ray::api::Serializer::Serialize(
      Response<T>{error_code, std::move(error_msg), std::move(result)});
}

inline static msgpack::sbuffer PackReturnValue(int error_code,
                                               std::string error_msg = "ok") {
  return ray::api::Serializer::Serialize(VoidResponse{error_code, std::move(error_msg)});
}

/// It's help to invoke functions and member functions, the class Invoker<Function> help
/// do type erase.
template <typename Function>
struct Invoker {
  /// Invoke functions by networking stream, at first deserialize the binary data to a
  /// tuple, then call function with tuple.
  static inline msgpack::sbuffer Apply(const Function &func, const char *data,
                                       size_t size) {
    using args_tuple = AddType_t<std::string, boost::callable_traits::args_t<Function>>;

    msgpack::sbuffer result;
    try {
      auto tp = ray::api::Serializer::Deserialize<args_tuple>(data, size);
      result = Invoker<Function>::Call(func, std::move(tp));
    } catch (msgpack::type_error &e) {
      result =
          PackReturnValue(ErrorCode::FAIL, std::string("invalid arguments: ") + e.what());
    } catch (const std::exception &e) {
      result = PackReturnValue(ErrorCode::FAIL,
                               std::string("function execute exception: ") + e.what());
    } catch (...) {
      result = PackReturnValue(ErrorCode::FAIL, "unknown exception");
    }

    return result;
  }

  template <typename F, size_t... I, typename Arg, typename... Args>
  static absl::result_of_t<F(Args...)> CallInternal(const F &f,
                                                    const absl::index_sequence<I...> &,
                                                    std::tuple<Arg, Args...> tup) {
    return f(std::move(std::get<I + 1>(tup))...);
  }

  template <typename F, typename Arg, typename... Args>
  static absl::enable_if_t<std::is_void<absl::result_of_t<F(Args...)>>::value,
                           msgpack::sbuffer>
  Call(const F &f, std::tuple<Arg, Args...> tp) {
    CallInternal(f, absl::make_index_sequence<sizeof...(Args)>{}, std::move(tp));
    return PackReturnValue(ErrorCode::OK);
  }

  template <typename F, typename Arg, typename... Args>
  static absl::enable_if_t<!std::is_void<absl::result_of_t<F(Args...)>>::value,
                           msgpack::sbuffer>
  Call(const F &f, std::tuple<Arg, Args...> tp) {
    auto r = CallInternal(f, absl::make_index_sequence<sizeof...(Args)>{}, std::move(tp));
    return PackReturnValue(ErrorCode::OK, "ok", r);
  }
};

/// Manage all ray remote functions, add remote functions by RAY_REMOTE, get functions by
/// TaskExecutionHandler.
class FunctionManager {
 public:
  static FunctionManager &Instance() {
    static FunctionManager instance;
    return instance;
  }

  std::function<msgpack::sbuffer(const char *, size_t)> *GetFunction(
      const std::string &func_name) {
    auto it = map_invokers_.find(func_name);
    if (it == map_invokers_.end()) {
      return nullptr;
    }

    return &it->second;
  }

  template <typename Function>
  bool RegisterRemoteFunction(std::string const &name, const Function &f) {
    /// Now it is just support free function, it will be
    /// improved to support member function later.
    auto pair = func_ptr_to_key_map_.emplace((uint64_t)f, name);
    if (!pair.second) {
      return false;
    }

    return RegisterNonMemberFunc(name, f);
  }

 private:
  FunctionManager() = default;
  ~FunctionManager() = default;
  FunctionManager(const FunctionManager &) = delete;
  FunctionManager(FunctionManager &&) = delete;

  template <typename Function>
  bool RegisterNonMemberFunc(std::string const &name, Function f) {
    return map_invokers_
        .emplace(name, std::bind(&Invoker<Function>::Apply, std::move(f),
                                 std::placeholders::_1, std::placeholders::_2))
        .second;
  }

  std::unordered_map<std::string, std::function<msgpack::sbuffer(const char *, size_t)>>
      map_invokers_;
  std::unordered_map<uintptr_t, std::string> func_ptr_to_key_map_;
};
}  // namespace internal
}  // namespace ray