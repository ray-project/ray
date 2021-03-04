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
namespace api {

namespace internal {

template <class, class>
struct AddType;

template <class First, class... Second>
struct AddType<First, std::tuple<Second...>> {
  using type = std::tuple<First, Second...>;
};

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
static std::string PackReturnValue(int error_code, std::string error_msg, T result) {
  auto buffer = Serializer::Serialize(
      Response<T>{error_code, std::move(error_msg), std::move(result)});
  return std::string(buffer.data(), buffer.size());
}

static std::string PackReturnValue(int error_code, std::string error_msg = "ok") {
  auto buffer = Serializer::Serialize(VoidResponse{error_code, std::move(error_msg)});
  return std::string(buffer.data(), buffer.size());
}

template <typename Function>
struct Invoker {
  static inline std::string Apply(const Function &func, const char *data, size_t size) {
    using args_tuple = AddType_t<std::string, boost::callable_traits::args_t<Function>>;

    std::string result;
    try {
      auto tp = Serializer::Deserialize<args_tuple>(data, size);
      Invoker<Function>::Call(func, result, std::move(tp));
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
  static typename std::result_of<F(Args...)>::type CallInternal(
      const F &f, const absl::index_sequence<I...> &, std::tuple<Arg, Args...> tup) {
    return f(std::move(std::get<I + 1>(tup))...);
  }

  template <typename F, typename Arg, typename... Args>
  static typename std::enable_if<
      std::is_void<typename std::result_of<F(Args...)>::type>::value>::type
  Call(const F &f, std::string &result, std::tuple<Arg, Args...> tp) {
    CallInternal(f, absl::make_index_sequence<sizeof...(Args)>{}, std::move(tp));
    result = PackReturnValue(ErrorCode::OK);
  }

  template <typename F, typename Arg, typename... Args>
  static typename std::enable_if<
      !std::is_void<typename std::result_of<F(Args...)>::type>::value>::type
  Call(const F &f, std::string &result, std::tuple<Arg, Args...> tp) {
    auto r = CallInternal(f, absl::make_index_sequence<sizeof...(Args)>{}, std::move(tp));
    result = PackReturnValue(ErrorCode::OK, "ok", r);
  }
};

class FunctionManager {
 public:
  static FunctionManager &Instance() {
    static FunctionManager instance;
    return instance;
  }

  std::pair<bool, std::function<std::string(const char *, size_t)> *> GetFunction(
      const std::string &func_name) {
    auto it = map_invokers_.find(func_name);
    if (it == map_invokers_.end()) {
      return {};
    }

    return std::make_pair(true, &it->second);
  }

  template <typename Function>
  bool RegisterRemoteFunction(std::string const &name, const Function &f) {
    /// Now it is just support free function, it will be
    /// improved to support member function later.
    auto pair = func_ptr_to_key_map_.emplace((uint64_t)f, name);
    if (!pair.second) {
      return false;
    }

    RegisterNonMemberFunc(name, f);
    return true;
  }

 private:
  FunctionManager() = default;
  ~FunctionManager() = default;
  FunctionManager(const FunctionManager &) = delete;
  FunctionManager(FunctionManager &&) = delete;

  template <typename Function>
  void RegisterNonMemberFunc(std::string const &name, Function f) {
    this->map_invokers_[name] = {std::bind(&Invoker<Function>::Apply, std::move(f),
                                           std::placeholders::_1, std::placeholders::_2)};
  }

  std::unordered_map<std::string, std::function<std::string(const char *, size_t)>>
      map_invokers_;
  std::unordered_map<uintptr_t, std::string> func_ptr_to_key_map_;
};
}  // namespace internal

}  // namespace api
}  // namespace ray