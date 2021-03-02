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
#include <unordered_map>

namespace ray {
namespace api {

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

template <class, class>
struct AddType;

template <class First, class... Second>
struct AddType<First, std::tuple<Second...>> {
  using type = std::tuple<First, Second...>;
};

template <class First, class Second>
using AddType_t = typename AddType<First, Second>::type;

namespace internal {

class Router {
 public:
  static Router &Instance() {
    static Router instance;
    return instance;
  }

  template <typename Function>
  bool RegisterRemoteFunction(std::string const &name, const Function &f) {
    /// Use std::addressof to get address of a function for free function now, it will be
    /// improved to support member function later.
    auto pair = func_ptr_to_key_map_.emplace((uint64_t)std::addressof(f), name);
    if (!pair.second) {
      return false;
    }

    RegisterNonMemberFunc(name, f);
    return true;
  }

  std::string Route(const char *data, std::size_t size) {
    std::string result;
    do {
      try {
        auto p = Serializer::Deserialize<std::tuple<std::string>>(data, size);
        auto &func_name = std::get<0>(p);
        auto it = map_invokers_.find(func_name);
        if (it == map_invokers_.end()) {
          result = PackReturnValue(ErrorCode::FAIL, "unknown function: " + func_name);
          break;
        }

        result = it->second(data, size);
        if (result.size() >= MAX_BUF_LEN) {
          result = PackReturnValue(
              ErrorCode::FAIL,
              "the response result is out of range: more than 10M " + func_name);
        }
      } catch (const std::exception &ex) {
        result = PackReturnValue(ErrorCode::FAIL, ex.what());
      }
    } while (0);

    return result;
  }

 private:
  Router() = default;
  ~Router() = default;
  Router(const Router &) = delete;
  Router(Router &&) = delete;

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

  template <typename Function>
  struct Invoker {
    static inline std::string Apply(const Function &func, const char *data, size_t size) {
      using args_tuple = AddType_t<std::string, boost::callable_traits::args_t<Function>>;

      std::string result;
      try {
        auto tp = Serializer::Deserialize<args_tuple>(data, size);
        Router::Call(func, result, std::move(tp));
      } catch (msgpack::type_error &e) {
        result = PackReturnValue(ErrorCode::FAIL,
                                 std::string("invalid arguments: ") + e.what());
      } catch (const std::exception &e) {
        result = PackReturnValue(ErrorCode::FAIL,
                                 std::string("function execute exception: ") + e.what());
      } catch (...) {
        result = PackReturnValue(ErrorCode::FAIL, "unknown exception");
      }

      return result;
    }
  };

  template <typename Function>
  void RegisterNonMemberFunc(std::string const &name, Function f) {
    this->map_invokers_[name] = {std::bind(&Invoker<Function>::Apply, std::move(f),
                                           std::placeholders::_1, std::placeholders::_2)};
  }

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

  std::unordered_map<std::string, std::function<std::string(const char *, size_t)>>
      map_invokers_;
  std::unordered_map<uintptr_t, std::string> func_ptr_to_key_map_;

  const static size_t MAX_BUF_LEN = 1024 * 10 * 10;
};
}  // namespace internal

#define RAY_REGISTER(f)                 \
  static auto ANONYMOUS_VARIABLE(var) = \
      internal::Router::Instance().RegisterRemoteFunction(#f, f);

#define CONCATENATE_DIRECT(s1, s2) s1##s2
#define CONCATENATE(s1, s2) CONCATENATE_DIRECT(s1, s2)
#ifdef _MSC_VER
#define ANONYMOUS_VARIABLE(str) CONCATENATE(str, __COUNTER__)
#else
#define ANONYMOUS_VARIABLE(str) CONCATENATE(str, __LINE__)
#endif
}  // namespace api
}  // namespace ray