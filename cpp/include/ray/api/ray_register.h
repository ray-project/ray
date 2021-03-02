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

template <typename OUT, typename IN>
inline static OUT AddressOf(IN in) {
  union {
    IN in;
    OUT out;
  } u = {in};

  return u.out;
};

class Router {
 public:
  static Router &Instance() {
    static Router instance;
    return instance;
  }

  template <typename Function>
  bool RegisterHandler(std::string const &name, const Function &f) {
    auto pair = func_ptr_to_key_map_.emplace(AddressOf<uintptr_t>(f), name);
    if (!pair.second) {
      return false;
    }

    RegisterNonmemberFunc(name, f);
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
          result = PackArgsStr(ErrorCode::FAIL, "unknown function: " + func_name);
          break;
        }

        it->second(data, size, result);
        if (result.size() >= MAX_BUF_LEN) {
          result = PackArgsStr(
              ErrorCode::FAIL,
              "the response result is out of range: more than 10M " + func_name);
        }
      } catch (const std::exception &ex) {
        result = PackArgsStr(ErrorCode::FAIL, ex.what());
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
  static typename std::result_of<F(Args...)>::type CallHelper(
      const F &f, const absl::index_sequence<I...> &, std::tuple<Arg, Args...> tup) {
    return f(std::move(std::get<I + 1>(tup))...);
  }

  template <typename F, typename Arg, typename... Args>
  static typename std::enable_if<
      std::is_void<typename std::result_of<F(Args...)>::type>::value>::type
  Call(const F &f, std::string &result, std::tuple<Arg, Args...> tp) {
    CallHelper(f, absl::make_index_sequence<sizeof...(Args)>{}, std::move(tp));
    result = PackArgsStr(ErrorCode::OK);
  }

  template <typename F, typename Arg, typename... Args>
  static typename std::enable_if<
      !std::is_void<typename std::result_of<F(Args...)>::type>::value>::type
  Call(const F &f, std::string &result, std::tuple<Arg, Args...> tp) {
    auto r = CallHelper(f, absl::make_index_sequence<sizeof...(Args)>{}, std::move(tp));
    result = PackArgsStr(ErrorCode::OK, "ok", r);
  }

  template <typename Function>
  struct invoker {
    static inline void Apply(const Function &func, const char *data, size_t size,
                             std::string &result) {
      using args_tuple = AddType_t<std::string, boost::callable_traits::args_t<Function>>;

      try {
        auto tp = Serializer::Deserialize<args_tuple>(data, size);
        Router::Call(func, result, std::move(tp));
      } catch (msgpack::type_error &e) {
        result =
            PackArgsStr(ErrorCode::FAIL, std::string("invalid arguments: ") + e.what());
      } catch (const std::exception &e) {
        result = PackArgsStr(ErrorCode::FAIL,
                             std::string("function execute exception: ") + e.what());
      } catch (...) {
        result = PackArgsStr(ErrorCode::FAIL, "unknown exception");
      }
    }
  };

  template <typename Function>
  void RegisterNonmemberFunc(std::string const &name, Function f) {
    this->map_invokers_[name] = {std::bind(&invoker<Function>::Apply, std::move(f),
                                           std::placeholders::_1, std::placeholders::_2,
                                           std::placeholders::_3)};
  }

  template <typename T>
  static std::string PackArgsStr(int error_code, std::string error_msg, T result) {
    auto buffer = Serializer::Serialize(
        Response<T>{error_code, std::move(error_msg), std::move(result)});
    return std::string(buffer.data(), buffer.size());
  }

  static std::string PackArgsStr(int error_code, std::string error_msg = "ok") {
    auto buffer = Serializer::Serialize(VoidResponse{error_code, std::move(error_msg)});
    return std::string(buffer.data(), buffer.size());
  }

  std::unordered_map<std::string,
                     std::function<void(const char *, size_t, std::string &)>>
      map_invokers_;
  std::unordered_map<uintptr_t, std::string> func_ptr_to_key_map_;

  const static size_t MAX_BUF_LEN = 1024 * 10 * 10;
};

#define RAY_REGISTER(f) \
  static auto ANONYMOUS_VARIABLE(var) = Router::Instance().RegisterHandler(#f, f);

#define CONCATENATE_DIRECT(s1, s2) s1##s2
#define CONCATENATE(s1, s2) CONCATENATE_DIRECT(s1, s2)
#ifdef _MSC_VER
#define ANONYMOUS_VARIABLE(str) CONCATENATE(str, __COUNTER__)
#else
#define ANONYMOUS_VARIABLE(str) CONCATENATE(str, __LINE__)
#endif
}  // namespace api
}  // namespace ray