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

#include "ray/core.h"

namespace ray {
namespace internal {

template <typename T>
inline static msgpack::sbuffer PackReturnValue(T result) {
  return ray::api::Serializer::Serialize(std::move(result));
}

inline static msgpack::sbuffer PackVoid() {
  return ray::api::Serializer::Serialize(msgpack::type::nil_t());
}

inline static msgpack::sbuffer PackError(std::string error_msg) {
  msgpack::sbuffer sbuffer;
  msgpack::packer<msgpack::sbuffer> packer(sbuffer);
  packer.pack(msgpack::type::nil_t());
  packer.pack(std::make_tuple((int)ray::rpc::ErrorType::TASK_EXECUTION_EXCEPTION,
                              std::move(error_msg)));

  return sbuffer;
}

/// It's help to invoke functions and member functions, the class Invoker<Function> help
/// do type erase.
template <typename Function>
struct Invoker {
  /// Invoke functions by networking stream, at first deserialize the binary data to a
  /// tuple, then call function with tuple.
  static inline msgpack::sbuffer Apply(
      const Function &func, const std::vector<std::shared_ptr<RayObject>> &args_buffer) {
    using ArgsTuple = boost::callable_traits::args_t<Function>;
    if (std::tuple_size<ArgsTuple>::value != args_buffer.size()) {
      return PackError("Arguments number not match");
    }

    msgpack::sbuffer result;
    ArgsTuple tp{};
    try {
      bool is_ok =
          GetArgsTuple(tp, args_buffer,
                       absl::make_index_sequence<std::tuple_size<ArgsTuple>::value>{});
      if (!is_ok) {
        return PackError("arguments error");
      }
      result = Invoker<Function>::Call(func, std::move(tp));
    } catch (msgpack::type_error &e) {
      result = PackError(std::string("invalid arguments: ") + e.what());
    } catch (const std::exception &e) {
      result = PackError(std::string("function execute exception: ") + e.what());
    } catch (...) {
      result = PackError("unknown exception");
    }

    return result;
  }

 private:
  template <typename T>
  static inline T ParseArg(char *data, size_t size, bool &is_ok) {
    auto pair = ray::api::Serializer::DeserializeWhenNil<T>(data, size);
    is_ok = pair.first;
    return pair.second;
  }

  static inline bool GetArgsTuple(
      std::tuple<> &tup, const std::vector<std::shared_ptr<RayObject>> &args_buffer,
      absl::index_sequence<>) {
    return true;
  }

  template <size_t... I, typename... Args>
  static inline bool GetArgsTuple(
      std::tuple<Args...> &tp, const std::vector<std::shared_ptr<RayObject>> &args_buffer,
      absl::index_sequence<I...>) {
    bool is_ok = true;
    (void)std::initializer_list<int>{
        (std::get<I>(tp) = ParseArg<Args>((char *)args_buffer.at(I)->GetData()->Data(),
                                          args_buffer.at(I)->GetData()->Size(), is_ok),
         0)...};
    return is_ok;
  }

  template <typename F, typename... Args>
  static absl::enable_if_t<std::is_void<absl::result_of_t<F(Args...)>>::value,
                           msgpack::sbuffer>
  Call(const F &f, std::tuple<Args...> tp) {
    CallInternal(f, absl::make_index_sequence<sizeof...(Args)>{}, std::move(tp));
    return PackVoid();
  }

  template <typename F, typename... Args>
  static absl::enable_if_t<!std::is_void<absl::result_of_t<F(Args...)>>::value,
                           msgpack::sbuffer>
  Call(const F &f, std::tuple<Args...> tp) {
    auto r = CallInternal(f, absl::make_index_sequence<sizeof...(Args)>{}, std::move(tp));
    return PackReturnValue(r);
  }

  template <typename F, size_t... I, typename... Args>
  static absl::result_of_t<F(Args...)> CallInternal(const F &f,
                                                    const absl::index_sequence<I...> &,
                                                    std::tuple<Args...> tup) {
    (void)tup;
    return f(std::move(std::get<I>(tup))...);
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

  std::function<msgpack::sbuffer(const std::vector<std::shared_ptr<RayObject>> &)>
      *GetFunction(const std::string &func_name) {
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

  template <typename Function>
  std::string GetFunctionName(const Function &f) {
    auto it = func_ptr_to_key_map_.find((uint64_t)f);
    if (it == func_ptr_to_key_map_.end()) {
      return "";
    }

    return it->second;
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
                                 std::placeholders::_1))
        .second;
  }

  std::unordered_map<std::string, std::function<msgpack::sbuffer(
                                      const std::vector<std::shared_ptr<RayObject>> &)>>
      map_invokers_;
  std::unordered_map<uintptr_t, std::string> func_ptr_to_key_map_;
};
}  // namespace internal
}  // namespace ray