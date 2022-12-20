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

#include <ray/api/common_types.h>
#include <ray/api/ray_runtime_holder.h>
#include <ray/api/serializer.h>
#include <ray/api/type_traits.h>

#include <boost/callable_traits.hpp>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_map>

namespace ray {
namespace internal {

template <typename T>
inline static std::enable_if_t<!std::is_pointer<T>::value, msgpack::sbuffer>
PackReturnValue(T result) {
  if constexpr (is_actor_handle_v<T>) {
    auto serialized_actor_handle =
        RayRuntimeHolder::Instance().Runtime()->SerializeActorHandle(result.ID());
    return Serializer::Serialize(serialized_actor_handle);
  }
  return Serializer::Serialize(std::move(result));
}

template <typename T>
inline static std::enable_if_t<std::is_pointer<T>::value, msgpack::sbuffer>
PackReturnValue(T result) {
  return Serializer::Serialize((uint64_t)result);
}

inline static msgpack::sbuffer PackVoid() {
  return Serializer::Serialize(msgpack::type::nil_t());
}

msgpack::sbuffer PackError(std::string error_msg);

/// It's help to invoke functions and member functions, the class Invoker<Function> help
/// do type erase.
template <typename Function>
struct Invoker {
  /// Invoke functions by networking stream, at first deserialize the binary data to a
  /// tuple, then call function with tuple.
  static inline msgpack::sbuffer Apply(const Function &func,
                                       const ArgsBufferList &args_buffer) {
    using RetrunType = boost::callable_traits::return_type_t<Function>;
    using ArgsTuple = RemoveReference_t<boost::callable_traits::args_t<Function>>;
    if (std::tuple_size<ArgsTuple>::value != args_buffer.size()) {
      throw std::invalid_argument("Arguments number not match");
    }

    msgpack::sbuffer result;
    ArgsTuple tp{};
    bool is_ok = GetArgsTuple(
        tp, args_buffer, std::make_index_sequence<std::tuple_size<ArgsTuple>::value>{});
    if (!is_ok) {
      throw std::invalid_argument("Arguments error");
    }

    result = Invoker<Function>::Call<RetrunType>(func, std::move(tp));
    return result;
  }

  static inline msgpack::sbuffer ApplyMember(const Function &func,
                                             msgpack::sbuffer *ptr,
                                             const ArgsBufferList &args_buffer) {
    using RetrunType = boost::callable_traits::return_type_t<Function>;
    using ArgsTuple =
        RemoveReference_t<RemoveFirst_t<boost::callable_traits::args_t<Function>>>;
    if (std::tuple_size<ArgsTuple>::value != args_buffer.size()) {
      throw std::invalid_argument("Arguments number not match");
    }

    msgpack::sbuffer result;
    ArgsTuple tp{};
    bool is_ok = GetArgsTuple(
        tp, args_buffer, std::make_index_sequence<std::tuple_size<ArgsTuple>::value>{});
    if (!is_ok) {
      throw std::invalid_argument("Arguments error");
    }

    uint64_t actor_ptr = Serializer::Deserialize<uint64_t>(ptr->data(), ptr->size());
    using Self = boost::callable_traits::class_of_t<Function>;
    Self *self = (Self *)actor_ptr;
    result = Invoker<Function>::CallMember<RetrunType>(func, self, std::move(tp));

    return result;
  }

 private:
  template <typename T>
  static inline T ParseArg(const ArgsBuffer &args_buffer, bool &is_ok) {
    is_ok = true;
    if constexpr (is_object_ref_v<T>) {
      // Construct an ObjectRef<T> by id.
      return T(std::string(args_buffer.data(), args_buffer.size()));
    } else if constexpr (is_actor_handle_v<T>) {
      auto actor_handle =
          Serializer::Deserialize<std::string>(args_buffer.data(), args_buffer.size());
      return T::FromBytes(actor_handle);
    } else {
      auto [success, value] =
          Serializer::DeserializeWhenNil<T>(args_buffer.data(), args_buffer.size());
      is_ok = success;
      return value;
    }
  }

  static inline bool GetArgsTuple(std::tuple<> &tup,
                                  const ArgsBufferList &args_buffer,
                                  std::index_sequence<>) {
    return true;
  }

  template <size_t... I, typename... Args>
  static inline bool GetArgsTuple(std::tuple<Args...> &tp,
                                  const ArgsBufferList &args_buffer,
                                  std::index_sequence<I...>) {
    bool is_ok = true;
    (void)std::initializer_list<int>{
        (std::get<I>(tp) = ParseArg<Args>(args_buffer.at(I), is_ok), 0)...};
    return is_ok;
  }

  template <typename R, typename F, typename... Args>
  static std::enable_if_t<std::is_void<R>::value, msgpack::sbuffer> Call(
      const F &f, std::tuple<Args...> args) {
    CallInternal<R>(f, std::make_index_sequence<sizeof...(Args)>{}, std::move(args));
    return PackVoid();
  }

  template <typename R, typename F, typename... Args>
  static std::enable_if_t<!std::is_void<R>::value, msgpack::sbuffer> Call(
      const F &f, std::tuple<Args...> args) {
    auto r =
        CallInternal<R>(f, std::make_index_sequence<sizeof...(Args)>{}, std::move(args));
    return PackReturnValue(r);
  }

  template <typename R, typename F, size_t... I, typename... Args>
  static R CallInternal(const F &f,
                        const std::index_sequence<I...> &,
                        std::tuple<Args...> args) {
    (void)args;
    using ArgsTuple = boost::callable_traits::args_t<F>;
    return f(((typename std::tuple_element<I, ArgsTuple>::type)std::get<I>(args))...);
  }

  template <typename R, typename F, typename Self, typename... Args>
  static std::enable_if_t<std::is_void<R>::value, msgpack::sbuffer> CallMember(
      const F &f, Self *self, std::tuple<Args...> args) {
    CallMemberInternal<R>(
        f, self, std::make_index_sequence<sizeof...(Args)>{}, std::move(args));
    return PackVoid();
  }

  template <typename R, typename F, typename Self, typename... Args>
  static std::enable_if_t<!std::is_void<R>::value, msgpack::sbuffer> CallMember(
      const F &f, Self *self, std::tuple<Args...> args) {
    auto r = CallMemberInternal<R>(
        f, self, std::make_index_sequence<sizeof...(Args)>{}, std::move(args));
    return PackReturnValue(r);
  }

  template <typename R, typename F, typename Self, size_t... I, typename... Args>
  static R CallMemberInternal(const F &f,
                              Self *self,
                              const std::index_sequence<I...> &,
                              std::tuple<Args...> args) {
    (void)args;
    using ArgsTuple = boost::callable_traits::args_t<F>;
    return (self->*f)(
        ((typename std::tuple_element<I + 1, ArgsTuple>::type) std::get<I>(args))...);
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

  std::pair<const RemoteFunctionMap_t &, const RemoteMemberFunctionMap_t &>
  GetRemoteFunctions() {
    return std::pair<const RemoteFunctionMap_t &, const RemoteMemberFunctionMap_t &>(
        map_invokers_, map_mem_func_invokers_);
  }

  RemoteFunction *GetFunction(const std::string &func_name) {
    auto it = map_invokers_.find(func_name);
    if (it == map_invokers_.end()) {
      return nullptr;
    }

    return &it->second;
  }

  template <typename Function>
  std::enable_if_t<!std::is_member_function_pointer<Function>::value, bool>
  RegisterRemoteFunction(std::string const &name, const Function &f) {
    auto pair = func_ptr_to_key_map_.emplace(GetAddress(f), name);
    if (!pair.second) {
      throw RayException("Duplicate RAY_REMOTE function: " + name);
    }

    bool ok = RegisterNonMemberFunc(name, f);
    if (!ok) {
      throw RayException("Duplicate RAY_REMOTE function: " + name);
    }

    return true;
  }

  template <typename Function>
  std::enable_if_t<std::is_member_function_pointer<Function>::value, bool>
  RegisterRemoteFunction(std::string const &name, const Function &f) {
    using Self = boost::callable_traits::class_of_t<Function>;
    auto key = std::make_pair(typeid(Self).name(), GetAddress(f));
    auto pair = mem_func_to_key_map_.emplace(std::move(key), name);
    if (!pair.second) {
      throw RayException("Duplicate RAY_REMOTE function: " + name);
    }

    bool ok = RegisterMemberFunc(name, f);
    if (!ok) {
      throw RayException("Duplicate RAY_REMOTE function: " + name);
    }

    return true;
  }

  template <typename Function>
  std::enable_if_t<!std::is_member_function_pointer<Function>::value, std::string>
  GetFunctionName(const Function &f) {
    auto it = func_ptr_to_key_map_.find(GetAddress(f));
    if (it == func_ptr_to_key_map_.end()) {
      return "";
    }

    return it->second;
  }

  template <typename Function>
  std::enable_if_t<std::is_member_function_pointer<Function>::value, std::string>
  GetFunctionName(const Function &f) {
    using Self = boost::callable_traits::class_of_t<Function>;
    auto key = std::make_pair(typeid(Self).name(), GetAddress(f));
    auto it = mem_func_to_key_map_.find(key);
    if (it == mem_func_to_key_map_.end()) {
      return "";
    }

    return it->second;
  }

  RemoteMemberFunction *GetMemberFunction(const std::string &func_name) {
    auto it = map_mem_func_invokers_.find(func_name);
    if (it == map_mem_func_invokers_.end()) {
      return nullptr;
    }

    return &it->second;
  }

 private:
  FunctionManager() = default;
  ~FunctionManager() = default;
  FunctionManager(const FunctionManager &) = delete;
  FunctionManager(FunctionManager &&) = delete;

  template <typename Function>
  bool RegisterNonMemberFunc(std::string const &name, Function f) {
    return map_invokers_
        .emplace(
            name,
            std::bind(&Invoker<Function>::Apply, std::move(f), std::placeholders::_1))
        .second;
  }

  template <typename Function>
  bool RegisterMemberFunc(std::string const &name, Function f) {
    return map_mem_func_invokers_
        .emplace(name,
                 std::bind(&Invoker<Function>::ApplyMember,
                           std::move(f),
                           std::placeholders::_1,
                           std::placeholders::_2))
        .second;
  }

  template <class Dest, class Source>
  Dest BitCast(const Source &source) {
    static_assert(sizeof(Dest) == sizeof(Source),
                  "BitCast requires source and destination to be the same size");

    Dest dest;
    memcpy(&dest, &source, sizeof(dest));
    return dest;
  }

  template <typename F>
  std::string GetAddress(F f) {
    auto arr = BitCast<std::array<char, sizeof(F)>>(f);
    return std::string(arr.data(), arr.size());
  }

  RemoteFunctionMap_t map_invokers_;
  RemoteMemberFunctionMap_t map_mem_func_invokers_;
  std::unordered_map<std::string, std::string> func_ptr_to_key_map_;
  std::map<std::pair<std::string, std::string>, std::string> mem_func_to_key_map_;
};
}  // namespace internal
}  // namespace ray