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

#include <boost/callable_traits.hpp>
#include <functional>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>

#include "absl/utility/utility.h"
#include "ray/core.h"

namespace ray {
namespace internal {

template <typename T>
struct is_smart_pointer : std::false_type {};
template <typename T>
struct is_smart_pointer<std::shared_ptr<T>> : std::true_type {};
template <typename T>
struct is_smart_pointer<std::unique_ptr<T>> : std::true_type {};

template <typename T>
inline static absl::enable_if_t<!std::is_pointer<T>::value && !is_smart_pointer<T>::value,
                                msgpack::sbuffer>
PackReturnValue(const T &result) {
  msgpack::sbuffer buffer;
  msgpack::pack(buffer, result);
  return buffer;
}

template <typename T>
inline static absl::enable_if_t<is_smart_pointer<T>::value, msgpack::sbuffer>
PackReturnValue(const T &result) {
  return ray::api::Serializer::Serialize(*result);
}

template <typename T>
inline static absl::enable_if_t<std::is_pointer<T>::value, msgpack::sbuffer>
PackReturnValue(const T &result) {
  std::unique_ptr<absl::remove_pointer_t<T>> scope_ptr(result);
  return ray::api::Serializer::Serialize(*result);
}

inline static msgpack::sbuffer PackVoid() {
  return ray::api::Serializer::Serialize(msgpack::type::nil_t());
}

inline static msgpack::sbuffer PackError(std::string error_msg) {
  msgpack::sbuffer sbuffer;
  msgpack::packer<msgpack::sbuffer> packer(sbuffer);
  packer.pack(msgpack::type::nil_t());
  packer.pack(std::move(error_msg));

  return sbuffer;
}

template <typename>
struct RemoveFirst;

template <class First, class... Second>
struct RemoveFirst<std::tuple<First, Second...>> {
  using type = std::tuple<Second...>;
};

template <class Tuple>
using RemoveFirst_t = typename RemoveFirst<Tuple>::type;

/// It's help to invoke functions and member functions, the class Invoker<Function> help
/// do type erase.
template <typename Function>
struct Invoker {
  /// Invoke functions by networking stream, at first deserialize the binary data to a
  /// tuple, then call function with tuple.
  static inline msgpack::sbuffer Apply(
      const Function &func, const std::vector<std::shared_ptr<RayObject>> &args_buffer) {
    using ArgsTuple = boost::callable_traits::args_t<Function>;
    if (std::tuple_size<ArgsTuple>::value != args_buffer.size() - 1) {
      return PackError("Arguments number not match");
    }

    std::vector<std::shared_ptr<RayObject>> new_vector;
    if (args_buffer.size() > 1) {
      auto first = args_buffer.begin() + 1;
      new_vector = std::vector<std::shared_ptr<RayObject>>(first, args_buffer.end());
    }

    msgpack::sbuffer result;
    ArgsTuple tp{};
    try {
      bool is_ok = GetArgsTuple(
          tp, new_vector, absl::make_index_sequence<std::tuple_size<ArgsTuple>::value>{});
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

  static inline msgpack::sbuffer ApplyMember(
      const Function &func, msgpack::sbuffer *ptr,
      const std::vector<std::shared_ptr<RayObject>> &args_buffer) {
    using ArgsTuple = RemoveFirst_t<boost::callable_traits::args_t<Function>>;
    if (std::tuple_size<ArgsTuple>::value != args_buffer.size() - 1) {
      return PackError("Arguments number not match");
    }

    if (ptr == nullptr) {
      return PackError("Arguments not match, actor buffer is null");
    }

    std::vector<std::shared_ptr<RayObject>> new_vector;
    if (args_buffer.size() > 1) {
      auto first = args_buffer.begin() + 1;
      new_vector = std::vector<std::shared_ptr<RayObject>>(first, args_buffer.end());
    }

    msgpack::sbuffer result;
    ArgsTuple tp{};
    try {
      bool is_ok = GetArgsTuple(
          tp, new_vector, absl::make_index_sequence<std::tuple_size<ArgsTuple>::value>{});
      if (!is_ok) {
        return PackError("arguments error");
      }
      result = Invoker<Function>::CallMember(func, ptr, std::move(tp));
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

  template <typename F, typename... Args>
  static absl::enable_if_t<std::is_void<boost::callable_traits::return_type_t<F>>::value,
                           msgpack::sbuffer>
  CallMember(const F &f, msgpack::sbuffer *ptr, std::tuple<Args...> tp) {
    CallMemberInternal(f, ptr, absl::make_index_sequence<sizeof...(Args)>{},
                       std::move(tp));
    return PackVoid();
  }

  template <typename F, typename... Args>
  static absl::enable_if_t<!std::is_void<boost::callable_traits::return_type_t<F>>::value,
                           msgpack::sbuffer>
  CallMember(const F &f, msgpack::sbuffer *ptr, std::tuple<Args...> tp) {
    auto r = CallMemberInternal(f, ptr, absl::make_index_sequence<sizeof...(Args)>{},
                                std::move(tp));
    return PackReturnValue(r);
  }

  template <typename F, size_t... I, typename... Args>
  static boost::callable_traits::return_type_t<F> CallMemberInternal(
      const F &f, msgpack::sbuffer *ptr, const absl::index_sequence<I...> &,
      std::tuple<Args...> tup) {
    (void)tup;
    using Self = boost::callable_traits::class_of_t<F>;
    Self self{};
    self = ray::api::Serializer::Deserialize<Self>(ptr->data(), ptr->size());
    return (self.*f)(std::move(std::get<I>(tup))...);
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

  std::function<msgpack::sbuffer(msgpack::sbuffer *,
                                 const std::vector<std::shared_ptr<RayObject>> &)>
      *GetMemberFunction(const std::string &func_name) {
    auto it = map_mem_func_invokers_.find(func_name);
    if (it == map_mem_func_invokers_.end()) {
      return nullptr;
    }

    return &it->second;
  }

  template <typename Function>
  absl::enable_if_t<!std::is_member_function_pointer<Function>::value, bool>
  RegisterRemoteFunction(std::string const &name, const Function &f) {
    auto pair = func_ptr_to_key_map_.emplace(GetAddress(f), name);
    if (!pair.second) {
      return false;
    }

    return RegisterNonMemberFunc(name, f);
  }

  template <typename Function>
  absl::enable_if_t<std::is_member_function_pointer<Function>::value, bool>
  RegisterRemoteFunction(std::string const &name, const Function &f) {
    auto pair = func_ptr_to_key_map_.emplace(GetAddress(f), name);
    if (!pair.second) {
      return false;
    }

    return RegisterMemberFunc(name, f);
  }

  template <typename Function>
  std::string GetFunctionName(const Function &f) {
    auto it = func_ptr_to_key_map_.find(GetAddress(f));
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

  template <typename Function>
  bool RegisterMemberFunc(std::string const &name, Function f) {
    return map_mem_func_invokers_
        .emplace(name, std::bind(&Invoker<Function>::ApplyMember, std::move(f),
                                 std::placeholders::_1, std::placeholders::_2))
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

  std::unordered_map<std::string, std::function<msgpack::sbuffer(
                                      const std::vector<std::shared_ptr<RayObject>> &)>>
      map_invokers_;
  std::unordered_map<std::string, std::function<msgpack::sbuffer(
                                      msgpack::sbuffer *,
                                      const std::vector<std::shared_ptr<RayObject>> &)>>
      map_mem_func_invokers_;
  std::unordered_map<std::string, std::string> func_ptr_to_key_map_;
};
}  // namespace internal
}  // namespace ray