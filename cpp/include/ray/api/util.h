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

#include <ray/api/object_ref.h>

#include <type_traits>
#include <typeinfo>
#ifndef _MSC_VER
#include <cxxabi.h>
#endif
#include <boost/callable_traits.hpp>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>

namespace ray {
namespace api {

template <class T>
std::string type_name() {
  typedef typename std::remove_reference<T>::type TR;
  std::unique_ptr<char, void (*)(void *)> own(
#ifndef _MSC_VER
      abi::__cxa_demangle(typeid(TR).name(), nullptr, nullptr, nullptr),
#else
      nullptr,
#endif
      std::free);
  std::string r = own != nullptr ? own.get() : typeid(TR).name();
  if (std::is_const<TR>::value) r += " const";
  if (std::is_volatile<TR>::value) r += " volatile";
  if (std::is_lvalue_reference<T>::value)
    r += "&";
  else if (std::is_rvalue_reference<T>::value)
    r += "&&";
  return r;
}

template <typename T>
struct FilterArgType {
  using type = T;
};

template <typename T>
struct FilterArgType<ObjectRef<T>> {
  using type = T;
};

template <typename FUNCTION, typename... Args>
absl::enable_if_t<!std::is_void<FUNCTION>::value &&
                  !std::is_member_function_pointer<FUNCTION>::value>
StaticCheck() {
  static_assert(std::is_same<std::tuple<typename FilterArgType<Args>::type...>,
                             boost::callable_traits::args_t<FUNCTION>>::value,
                "arguments not match");
}

template <typename FUNCTION, typename... Args>
absl::enable_if_t<!std::is_void<FUNCTION>::value &&
                  std::is_member_function_pointer<FUNCTION>::value>
StaticCheck() {
  using ActorType = boost::callable_traits::class_of_t<FUNCTION>;
  static_assert(
      std::is_same<std::tuple<ActorType &, typename FilterArgType<Args>::type...>,
                   boost::callable_traits::args_t<FUNCTION>>::value,
      "arguments not match");
}

template <typename FUNCTION, typename... Args>
absl::enable_if_t<std::is_void<FUNCTION>::value> StaticCheck() {}

}  // namespace api
}  // namespace ray