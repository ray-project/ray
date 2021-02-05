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
#include <boost/any.hpp>
#include <map>

#include "func_name.h"
#include "overload.h"

namespace ray {
template <typename OUT, typename IN>
inline static OUT address_of(IN in) {
  union {
    IN in;
    OUT out;
  } u = {in};

  return u.out;
};

static std::map<absl::string_view, boost::any> g_name_to_func_map;
static std::map<void *, boost::any> g_addr_to_func_map;
static std::map<void *, absl::string_view> g_addr_to_name_map;

template <typename F>
static inline auto get_function(F f) {
  auto it = ray::g_addr_to_func_map.find(address_of<void *>(f));
  if (it == ray::g_addr_to_func_map.end()) {
    std::cout << "func not exist\n";
    return (F) nullptr;
  }

  auto fn = boost::any_cast<F>(it->second);
  return fn;
}

template <typename F>
static inline absl::string_view get_function_name(F f) {
  auto it = ray::g_addr_to_name_map.find(address_of<void *>(f));
  if (it == ray::g_addr_to_name_map.end()) {
    std::cout << "func not exist\n";
    return {};
  }

  return it->second;
}

static inline boost::any get_function(absl::string_view func_name) {
  auto it = ray::g_name_to_func_map.find(func_name);
  if (it == ray::g_name_to_func_map.end()) {
    std::cout << "func not exist\n";
    return {};
  }

  return it->second;
}

template <typename F, typename... Args>
inline static auto call_func(F f, Args &&...args) {
  if (f == nullptr) {
    throw std::invalid_argument("function is null!");
  }
  return f(std::forward<Args>(args)...);
}

template <typename Self, typename F, typename... Args>
inline static auto call_func(Self &self, F f, Args &&...args)
    -> std::enable_if_t<std::is_class<Self>::value,
                        decltype((self.*f)(std::forward<Args>(args)...))> {
  if (f == nullptr) {
    throw std::invalid_argument("function is null!");
  }
  return (self.*f)(std::forward<Args>(args)...);
}

template <typename F, typename... Args>
std::enable_if_t<!std::is_class<F>::value> call_test(F f, Args &&...args) {
  auto it = ray::g_addr_to_func_map.find(address_of<void *>(f));
  if (it != ray::g_addr_to_func_map.end()) {
    auto fn = boost::any_cast<F>(it->second);
    fn(std::forward<Args>(args)...);
  } else {
    std::cout << "func not exist\n";
  }
}

template <typename Self, typename F, typename... Args>
std::enable_if_t<std::is_class<Self>::value> call_test(Self self, const F &f,
                                                       Args &&...args) {
  auto it = ray::g_addr_to_func_map.find(address_of<void *>(f));
  if (it != ray::g_addr_to_func_map.end()) {
    auto fn = boost::any_cast<F>(it->second);
    (self.*fn)(std::forward<Args>(args)...);
  } else {
    std::cout << "func not exist\n";
  }
}

template <typename S, typename T>
inline static bool register_func(S s, T t) {
  bool r = g_name_to_func_map.emplace(s, t).second;
  if (!r) {
    std::cout << s << " function has been registered before!\n";
    throw std::logic_error("the function: " + std::string(s) +
                           " has been registered before");
  }

  r = g_addr_to_func_map.emplace(address_of<void *>(t), t).second;
  if (!r) {
    std::cout << s << " has been registered before!\n";
    throw std::logic_error("the function: " + std::string(s) +
                           " has been registered before");
  }

  g_addr_to_name_map.emplace(address_of<void *>(t), s);

  std::cout << s << " " << address_of<void *>(t) << '\n';
  return true;
}

template <typename T, typename... U>
inline static int register_functions(T t, U... u) {
  int index = 0;
  (void)std::initializer_list<int>{(register_func(t[index++], u), 0)...};
  return 0;
}

#define RAY_REGISTER(...)                                                                \
  static auto ANONYMOUS_VARIABLE(var) = ray::register_functions(                         \
      std::array<absl::string_view, GET_ARG_COUNT(__VA_ARGS__)>{                         \
          MARCO_EXPAND(MACRO_CONCAT(CON_STR, GET_ARG_COUNT(__VA_ARGS__))(__VA_ARGS__))}, \
      __VA_ARGS__);

#define RayFunc(f, ...) underload<__VA_ARGS__>(f)
#define RayMemberFunc(f, cv, ...) underload<cv, __VA_ARGS__>(f)

#define CONCATENATE_DIRECT(s1, s2) s1##s2
#define CONCATENATE(s1, s2) CONCATENATE_DIRECT(s1, s2)
#ifdef _MSC_VER  // Necessary for edit & continue in MS Visual C++.
#define ANONYMOUS_VARIABLE(str) CONCATENATE(str, __COUNTER__)
#else
#define ANONYMOUS_VARIABLE(str) CONCATENATE(str, __LINE__)
#endif
}  // namespace ray
