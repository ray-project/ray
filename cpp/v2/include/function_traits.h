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

#include <type_traits>

namespace ray {
  template<typename T>
  struct function_traits;

  template<typename Ret, typename Arg, typename... Args>
  struct function_traits<Ret(Arg, Args...)> {
  public:
    enum {
      arity = sizeof...(Args) + 1
    };

    typedef Ret function_type(Arg, Args...);
    using Self = function_type;
    typedef Ret return_type;
    using stl_function_type = std::function<function_type>;

    typedef Ret(*pointer)(Arg, Args...);

    using args_tuple = std::tuple<std::remove_const_t<std::remove_reference_t<Arg>>, std::remove_const_t<std::remove_reference_t<Args>>...>;
//    using args_tuple = std::tuple<std::string, Arg, std::remove_const_t<std::remove_reference_t<Args>>...>;
  };

  template<typename Ret>
  struct function_traits<Ret()> {
  public:
    enum {
      arity = 0
    };

    typedef Ret function_type();
    using Self = function_type;
    typedef Ret return_type;
    using stl_function_type = std::function<function_type>;

    typedef Ret(*pointer)();

    typedef std::tuple<> tuple_type;
    typedef std::tuple<> bare_tuple_type;
    using args_tuple = std::tuple<std::string>;
    using args_tuple_2nd = std::tuple<std::string>;
  };

  template<typename Ret, typename... Args>
  struct function_traits<Ret(*)(Args...)> : function_traits<Ret(Args...)> {
  };

  template<typename Ret, typename... Args>
  struct function_traits<std::function<Ret(Args...)>> : function_traits<Ret(Args...)> {
  };

  template<typename ReturnType, typename ClassType, typename... Args>
  struct function_traits<ReturnType(ClassType::*)(Args...)> : function_traits<ReturnType(Args...)> {
    using Self = ClassType;
  };

  template<typename ReturnType, typename ClassType, typename... Args>
  struct function_traits<ReturnType(ClassType::*)(Args...) const> : function_traits<ReturnType(Args...)> {
    using Self = ClassType;
  };

  template<typename Callable>
  struct function_traits : function_traits<decltype(&Callable::operator())> {
  };
}