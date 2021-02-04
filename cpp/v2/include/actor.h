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
#include "object_ref.h"
#include "function_traits.h"

namespace ray {

template <typename T, typename F>
struct ActorTask {

  template <typename Arg, typename... Args> auto Remote(Arg arg, Args... args) {
    // TODO
    // send function name and arguments to the remote node
    using args_tuple = typename function_traits<F>::args_tuple;
    using input_args_tuple =
        std::tuple<std::remove_const_t<std::remove_reference_t<Arg>>,
                   std::remove_const_t<std::remove_reference_t<Args>>...>;

    static_assert(std::tuple_size<args_tuple>::value ==
                      std::tuple_size<input_args_tuple>::value,
                  "arguments not match");

    auto tp = get_arguments<args_tuple, input_args_tuple>(
        std::make_tuple(arg, args...));
    // TODO will send to the remote node.
    (void)tp;

    using R = typename function_traits<F>::return_type;
    return get_result<R>(tp);
    // R result = absl::apply(f_, tp);// Just for test.
    // return ObjectRef<R>{result};
  }

  auto Remote() {
    // TODO
    // send function name and arguments to the remote node

    using R = std::result_of_t<decltype(f_)()>;
    return get_result<R>();
  }

  T t_;
  F f_;

private:
  template <typename R>
  std::enable_if_t<std::is_void<R>::value, ObjectRef<R>> get_result() {
    t_.*f_();
    return ObjectRef<R>{};
  }

  template <typename R>
  std::enable_if_t<!std::is_void<R>::value, ObjectRef<R>> get_result() {
    return ObjectRef<R>{t_.*f_()};
  }

  template <typename R, typename Tuple>
  std::enable_if_t<std::is_void<R>::value, ObjectRef<R>>
  get_result(const Tuple &tp) {
    apply(f_, t_, tp);
    return ObjectRef<R>{};
  }

  template <typename R, typename Tuple>
  std::enable_if_t<!std::is_void<R>::value, ObjectRef<R>>
  get_result(const Tuple &tp) {
    return ObjectRef<R>{apply(f_, t_, tp)};
  }

  template <class Tuple, std::size_t... I>
  constexpr decltype(auto) apply_impl(const F& f, T& t, Tuple &&tp,
                                      std::index_sequence<I...>) {
    return (t.*f)(std::get<I>(std::forward<Tuple>(tp))...);
  }

  template <class Tuple>
  constexpr decltype(auto) apply(const F& f, T &t, Tuple &&tp) {
    return apply_impl(
        f, t, std::forward<Tuple>(tp),
        std::make_index_sequence<
            std::tuple_size<std::remove_reference_t<Tuple>>::value>{});
  }
};

template <typename T> struct ActorHandle {

  template<typename F>
  auto Task(F f){
    //check if is exist
    return ActorTask<T, F>{t_, f};
  }

  T t_;//Just for test
};

template<typename T>
struct Actor_t {

  template<typename... Args>
  auto Remote(Args... args){
    //TODO
    //send the Type or create function and arguments to the remote node.
    return get_result<T>(args...);
  }

  T creat_func_ = {};

private:
  template<typename U, typename... Args>
  std::enable_if_t<std::is_class<U>::value, ActorHandle<U>> get_result(Args... args){
    using R = decltype(U{args...});
    return ActorHandle<R>{};
  }

  template<typename U,typename... Args>
  std::enable_if_t<!std::is_class<U>::value, ActorHandle<std::result_of_t<U(Args...)>>> get_result(Args... args){
    using R = decltype(creat_func_(args...));
    return ActorHandle<R>{};
  }
};


template<typename T>
auto Actor(){
  return Actor_t<T>{};
}

template<typename F>
auto Actor(F create_func){
  return Actor_t<F>{create_func};
}
}
