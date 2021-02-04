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

  template<typename... Args>
  auto Remote(Args... args){
    //TODO
    //send to the remote node
    using R = typename function_traits<F>::return_type;
    return ObjectRef<R>{};
  }

  const T& t_;
  F f_;
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
