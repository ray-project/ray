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

template <typename T> struct ObjectRef {

  auto Get() const { return GetImpl<T>(); }

private:
  template <typename R> std::enable_if_t<std::is_void<R>::value> GetImpl() const {}

  template <typename R> std::enable_if_t<!std::is_void<R>::value, T> GetImpl() const {
    return T{};
  }
};

template <typename T>
inline static ObjectRef<T> Put(const T &obj){
  return {};
}

template <typename T>
inline static ObjectRef<T> Get(const ObjectRef<T> &object){
  return {};
}
}