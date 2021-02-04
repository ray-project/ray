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

  auto Get() const {
    //TODO
    return t_; // Just for test.
  }

  T t_;
};

template <> struct ObjectRef<void> {
  void Get(){
    //TODO
  }
};

template <typename T>
static ObjectRef<T> Put(const T &obj){
  return {};
}

template <typename T>
static ObjectRef<T> Get(const ObjectRef<T> &object){
  return {};
}
}
