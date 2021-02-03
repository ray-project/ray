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

#include <boost/parameter.hpp>
#include <type_traits>
#include <string>

namespace ray {

// Define init arguments tag name.
BOOST_PARAMETER_KEYWORD(tag, num_cpus);
BOOST_PARAMETER_KEYWORD(tag, num_gpus);
BOOST_PARAMETER_KEYWORD(tag, object_store_memory);
BOOST_PARAMETER_KEYWORD(tag, ignore_reinit_error);
BOOST_PARAMETER_KEYWORD(tag, ray_address);

struct ray_conf {
  int num_cpus_ = 1;
  int num_gpus_ = 1;
  int object_store_memory_ = 16000;
  bool ignore_reinit_error_ = true;
  std::string ray_address_ = "auto";
};

namespace {
inline static void init_arg(
    ray_conf &conf,
    boost::parameter::aux::tagged_argument<ray::tag::num_cpus, const int> arg) {
  conf.num_cpus_ = arg[num_cpus];
}

inline static void init_arg(
    ray_conf &conf,
    boost::parameter::aux::tagged_argument<ray::tag::num_gpus, const int> arg) {
  conf.num_gpus_ = arg[num_gpus];
}

inline static void
init_arg(ray_conf &conf,
         boost::parameter::aux::tagged_argument<ray::tag::object_store_memory,
                                                const int>
             arg) {
  conf.object_store_memory_ = arg[object_store_memory];
}

inline static void
init_arg(ray_conf &conf,
         boost::parameter::aux::tagged_argument<ray::tag::ignore_reinit_error,
                                                const bool>
             arg) {
  conf.ignore_reinit_error_ = arg[ignore_reinit_error];
}

inline static void init_arg(
    ray_conf &conf,
    boost::parameter::aux::tagged_argument<ray::tag::ray_address, std::string>
        arg) {
  conf.ray_address_ = arg[ray_address];
}

inline static void
init_arg(ray_conf &conf,
         boost::parameter::aux::tagged_argument<ray::tag::ray_address,
                                                const char *const>
             arg) {
  conf.ray_address_ = arg[ray_address];
}

template <size_t N>
inline static void init_arg(
    ray_conf &conf,
    boost::parameter::aux::tagged_argument<ray::tag::ray_address, const char[N]>
        arg) {
  conf.ray_address_ = arg[ray_address];
}

}

template <typename... Args> inline static bool Init(Args... args) {
  ray_conf conf{};
  (void)std::initializer_list<int>{(init_arg(conf, args), 0)...};
  return true;
}
}