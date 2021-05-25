
#pragma once

#include <ray/api/object_ref.h>
#include <ray/api/serializer.h>

#include <msgpack.hpp>

#include "ray/common/task/task_util.h"

namespace ray {
namespace api {

/// Check T is ObjectRef or not.
template <typename T>
struct is_object_ref : std::false_type {};

template <typename T>
struct is_object_ref<ObjectRef<T>> : std::true_type {};

class Arguments {
 public:
  template <typename ArgType>
  static void WrapArgsImpl(std::vector<ray::api::TaskArg> *task_args, ArgType &arg) {
    static_assert(!is_object_ref<ArgType>::value, "ObjectRef can not be wrapped");

    msgpack::sbuffer buffer = Serializer::Serialize(arg);
    ray::api::TaskArg task_arg;
    task_arg.buf = std::move(buffer);
    /// Pass by value.
    task_args->emplace_back(std::move(task_arg));
  }

  template <typename ArgType>
  static void WrapArgsImpl(std::vector<ray::api::TaskArg> *task_args,
                           ObjectRef<ArgType> &arg) {
    /// Pass by reference.
    ray::api::TaskArg task_arg{};
    task_arg.id = arg.ID();
    task_args->emplace_back(std::move(task_arg));
  }

  template <typename... OtherArgTypes>
  static void WrapArgs(std::vector<ray::api::TaskArg> *task_args,
                       OtherArgTypes &... args) {
    (void)std::initializer_list<int>{(WrapArgsImpl(task_args, args), 0)...};
    /// Silence gcc warning error.
    (void)task_args;
  }
};

}  // namespace api
}  // namespace ray