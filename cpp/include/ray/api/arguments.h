
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
  static void WrapArgsImpl(std::vector<std::unique_ptr<::ray::TaskArg>> *task_args,
                           ArgType &arg) {
    static_assert(!is_object_ref<ArgType>::value, "ObjectRef can not be wrapped");

    msgpack::sbuffer buffer = Serializer::Serialize(arg);
    auto memory_buffer = std::make_shared<::ray::LocalMemoryBuffer>(
        reinterpret_cast<uint8_t *>(buffer.data()), buffer.size(), true);
    /// Pass by value.
    auto task_arg = new TaskArgByValue(std::make_shared<::ray::RayObject>(
        memory_buffer, nullptr, std::vector<ObjectID>()));
    task_args->emplace_back(task_arg);
  }

  template <typename ArgType>
  static void WrapArgsImpl(std::vector<std::unique_ptr<::ray::TaskArg>> *task_args,
                           ObjectRef<ArgType> &arg) {
    /// Pass by reference.
    auto task_arg = new TaskArgByReference(arg.ID(), rpc::Address());
    task_args->emplace_back(task_arg);
  }

  template <typename... OtherArgTypes>
  static void WrapArgs(std::vector<std::unique_ptr<::ray::TaskArg>> *task_args,
                       OtherArgTypes &... args) {
    (void)std::initializer_list<int>{(WrapArgsImpl(task_args, args), 0)...};
    /// Silence gcc warning error.
    (void)task_args;
  }

  template <typename ArgType>
  static void UnwrapArgsImpl(const std::vector<std::shared_ptr<RayObject>> &args_buffer,
                             int &arg_index, std::shared_ptr<ArgType> *arg) {
    auto arg_buffer = args_buffer[arg_index]->GetData();
    *arg = Serializer::Deserialize<std::shared_ptr<ArgType>>(
        (const char *)arg_buffer->Data(), arg_buffer->Size());

    arg_index++;
  }

  template <typename... OtherArgTypes>
  static void UnwrapArgs(const std::vector<std::shared_ptr<RayObject>> &args_buffer,
                         int &arg_index, std::shared_ptr<OtherArgTypes> *... args) {
    (void)std::initializer_list<int>{
        (UnwrapArgsImpl(args_buffer, arg_index, args), 0)...};
  }
};

}  // namespace api
}  // namespace ray