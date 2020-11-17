
#pragma once

#include <ray/api/object_ref.h>
#include <ray/api/serializer.h>
#include "ray/common/task/task_util.h"

#include <msgpack.hpp>

namespace ray {
namespace api {

class Arguments {
 public:
  static void WrapArgs(std::vector<std::unique_ptr<::ray::TaskArg>> *task_args);

  template <typename Arg1Type>
  static void WrapArgs(std::vector<std::unique_ptr<::ray::TaskArg>> *task_args,
                       Arg1Type &arg1);

  template <typename Arg1Type>
  static void WrapArgs(std::vector<std::unique_ptr<::ray::TaskArg>> *task_args,
                       ObjectRef<Arg1Type> &arg1);

  template <typename Arg1Type, typename... OtherArgTypes>
  static void WrapArgs(std::vector<std::unique_ptr<::ray::TaskArg>> *task_args,
                       Arg1Type &arg1, OtherArgTypes &... args);

  static void UnwrapArgs(const std::vector<std::shared_ptr<RayObject>> &args_buffer,
                         int &arg_index);

  template <typename Arg1Type>
  static void UnwrapArgs(const std::vector<std::shared_ptr<RayObject>> &args_buffer,
                         int &arg_index, std::shared_ptr<Arg1Type> *arg1);

  template <typename Arg1Type, typename... OtherArgTypes>
  static void UnwrapArgs(const std::vector<std::shared_ptr<RayObject>> &args_buffer,
                         int &arg_index, std::shared_ptr<Arg1Type> *arg1,
                         std::shared_ptr<OtherArgTypes> *... args);
};

// --------- inline implementation ------------
#include <typeinfo>

inline void Arguments::WrapArgs(std::vector<std::unique_ptr<::ray::TaskArg>> *task_args) {
}

template <typename Arg1Type>
inline void Arguments::WrapArgs(std::vector<std::unique_ptr<::ray::TaskArg>> *task_args,
                                Arg1Type &arg1) {
  /// TODO(Guyang Song): optimize the memory copy.
  msgpack::sbuffer buffer;
  msgpack::packer<msgpack::sbuffer> packer(buffer);
  /// Notice ObjectRefClassPrefix should be modified by ObjectRef class name or namespace.
  static const std::string ObjectRefClassPrefix = "N3ray3api9ObjectRef";
  std::string type_name = typeid(arg1).name();
  RAY_CHECK(type_name.rfind(ObjectRefClassPrefix, 0) != 0)
      << "ObjectRef can not be wrapped";
  Serializer::Serialize(packer, arg1);
  auto memory_buffer = std::make_shared<::ray::LocalMemoryBuffer>(
      reinterpret_cast<uint8_t *>(buffer.data()), buffer.size(), true);
  /// Pass by value.
  auto task_arg = new TaskArgByValue(std::make_shared<::ray::RayObject>(
      memory_buffer, nullptr, std::vector<ObjectID>()));
  task_args->emplace_back(task_arg);
}

template <typename Arg1Type>
inline void Arguments::WrapArgs(std::vector<std::unique_ptr<::ray::TaskArg>> *task_args,
                                ObjectRef<Arg1Type> &arg1) {
  /// Pass by reference.
  auto task_arg = new TaskArgByReference(arg1.ID(), rpc::Address());
  task_args->emplace_back(task_arg);
}

template <typename Arg1Type, typename... OtherArgTypes>
inline void Arguments::WrapArgs(std::vector<std::unique_ptr<::ray::TaskArg>> *task_args,
                                Arg1Type &arg1, OtherArgTypes &... args) {
  WrapArgs(task_args, arg1);
  WrapArgs(task_args, args...);
}

inline void Arguments::UnwrapArgs(
    const std::vector<std::shared_ptr<RayObject>> &args_buffer, int &arg_index) {}

template <typename Arg1Type>
inline void Arguments::UnwrapArgs(
    const std::vector<std::shared_ptr<RayObject>> &args_buffer, int &arg_index,
    std::shared_ptr<Arg1Type> *arg1) {
  std::shared_ptr<msgpack::sbuffer> sbuffer;
  auto arg_buffer = args_buffer[arg_index]->GetData();
  sbuffer = std::make_shared<msgpack::sbuffer>(arg_buffer->Size());
  /// TODO(Guyang Song): Avoid the memory copy.
  sbuffer->write(reinterpret_cast<const char *>(arg_buffer->Data()), arg_buffer->Size());
  msgpack::unpacker unpacker;
  unpacker.reserve_buffer(sbuffer->size());
  memcpy(unpacker.buffer(), sbuffer->data(), sbuffer->size());
  unpacker.buffer_consumed(sbuffer->size());
  Serializer::Deserialize(unpacker, arg1);
  arg_index++;
}

template <typename Arg1Type, typename... OtherArgTypes>
inline void Arguments::UnwrapArgs(
    const std::vector<std::shared_ptr<RayObject>> &args_buffer, int &arg_index,
    std::shared_ptr<Arg1Type> *arg1, std::shared_ptr<OtherArgTypes> *... args) {
  UnwrapArgs(args_buffer, arg_index, arg1);
  UnwrapArgs(args_buffer, arg_index, args...);
}

}  // namespace api
}  // namespace ray