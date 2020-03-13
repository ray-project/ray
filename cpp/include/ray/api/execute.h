#pragma once
#include <ray/api/arguments.h>
#include <ray/api/ray_object.h>
#include <ray/core.h>

namespace ray { namespace api {

// class Arguments;

// 0 args
template <typename RT>
std::shared_ptr<msgpack::sbuffer> exec_function(uintptr_t base_addr, int32_t func_offset,
                                                std::shared_ptr<msgpack::sbuffer> args) {
  RT rt;
  typedef RT (*FUNC)();
  FUNC func = (FUNC)(base_addr + func_offset);
  rt = (*func)();

  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::wrap(packer, rt);
  return buffer;
}

// 1 args
template <typename RT, typename T1>
std::shared_ptr<msgpack::sbuffer> exec_function(uintptr_t base_addr, int32_t func_offset,
                                                std::shared_ptr<msgpack::sbuffer> args) {
  RT rt;
  T1 t1;
  msgpack::unpacker unpacker;
  unpacker.reserve_buffer(args->size());
  memcpy(unpacker.buffer(), args->data(), args->size());
  unpacker.buffer_consumed(args->size());

  bool rayObjectFlag1;
  Arguments::unwrap(unpacker, rayObjectFlag1);
  if (rayObjectFlag1) {
    RayObject<T1> rayObject1;
    Arguments::unwrap(unpacker, rayObject1);
    t1 = *rayObject1.get();
  } else {
    Arguments::unwrap(unpacker, t1);
  }
  typedef RT (*FUNC)(T1);
  FUNC func = (FUNC)(base_addr + func_offset);
  rt = (*func)(t1);

  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::wrap(packer, rt);
  return buffer;
}

// 2 args
template <typename RT, typename T1, typename T2>
std::shared_ptr<msgpack::sbuffer> exec_function(uintptr_t base_addr, int32_t func_offset,
                                                std::shared_ptr<msgpack::sbuffer> args,
                                                TaskType type) {
  RT rt;
  T1 t1;
  T2 t2;
  msgpack::unpacker unpacker;
  unpacker.reserve_buffer(args->size());
  memcpy(unpacker.buffer(), args->data(), args->size());
  unpacker.buffer_consumed(args->size());

  bool rayObjectFlag1;
  bool rayObjectFlag2;
  Arguments::unwrap(unpacker, rayObjectFlag1);
  if (rayObjectFlag1) {
    RayObject<T1> rayObject1;
    Arguments::unwrap(unpacker, rayObject1);
    t1 = *rayObject1.get();
  } else {
    Arguments::unwrap(unpacker, t1);
  }
  Arguments::unwrap(unpacker, rayObjectFlag2);
  if (rayObjectFlag2) {
    RayObject<T2> rayObject2;
    Arguments::unwrap(unpacker, rayObject2);
    t2 = *rayObject2.get();
  } else {
    Arguments::unwrap(unpacker, t2);
  }
  typedef RT (*FUNC)(T1, T2);
  FUNC func = (FUNC)(base_addr + func_offset);
  rt = (*func)(t1, t2);

  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::wrap(packer, rt);
  return buffer;
}

// 0 args
template <typename RT>
std::shared_ptr<msgpack::sbuffer> create_actor_exec_function(
    uintptr_t base_addr, int32_t func_offset, std::shared_ptr<msgpack::sbuffer> args) {
  RT rt;
  typedef RT (*FUNC)();
  FUNC func = (FUNC)(base_addr + func_offset);
  rt = (*func)();

  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::wrap(packer, (uintptr_t)(rt));
  return buffer;
}

// 1 args
template <typename RT, typename T1>
std::shared_ptr<msgpack::sbuffer> create_actor_exec_function(
    uintptr_t base_addr, int32_t func_offset, std::shared_ptr<msgpack::sbuffer> args) {
  RT rt;
  T1 t1;
  msgpack::unpacker unpacker;
  unpacker.reserve_buffer(args->size());
  memcpy(unpacker.buffer(), args->data(), args->size());
  unpacker.buffer_consumed(args->size());

  bool rayObjectFlag1;
  Arguments::unwrap(unpacker, rayObjectFlag1);
  if (rayObjectFlag1) {
    RayObject<T1> rayObject1;
    Arguments::unwrap(unpacker, rayObject1);
    t1 = *rayObject1.get();
  } else {
    Arguments::unwrap(unpacker, t1);
  }
  typedef RT (*FUNC)(T1);
  FUNC func = (FUNC)(base_addr + func_offset);
  rt = (*func)(t1);

  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::wrap(packer, (uintptr_t)(rt));
  return buffer;
}

// 2 args
template <typename RT, typename T1, typename T2>
std::shared_ptr<msgpack::sbuffer> create_actor_exec_function(
    uintptr_t base_addr, int32_t func_offset, std::shared_ptr<msgpack::sbuffer> args) {
  RT rt;
  T1 t1;
  T2 t2;
  msgpack::unpacker unpacker;
  unpacker.reserve_buffer(args->size());
  memcpy(unpacker.buffer(), args->data(), args->size());
  unpacker.buffer_consumed(args->size());

  bool rayObjectFlag1;
  bool rayObjectFlag2;
  Arguments::unwrap(unpacker, rayObjectFlag1);
  if (rayObjectFlag1) {
    RayObject<T1> rayObject1;
    Arguments::unwrap(unpacker, rayObject1);
    t1 = *rayObject1.get();
  } else {
    Arguments::unwrap(unpacker, t1);
  }
  Arguments::unwrap(unpacker, rayObjectFlag2);
  if (rayObjectFlag2) {
    RayObject<T2> rayObject2;
    Arguments::unwrap(unpacker, rayObject2);
    t2 = *rayObject2.get();
  } else {
    Arguments::unwrap(unpacker, t2);
  }
  typedef RT (*FUNC)(T1, T2);
  FUNC func = (FUNC)(base_addr + func_offset);
  rt = (*func)(t1, t2);

  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::wrap(packer, (uintptr_t)(rt));
  return buffer;
}

// 0 args
template <typename RT, typename O>
std::shared_ptr<msgpack::sbuffer> actor_exec_function(
    uintptr_t base_addr, int32_t func_offset, std::shared_ptr<msgpack::sbuffer> args,
    std::shared_ptr<msgpack::sbuffer> object) {
  msgpack::unpacker actor_unpacker;
  actor_unpacker.reserve_buffer(object->size());
  memcpy(actor_unpacker.buffer(), object->data(), object->size());
  actor_unpacker.buffer_consumed(object->size());
  uintptr_t actor_ptr;
  Arguments::unwrap(actor_unpacker, actor_ptr);
  O *actor_object = (O *)actor_ptr;

  RT rt;
  typedef RT (O::*FUNC)();
  member_function_ptr_holder holder;
  holder.value[0] = base_addr + func_offset;
  holder.value[1] = 0;
  FUNC func = *((FUNC *)&holder);
  rt = (actor_object->*func)();

  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::wrap(packer, rt);
  return buffer;
}

// 1 args
template <typename RT, typename O, typename T1>
std::shared_ptr<msgpack::sbuffer> actor_exec_function(
    uintptr_t base_addr, int32_t func_offset, std::shared_ptr<msgpack::sbuffer> args,
    std::shared_ptr<msgpack::sbuffer> object) {
  msgpack::unpacker actor_unpacker;
  actor_unpacker.reserve_buffer(object->size());
  memcpy(actor_unpacker.buffer(), object->data(), object->size());
  actor_unpacker.buffer_consumed(object->size());
  uintptr_t actor_ptr;
  Arguments::unwrap(actor_unpacker, actor_ptr);
  O *actor_object = (O *)actor_ptr;

  RT rt;
  T1 t1;
  msgpack::unpacker unpacker;
  unpacker.reserve_buffer(args->size());
  memcpy(unpacker.buffer(), args->data(), args->size());
  unpacker.buffer_consumed(args->size());

  bool rayObjectFlag1;
  Arguments::unwrap(unpacker, rayObjectFlag1);
  if (rayObjectFlag1) {
    RayObject<T1> rayObject1;
    Arguments::unwrap(unpacker, rayObject1);
    t1 = *rayObject1.get();
  } else {
    Arguments::unwrap(unpacker, t1);
  }
  typedef RT (O::*FUNC)(T1);
  member_function_ptr_holder holder;
  holder.value[0] = base_addr + func_offset;
  holder.value[1] = 0;
  FUNC func = *((FUNC *)&holder);
  rt = (actor_object->*func)(t1);

  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::wrap(packer, rt);
  return buffer;
}

// 2 args
template <typename RT, typename O, typename T1, typename T2>
std::shared_ptr<msgpack::sbuffer> actor_exec_function(
    uintptr_t base_addr, int32_t func_offset, std::shared_ptr<msgpack::sbuffer> args,
    std::shared_ptr<msgpack::sbuffer> object) {
  msgpack::unpacker actor_unpacker;
  actor_unpacker.reserve_buffer(object->size());
  memcpy(actor_unpacker.buffer(), object->data(), object->size());
  actor_unpacker.buffer_consumed(object->size());
  uintptr_t actor_ptr;
  Arguments::unwrap(actor_unpacker, actor_ptr);
  O *actor_object = (O *)actor_ptr;

  RT rt;
  T1 t1;
  T2 t2;
  msgpack::unpacker unpacker;
  unpacker.reserve_buffer(args->size());
  memcpy(unpacker.buffer(), args->data(), args->size());
  unpacker.buffer_consumed(args->size());

  bool rayObjectFlag1;
  bool rayObjectFlag2;
  Arguments::unwrap(unpacker, rayObjectFlag1);
  if (rayObjectFlag1) {
    RayObject<T1> rayObject1;
    Arguments::unwrap(unpacker, rayObject1);
    t1 = *rayObject1.get();
  } else {
    Arguments::unwrap(unpacker, t1);
  }
  Arguments::unwrap(unpacker, rayObjectFlag2);
  if (rayObjectFlag2) {
    RayObject<T2> rayObject2;
    Arguments::unwrap(unpacker, rayObject2);
    t2 = *rayObject2.get();
  } else {
    Arguments::unwrap(unpacker, t2);
  }
  typedef RT (O::*FUNC)(T1, T2);
  member_function_ptr_holder holder;
  holder.value[0] = base_addr + func_offset;
  holder.value[1] = 0;
  FUNC func = *((FUNC *)&holder);
  rt = (actor_object->*func)(t1, t2);

  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::wrap(packer, rt);
  return buffer;
}
}  }// namespace ray::api