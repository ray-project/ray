#pragma once
#include <ray/api/RayObject.h>
#include "impl/Arguments.h"

namespace ray {

//class Arguments;

// 0 args
template <typename RT>
std::vector< ::ray::blob> exec_function(uintptr_t base_addr, int32_t func_offset,
                                        const ::ray::blob &args) {
  RT rt;
  typedef RT (*FUNC)();
  FUNC func = (FUNC)(base_addr + func_offset);
  rt = (*func)();

  ::ray::binary_writer writer;
  Arguments::wrap(writer, rt);
  std::vector< ::ray::blob> blobs;
  writer.get_buffers(blobs);
  return blobs;
}

// 1 args
template <typename RT, typename T1>
std::vector< ::ray::blob> exec_function(uintptr_t base_addr, int32_t func_offset,
                                        const ::ray::blob &args) {
  RT rt;
  T1 t1;
  bool rayObjectFlag1;
  ::ray::binary_reader reader(args);
  Arguments::unwrap(reader, rayObjectFlag1);
  if (rayObjectFlag1) {
    RayObject<T1> rayObject1;
    Arguments::unwrap(reader, rayObject1);
    t1 = *rayObject1.get();
  } else {
    Arguments::unwrap(reader, t1);
  }
  typedef RT (*FUNC)(T1);
  FUNC func = (FUNC)(base_addr + func_offset);
  rt = (*func)(t1);

  ::ray::binary_writer writer;
  Arguments::wrap(writer, rt);
  std::vector< ::ray::blob> blobs;
  writer.get_buffers(blobs);
  return blobs;
}

// 2 args
template <typename RT, typename T1, typename T2>
std::vector< ::ray::blob> exec_function(uintptr_t base_addr, int32_t func_offset,
                                        const ::ray::blob &args) {
  RT rt;
  T1 t1;
  T2 t2;
  bool rayObjectFlag1;
  bool rayObjectFlag2;
  ::ray::binary_reader reader(args);
  Arguments::unwrap(reader, rayObjectFlag1);
  if (rayObjectFlag1) {
    RayObject<T1> rayObject1;
    Arguments::unwrap(reader, rayObject1);
    t1 = *rayObject1.get();
  } else {
    Arguments::unwrap(reader, t1);
  }
  Arguments::unwrap(reader, rayObjectFlag2);
  if (rayObjectFlag2) {
    RayObject<T2> rayObject2;
    Arguments::unwrap(reader, rayObject2);
    t2 = *rayObject2.get();
  } else {
    Arguments::unwrap(reader, t2);
  }
  typedef RT (*FUNC)(T1, T2);
  FUNC func = (FUNC)(base_addr + func_offset);
  rt = (*func)(t1, t2);

  ::ray::binary_writer writer;
  Arguments::wrap(writer, rt);
  std::vector< ::ray::blob> blobs;
  writer.get_buffers(blobs);
  return blobs;
}

// 0 args
template <typename RT, typename O>
std::vector< ::ray::blob> actor_exec_function(uintptr_t base_addr, int32_t func_offset,
                                              const ::ray::blob &args,
                                              ::ray::blob &object) {
  RT rt;
  O *actor_object = (O *)*(uint64_t *)object.data();
  typedef RT (O::*FUNC)();
  member_function_ptr_holder holder;
  holder.value[0] = base_addr + func_offset;
  holder.value[1] = 0;
  FUNC func = *((FUNC *)&holder);
  rt = (actor_object->*func)();

  ::ray::binary_writer writer;
  Arguments::wrap(writer, rt);
  std::vector< ::ray::blob> blobs;
  writer.get_buffers(blobs);
  return blobs;
}

// 1 args
template <typename RT, typename O, typename T1>
std::vector< ::ray::blob> actor_exec_function(uintptr_t base_addr, int32_t func_offset,
                                              const ::ray::blob &args,
                                              ::ray::blob &object) {
  RT rt;
  O *actor_object = (O *)*(uint64_t *)object.data();
  T1 t1;
  bool rayObjectFlag1;
  ::ray::binary_reader reader(args);
  Arguments::unwrap(reader, rayObjectFlag1);
  if (rayObjectFlag1) {
    RayObject<T1> rayObject1;
    Arguments::unwrap(reader, rayObject1);
    t1 = *rayObject1.get();
  } else {
    Arguments::unwrap(reader, t1);
  }
  typedef RT (O::*FUNC)(T1);
  member_function_ptr_holder holder;
  holder.value[0] = base_addr + func_offset;
  holder.value[1] = 0;
  FUNC func = *((FUNC *)&holder);
  rt = (actor_object->*func)(t1);

  ::ray::binary_writer writer;
  Arguments::wrap(writer, rt);
  std::vector< ::ray::blob> blobs;
  writer.get_buffers(blobs);
  return blobs;
}

// 2 args
template <typename RT, typename O, typename T1, typename T2>
std::vector< ::ray::blob> actor_exec_function(uintptr_t base_addr, int32_t func_offset,
                                              const ::ray::blob &args,
                                              ::ray::blob &object) {
  RT rt;
  O *actor_object = (O *)*(uint64_t *)object.data();
  T1 t1;
  T2 t2;
  bool rayObjectFlag1;
  bool rayObjectFlag2;
  ::ray::binary_reader reader(args);
  Arguments::unwrap(reader, rayObjectFlag1);
  if (rayObjectFlag1) {
    RayObject<T1> rayObject1;
    Arguments::unwrap(reader, rayObject1);
    t1 = *rayObject1.get();
  } else {
    Arguments::unwrap(reader, t1);
  }
  Arguments::unwrap(reader, rayObjectFlag2);
  if (rayObjectFlag2) {
    RayObject<T2> rayObject2;
    Arguments::unwrap(reader, rayObject2);
    t2 = *rayObject2.get();
  } else {
    Arguments::unwrap(reader, t2);
  }
  typedef RT (O::*FUNC)(T1, T2);
  member_function_ptr_holder holder;
  holder.value[0] = base_addr + func_offset;
  holder.value[1] = 0;
  FUNC func = *((FUNC *)&holder);
  rt = (actor_object->*func)(t1, t2);

  ::ray::binary_writer writer;
  Arguments::wrap(writer, rt);
  std::vector< ::ray::blob> blobs;
  writer.get_buffers(blobs);
  return blobs;
}
}