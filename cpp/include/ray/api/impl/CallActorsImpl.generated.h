// TODO: code generation

// 0 args
template <typename R, typename O>
RayObject<R> Ray::call(R (O::*func)(), RayActor<O> &actor) {
  member_function_ptr_holder holder = *(member_function_ptr_holder *)(&func);
  ::ray::binary_writer writer;
  Arguments::wrap(writer);
  std::vector< ::ray::blob> blobs;
  writer.get_buffers(blobs);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(holder.value[0]);
  ptr.value[1] = reinterpret_cast<uintptr_t>(actor_exec_function<R, O>);
  auto id = _impl->call(ptr, actor.id(), std::move(blobs));
  return RayObject<R>(std::move(*id));
}

// 1 args
template <typename R, typename O, typename T1>
RayObject<R> Ray::call(R (O::*func)(T1), RayActor<O> &actor, T1 arg1) {
  member_function_ptr_holder holder = *(member_function_ptr_holder *)(&func);
  ::ray::binary_writer writer;
  FunctionArgument<T1> funcArg1(false, arg1);
  Arguments::wrap(writer, funcArg1);
  std::vector< ::ray::blob> blobs;
  writer.get_buffers(blobs);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(holder.value[0]);
  ptr.value[1] = reinterpret_cast<uintptr_t>(actor_exec_function<R, O, T1>);
  auto id = _impl->call(ptr, actor.id(), std::move(blobs));
  return RayObject<R>(std::move(*id));
}

template <typename R, typename O, typename T1>
RayObject<R> Ray::call(R (O::*func)(T1), RayActor<O> &actor, RayObject<T1> &arg1) {
  member_function_ptr_holder holder = *(member_function_ptr_holder *)(&func);
  ::ray::binary_writer writer;
  FunctionArgument<RayObject<T1> > funcArg1(true, arg1);
  Arguments::wrap(writer, funcArg1);
  std::vector< ::ray::blob> blobs;
  writer.get_buffers(blobs);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(holder.value[0]);
  ptr.value[1] = reinterpret_cast<uintptr_t>(actor_exec_function<R, O, T1>);
  auto id = _impl->call(ptr, actor.id(), std::move(blobs));
  return RayObject<R>(std::move(*id));
}

// 2 args
template <typename R, typename O, typename T1, typename T2>
RayObject<R> Ray::call(R (O::*func)(T1, T2), RayActor<O> &actor, T1 arg1, T2 arg2) {
  member_function_ptr_holder holder = *(member_function_ptr_holder *)(&func);
  ::ray::binary_writer writer;
  FunctionArgument<T1> funcArg1(false, arg1);
  FunctionArgument<T2> funcArg2(false, arg2);
  Arguments::wrap(writer, funcArg1, funcArg2);
  std::vector< ::ray::blob> blobs;
  writer.get_buffers(blobs);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(holder.value[0]);
  ptr.value[1] = reinterpret_cast<uintptr_t>(actor_exec_function<R, O, T1, T2>);
  auto id = _impl->call(ptr, actor.id(), std::move(blobs));
  return RayObject<R>(std::move(*id));
}

template <typename R, typename O, typename T1, typename T2>
RayObject<R> Ray::call(R (O::*func)(T1, T2), RayActor<O> &actor, RayObject<T1> &arg1,
                       T2 arg2) {
  member_function_ptr_holder holder = *(member_function_ptr_holder *)(&func);
  ::ray::binary_writer writer;
  FunctionArgument<RayObject<T1> > funcArg1(true, arg1);
  FunctionArgument<T2> funcArg2(false, arg2);
  Arguments::wrap(writer, funcArg1, funcArg2);
  std::vector< ::ray::blob> blobs;
  writer.get_buffers(blobs);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(holder.value[0]);
  ptr.value[1] = reinterpret_cast<uintptr_t>(actor_exec_function<R, O, T1, T2>);
  auto id = _impl->call(ptr, actor.id(), std::move(blobs));
  return RayObject<R>(std::move(*id));
}

template <typename R, typename O, typename T1, typename T2>
RayObject<R> Ray::call(R (O::*func)(T1, T2), RayActor<O> &actor, T1 arg1,
                       RayObject<T2> &arg2) {
  member_function_ptr_holder holder = *(member_function_ptr_holder *)(&func);
  ::ray::binary_writer writer;
  FunctionArgument<T1> funcArg1(false, arg1);
  FunctionArgument<RayObject<T2> > funcArg2(true, arg2);
  Arguments::wrap(writer, funcArg1, funcArg2);
  std::vector< ::ray::blob> blobs;
  writer.get_buffers(blobs);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(holder.value[0]);
  ptr.value[1] = reinterpret_cast<uintptr_t>(actor_exec_function<R, O, T1, T2>);
  auto id = _impl->call(ptr, actor.id(), std::move(blobs));
  return RayObject<R>(std::move(*id));
}

template <typename R, typename O, typename T1, typename T2>
RayObject<R> Ray::call(R (O::*func)(T1, T2), RayActor<O> &actor, RayObject<T1> &arg1,
                       RayObject<T2> &arg2) {
  member_function_ptr_holder holder = *(member_function_ptr_holder *)(&func);
  ::ray::binary_writer writer;
  FunctionArgument<RayObject<T1> > funcArg1(true, arg1);
  FunctionArgument<RayObject<T2> > funcArg2(true, arg2);
  Arguments::wrap(writer, funcArg1, funcArg2);
  std::vector< ::ray::blob> blobs;
  writer.get_buffers(blobs);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(holder.value[0]);
  ptr.value[1] = reinterpret_cast<uintptr_t>(actor_exec_function<R, O, T1, T2>);
  auto id = _impl->call(ptr, actor.id(), std::move(blobs));
  return RayObject<R>(std::move(*id));
}
