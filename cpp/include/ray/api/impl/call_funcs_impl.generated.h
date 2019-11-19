// TODO: code generation

// 0 args
template <typename R>
RayObject<R> Ray::call(R (*func)()) {
  ::ray::binary_writer writer;
  Arguments::wrap(writer);
  std::vector< ::ray::blob> blobs;
  writer.get_buffers(blobs);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(exec_function<R>);
  auto id = _impl->call(ptr, std::move(blobs));
  return RayObject<R>(std::move(*id));
}

// 1 args
template <typename R, typename T1>
RayObject<R> Ray::call(R (*func)(T1), T1 arg1) {
  ::ray::binary_writer writer;
  FunctionArgument<T1> funcArg1(false, arg1);
  Arguments::wrap(writer, funcArg1);
  std::vector< ::ray::blob> blobs;
  writer.get_buffers(blobs);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(exec_function<R, T1>);
  auto id = _impl->call(ptr, std::move(blobs));
  return RayObject<R>(std::move(*id));
}

template <typename R, typename T1>
RayObject<R> Ray::call(R (*func)(T1), RayObject<T1> &arg1) {
  ::ray::binary_writer writer;
  FunctionArgument<RayObject<T1> > funcArg1(true, arg1);
  Arguments::wrap(writer, funcArg1);
  std::vector< ::ray::blob> blobs;
  writer.get_buffers(blobs);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(exec_function<R, T1>);
  auto id = _impl->call(ptr, std::move(blobs));
  return RayObject<R>(std::move(*id));
}

// 2 args
template <typename R, typename T1, typename T2>
RayObject<R> Ray::call(R (*func)(T1, T2), T1 arg1, T2 arg2) {
  ::ray::binary_writer writer;
  FunctionArgument<T1> funcArg1(false, arg1);
  FunctionArgument<T2> funcArg2(false, arg2);
  Arguments::wrap(writer, funcArg1, funcArg2);
  std::vector< ::ray::blob> blobs;
  writer.get_buffers(blobs);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(exec_function<R, T1, T2>);
  auto id = _impl->call(ptr, std::move(blobs));
  return RayObject<R>(std::move(*id));
}

template <typename R, typename T1, typename T2>
RayObject<R> Ray::call(R (*func)(T1, T2), RayObject<T1> &arg1, T2 arg2) {
  ::ray::binary_writer writer;
  FunctionArgument<RayObject<T1> > funcArg1(true, arg1);
  FunctionArgument<T2> funcArg2(false, arg2);
  Arguments::wrap(writer, funcArg1, funcArg2);
  std::vector< ::ray::blob> blobs;
  writer.get_buffers(blobs);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(exec_function<R, T1, T2>);
  auto id = _impl->call(ptr, std::move(blobs));
  return RayObject<R>(std::move(*id));
}

template <typename R, typename T1, typename T2>
RayObject<R> Ray::call(R (*func)(T1, T2), T1 arg1, RayObject<T2> &arg2) {
  ::ray::binary_writer writer;
  FunctionArgument<T1> funcArg1(false, arg1);
  FunctionArgument<RayObject<T2> > funcArg2(true, arg2);
  Arguments::wrap(writer, funcArg1, funcArg2);
  std::vector< ::ray::blob> blobs;
  writer.get_buffers(blobs);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(exec_function<R, T1, T2>);
  auto id = _impl->call(ptr, std::move(blobs));
  return RayObject<R>(std::move(*id));
}

template <typename R, typename T1, typename T2>
RayObject<R> Ray::call(R (*func)(T1, T2), RayObject<T1> &arg1, RayObject<T2> &arg2) {
  ::ray::binary_writer writer;
  FunctionArgument<RayObject<T1> > funcArg1(true, arg1);
  FunctionArgument<RayObject<T2> > funcArg2(true, arg2);
  Arguments::wrap(writer, funcArg1, funcArg2);
  std::vector< ::ray::blob> blobs;
  writer.get_buffers(blobs);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(exec_function<R, T1, T2>);
  auto id = _impl->call(ptr, std::move(blobs));
  return RayObject<R>(std::move(*id));
}