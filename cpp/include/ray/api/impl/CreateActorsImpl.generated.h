// TODO: code generation

// 0 args
template <typename T>
RayActor<T> Ray::create(T *(*func)()) {
  ::ray::binary_writer writer;
  Arguments::wrap(writer);
  std::vector< ::ray::blob> data;
  writer.get_buffers(data);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(exec_function<T *>);
  auto id = _impl->create(ptr, std::move(data));
  return RayActor<T>(std::move(*id));
}

// 1 args
template <typename T, typename T1>
RayActor<T> Ray::create(T *(*func)(T1), T1 arg1) {
  ::ray::binary_writer writer;
  FunctionArgument<T1> funcArg1(false, arg1);
  Arguments::wrap(writer, funcArg1);
  std::vector< ::ray::blob> blobs;
  writer.get_buffers(blobs);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(exec_function<T *, T1>);
  auto id = _impl->create(ptr, std::move(blobs));
  return RayActor<T>(std::move(*id));
}

template <typename T, typename T1>
RayActor<T> Ray::create(T *(*func)(T1), RayObject<T1> &arg1) {
  ::ray::binary_writer writer;
  FunctionArgument<RayObject<T1> > funcArg1(true, arg1);
  Arguments::wrap(writer, funcArg1);
  std::vector< ::ray::blob> blobs;
  writer.get_buffers(blobs);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(exec_function<T *, T1>);
  auto id = _impl->create(ptr, std::move(blobs));
  return RayActor<T>(std::move(*id));
}

// 2 args
template <typename T, typename T1, typename T2>
RayActor<T> Ray::create(T *(*func)(T1, T2), T1 arg1, T2 arg2) {
  ::ray::binary_writer writer;
  FunctionArgument<T1> funcArg1(false, arg1);
  FunctionArgument<T2> funcArg2(false, arg2);
  Arguments::wrap(writer, funcArg1, funcArg2);
  std::vector< ::ray::blob> blobs;
  writer.get_buffers(blobs);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(exec_function<T *, T1, T2>);
  auto id = _impl->create(ptr, std::move(blobs));
  return RayActor<T>(std::move(*id));
}

template <typename T, typename T1, typename T2>
RayActor<T> Ray::create(T *(*func)(T1, T2), RayObject<T1> &arg1, T2 arg2) {
  ::ray::binary_writer writer;
  FunctionArgument<RayObject<T1> > funcArg1(true, arg1);
  FunctionArgument<T2> funcArg2(false, arg2);
  Arguments::wrap(writer, funcArg1, funcArg2);
  std::vector< ::ray::blob> blobs;
  writer.get_buffers(blobs);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(exec_function<T *, T1, T2>);
  auto id = _impl->create(ptr, std::move(blobs));
  return RayActor<T>(std::move(*id));
}

template <typename T, typename T1, typename T2>
RayActor<T> Ray::create(T *(*func)(T1, T2), T1 arg1, RayObject<T2> &arg2) {
  ::ray::binary_writer writer;
  FunctionArgument<T1> funcArg1(false, arg1);
  FunctionArgument<RayObject<T2> > funcArg2(true, arg2);
  Arguments::wrap(writer, funcArg1, funcArg2);
  std::vector< ::ray::blob> blobs;
  writer.get_buffers(blobs);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(exec_function<T *, T1, T2>);
  auto id = _impl->create(ptr, std::move(blobs));
  return RayActor<T>(std::move(*id));
}

template <typename T, typename T1, typename T2>
RayActor<T> Ray::create(T *(*func)(T1, T2), RayObject<T1> &arg1, RayObject<T2> &arg2) {
  ::ray::binary_writer writer;
  FunctionArgument<RayObject<T1> > funcArg1(true, arg1);
  FunctionArgument<RayObject<T2> > funcArg2(true, arg2);
  Arguments::wrap(writer, funcArg1, funcArg2);
  std::vector< ::ray::blob> blobs;
  writer.get_buffers(blobs);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(exec_function<T *, T1, T2>);
  auto id = _impl->create(ptr, std::move(blobs));
  return RayActor<T>(std::move(*id));
}