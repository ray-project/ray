// TODO: code generation

// 0 args
template <typename R>
RayObject<R> Ray::call(R (*func)()) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::wrap(packer);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(exec_function<R>);
  auto id = _impl->call(ptr, buffer);
  return RayObject<R>(std::move(*id));
}

// 1 args
template <typename R, typename T1>
RayObject<R> Ray::call(R (*func)(T1), T1 arg1) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::wrap(packer, false, arg1);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(exec_function<R, T1>);
  auto id = _impl->call(ptr, buffer);
  return RayObject<R>(std::move(*id));
}

template <typename R, typename T1>
RayObject<R> Ray::call(R (*func)(T1), RayObject<T1> &arg1) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::wrap(packer, true, arg1);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(exec_function<R, T1>);
  auto id = _impl->call(ptr, buffer);
  return RayObject<R>(std::move(*id));
}

// 2 args
template <typename R, typename T1, typename T2>
RayObject<R> Ray::call(R (*func)(T1, T2), T1 arg1, T2 arg2) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::wrap(packer, false, arg1, false, arg2);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(exec_function<R, T1, T2>);
  auto id = _impl->call(ptr, buffer);
  return RayObject<R>(std::move(*id));
}

template <typename R, typename T1, typename T2>
RayObject<R> Ray::call(R (*func)(T1, T2), RayObject<T1> &arg1, T2 arg2) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::wrap(packer, true, arg1, false, arg2);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(exec_function<R, T1, T2>);
  auto id = _impl->call(ptr, buffer);
  return RayObject<R>(std::move(*id));
}

template <typename R, typename T1, typename T2>
RayObject<R> Ray::call(R (*func)(T1, T2), T1 arg1, RayObject<T2> &arg2) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::wrap(packer, false, arg1, true, arg2);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(exec_function<R, T1, T2>);
  auto id = _impl->call(ptr, buffer);
  return RayObject<R>(std::move(*id));
}

template <typename R, typename T1, typename T2>
RayObject<R> Ray::call(R (*func)(T1, T2), RayObject<T1> &arg1, RayObject<T2> &arg2) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::wrap(packer, true, arg1, true, arg2);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(exec_function<R, T1, T2>);
  auto id = _impl->call(ptr, buffer);
  return RayObject<R>(std::move(*id));
}