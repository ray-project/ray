// TODO(Guyang Song): code generation

// 0 args
template <typename R>
RayObject<R> Ray::Call(Func0<R> func) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::Wrap(packer);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(exec_function<R>);
  auto id = _impl->Call(ptr, buffer);
  return RayObject<R>(std::move(id));
}

// 1 args
template <typename R, typename T1>
RayObject<R> Ray::Call(Func1<R, T1> func, T1 arg1) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::Wrap(packer, false, arg1);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(exec_function<R, T1>);
  auto id = _impl->Call(ptr, buffer);
  return RayObject<R>(std::move(id));
}

template <typename R, typename T1>
RayObject<R> Ray::Call(Func1<R, T1> func, RayObject<T1> &arg1) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::Wrap(packer, true, arg1);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(exec_function<R, T1>);
  auto id = _impl->Call(ptr, buffer);
  return RayObject<R>(std::move(id));
}

// 2 args
template <typename R, typename T1, typename T2>
RayObject<R> Ray::Call(Func2<R, T1, T2> func, T1 arg1, T2 arg2) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::Wrap(packer, false, arg1, false, arg2);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(exec_function<R, T1, T2>);
  auto id = _impl->Call(ptr, buffer);
  return RayObject<R>(std::move(id));
}

template <typename R, typename T1, typename T2>
RayObject<R> Ray::Call(Func2<R, T1, T2> func, RayObject<T1> &arg1, T2 arg2) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::Wrap(packer, true, arg1, false, arg2);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(exec_function<R, T1, T2>);
  auto id = _impl->Call(ptr, buffer);
  return RayObject<R>(std::move(id));
}

template <typename R, typename T1, typename T2>
RayObject<R> Ray::Call(Func2<R, T1, T2> func, T1 arg1, RayObject<T2> &arg2) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::Wrap(packer, false, arg1, true, arg2);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(exec_function<R, T1, T2>);
  auto id = _impl->Call(ptr, buffer);
  return RayObject<R>(std::move(id));
}

template <typename R, typename T1, typename T2>
RayObject<R> Ray::Call(Func2<R, T1, T2> func, RayObject<T1> &arg1, RayObject<T2> &arg2) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::Wrap(packer, true, arg1, true, arg2);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(exec_function<R, T1, T2>);
  auto id = _impl->Call(ptr, buffer);
  return RayObject<R>(std::move(id));
}