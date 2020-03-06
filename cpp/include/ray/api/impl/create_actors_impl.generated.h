// TODO: code generation

// 0 args
template <typename T>
RayActor<T> Ray::createActor(T *(*func)()) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::wrap(packer);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(create_actor_exec_function<T *>);
  auto id = _impl->create(ptr, buffer);
  return RayActor<T>(std::move(*id));
}

// 1 args
template <typename T, typename T1>
RayActor<T> Ray::createActor(T *(*func)(T1), T1 arg1) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::wrap(packer, false, arg1);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(create_actor_exec_function<T *, T1>);
  auto id = _impl->create(ptr, buffer);
  return RayActor<T>(std::move(*id));
}

template <typename T, typename T1>
RayActor<T> Ray::createActor(T *(*func)(T1), RayObject<T1> &arg1) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::wrap(packer, true, arg1);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(create_actor_exec_function<T *, T1>);
  auto id = _impl->create(ptr, buffer);
  return RayActor<T>(std::move(*id));
}

// 2 args
template <typename T, typename T1, typename T2>
RayActor<T> Ray::createActor(T *(*func)(T1, T2), T1 arg1, T2 arg2) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::wrap(packer, false, arg1, false, arg2);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(create_actor_exec_function<T *, T1, T2>);
  auto id = _impl->create(ptr, buffer);
  return RayActor<T>(std::move(*id));
}

template <typename T, typename T1, typename T2>
RayActor<T> Ray::createActor(T *(*func)(T1, T2), RayObject<T1> &arg1, T2 arg2) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::wrap(packer, true, arg1, false, arg2);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(create_actor_exec_function<T *, T1, T2>);
  auto id = _impl->create(ptr, buffer);
  return RayActor<T>(std::move(*id));
}

template <typename T, typename T1, typename T2>
RayActor<T> Ray::createActor(T *(*func)(T1, T2), T1 arg1, RayObject<T2> &arg2) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::wrap(packer, false, arg1, true, arg2);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(create_actor_exec_function<T *, T1, T2>);
  auto id = _impl->create(ptr, buffer);
  return RayActor<T>(std::move(*id));
}

template <typename T, typename T1, typename T2>
RayActor<T> Ray::createActor(T *(*func)(T1, T2), RayObject<T1> &arg1,
                             RayObject<T2> &arg2) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::wrap(packer, true, arg1, true, arg2);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(func);
  ptr.value[1] = reinterpret_cast<uintptr_t>(create_actor_exec_function<T *, T1, T2>);
  auto id = _impl->create(ptr, buffer);
  return RayActor<T>(std::move(*id));
}