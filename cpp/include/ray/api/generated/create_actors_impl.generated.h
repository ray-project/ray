// TODO(Guyang Song): code generation

// 0 args
template <typename R>
RayActor<R> Ray::CreateActor(CreateFunc0<R> createFunc) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::Wrap(packer);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(createFunc);
  ptr.value[1] = reinterpret_cast<uintptr_t>(create_actor_exec_function<R *>);
  auto id = _impl->Create(ptr, buffer);
  return RayActor<R>(id);
}

// 1 args
template <typename R, typename T1>
RayActor<R> Ray::CreateActor(CreateFunc1<R, T1> createFunc, T1 arg1) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::Wrap(packer, false, arg1);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(createFunc);
  ptr.value[1] = reinterpret_cast<uintptr_t>(create_actor_exec_function<R *, T1>);
  auto id = _impl->Create(ptr, buffer);
  return RayActor<R>(id);
}

template <typename R, typename T1>
RayActor<R> Ray::CreateActor(CreateFunc1<R, T1> createFunc, RayObject<T1> &arg1) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::Wrap(packer, true, arg1);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(createFunc);
  ptr.value[1] = reinterpret_cast<uintptr_t>(create_actor_exec_function<R *, T1>);
  auto id = _impl->Create(ptr, buffer);
  return RayActor<R>(id);
}

// 2 args
template <typename R, typename T1, typename T2>
RayActor<R> Ray::CreateActor(CreateFunc2<R, T1, T2> createFunc, T1 arg1, T2 arg2) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::Wrap(packer, false, arg1, false, arg2);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(createFunc);
  ptr.value[1] = reinterpret_cast<uintptr_t>(create_actor_exec_function<R *, T1, T2>);
  auto id = _impl->Create(ptr, buffer);
  return RayActor<R>(id);
}

template <typename R, typename T1, typename T2>
RayActor<R> Ray::CreateActor(CreateFunc2<R, T1, T2> createFunc, RayObject<T1> &arg1,
                             T2 arg2) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::Wrap(packer, true, arg1, false, arg2);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(createFunc);
  ptr.value[1] = reinterpret_cast<uintptr_t>(create_actor_exec_function<R *, T1, T2>);
  auto id = _impl->Create(ptr, buffer);
  return RayActor<R>(id);
}

template <typename R, typename T1, typename T2>
RayActor<R> Ray::CreateActor(CreateFunc2<R, T1, T2> createFunc, T1 arg1,
                             RayObject<T2> &arg2) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::Wrap(packer, false, arg1, true, arg2);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(createFunc);
  ptr.value[1] = reinterpret_cast<uintptr_t>(create_actor_exec_function<R *, T1, T2>);
  auto id = _impl->Create(ptr, buffer);
  return RayActor<R>(id);
}

template <typename R, typename T1, typename T2>
RayActor<R> Ray::CreateActor(CreateFunc2<R, T1, T2> createFunc, RayObject<T1> &arg1,
                             RayObject<T2> &arg2) {
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::Wrap(packer, true, arg1, true, arg2);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(createFunc);
  ptr.value[1] = reinterpret_cast<uintptr_t>(create_actor_exec_function<R *, T1, T2>);
  auto id = _impl->Create(ptr, buffer);
  return RayActor<R>(id);
}