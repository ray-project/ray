// TODO: code generation

// 0 args
template <typename R, typename O>
RayObject<R> Ray::call(ActorFunc0<O, R> actorFunc, RayActor<O> &actor) {
  member_function_ptr_holder holder = *(member_function_ptr_holder *)(&actorFunc);
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::wrap(packer);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(holder.value[0]);
  ptr.value[1] = reinterpret_cast<uintptr_t>(actor_exec_function<R, O>);
  auto id = _impl->call(ptr, actor.id(), buffer);
  return RayObject<R>(std::move(*id));
}

// 1 args
template <typename R, typename O, typename T1>
RayObject<R> Ray::call(ActorFunc1<O, R, T1> actorFunc, RayActor<O> &actor, T1 arg1) {
  member_function_ptr_holder holder = *(member_function_ptr_holder *)(&actorFunc);
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::wrap(packer, false, arg1);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(holder.value[0]);
  ptr.value[1] = reinterpret_cast<uintptr_t>(actor_exec_function<R, O, T1>);
  auto id = _impl->call(ptr, actor.id(), buffer);
  return RayObject<R>(std::move(*id));
}

template <typename R, typename O, typename T1>
RayObject<R> Ray::call(ActorFunc1<O, R, T1> actorFunc, RayActor<O> &actor, RayObject<T1> &arg1) {
  member_function_ptr_holder holder = *(member_function_ptr_holder *)(&actorFunc);
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::wrap(packer, true, arg1);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(holder.value[0]);
  ptr.value[1] = reinterpret_cast<uintptr_t>(actor_exec_function<R, O, T1>);
  auto id = _impl->call(ptr, actor.id(), buffer);
  return RayObject<R>(std::move(*id));
}

// 2 args
template <typename R, typename O, typename T1, typename T2>
RayObject<R> Ray::call(ActorFunc2<O, R, T1, T2> actorFunc, RayActor<O> &actor, T1 arg1, T2 arg2) {
  member_function_ptr_holder holder = *(member_function_ptr_holder *)(&actorFunc);
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::wrap(packer, false, arg1, false, arg2);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(holder.value[0]);
  ptr.value[1] = reinterpret_cast<uintptr_t>(actor_exec_function<R, O, T1, T2>);
  auto id = _impl->call(ptr, actor.id(), buffer);
  return RayObject<R>(std::move(*id));
}

template <typename R, typename O, typename T1, typename T2>
RayObject<R> Ray::call(ActorFunc2<O, R, T1, T2> actorFunc, RayActor<O> &actor, RayObject<T1> &arg1,
                       T2 arg2) {
  member_function_ptr_holder holder = *(member_function_ptr_holder *)(&actorFunc);
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::wrap(packer, true, arg1, false, arg2);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(holder.value[0]);
  ptr.value[1] = reinterpret_cast<uintptr_t>(actor_exec_function<R, O, T1, T2>);
  auto id = _impl->call(ptr, actor.id(), buffer);
  return RayObject<R>(std::move(*id));
}

template <typename R, typename O, typename T1, typename T2>
RayObject<R> Ray::call(ActorFunc2<O, R, T1, T2> actorFunc, RayActor<O> &actor, T1 arg1,
                       RayObject<T2> &arg2) {
  member_function_ptr_holder holder = *(member_function_ptr_holder *)(&actorFunc);
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::wrap(packer, false, arg1, true, arg2);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(holder.value[0]);
  ptr.value[1] = reinterpret_cast<uintptr_t>(actor_exec_function<R, O, T1, T2>);
  auto id = _impl->call(ptr, actor.id(), buffer);
  return RayObject<R>(std::move(*id));
}

template <typename R, typename O, typename T1, typename T2>
RayObject<R> Ray::call(ActorFunc2<O, R, T1, T2> actorFunc, RayActor<O> &actor, RayObject<T1> &arg1,
                       RayObject<T2> &arg2) {
  member_function_ptr_holder holder = *(member_function_ptr_holder *)(&actorFunc);
  std::shared_ptr<msgpack::sbuffer> buffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(buffer.get());
  Arguments::wrap(packer, true, arg1, true, arg2);
  remote_function_ptr_holder ptr;
  ptr.value[0] = reinterpret_cast<uintptr_t>(holder.value[0]);
  ptr.value[1] = reinterpret_cast<uintptr_t>(actor_exec_function<R, O, T1, T2>);
  auto id = _impl->call(ptr, actor.id(), buffer);
  return RayObject<R>(std::move(*id));
}
