// TODO: code generation

// 0 args
template <typename O>
template <typename R>
RayObject<R> RayActor<O>::call(ActorFunc0<O, R> actorFunc) {
  return Ray::call(actorFunc, *this);
}

// 1 args
template <typename O>
template <typename R, typename T1>
RayObject<R> RayActor<O>::call(ActorFunc1<O, R, T1> actorFunc, T1 arg1) {
  return Ray::call(actorFunc, *this, arg1);
}

template <typename O>
template <typename R, typename T1>
RayObject<R> RayActor<O>::call(ActorFunc1<O, R, T1> actorFunc, RayObject<T1> &arg1) {
  return Ray::call(actorFunc, *this, arg1);
}

// 2 args
template <typename O>
template <typename R, typename T1, typename T2>
RayObject<R> RayActor<O>::call(ActorFunc2<O, R, T1, T2> actorFunc, T1 arg1, T2 arg2) {
  return Ray::call(actorFunc, *this, arg1, arg2);
}

template <typename O>
template <typename R, typename T1, typename T2>
RayObject<R> RayActor<O>::call(ActorFunc2<O, R, T1, T2> actorFunc, RayObject<T1> &arg1, T2 arg2) {
  return Ray::call(actorFunc, *this, arg1, arg2);
}

template <typename O>
template <typename R, typename T1, typename T2>
RayObject<R> RayActor<O>::call(ActorFunc2<O, R, T1, T2> actorFunc, T1 arg1, RayObject<T2> &arg2) {
  return Ray::call(actorFunc, *this, arg1, arg2);
}

template <typename O>
template <typename R, typename T1, typename T2>
RayObject<R> RayActor<O>::call(ActorFunc2<O, R, T1, T2> actorFunc, RayObject<T1> &arg1,
                               RayObject<T2> &arg2) {
  return Ray::call(actorFunc, *this, arg1, arg2);
}
