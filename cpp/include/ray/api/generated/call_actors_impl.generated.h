// TODO(Guyang Song): code generation
// 0 args
template <typename R, typename O>
RayObject<R> Ray::Call(ActorFunc0<O, R> actorFunc, RayActor<O> &actor) {
  return CallActorInternal<R, O>(actorFunc, ActorexecFunction<R, O>, actor);
}

// 1 args
template <typename R, typename O, typename T1>
RayObject<R> Ray::Call(ActorFunc1<O, R, T1> actorFunc, RayActor<O> &actor, T1 arg1) {
  return CallActorInternal<R, O>(actorFunc, ActorexecFunction<R, O, T1>, actor, arg1);
}

template <typename R, typename O, typename T1>
RayObject<R> Ray::Call(ActorFunc1<O, R, T1> actorFunc, RayActor<O> &actor,
                       RayObject<T1> &arg1) {
  return CallActorInternal<R, O>(actorFunc, ActorexecFunction<R, O, T1>, actor, arg1);
}

// 2 args
template <typename R, typename O, typename T1, typename T2>
RayObject<R> Ray::Call(ActorFunc2<O, R, T1, T2> actorFunc, RayActor<O> &actor, T1 arg1,
                       T2 arg2) {
  return CallActorInternal<R, O>(actorFunc, ActorexecFunction<R, O, T1, T2>, actor, arg1,
                                 arg2);
}

template <typename R, typename O, typename T1, typename T2>
RayObject<R> Ray::Call(ActorFunc2<O, R, T1, T2> actorFunc, RayActor<O> &actor,
                       RayObject<T1> &arg1, T2 arg2) {
  return CallActorInternal<R, O>(actorFunc, ActorexecFunction<R, O, T1, T2>, actor, arg1,
                                 arg2);
}

template <typename R, typename O, typename T1, typename T2>
RayObject<R> Ray::Call(ActorFunc2<O, R, T1, T2> actorFunc, RayActor<O> &actor, T1 arg1,
                       RayObject<T2> &arg2) {
  return CallActorInternal<R, O>(actorFunc, ActorexecFunction<R, O, T1, T2>, actor, arg1,
                                 arg2);
}

template <typename R, typename O, typename T1, typename T2>
RayObject<R> Ray::Call(ActorFunc2<O, R, T1, T2> actorFunc, RayActor<O> &actor,
                       RayObject<T1> &arg1, RayObject<T2> &arg2) {
  return CallActorInternal<R, O>(actorFunc, ActorexecFunction<R, O, T1, T2>, actor, arg1,
                                 arg2);
}
