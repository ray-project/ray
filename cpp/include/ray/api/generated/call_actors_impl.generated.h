// TODO(Guyang Song): code generation
// 0 args
template <typename ReturnType, typename ActorType>
ActorTaskCaller<ReturnType> Ray::Task(ActorFunc0<ActorType, ReturnType> actor_func,
                                      ActorHandle<ActorType> &actor) {
  return CallActorInternal<ReturnType, ActorType>(
      actor_func, ActorExecFunction<ReturnType, ActorType>, actor);
}

// 1 arg
template <typename ReturnType, typename ActorType, typename Arg1Type>
ActorTaskCaller<ReturnType> Ray::Task(
    ActorFunc1<ActorType, ReturnType, Arg1Type> actor_func, ActorHandle<ActorType> &actor,
    Arg1Type arg1) {
  return CallActorInternal<ReturnType, ActorType>(
      actor_func, ActorExecFunction<ReturnType, ActorType, Arg1Type>, actor, arg1);
}

template <typename ReturnType, typename ActorType, typename Arg1Type>
ActorTaskCaller<ReturnType> Ray::Task(
    ActorFunc1<ActorType, ReturnType, Arg1Type> actor_func, ActorHandle<ActorType> &actor,
    ObjectRef<Arg1Type> &arg1) {
  return CallActorInternal<ReturnType, ActorType>(
      actor_func, ActorExecFunction<ReturnType, ActorType, Arg1Type>, actor, arg1);
}

// 2 args
template <typename ReturnType, typename ActorType, typename Arg1Type, typename Arg2Type>
ActorTaskCaller<ReturnType> Ray::Task(
    ActorFunc2<ActorType, ReturnType, Arg1Type, Arg2Type> actor_func,
    ActorHandle<ActorType> &actor, Arg1Type arg1, Arg2Type arg2) {
  return CallActorInternal<ReturnType, ActorType>(
      actor_func, ActorExecFunction<ReturnType, ActorType, Arg1Type, Arg2Type>, actor,
      arg1, arg2);
}

template <typename ReturnType, typename ActorType, typename Arg1Type, typename Arg2Type>
ActorTaskCaller<ReturnType> Ray::Task(
    ActorFunc2<ActorType, ReturnType, Arg1Type, Arg2Type> actor_func,
    ActorHandle<ActorType> &actor, ObjectRef<Arg1Type> &arg1, Arg2Type arg2) {
  return CallActorInternal<ReturnType, ActorType>(
      actor_func, ActorExecFunction<ReturnType, ActorType, Arg1Type, Arg2Type>, actor,
      arg1, arg2);
}

template <typename ReturnType, typename ActorType, typename Arg1Type, typename Arg2Type>
ActorTaskCaller<ReturnType> Ray::Task(
    ActorFunc2<ActorType, ReturnType, Arg1Type, Arg2Type> actor_func,
    ActorHandle<ActorType> &actor, Arg1Type arg1, ObjectRef<Arg2Type> &arg2) {
  return CallActorInternal<ReturnType, ActorType>(
      actor_func, ActorExecFunction<ReturnType, ActorType, Arg1Type, Arg2Type>, actor,
      arg1, arg2);
}

template <typename ReturnType, typename ActorType, typename Arg1Type, typename Arg2Type>
ActorTaskCaller<ReturnType> Ray::Task(
    ActorFunc2<ActorType, ReturnType, Arg1Type, Arg2Type> actor_func,
    ActorHandle<ActorType> &actor, ObjectRef<Arg1Type> &arg1, ObjectRef<Arg2Type> &arg2) {
  return CallActorInternal<ReturnType, ActorType>(
      actor_func, ActorExecFunction<ReturnType, ActorType, Arg1Type, Arg2Type>, actor,
      arg1, arg2);
}
