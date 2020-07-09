// TODO(Guyang Song): code generation

// 0 args
template <typename ActorType>
template <typename ReturnType>
ActorTaskCaller<ReturnType> ActorHandle<ActorType>::Task(
    ActorFunc0<ActorType, ReturnType> actor_func) {
  return Ray::Task(actor_func, *this);
}

// 1 arg
template <typename ActorType>
template <typename ReturnType, typename Arg1Type>
ActorTaskCaller<ReturnType> ActorHandle<ActorType>::Task(
    ActorFunc1<ActorType, ReturnType, Arg1Type> actor_func, Arg1Type arg1) {
  return Ray::Task(actor_func, *this, arg1);
}

template <typename ActorType>
template <typename ReturnType, typename Arg1Type>
ActorTaskCaller<ReturnType> ActorHandle<ActorType>::Task(
    ActorFunc1<ActorType, ReturnType, Arg1Type> actor_func, ObjectRef<Arg1Type> &arg1) {
  return Ray::Task(actor_func, *this, arg1);
}

// 2 args
template <typename ActorType>
template <typename ReturnType, typename Arg1Type, typename Arg2Type>
ActorTaskCaller<ReturnType> ActorHandle<ActorType>::Task(
    ActorFunc2<ActorType, ReturnType, Arg1Type, Arg2Type> actor_func, Arg1Type arg1,
    Arg2Type arg2) {
  return Ray::Task(actor_func, *this, arg1, arg2);
}

template <typename ActorType>
template <typename ReturnType, typename Arg1Type, typename Arg2Type>
ActorTaskCaller<ReturnType> ActorHandle<ActorType>::Task(
    ActorFunc2<ActorType, ReturnType, Arg1Type, Arg2Type> actor_func,
    ObjectRef<Arg1Type> &arg1, Arg2Type arg2) {
  return Ray::Task(actor_func, *this, arg1, arg2);
}

template <typename ActorType>
template <typename ReturnType, typename Arg1Type, typename Arg2Type>
ActorTaskCaller<ReturnType> ActorHandle<ActorType>::Task(
    ActorFunc2<ActorType, ReturnType, Arg1Type, Arg2Type> actor_func, Arg1Type arg1,
    ObjectRef<Arg2Type> &arg2) {
  return Ray::Task(actor_func, *this, arg1, arg2);
}

template <typename ActorType>
template <typename ReturnType, typename Arg1Type, typename Arg2Type>
ActorTaskCaller<ReturnType> ActorHandle<ActorType>::Task(
    ActorFunc2<ActorType, ReturnType, Arg1Type, Arg2Type> actor_func,
    ObjectRef<Arg1Type> &arg1, ObjectRef<Arg2Type> &arg2) {
  return Ray::Task(actor_func, *this, arg1, arg2);
}
