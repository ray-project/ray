// TODO(Guyang Song): code generation

// 0 args
template <typename ActorType>
template <typename ReturnType>
RayObject<ReturnType> RayActor<ActorType>::Call(
    ActorFunc0<ActorType, ReturnType> actor_func) {
  return Ray::Call(actor_func, *this);
}

// 1 arg
template <typename ActorType>
template <typename ReturnType, typename Arg1Type>
RayObject<ReturnType> RayActor<ActorType>::Call(
    ActorFunc1<ActorType, ReturnType, Arg1Type> actor_func, Arg1Type arg1) {
  return Ray::Call(actor_func, *this, arg1);
}

template <typename ActorType>
template <typename ReturnType, typename Arg1Type>
RayObject<ReturnType> RayActor<ActorType>::Call(
    ActorFunc1<ActorType, ReturnType, Arg1Type> actor_func, RayObject<Arg1Type> &arg1) {
  return Ray::Call(actor_func, *this, arg1);
}

// 2 args
template <typename ActorType>
template <typename ReturnType, typename Arg1Type, typename Arg2Type>
RayObject<ReturnType> RayActor<ActorType>::Call(
    ActorFunc2<ActorType, ReturnType, Arg1Type, Arg2Type> actor_func, Arg1Type arg1,
    Arg2Type arg2) {
  return Ray::Call(actor_func, *this, arg1, arg2);
}

template <typename ActorType>
template <typename ReturnType, typename Arg1Type, typename Arg2Type>
RayObject<ReturnType> RayActor<ActorType>::Call(
    ActorFunc2<ActorType, ReturnType, Arg1Type, Arg2Type> actor_func,
    RayObject<Arg1Type> &arg1, Arg2Type arg2) {
  return Ray::Call(actor_func, *this, arg1, arg2);
}

template <typename ActorType>
template <typename ReturnType, typename Arg1Type, typename Arg2Type>
RayObject<ReturnType> RayActor<ActorType>::Call(
    ActorFunc2<ActorType, ReturnType, Arg1Type, Arg2Type> actor_func, Arg1Type arg1,
    RayObject<Arg2Type> &arg2) {
  return Ray::Call(actor_func, *this, arg1, arg2);
}

template <typename ActorType>
template <typename ReturnType, typename Arg1Type, typename Arg2Type>
RayObject<ReturnType> RayActor<ActorType>::Call(
    ActorFunc2<ActorType, ReturnType, Arg1Type, Arg2Type> actor_func,
    RayObject<Arg1Type> &arg1, RayObject<Arg2Type> &arg2) {
  return Ray::Call(actor_func, *this, arg1, arg2);
}
