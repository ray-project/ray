// TODO: code generation

// 0 args
template <typename R>
RayObject<R> Call(ActorFunc0<O, R> actorFunc);
// 1 args
template <typename R, typename T1>
RayObject<R> Call(ActorFunc1<O, R, T1> actorFunc, T1 arg1);

template <typename R, typename T1>
RayObject<R> Call(ActorFunc1<O, R, T1> actorFunc, RayObject<T1> &arg1);

// 2 args
template <typename R, typename T1, typename T2>
RayObject<R> Call(ActorFunc2<O, R, T1, T2> actorFunc, T1 arg1, T2 arg2);

template <typename R, typename T1, typename T2>
RayObject<R> Call(ActorFunc2<O, R, T1, T2> actorFunc, RayObject<T1> &arg1, T2 arg2);

template <typename R, typename T1, typename T2>
RayObject<R> Call(ActorFunc2<O, R, T1, T2> actorFunc, T1 arg1, RayObject<T2> &arg2);

template <typename R, typename T1, typename T2>
RayObject<R> Call(ActorFunc2<O, R, T1, T2> actorFunc, RayObject<T1> &arg1, RayObject<T2> &arg2);
