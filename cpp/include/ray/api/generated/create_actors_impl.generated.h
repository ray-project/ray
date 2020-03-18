// TODO(Guyang Song): code generation

// 0 args
template <typename R>
RayActor<R> Ray::CreateActor(CreateFunc0<R> createFunc) {
  return CreateActorInternal<R>(createFunc, CreateActorexecFunction<R *>);
}

// 1 args
template <typename R, typename T1>
RayActor<R> Ray::CreateActor(CreateFunc1<R, T1> createFunc, T1 arg1) {
  return CreateActorInternal<R>(createFunc, CreateActorexecFunction<R *, T1>, arg1);
}

template <typename R, typename T1>
RayActor<R> Ray::CreateActor(CreateFunc1<R, T1> createFunc, RayObject<T1> &arg1) {
  return CreateActorInternal<R>(createFunc, CreateActorexecFunction<R *, T1>, arg1);
}

// 2 args
template <typename R, typename T1, typename T2>
RayActor<R> Ray::CreateActor(CreateFunc2<R, T1, T2> createFunc, T1 arg1, T2 arg2) {
  return CreateActorInternal<R>(createFunc, CreateActorexecFunction<R *, T1, T2>, arg1,
                                arg2);
}

template <typename R, typename T1, typename T2>
RayActor<R> Ray::CreateActor(CreateFunc2<R, T1, T2> createFunc, RayObject<T1> &arg1,
                             T2 arg2) {
  return CreateActorInternal<R>(createFunc, CreateActorexecFunction<R *, T1, T2>, arg1,
                                arg2);
}

template <typename R, typename T1, typename T2>
RayActor<R> Ray::CreateActor(CreateFunc2<R, T1, T2> createFunc, T1 arg1,
                             RayObject<T2> &arg2) {
  return CreateActorInternal<R>(createFunc, CreateActorexecFunction<R *, T1, T2>, arg1,
                                arg2);
}

template <typename R, typename T1, typename T2>
RayActor<R> Ray::CreateActor(CreateFunc2<R, T1, T2> createFunc, RayObject<T1> &arg1,
                             RayObject<T2> &arg2) {
  return CreateActorInternal<R>(createFunc, CreateActorexecFunction<R *, T1, T2>, arg1,
                                arg2);
}