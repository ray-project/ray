// TODO(Guyang Song): code generation

// 0 args
template <typename ReturnType>
RayActor<ReturnType> Ray::CreateActor(CreateActorFunc0<ReturnType> create_func) {
  return CreateActorInternal<ReturnType>(create_func,
                                         CreateActorExecFunction<ReturnType *>);
}

// 1 arg
template <typename ReturnType, typename Arg1Type>
RayActor<ReturnType> Ray::CreateActor(CreateActorFunc1<ReturnType, Arg1Type> create_func,
                                      Arg1Type arg1) {
  return CreateActorInternal<ReturnType>(
      create_func, CreateActorExecFunction<ReturnType *, Arg1Type>, arg1);
}

template <typename ReturnType, typename Arg1Type>
RayActor<ReturnType> Ray::CreateActor(CreateActorFunc1<ReturnType, Arg1Type> create_func,
                                      RayObject<Arg1Type> &arg1) {
  return CreateActorInternal<ReturnType>(
      create_func, CreateActorExecFunction<ReturnType *, Arg1Type>, arg1);
}

// 2 args
template <typename ReturnType, typename Arg1Type, typename Arg2Type>
RayActor<ReturnType> Ray::CreateActor(
    CreateActorFunc2<ReturnType, Arg1Type, Arg2Type> create_func, Arg1Type arg1,
    Arg2Type arg2) {
  return CreateActorInternal<ReturnType>(
      create_func, CreateActorExecFunction<ReturnType *, Arg1Type, Arg2Type>, arg1, arg2);
}

template <typename ReturnType, typename Arg1Type, typename Arg2Type>
RayActor<ReturnType> Ray::CreateActor(
    CreateActorFunc2<ReturnType, Arg1Type, Arg2Type> create_func,
    RayObject<Arg1Type> &arg1, Arg2Type arg2) {
  return CreateActorInternal<ReturnType>(
      create_func, CreateActorExecFunction<ReturnType *, Arg1Type, Arg2Type>, arg1, arg2);
}

template <typename ReturnType, typename Arg1Type, typename Arg2Type>
RayActor<ReturnType> Ray::CreateActor(
    CreateActorFunc2<ReturnType, Arg1Type, Arg2Type> create_func, Arg1Type arg1,
    RayObject<Arg2Type> &arg2) {
  return CreateActorInternal<ReturnType>(
      create_func, CreateActorExecFunction<ReturnType *, Arg1Type, Arg2Type>, arg1, arg2);
}

template <typename ReturnType, typename Arg1Type, typename Arg2Type>
RayActor<ReturnType> Ray::CreateActor(
    CreateActorFunc2<ReturnType, Arg1Type, Arg2Type> create_func,
    RayObject<Arg1Type> &arg1, RayObject<Arg2Type> &arg2) {
  return CreateActorInternal<ReturnType>(
      create_func, CreateActorExecFunction<ReturnType *, Arg1Type, Arg2Type>, arg1, arg2);
}