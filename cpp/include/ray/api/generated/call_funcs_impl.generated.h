// TODO(Guyang Song): code generation

// 0 args
template <typename ReturnType>
RayObject<ReturnType> Ray::Call(Func0<ReturnType> func) {
  return CallInternal<ReturnType>(func, NormalExecFunction<ReturnType>);
}

// 1 arg
template <typename ReturnType, typename Arg1Type>
RayObject<ReturnType> Ray::Call(Func1<ReturnType, Arg1Type> func, Arg1Type arg1) {
  return CallInternal<ReturnType>(func, NormalExecFunction<ReturnType, Arg1Type>, arg1);
}

template <typename ReturnType, typename Arg1Type>
RayObject<ReturnType> Ray::Call(Func1<ReturnType, Arg1Type> func,
                                RayObject<Arg1Type> &arg1) {
  return CallInternal<ReturnType>(func, NormalExecFunction<ReturnType, Arg1Type>, arg1);
}

// 2 args
template <typename ReturnType, typename Arg1Type, typename Arg2Type>
RayObject<ReturnType> Ray::Call(Func2<ReturnType, Arg1Type, Arg2Type> func, Arg1Type arg1,
                                Arg2Type arg2) {
  return CallInternal<ReturnType>(
      func, NormalExecFunction<ReturnType, Arg1Type, Arg2Type>, arg1, arg2);
}

template <typename ReturnType, typename Arg1Type, typename Arg2Type>
RayObject<ReturnType> Ray::Call(Func2<ReturnType, Arg1Type, Arg2Type> func,
                                RayObject<Arg1Type> &arg1, Arg2Type arg2) {
  return CallInternal<ReturnType>(
      func, NormalExecFunction<ReturnType, Arg1Type, Arg2Type>, arg1, arg2);
}

template <typename ReturnType, typename Arg1Type, typename Arg2Type>
RayObject<ReturnType> Ray::Call(Func2<ReturnType, Arg1Type, Arg2Type> func, Arg1Type arg1,
                                RayObject<Arg2Type> &arg2) {
  return CallInternal<ReturnType>(
      func, NormalExecFunction<ReturnType, Arg1Type, Arg2Type>, arg1, arg2);
}

template <typename ReturnType, typename Arg1Type, typename Arg2Type>
RayObject<ReturnType> Ray::Call(Func2<ReturnType, Arg1Type, Arg2Type> func,
                                RayObject<Arg1Type> &arg1, RayObject<Arg2Type> &arg2) {
  return CallInternal<ReturnType>(
      func, NormalExecFunction<ReturnType, Arg1Type, Arg2Type>, arg1, arg2);
}