// TODO(Guyang Song): code generation

// 0 args
template <typename R>
RayObject<R> Ray::Call(Func0<R> func) {
  return CallInternal<R>(func, ExecFunction<R>);
}

// 1 args
template <typename R, typename T1>
RayObject<R> Ray::Call(Func1<R, T1> func, T1 arg1) {
  return CallInternal<R>(func, ExecFunction<R, T1>, arg1);
}

template <typename R, typename T1>
RayObject<R> Ray::Call(Func1<R, T1> func, RayObject<T1> &arg1) {
  return CallInternal<R>(func, ExecFunction<R, T1>, arg1);
}

// 2 args
template <typename R, typename T1, typename T2>
RayObject<R> Ray::Call(Func2<R, T1, T2> func, T1 arg1, T2 arg2) {
  return CallInternal<R>(func, ExecFunction<R, T1, T2>, arg1, arg2);
}

template <typename R, typename T1, typename T2>
RayObject<R> Ray::Call(Func2<R, T1, T2> func, RayObject<T1> &arg1, T2 arg2) {
  return CallInternal<R>(func, ExecFunction<R, T1, T2>, arg1, arg2);
}

template <typename R, typename T1, typename T2>
RayObject<R> Ray::Call(Func2<R, T1, T2> func, T1 arg1, RayObject<T2> &arg2) {
  return CallInternal<R>(func, ExecFunction<R, T1, T2>, arg1, arg2);
}

template <typename R, typename T1, typename T2>
RayObject<R> Ray::Call(Func2<R, T1, T2> func, RayObject<T1> &arg1, RayObject<T2> &arg2) {
  return CallInternal<R>(func, ExecFunction<R, T1, T2>, arg1, arg2);
}