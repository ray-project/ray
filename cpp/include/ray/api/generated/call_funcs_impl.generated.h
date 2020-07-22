// TODO(Guyang Song): code generation

// 0 args
template <typename ReturnType>
TaskCaller<ReturnType> Ray::Task(Func0<ReturnType> func) {
  return TaskInternal<ReturnType>(func, NormalExecFunction<ReturnType>);
}

// 1 arg
template <typename ReturnType, typename Arg1Type>
TaskCaller<ReturnType> Ray::Task(Func1<ReturnType, Arg1Type> func, Arg1Type arg1) {
  return TaskInternal<ReturnType>(func, NormalExecFunction<ReturnType, Arg1Type>, arg1);
}

template <typename ReturnType, typename Arg1Type>
TaskCaller<ReturnType> Ray::Task(Func1<ReturnType, Arg1Type> func,
                                 ObjectRef<Arg1Type> &arg1) {
  return TaskInternal<ReturnType>(func, NormalExecFunction<ReturnType, Arg1Type>, arg1);
}

// 2 args
template <typename ReturnType, typename Arg1Type, typename Arg2Type>
TaskCaller<ReturnType> Ray::Task(Func2<ReturnType, Arg1Type, Arg2Type> func,
                                 Arg1Type arg1, Arg2Type arg2) {
  return TaskInternal<ReturnType>(
      func, NormalExecFunction<ReturnType, Arg1Type, Arg2Type>, arg1, arg2);
}

template <typename ReturnType, typename Arg1Type, typename Arg2Type>
TaskCaller<ReturnType> Ray::Task(Func2<ReturnType, Arg1Type, Arg2Type> func,
                                 ObjectRef<Arg1Type> &arg1, Arg2Type arg2) {
  return TaskInternal<ReturnType>(
      func, NormalExecFunction<ReturnType, Arg1Type, Arg2Type>, arg1, arg2);
}

template <typename ReturnType, typename Arg1Type, typename Arg2Type>
TaskCaller<ReturnType> Ray::Task(Func2<ReturnType, Arg1Type, Arg2Type> func,
                                 Arg1Type arg1, ObjectRef<Arg2Type> &arg2) {
  return TaskInternal<ReturnType>(
      func, NormalExecFunction<ReturnType, Arg1Type, Arg2Type>, arg1, arg2);
}

template <typename ReturnType, typename Arg1Type, typename Arg2Type>
TaskCaller<ReturnType> Ray::Task(Func2<ReturnType, Arg1Type, Arg2Type> func,
                                 ObjectRef<Arg1Type> &arg1, ObjectRef<Arg2Type> &arg2) {
  return TaskInternal<ReturnType>(
      func, NormalExecFunction<ReturnType, Arg1Type, Arg2Type>, arg1, arg2);
}