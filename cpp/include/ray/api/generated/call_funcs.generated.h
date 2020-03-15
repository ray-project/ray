// TODO(Guyang Song): code generation

// 0 args
template <typename R>
static RayObject<R> Call(Func0<R> func);

// 1 args
template <typename R, typename T1>
static RayObject<R> Call(Func1<R, T1> func, T1 arg1);

template <typename R, typename T1>
static RayObject<R> Call(Func1<R, T1> func, RayObject<T1> &arg1);

// 2 args
template <typename R, typename T1, typename T2>
static RayObject<R> Call(Func2<R, T1, T2> func, T1 arg1, T2 arg2);

template <typename R, typename T1, typename T2>
static RayObject<R> Call(Func2<R, T1, T2> func, RayObject<T1> &arg1, T2 arg2);

template <typename R, typename T1, typename T2>
static RayObject<R> Call(Func2<R, T1, T2> func, T1 arg1, RayObject<T2> &arg2);

template <typename R, typename T1, typename T2>
static RayObject<R> Call(Func2<R, T1, T2> func, RayObject<T1> &arg1, RayObject<T2> &arg2);