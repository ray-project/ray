

// TODO: code generation

// 0 args
template <typename R>
RayObject<R> call(R (O::*func)());

// 1 args
template <typename R, typename T1>
RayObject<R> call(R (O::*func)(T1), T1 arg1);

template <typename R, typename T1>
RayObject<R> call(R (O::*func)(T1), RayObject<T1> &arg1);

// 2 args
template <typename R, typename T1, typename T2>
RayObject<R> call(R (O::*func)(T1, T2), T1 arg1, T2 arg2);

template <typename R, typename T1, typename T2>
RayObject<R> call(R (O::*func)(T1, T2), RayObject<T1> &arg1, T2 arg2);

template <typename R, typename T1, typename T2>
RayObject<R> call(R (O::*func)(T1, T2), T1 arg1, RayObject<T2> &arg2);

template <typename R, typename T1, typename T2>
RayObject<R> call(R (O::*func)(T1, T2), RayObject<T1> &arg1, RayObject<T2> &arg2);
