// TODO: code generation

// 0 args
template <typename O>
template <typename R>
RayObject<R> RayActor<O>::call(R (O::*func)()) {
    return Ray::call(func, *this);
}

// 1 args
template <typename O>
template <typename R, typename T1>
RayObject<R> RayActor<O>::call(R (O::*func)(T1), T1 arg1) {
    return Ray::call(func, *this, arg1);
}

template <typename O>
template <typename R, typename T1>
RayObject<R> RayActor<O>::call(R (O::*func)(T1), RayObject<T1> &arg1) {
    return Ray::call(func, *this, arg1);
}

// 2 args
template <typename O>
template <typename R, typename T1, typename T2>
RayObject<R> RayActor<O>::call(R (O::*func)(T1, T2), T1 arg1, T2 arg2) {
    return Ray::call(func, *this, arg1, arg2);
}

template <typename O>
template <typename R, typename T1, typename T2>
RayObject<R> RayActor<O>::call(R (O::*func)(T1, T2), RayObject<T1> &arg1, T2 arg2) {
    return Ray::call(func, *this, arg1, arg2);
}

template <typename O>
template <typename R, typename T1, typename T2>
RayObject<R> RayActor<O>::call(R (O::*func)(T1, T2), T1 arg1, RayObject<T2> &arg2) {
    return Ray::call(func, *this, arg1, arg2);
}

template <typename O>
template <typename R, typename T1, typename T2>
RayObject<R> RayActor<O>::call(R (O::*func)(T1, T2), RayObject<T1> &arg1, RayObject<T2> &arg2) {
    return Ray::call(func, *this, arg1, arg2);
}
