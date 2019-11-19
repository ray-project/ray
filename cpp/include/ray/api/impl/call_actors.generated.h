

// TODO: code generation

// 0 args
template <typename R, typename O>
static RayObject<R> call(R (O::*func)(), RayActor<O> &actor);

// 1 args
template <typename R, typename O, typename T1>
static RayObject<R> call(R (O::*func)(T1), RayActor<O> &actor, T1 arg1);

template <typename R, typename O, typename T1>
static RayObject<R> call(R (O::*func)(T1), RayActor<O> &actor, RayObject<T1> &arg1);

// 2 args
template <typename R, typename O, typename T1, typename T2>
static RayObject<R> call(R (O::*func)(T1, T2), RayActor<O> &actor, T1 arg1, T2 arg2);

template <typename R, typename O, typename T1, typename T2>
static RayObject<R> call(R (O::*func)(T1, T2), RayActor<O> &actor, RayObject<T1> &arg1,
                         T2 arg2);

template <typename R, typename O, typename T1, typename T2>
static RayObject<R> call(R (O::*func)(T1, T2), RayActor<O> &actor, T1 arg1,
                         RayObject<T2> &arg2);

template <typename R, typename O, typename T1, typename T2>
static RayObject<R> call(R (O::*func)(T1, T2), RayActor<O> &actor, RayObject<T1> &arg1,
                         RayObject<T2> &arg2);
