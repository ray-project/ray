// TODO: code generation

// 0 args
template <typename R>
static RayActor<R> CreateActor(CreateFunc0<R> createFunc);

// 1 args
template <typename R, typename T1>
static RayActor<R> CreateActor(CreateFunc1<R, T1> createFunc, T1 arg1);

template <typename R,typename T1>
static RayActor<R> CreateActor(CreateFunc1<R, T1> createFunc, RayObject<T1> &arg1);

// 2 args
template <typename R,typename T1, typename T2>
static RayActor<R> CreateActor(CreateFunc2<R, T1, T2> createFunc, T1 arg1, T2 arg2);

template <typename R,typename T1, typename T2>
static RayActor<R> CreateActor(CreateFunc2<R, T1, T2> createFunc, RayObject<T1> &arg1, T2 arg2);

template <typename R,typename T1, typename T2>
static RayActor<R> CreateActor(CreateFunc2<R, T1, T2> createFunc, T1 arg1, RayObject<T2> &arg2);

template <typename R,typename T1, typename T2>
static RayActor<R> CreateActor(CreateFunc2<R, T1, T2> createFunc, RayObject<T1> &arg1,
                               RayObject<T2> &arg2);