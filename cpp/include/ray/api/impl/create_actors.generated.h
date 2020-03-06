// TODO: code generation

// 0 args
template <typename T>
static RayActor<T> createActor(T *(*func)());

// 1 args
template <typename T, typename T1>
static RayActor<T> createActor(T *(*func)(T1), T1 arg1);

template <typename T, typename T1>
static RayActor<T> createActor(T *(*func)(T1), RayObject<T1> &arg1);

// 2 args
template <typename T, typename T1, typename T2>
static RayActor<T> createActor(T *(*func)(T1, T2), T1 arg1, T2 arg2);

template <typename T, typename T1, typename T2>
static RayActor<T> createActor(T *(*func)(T1, T2), RayObject<T1> &arg1, T2 arg2);

template <typename T, typename T1, typename T2>
static RayActor<T> createActor(T *(*func)(T1, T2), T1 arg1, RayObject<T2> &arg2);

template <typename T, typename T1, typename T2>
static RayActor<T> createActor(T *(*func)(T1, T2), RayObject<T1> &arg1,
                               RayObject<T2> &arg2);