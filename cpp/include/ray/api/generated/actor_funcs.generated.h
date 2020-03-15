

// TODO(Guyang Song): code generation

// 0 args
template <typename O, typename R>
using ActorFunc0 = R (O::*)();

// 1 args
template <typename O, typename R, typename T1>
using ActorFunc1 = R (O::*)(T1);

// 2 args
template <typename O, typename R, typename T1, typename T2>
using ActorFunc2 = R (O::*)(T1, T2);
