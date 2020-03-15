

// TODO(Guyang Song): code generation

// 0 args
template <typename R>
using CreateFunc0 = R *(*)();

// 1 args
template <typename R, typename T1>
using CreateFunc1 = R *(*)(T1);

// 2 args
template <typename R, typename T1, typename T2>
using CreateFunc2 = R *(*)(T1, T2);
