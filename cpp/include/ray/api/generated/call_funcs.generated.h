/// This file is auto-generated. DO NOT EDIT.
/// The following `Call` methods are used to call remote functions.
/// Their arguments and return types are as following:
/// \param[in] func The function pointer to be remote execution.
/// \param[in] arg1...argn The function arguments passed by a value or ObjectRef.
/// \return TaskCaller.

// TODO(Guyang Song): code generation

// 0 args
template <typename ReturnType>
static TaskCaller<ReturnType> Task(Func0<ReturnType> func);

// 1 arg
template <typename ReturnType, typename Arg1Type>
static TaskCaller<ReturnType> Task(Func1<ReturnType, Arg1Type> func, Arg1Type arg1);

template <typename ReturnType, typename Arg1Type>
static TaskCaller<ReturnType> Task(Func1<ReturnType, Arg1Type> func,
                                   ObjectRef<Arg1Type> &arg1);

// 2 args
template <typename ReturnType, typename Arg1Type, typename Arg2Type>
static TaskCaller<ReturnType> Task(Func2<ReturnType, Arg1Type, Arg2Type> func,
                                   Arg1Type arg1, Arg2Type arg2);

template <typename ReturnType, typename Arg1Type, typename Arg2Type>
static TaskCaller<ReturnType> Task(Func2<ReturnType, Arg1Type, Arg2Type> func,
                                   ObjectRef<Arg1Type> &arg1, Arg2Type arg2);

template <typename ReturnType, typename Arg1Type, typename Arg2Type>
static TaskCaller<ReturnType> Task(Func2<ReturnType, Arg1Type, Arg2Type> func,
                                   Arg1Type arg1, ObjectRef<Arg2Type> &arg2);

template <typename ReturnType, typename Arg1Type, typename Arg2Type>
static TaskCaller<ReturnType> Task(Func2<ReturnType, Arg1Type, Arg2Type> func,
                                   ObjectRef<Arg1Type> &arg1, ObjectRef<Arg2Type> &arg2);