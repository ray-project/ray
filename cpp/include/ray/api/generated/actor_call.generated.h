/// This file is auto-generated. DO NOT EDIT.
/// The following `Call` methods are used to call remote functions of actors.
/// Their arguments and return types are as following:
/// \param[in] actor_func The function pointer to be remote execution.
/// \param[in] arg1...argn The function arguments passed by a value or RayObject.
/// \return RayObject.

// TODO(Guyang Song): code generation

// 0 args
template <typename ReturnType>
RayObject<ReturnType> Call(ActorFunc0<ActorType, ReturnType> actor_func);
// 1 arg
template <typename ReturnType, typename Arg1Type>
RayObject<ReturnType> Call(ActorFunc1<ActorType, ReturnType, Arg1Type> actor_func,
                           Arg1Type arg1);

template <typename ReturnType, typename Arg1Type>
RayObject<ReturnType> Call(ActorFunc1<ActorType, ReturnType, Arg1Type> actor_func,
                           RayObject<Arg1Type> &arg1);

// 2 args
template <typename ReturnType, typename Arg1Type, typename Arg2Type>
RayObject<ReturnType> Call(
    ActorFunc2<ActorType, ReturnType, Arg1Type, Arg2Type> actor_func, Arg1Type arg1,
    Arg2Type arg2);

template <typename ReturnType, typename Arg1Type, typename Arg2Type>
RayObject<ReturnType> Call(
    ActorFunc2<ActorType, ReturnType, Arg1Type, Arg2Type> actor_func,
    RayObject<Arg1Type> &arg1, Arg2Type arg2);

template <typename ReturnType, typename Arg1Type, typename Arg2Type>
RayObject<ReturnType> Call(
    ActorFunc2<ActorType, ReturnType, Arg1Type, Arg2Type> actor_func, Arg1Type arg1,
    RayObject<Arg2Type> &arg2);

template <typename ReturnType, typename Arg1Type, typename Arg2Type>
RayObject<ReturnType> Call(
    ActorFunc2<ActorType, ReturnType, Arg1Type, Arg2Type> actor_func,
    RayObject<Arg1Type> &arg1, RayObject<Arg2Type> &arg2);
