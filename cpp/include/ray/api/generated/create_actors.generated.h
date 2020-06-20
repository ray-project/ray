/// This file is auto-generated. DO NOT EDIT.
/// The following `Call` methods are used to call remote functions and create an actor.
/// Their arguments and return types are as following:
/// \param[in] create_func The function pointer to be remote execution.
/// \param[in] arg1...argn The function arguments passed by a value or ObjectRef.
/// \return ActorHandle.

// TODO(Guyang Song): code generation

// 0 args
template <typename ReturnType>
static ActorHandle<ReturnType> Actor(CreateActorFunc0<ReturnType> create_func);

// 1 arg
template <typename ReturnType, typename Arg1Type>
static ActorHandle<ReturnType> Actor(CreateActorFunc1<ReturnType, Arg1Type> create_func,
                                     Arg1Type arg1);

template <typename ReturnType, typename Arg1Type>
static ActorHandle<ReturnType> Actor(CreateActorFunc1<ReturnType, Arg1Type> create_func,
                                     ObjectRef<Arg1Type> &arg1);

// 2 args
template <typename ReturnType, typename Arg1Type, typename Arg2Type>
static ActorHandle<ReturnType> Actor(
    CreateActorFunc2<ReturnType, Arg1Type, Arg2Type> create_func, Arg1Type arg1,
    Arg2Type arg2);

template <typename ReturnType, typename Arg1Type, typename Arg2Type>
static ActorHandle<ReturnType> Actor(
    CreateActorFunc2<ReturnType, Arg1Type, Arg2Type> create_func,
    ObjectRef<Arg1Type> &arg1, Arg2Type arg2);

template <typename ReturnType, typename Arg1Type, typename Arg2Type>
static ActorHandle<ReturnType> Actor(
    CreateActorFunc2<ReturnType, Arg1Type, Arg2Type> create_func, Arg1Type arg1,
    ObjectRef<Arg2Type> &arg2);

template <typename ReturnType, typename Arg1Type, typename Arg2Type>
static ActorHandle<ReturnType> Actor(
    CreateActorFunc2<ReturnType, Arg1Type, Arg2Type> create_func,
    ObjectRef<Arg1Type> &arg1, ObjectRef<Arg2Type> &arg2);