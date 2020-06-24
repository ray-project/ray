/// This file is auto-generated. DO NOT EDIT.
/// The following `Call` methods are used to call remote functions and create an actor.
/// Their arguments and return types are as following:
/// \param[in] create_func The function pointer to be remote execution.
/// \param[in] arg1...argn The function arguments passed by a value or ObjectRef.
/// \return ActorCreator.

// TODO(Guyang Song): code generation

// 0 args
template <typename ActorType>
static ActorCreator<ActorType> Actor(CreateActorFunc0<ActorType> create_func);

// 1 arg
template <typename ActorType, typename Arg1Type>
static ActorCreator<ActorType> Actor(CreateActorFunc1<ActorType, Arg1Type> create_func,
                                     Arg1Type arg1);

template <typename ActorType, typename Arg1Type>
static ActorCreator<ActorType> Actor(CreateActorFunc1<ActorType, Arg1Type> create_func,
                                     ObjectRef<Arg1Type> &arg1);

// 2 args
template <typename ActorType, typename Arg1Type, typename Arg2Type>
static ActorCreator<ActorType> Actor(
    CreateActorFunc2<ActorType, Arg1Type, Arg2Type> create_func, Arg1Type arg1,
    Arg2Type arg2);

template <typename ActorType, typename Arg1Type, typename Arg2Type>
static ActorCreator<ActorType> Actor(
    CreateActorFunc2<ActorType, Arg1Type, Arg2Type> create_func,
    ObjectRef<Arg1Type> &arg1, Arg2Type arg2);

template <typename ActorType, typename Arg1Type, typename Arg2Type>
static ActorCreator<ActorType> Actor(
    CreateActorFunc2<ActorType, Arg1Type, Arg2Type> create_func, Arg1Type arg1,
    ObjectRef<Arg2Type> &arg2);

template <typename ActorType, typename Arg1Type, typename Arg2Type>
static ActorCreator<ActorType> Actor(
    CreateActorFunc2<ActorType, Arg1Type, Arg2Type> create_func,
    ObjectRef<Arg1Type> &arg1, ObjectRef<Arg2Type> &arg2);