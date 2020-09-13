

// TODO(Guyang Song): code generation

// 0 args
template <typename ActorType, typename ReturnType>
using ActorFunc0 = ReturnType (ActorType::*)();

// 1 arg
template <typename ActorType, typename ReturnType, typename Arg1Type>
using ActorFunc1 = ReturnType (ActorType::*)(Arg1Type);

// 2 args
template <typename ActorType, typename ReturnType, typename Arg1Type, typename Arg2Type>
using ActorFunc2 = ReturnType (ActorType::*)(Arg1Type, Arg2Type);
