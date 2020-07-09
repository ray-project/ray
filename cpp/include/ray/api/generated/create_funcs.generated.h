
// TODO(Guyang Song): code generation

// 0 args
template <typename ReturnType>
using CreateActorFunc0 = ReturnType *(*)();

// 1 arg
template <typename ReturnType, typename Arg1Type>
using CreateActorFunc1 = ReturnType *(*)(Arg1Type);

// 2 args
template <typename ReturnType, typename Arg1Type, typename Arg2Type>
using CreateActorFunc2 = ReturnType *(*)(Arg1Type, Arg2Type);
