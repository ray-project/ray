

// TODO(Guyang Song): code generation

// 0 args
template <typename ReturnType, typename ActorType>
static RayObject<ReturnType> Call(ActorFunc0<ActorType, ReturnType> actor_func,
                                  RayActor<ActorType> &actor);

// 1 arg
template <typename ReturnType, typename ActorType, typename Arg1Type>
static RayObject<ReturnType> Call(ActorFunc1<ActorType, ReturnType, Arg1Type> actor_func,
                                  RayActor<ActorType> &actor, Arg1Type arg1);

template <typename ReturnType, typename ActorType, typename Arg1Type>
static RayObject<ReturnType> Call(ActorFunc1<ActorType, ReturnType, Arg1Type> actor_func,
                                  RayActor<ActorType> &actor, RayObject<Arg1Type> &arg1);

// 2 args
template <typename ReturnType, typename ActorType, typename Arg1Type, typename Arg2Type>
static RayObject<ReturnType> Call(
    ActorFunc2<ActorType, ReturnType, Arg1Type, Arg2Type> actor_func,
    RayActor<ActorType> &actor, Arg1Type arg1, Arg2Type arg2);

template <typename ReturnType, typename ActorType, typename Arg1Type, typename Arg2Type>
static RayObject<ReturnType> Call(
    ActorFunc2<ActorType, ReturnType, Arg1Type, Arg2Type> actor_func,
    RayActor<ActorType> &actor, RayObject<Arg1Type> &arg1, Arg2Type arg2);

template <typename ReturnType, typename ActorType, typename Arg1Type, typename Arg2Type>
static RayObject<ReturnType> Call(
    ActorFunc2<ActorType, ReturnType, Arg1Type, Arg2Type> actor_func,
    RayActor<ActorType> &actor, Arg1Type arg1, RayObject<Arg2Type> &arg2);

template <typename ReturnType, typename ActorType, typename Arg1Type, typename Arg2Type>
static RayObject<ReturnType> Call(
    ActorFunc2<ActorType, ReturnType, Arg1Type, Arg2Type> actor_func,
    RayActor<ActorType> &actor, RayObject<Arg1Type> &arg1, RayObject<Arg2Type> &arg2);
