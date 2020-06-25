

// TODO(Guyang Song): code generation

// 0 args
template <typename ReturnType, typename ActorType>
static ActorTaskCaller<ReturnType> Task(ActorFunc0<ActorType, ReturnType> actor_func,
                                        ActorHandle<ActorType> &actor);

// 1 arg
template <typename ReturnType, typename ActorType, typename Arg1Type>
static ActorTaskCaller<ReturnType> Task(
    ActorFunc1<ActorType, ReturnType, Arg1Type> actor_func, ActorHandle<ActorType> &actor,
    Arg1Type arg1);

template <typename ReturnType, typename ActorType, typename Arg1Type>
static ActorTaskCaller<ReturnType> Task(
    ActorFunc1<ActorType, ReturnType, Arg1Type> actor_func, ActorHandle<ActorType> &actor,
    ObjectRef<Arg1Type> &arg1);

// 2 args
template <typename ReturnType, typename ActorType, typename Arg1Type, typename Arg2Type>
static ActorTaskCaller<ReturnType> Task(
    ActorFunc2<ActorType, ReturnType, Arg1Type, Arg2Type> actor_func,
    ActorHandle<ActorType> &actor, Arg1Type arg1, Arg2Type arg2);

template <typename ReturnType, typename ActorType, typename Arg1Type, typename Arg2Type>
static ActorTaskCaller<ReturnType> Task(
    ActorFunc2<ActorType, ReturnType, Arg1Type, Arg2Type> actor_func,
    ActorHandle<ActorType> &actor, ObjectRef<Arg1Type> &arg1, Arg2Type arg2);

template <typename ReturnType, typename ActorType, typename Arg1Type, typename Arg2Type>
static ActorTaskCaller<ReturnType> Task(
    ActorFunc2<ActorType, ReturnType, Arg1Type, Arg2Type> actor_func,
    ActorHandle<ActorType> &actor, Arg1Type arg1, ObjectRef<Arg2Type> &arg2);

template <typename ReturnType, typename ActorType, typename Arg1Type, typename Arg2Type>
static ActorTaskCaller<ReturnType> Task(
    ActorFunc2<ActorType, ReturnType, Arg1Type, Arg2Type> actor_func,
    ActorHandle<ActorType> &actor, ObjectRef<Arg1Type> &arg1, ObjectRef<Arg2Type> &arg2);
