/// This file is auto-generated. DO NOT EDIT.
/// The following execution functions are wrappers of remote functions.
/// Execution functions make remote functions executable in distributed system.
/// NormalExecFunction the wrapper of normal remote function.
/// CreateActorExecFunction the wrapper of actor creation function.
/// ActorExecFunction the wrapper of actor member function.

// TODO(Guyang Song): code generation

template <typename ReturnType, typename CastReturnType, typename... OtherArgTypes>
std::shared_ptr<msgpack::sbuffer> ExecuteNormalFunction(
    uintptr_t base_addr, size_t func_offset,
    const std::vector<std::shared_ptr<RayObject>> &args_buffer, TaskType task_type,
    std::shared_ptr<OtherArgTypes> &... args) {
  int arg_index = 0;
  Arguments::UnwrapArgs(args_buffer, arg_index, &args...);

  ReturnType return_value;
  typedef ReturnType (*Func)(OtherArgTypes...);
  Func func = (Func)(base_addr + func_offset);
  return_value = (*func)(*args...);

  // TODO: No need use shared_ptr here, refactor later.
  return std::make_shared<msgpack::sbuffer>(
      Serializer::Serialize((CastReturnType)(return_value)));
}

template <typename ReturnType, typename ActorType, typename... OtherArgTypes>
std::shared_ptr<msgpack::sbuffer> ExecuteActorFunction(
    uintptr_t base_addr, size_t func_offset,
    const std::vector<std::shared_ptr<RayObject>> &args_buffer,
    std::shared_ptr<msgpack::sbuffer> &actor_buffer,
    std::shared_ptr<OtherArgTypes> &... args) {
  uintptr_t actor_ptr = Serializer::Deserialize<uintptr_t>(
      (const char *)actor_buffer->data(), actor_buffer->size());
  ActorType *actor_object = (ActorType *)actor_ptr;

  int arg_index = 0;
  Arguments::UnwrapArgs(args_buffer, arg_index, &args...);

  ReturnType return_value;
  typedef ReturnType (ActorType::*Func)(OtherArgTypes...);
  MemberFunctionPtrHolder holder;
  holder.value[0] = base_addr + func_offset;
  holder.value[1] = 0;
  Func func = *((Func *)&holder);
  return_value = (actor_object->*func)(*args...);

  // TODO: No need use shared_ptr here, refactor later.
  return std::make_shared<msgpack::sbuffer>(Serializer::Serialize(return_value));
}

// 0 args
template <typename ReturnType>
std::shared_ptr<msgpack::sbuffer> NormalExecFunction(
    uintptr_t base_addr, size_t func_offset,
    const std::vector<std::shared_ptr<RayObject>> &args_buffer) {
  return ExecuteNormalFunction<ReturnType, ReturnType>(
      base_addr, func_offset, args_buffer, TaskType::NORMAL_TASK);
}

// 1 arg
template <typename ReturnType, typename Arg1Type>
std::shared_ptr<msgpack::sbuffer> NormalExecFunction(
    uintptr_t base_addr, size_t func_offset,
    const std::vector<std::shared_ptr<RayObject>> &args_buffer) {
  std::shared_ptr<Arg1Type> arg1_ptr;
  return ExecuteNormalFunction<ReturnType, ReturnType>(
      base_addr, func_offset, args_buffer, TaskType::NORMAL_TASK, arg1_ptr);
}

// 2 args
template <typename ReturnType, typename Arg1Type, typename Arg2Type>
std::shared_ptr<msgpack::sbuffer> NormalExecFunction(
    uintptr_t base_addr, size_t func_offset,
    const std::vector<std::shared_ptr<RayObject>> &args_buffer) {
  std::shared_ptr<Arg1Type> arg1_ptr;
  std::shared_ptr<Arg2Type> arg2_ptr;
  return ExecuteNormalFunction<ReturnType, ReturnType>(
      base_addr, func_offset, args_buffer, TaskType::NORMAL_TASK, arg1_ptr, arg2_ptr);
}

// 0 args
template <typename ReturnType>
std::shared_ptr<msgpack::sbuffer> CreateActorExecFunction(
    uintptr_t base_addr, size_t func_offset,
    const std::vector<std::shared_ptr<RayObject>> &args_buffer) {
  return ExecuteNormalFunction<ReturnType, uintptr_t>(base_addr, func_offset, args_buffer,
                                                      TaskType::ACTOR_CREATION_TASK);
}

// 1 arg
template <typename ReturnType, typename Arg1Type>
std::shared_ptr<msgpack::sbuffer> CreateActorExecFunction(
    uintptr_t base_addr, size_t func_offset,
    const std::vector<std::shared_ptr<RayObject>> &args_buffer) {
  std::shared_ptr<Arg1Type> arg1_ptr;
  return ExecuteNormalFunction<ReturnType, uintptr_t>(
      base_addr, func_offset, args_buffer, TaskType::ACTOR_CREATION_TASK, arg1_ptr);
}

// 2 args
template <typename ReturnType, typename Arg1Type, typename Arg2Type>
std::shared_ptr<msgpack::sbuffer> CreateActorExecFunction(
    uintptr_t base_addr, size_t func_offset,
    const std::vector<std::shared_ptr<RayObject>> &args_buffer) {
  std::shared_ptr<Arg1Type> arg1_ptr;
  std::shared_ptr<Arg2Type> arg2_ptr;
  return ExecuteNormalFunction<ReturnType, uintptr_t>(base_addr, func_offset, args_buffer,
                                                      TaskType::ACTOR_CREATION_TASK,
                                                      arg1_ptr, arg2_ptr);
}

// 0 args
template <typename ReturnType, typename ActorType>
std::shared_ptr<msgpack::sbuffer> ActorExecFunction(
    uintptr_t base_addr, size_t func_offset,
    const std::vector<std::shared_ptr<RayObject>> &args_buffer,
    std::shared_ptr<msgpack::sbuffer> &actor_buffer) {
  return ExecuteActorFunction<ReturnType, ActorType>(base_addr, func_offset, args_buffer,
                                                     actor_buffer);
}

// 1 arg
template <typename ReturnType, typename ActorType, typename Arg1Type>
std::shared_ptr<msgpack::sbuffer> ActorExecFunction(
    uintptr_t base_addr, size_t func_offset,
    const std::vector<std::shared_ptr<RayObject>> &args_buffer,
    std::shared_ptr<msgpack::sbuffer> &actor_buffer) {
  std::shared_ptr<Arg1Type> arg1_ptr;
  return ExecuteActorFunction<ReturnType, ActorType>(base_addr, func_offset, args_buffer,
                                                     actor_buffer, arg1_ptr);
}

// 2 args
template <typename ReturnType, typename ActorType, typename Arg1Type, typename Arg2Type>
std::shared_ptr<msgpack::sbuffer> ActorExecFunction(
    uintptr_t base_addr, size_t func_offset,
    const std::vector<std::shared_ptr<RayObject>> &args_buffer,
    std::shared_ptr<msgpack::sbuffer> &actor_buffer) {
  std::shared_ptr<Arg1Type> arg1_ptr;
  std::shared_ptr<Arg2Type> arg2_ptr;
  return ExecuteActorFunction<ReturnType, ActorType>(base_addr, func_offset, args_buffer,
                                                     actor_buffer, arg1_ptr, arg2_ptr);
}
