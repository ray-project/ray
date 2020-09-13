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
    std::shared_ptr<msgpack::sbuffer> &args_buffer, TaskType task_type,
    std::shared_ptr<OtherArgTypes> &... args) {
  msgpack::unpacker unpacker;
  unpacker.reserve_buffer(args_buffer->size());
  memcpy(unpacker.buffer(), args_buffer->data(), args_buffer->size());
  unpacker.buffer_consumed(args_buffer->size());
  Arguments::UnwrapArgs(unpacker, &args...);

  ReturnType return_value;
  typedef ReturnType (*Func)(OtherArgTypes...);
  Func func = (Func)(base_addr + func_offset);
  return_value = (*func)(*args...);

  std::shared_ptr<msgpack::sbuffer> returnBuffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(returnBuffer.get());
  Serializer::Serialize(packer, (CastReturnType)(return_value));

  return returnBuffer;
}

template <typename ReturnType, typename ActorType, typename... OtherArgTypes>
std::shared_ptr<msgpack::sbuffer> ExecuteActorFunction(
    uintptr_t base_addr, size_t func_offset,
    std::shared_ptr<msgpack::sbuffer> &args_buffer,
    std::shared_ptr<msgpack::sbuffer> &actor_buffer,
    std::shared_ptr<OtherArgTypes> &... args) {
  msgpack::unpacker actor_unpacker;
  actor_unpacker.reserve_buffer(actor_buffer->size());
  memcpy(actor_unpacker.buffer(), actor_buffer->data(), actor_buffer->size());
  actor_unpacker.buffer_consumed(actor_buffer->size());
  uintptr_t actor_ptr;
  Serializer::Deserialize(actor_unpacker, &actor_ptr);
  ActorType *actor_object = (ActorType *)actor_ptr;

  msgpack::unpacker unpacker;
  unpacker.reserve_buffer(args_buffer->size());
  memcpy(unpacker.buffer(), args_buffer->data(), args_buffer->size());
  unpacker.buffer_consumed(args_buffer->size());
  Arguments::UnwrapArgs(unpacker, &args...);

  ReturnType return_value;
  typedef ReturnType (ActorType::*Func)(OtherArgTypes...);
  MemberFunctionPtrHolder holder;
  holder.value[0] = base_addr + func_offset;
  holder.value[1] = 0;
  Func func = *((Func *)&holder);
  return_value = (actor_object->*func)(*args...);

  std::shared_ptr<msgpack::sbuffer> returnBuffer(new msgpack::sbuffer());
  msgpack::packer<msgpack::sbuffer> packer(returnBuffer.get());
  Serializer::Serialize(packer, return_value);
  return returnBuffer;
}

// 0 args
template <typename ReturnType>
std::shared_ptr<msgpack::sbuffer> NormalExecFunction(
    uintptr_t base_addr, size_t func_offset,
    std::shared_ptr<msgpack::sbuffer> &args_buffer) {
  return ExecuteNormalFunction<ReturnType, ReturnType>(
      base_addr, func_offset, args_buffer, TaskType::NORMAL_TASK);
}

// 1 arg
template <typename ReturnType, typename Arg1Type>
std::shared_ptr<msgpack::sbuffer> NormalExecFunction(
    uintptr_t base_addr, size_t func_offset,
    std::shared_ptr<msgpack::sbuffer> &args_buffer) {
  std::shared_ptr<Arg1Type> arg1_ptr;
  return ExecuteNormalFunction<ReturnType, ReturnType>(
      base_addr, func_offset, args_buffer, TaskType::NORMAL_TASK, arg1_ptr);
}

// 2 args
template <typename ReturnType, typename Arg1Type, typename Arg2Type>
std::shared_ptr<msgpack::sbuffer> NormalExecFunction(
    uintptr_t base_addr, size_t func_offset,
    std::shared_ptr<msgpack::sbuffer> &args_buffer) {
  std::shared_ptr<Arg1Type> arg1_ptr;
  std::shared_ptr<Arg2Type> arg2_ptr;
  return ExecuteNormalFunction<ReturnType, ReturnType>(
      base_addr, func_offset, args_buffer, TaskType::NORMAL_TASK, arg1_ptr, arg2_ptr);
}

// 0 args
template <typename ReturnType>
std::shared_ptr<msgpack::sbuffer> CreateActorExecFunction(
    uintptr_t base_addr, size_t func_offset,
    std::shared_ptr<msgpack::sbuffer> &args_buffer) {
  return ExecuteNormalFunction<ReturnType, uintptr_t>(base_addr, func_offset, args_buffer,
                                                      TaskType::ACTOR_CREATION_TASK);
}

// 1 arg
template <typename ReturnType, typename Arg1Type>
std::shared_ptr<msgpack::sbuffer> CreateActorExecFunction(
    uintptr_t base_addr, size_t func_offset,
    std::shared_ptr<msgpack::sbuffer> &args_buffer) {
  std::shared_ptr<Arg1Type> arg1_ptr;
  return ExecuteNormalFunction<ReturnType, uintptr_t>(
      base_addr, func_offset, args_buffer, TaskType::ACTOR_CREATION_TASK, arg1_ptr);
}

// 2 args
template <typename ReturnType, typename Arg1Type, typename Arg2Type>
std::shared_ptr<msgpack::sbuffer> CreateActorExecFunction(
    uintptr_t base_addr, size_t func_offset,
    std::shared_ptr<msgpack::sbuffer> &args_buffer) {
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
    std::shared_ptr<msgpack::sbuffer> &args_buffer,
    std::shared_ptr<msgpack::sbuffer> &actor_buffer) {
  return ExecuteActorFunction<ReturnType, ActorType>(base_addr, func_offset, args_buffer,
                                                     actor_buffer);
}

// 1 arg
template <typename ReturnType, typename ActorType, typename Arg1Type>
std::shared_ptr<msgpack::sbuffer> ActorExecFunction(
    uintptr_t base_addr, size_t func_offset,
    std::shared_ptr<msgpack::sbuffer> &args_buffer,
    std::shared_ptr<msgpack::sbuffer> &actor_buffer) {
  std::shared_ptr<Arg1Type> arg1_ptr;
  return ExecuteActorFunction<ReturnType, ActorType>(base_addr, func_offset, args_buffer,
                                                     actor_buffer, arg1_ptr);
}

// 2 args
template <typename ReturnType, typename ActorType, typename Arg1Type, typename Arg2Type>
std::shared_ptr<msgpack::sbuffer> ActorExecFunction(
    uintptr_t base_addr, size_t func_offset,
    std::shared_ptr<msgpack::sbuffer> &args_buffer,
    std::shared_ptr<msgpack::sbuffer> &actor_buffer) {
  std::shared_ptr<Arg1Type> arg1_ptr;
  std::shared_ptr<Arg2Type> arg2_ptr;
  return ExecuteActorFunction<ReturnType, ActorType>(base_addr, func_offset, args_buffer,
                                                     actor_buffer, arg1_ptr, arg2_ptr);
}
