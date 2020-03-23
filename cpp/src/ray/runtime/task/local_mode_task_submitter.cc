
#include <boost/asio/post.hpp>
#include <memory>

#include <ray/api/ray_exception.h>
#include "../../util/address_helper.h"
#include "../abstract_ray_runtime.h"
#include "local_mode_task_submitter.h"

namespace ray {
namespace api {

LocalModeTaskSubmitter::LocalModeTaskSubmitter() {
  thread_pool_.reset(new boost::asio::thread_pool(10));
}

ObjectID LocalModeTaskSubmitter::Submit(const InvocationSpec &invocation, TaskType type) {
  /// TODO(Guyang Song): Make the infomation of TaskSpecification more reasonable
  /// We just reuse the TaskSpecification class and make the single process mode work.
  /// Maybe some infomation of TaskSpecification are not reasonable or invalid.
  /// We will enhance this after implement the cluster mode.
  auto functionDescriptor = FunctionDescriptorBuilder::BuildCpp(
      "SingleProcess", std::to_string(invocation.func_offset),
      std::to_string(invocation.exec_func_offset));
  AbstractRayRuntime &rayRuntime = AbstractRayRuntime::GetInstance();
  rpc::Address address;
  std::unordered_map<std::string, double> required_resources;
  std::unordered_map<std::string, double> required_placement_resources;
  TaskSpecBuilder builder;
  builder.SetCommonTaskSpec(invocation.task_id, rpc::Language::CPP, functionDescriptor,
                            rayRuntime.GetCurrentJobID(), rayRuntime.GetCurrentTaskId(),
                            0, rayRuntime.GetCurrentTaskId(), address, 1, false,
                            required_resources, required_placement_resources);
  if (type == TaskType::NORMAL_TASK) {
  } else if (type == TaskType::ACTOR_CREATION_TASK) {
    builder.SetActorCreationTaskSpec(invocation.actor_id);
  } else if (type == TaskType::ACTOR_TASK) {
    const TaskID actor_creation_task_id =
        TaskID::ForActorCreationTask(invocation.actor_id);
    const ObjectID actor_creation_dummy_object_id = ObjectID::ForTaskReturn(
        actor_creation_task_id, 1, static_cast<int>(ray::TaskTransportType::RAYLET));
    builder.SetActorTaskSpec(invocation.actor_id, actor_creation_dummy_object_id,
                             ObjectID(), invocation.actor_counter);
  } else {
    throw RayException("unknown task type");
  }
  auto buffer = std::make_shared<LocalMemoryBuffer>(
      reinterpret_cast<uint8_t *>(invocation.args->data()), invocation.args->size(),
      true);
  /// TODO(Guyang Song): Use both 'AddByRefArg' and 'AddByValueArg' to distinguish
  builder.AddByValueArg(::ray::RayObject(buffer, nullptr, std::vector<ObjectID>()));
  auto taskSpecification = builder.Build();
  ObjectID return_object_id =
      taskSpecification.ReturnId(0, ray::TaskTransportType::RAYLET);

  std::shared_ptr<msgpack::sbuffer> actor;
  std::shared_ptr<absl::Mutex> mutex;
  if (type == TaskType::ACTOR_TASK) {
    absl::MutexLock lock(&actorContextsMutex_);
    actor = actorContexts_.at(invocation.actor_id).get()->currentActor;
    mutex = actorContexts_.at(invocation.actor_id).get()->actorMutex;
  }
  if (type == TaskType::ACTOR_CREATION_TASK || type == TaskType::ACTOR_TASK) {
    /// Execute actor task directly in the main thread because we have not support actor
    /// handle. We must guarantee the actor task executed by calling order.
    TaskExecutor::Invoke(taskSpecification, actor);
  } else {
    boost::asio::post(*thread_pool_.get(), std::bind(
                                               [actor, mutex](TaskSpecification &ts) {
                                                 if (mutex) {
                                                   absl::MutexLock lock(mutex.get());
                                                 }
                                                 TaskExecutor::Invoke(ts, actor);
                                               },
                                               std::move(taskSpecification)));
  }
  return return_object_id;
}

ObjectID LocalModeTaskSubmitter::SubmitTask(const InvocationSpec &invocation) {
  return Submit(invocation, TaskType::NORMAL_TASK);
}

ActorID LocalModeTaskSubmitter::CreateActor(RemoteFunctionPtrHolder &fptr,
                                            std::shared_ptr<msgpack::sbuffer> args) {
  AbstractRayRuntime &runtime = AbstractRayRuntime::GetInstance();
  ActorID id = runtime.GetNextActorID();
  typedef std::shared_ptr<msgpack::sbuffer> (*ExecFunction)(
      uintptr_t base_addr, size_t func_offset, std::shared_ptr<msgpack::sbuffer> args);
  ExecFunction exec_function = (ExecFunction)(fptr.value[1]);
  auto data = (*exec_function)(dynamic_library_base_addr,
                               (size_t)(fptr.value[0] - dynamic_library_base_addr), args);
  std::unique_ptr<ActorContext> actorContext(new ActorContext());
  actorContext->currentActor = data;
  absl::MutexLock lock(&actorContextsMutex_);
  actorContexts_.emplace(id, std::move(actorContext));
  return id;
}

ObjectID LocalModeTaskSubmitter::SubmitActorTask(const InvocationSpec &invocation) {
  return Submit(invocation, TaskType::ACTOR_TASK);
}

}  // namespace api
}  // namespace ray
