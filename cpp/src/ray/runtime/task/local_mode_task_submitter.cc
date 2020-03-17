
#include <boost/asio/post.hpp>
#include <memory>

#include "../../address_helper.h"
#include "../abstract_ray_runtime.h"
#include "invocation_executor.h"
#include "local_mode_task_submitter.h"

namespace ray {
namespace api {

void my_task() {}

LocalModeTaskSubmitter::LocalModeTaskSubmitter() {
  pool_.reset(new boost::asio::thread_pool(10));
}

ObjectID LocalModeTaskSubmitter::BuildReturnId(const TaskID &taskId) {
  return ObjectID::FromRandom();
}

ObjectID LocalModeTaskSubmitter::Submit(const InvocationSpec &invocation, TaskType type) {
  /// TODO(Guyang Song): Make the infomation of TaskSpecification more reasonable
  /// We just reuse the TaskSpecification class and make the single process mode work.
  /// Maybe some infomation of TaskSpecification are not reasonable or invalid.
  /// We will enhance this after implement the cluster mode.
  auto functionDescriptor = FunctionDescriptorBuilder::BuildCpp(
      "SingleProcess", std::to_string(invocation.funcOffset),
      std::to_string(invocation.execFuncOffset));
  AbstractRayRuntime &rayRuntime = AbstractRayRuntime::GetInstance();
  rpc::Address address;
  std::unordered_map<std::string, double> required_resources;
  std::unordered_map<std::string, double> required_placement_resources;
  TaskSpecBuilder builder;
  builder.SetCommonTaskSpec(invocation.taskId, rpc::Language::CPP, functionDescriptor,
                            rayRuntime.GetCurrentJobId(), rayRuntime.GetCurrentTaskId(),
                            0, rayRuntime.GetCurrentTaskId(), address, 1, false,
                            required_resources, required_placement_resources);
  if (type == TaskType::NORMAL_TASK) {
  } else if (type == TaskType::ACTOR_CREATION_TASK) {
    builder.SetActorCreationTaskSpec(invocation.actorId);
  } else if (type == TaskType::ACTOR_TASK) {
    const TaskID actor_creation_task_id =
        TaskID::ForActorCreationTask(invocation.actorId);
    const ObjectID actor_creation_dummy_object_id = ObjectID::ForTaskReturn(
        actor_creation_task_id, 1, static_cast<int>(ray::TaskTransportType::RAYLET));
    builder.SetActorTaskSpec(invocation.actorId, actor_creation_dummy_object_id,
                             ObjectID(), invocation.actorCounter);
  } else {
    throw "unknown task type";
  }
  auto buffer = std::make_shared<LocalMemoryBuffer>(
      reinterpret_cast<uint8_t *>(invocation.args->data()), invocation.args->size(),
      true);
  /// TODO(Guyang Song): Use both 'AddByRefArg' and 'AddByValueArg' to distinguish
  builder.AddByValueArg(::ray::RayObject(buffer, nullptr, std::vector<ObjectID>()));
  auto taskSpecification = builder.Build();
  ObjectID rt = taskSpecification.ReturnId(0, ray::TaskTransportType::RAYLET);

  std::shared_ptr<msgpack::sbuffer> actor;
  std::shared_ptr<std::mutex> mutex;
  if (type == TaskType::ACTOR_TASK) {
    actorContextsMutex_.lock();
    actor = actorContexts_.at(invocation.actorId).get()->currentActor;
    mutex = actorContexts_.at(invocation.actorId).get()->actorMutex;
    actorContextsMutex_.unlock();
  }
  if (type == TaskType::ACTOR_CREATION_TASK || type == TaskType::ACTOR_TASK) {
    /// Execute actor task directly in the main thread because we have not support actor
    /// handle. We must guarantee the actor task executed by calling order.
    InvocationExecutor::Execute(taskSpecification, actor);
  } else {
    boost::asio::post(*pool_.get(), std::bind(
                                        [actor, mutex](TaskSpecification &ts) {
                                          if (mutex) {
                                            mutex->lock();
                                          }
                                          InvocationExecutor::Execute(ts, actor);
                                          if (mutex) {
                                            mutex->unlock();
                                          }
                                        },
                                        std::move(taskSpecification)));
  }
  return rt;
}

ObjectID LocalModeTaskSubmitter::SubmitTask(const InvocationSpec &invocation) {
  return Submit(invocation, TaskType::NORMAL_TASK);
}

ActorID LocalModeTaskSubmitter::CreateActor(remote_function_ptr_holder &fptr,
                                            std::shared_ptr<msgpack::sbuffer> args) {
  AbstractRayRuntime &runtime = AbstractRayRuntime::GetInstance();
  ActorID id = runtime.GetNextActorID();
  typedef std::shared_ptr<msgpack::sbuffer> (*EXEC_FUNCTION)(
      uintptr_t base_addr, int32_t func_offset, std::shared_ptr<msgpack::sbuffer> args);
  EXEC_FUNCTION exec_function = (EXEC_FUNCTION)(fptr.value[1]);
  auto data =
      (*exec_function)(dylib_base_addr, (int32_t)(fptr.value[0] - dylib_base_addr), args);
  std::unique_ptr<ActorContext> actorContext(new ActorContext());
  actorContext->currentActor = data;
  actorContextsMutex_.lock();
  actorContexts_.emplace(id, std::move(actorContext));
  actorContextsMutex_.unlock();
  return id;
}

ObjectID LocalModeTaskSubmitter::SubmitActorTask(const InvocationSpec &invocation) {
  return Submit(invocation, TaskType::ACTOR_TASK);
}

}  // namespace api
}  // namespace ray