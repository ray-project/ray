
#include <boost/asio/post.hpp>
#include <memory>

#include "../../agent.h"
#include "../abstract_ray_runtime.h"
#include "invocation_executor.h"
#include "local_mode_task_submitter.h"

namespace ray { namespace api {

void my_task() {}

LocalModeTaskSubmitter::LocalModeTaskSubmitter() {
  _pool.reset(new boost::asio::thread_pool(10));
}

ObjectID LocalModeTaskSubmitter::BuildReturnId(
    const TaskID &taskId) {
return ObjectID::FromRandom();
}

ObjectID LocalModeTaskSubmitter::Submit(const InvocationSpec &invocation,
                                                         TaskType type) {
  std::unique_ptr<TaskSpec> ts(new TaskSpec());
  ts->type = type;
  ts->taskId = invocation.taskId;
  ts->actorId = invocation.actorId;
  ts->actorCounter = invocation.actorCounter;
  AbstractRayRuntime &rayRuntime = AbstractRayRuntime::GetInstance();
  ts->parentTaskId = rayRuntime.GetCurrentTaskId();
  ts->args = invocation.args;
  ts->SetFuncOffset(invocation.funcOffset);
  ts->SetexecFuncOffset(invocation.execFuncOffset);
  ObjectID rt = BuildReturnId(invocation.taskId);
  ts->returnId = rt;
  std::shared_ptr<msgpack::sbuffer> actor;
  std::shared_ptr<std::mutex> mutex;
  if (type == TaskType::ACTOR_TASK) {
    _actorContextsMutex.lock();
    actor = _actorContexts.at(invocation.actorId).get()->currentActor;
    mutex = _actorContexts.at(invocation.actorId).get()->actorMutex;
    _actorContextsMutex.unlock();
  }
  if (type == TaskType::ACTOR_CREATION_TASK || type == TaskType::ACTOR_TASK) {
    /// Execute actor task directly in the main thread because we have not support actor handle. 
    /// We must guarantee the actor task executed by calling order.
    InvocationExecutor::Execute(*ts, actor);
  } else {
      boost::asio::post(*_pool.get(), std::bind(
                                      [actor, mutex](std::unique_ptr<TaskSpec> &ts) {
                                        if (mutex) {
                                          mutex->lock();
                                        }
                                        InvocationExecutor::Execute(*ts, actor);
                                        if (mutex) {
                                          mutex->unlock();
                                        }
                                      },
                                      std::move(ts)));
  }
  return rt;
}

ObjectID LocalModeTaskSubmitter::SubmitTask(
    const InvocationSpec &invocation) {
  return Submit(invocation, TaskType::NORMAL_TASK);
}

ActorID LocalModeTaskSubmitter::CreateActor(
    remote_function_ptr_holder &fptr, std::shared_ptr<msgpack::sbuffer> args) {
  AbstractRayRuntime &runtime = AbstractRayRuntime::GetInstance();
  ActorID id = runtime.GetNextActorID();
  typedef std::shared_ptr<msgpack::sbuffer> (*EXEC_FUNCTION)(
      uintptr_t base_addr, int32_t func_offset, std::shared_ptr<msgpack::sbuffer> args);
  EXEC_FUNCTION exec_function = (EXEC_FUNCTION)(fptr.value[1]);
  auto data =
      (*exec_function)(dylib_base_addr, (int32_t)(fptr.value[0] - dylib_base_addr), args);
  std::unique_ptr<ActorContext> actorContext(new ActorContext());
  actorContext->currentActor = data;
  _actorContextsMutex.lock();
  _actorContexts.emplace(id, std::move(actorContext));
  _actorContextsMutex.unlock();
  return id;
}

ObjectID LocalModeTaskSubmitter::SubmitActorTask(
    const InvocationSpec &invocation) {
  return Submit(invocation, TaskType::ACTOR_TASK);
}

}  }// namespace ray::api