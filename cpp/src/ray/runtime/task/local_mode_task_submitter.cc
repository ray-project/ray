
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

ObjectID LocalModeTaskSubmitter::buildReturnId(
    const TaskID &taskId) {
return ObjectID::FromRandom();
}

ObjectID LocalModeTaskSubmitter::submit(const InvocationSpec &invocation,
                                                         TaskType type) {
  std::unique_ptr<LocalTaskSpec> ts(new LocalTaskSpec());
  ts->type = type;
  ts->taskId = invocation.taskId;
  ts->actorId = invocation.actorId;
  ts->actorCounter = invocation.actorCounter;
  AbstractRayRuntime &rayRuntime = AbstractRayRuntime::getInstance();
  ts->parentTaskId = rayRuntime.getCurrentTaskId();
  ts->args = invocation.args;
  ts->set_func_offset(invocation.func_offset);
  ts->set_exec_func_offset(invocation.exec_func_offset);
  ObjectID rt = buildReturnId(invocation.taskId);
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
    InvocationExecutor::execute(*ts, actor);
  } else {
      boost::asio::post(*_pool.get(), std::bind(
                                      [actor, mutex](std::unique_ptr<LocalTaskSpec> &ts) {
                                        if (mutex) {
                                          mutex->lock();
                                        }
                                        InvocationExecutor::execute(*ts, actor);
                                        if (mutex) {
                                          mutex->unlock();
                                        }
                                      },
                                      std::move(ts)));
  }
  return rt;
}

ObjectID LocalModeTaskSubmitter::submitTask(
    const InvocationSpec &invocation) {
  return submit(invocation, TaskType::NORMAL_TASK);
}

ActorID LocalModeTaskSubmitter::createActor(
    remote_function_ptr_holder &fptr, std::shared_ptr<msgpack::sbuffer> args) {
  AbstractRayRuntime &runtime = AbstractRayRuntime::getInstance();
  ActorID id = runtime.getNextActorID();
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

ObjectID LocalModeTaskSubmitter::submitActorTask(
    const InvocationSpec &invocation) {
  return submit(invocation, TaskType::ACTOR_TASK);
}

}  }// namespace ray::api