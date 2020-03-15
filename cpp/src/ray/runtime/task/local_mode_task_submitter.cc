
#include <boost/asio/post.hpp>
#include <iostream>
#include <memory>

#include "../../agent.h"
#include "../../util/blob_util.h"
#include "../ray_runtime.h"
#include "invocation_executor.h"
#include "local_mode_task_submitter.h"

namespace ray {

void my_task() {}

LocalModeTaskSubmitter::LocalModeTaskSubmitter() {
  _pool.reset(new boost::asio::thread_pool(10));
}

std::list<std::unique_ptr<UniqueId>> LocalModeTaskSubmitter::buildReturnIds(
    const UniqueId &taskId, int returnCount) {
  std::list<std::unique_ptr<UniqueId>> returnIds;
  for (int i = 0; i < returnCount; i++) {
    returnIds.push_back(taskId.taskComputeReturnId(i));
  }
  return returnIds;
}

std::unique_ptr<UniqueId> LocalModeTaskSubmitter::submit(const InvocationSpec &invocation,
                                                         TaskType type) {
  std::unique_ptr<TaskSpec> ts(new TaskSpec());
  ts->type = type;
  ts->taskId = invocation.taskId;
  ts->actorId = invocation.actorId;
  ts->actorCounter = invocation.actorCounter;
  RayRuntime &rayRuntime = RayRuntime::getInstance();
  TaskSpec *current = rayRuntime.getCurrentTask();
  ts->driverId = current->driverId;
  ts->parentTaskId = current->taskId;
  ts->parentCounter = rayRuntime.getNextPutIndex();
  ts->args = std::move(invocation.args);
  ts->set_func_offset(invocation.func_offset);
  ts->set_exec_func_offset(invocation.exec_func_offset);
  ts->returnIds = buildReturnIds(invocation.taskId, 1);
  auto rt = ts->returnIds.front()->copy();
  ::ray::blob *actor = NULL;
  if (type == TaskType::ACTOR_TASK) {
    _actorContextsMutex.lock();
    actor = _actorContexts.at(invocation.actorId).get()->currentActor.get();
    _actorContextsMutex.unlock();
  }
  boost::asio::post(*_pool.get(), std::bind(
                                      [actor](std::unique_ptr<TaskSpec> &ts) {
                                        InvocationExecutor::execute(*ts, actor);
                                      },
                                      std::move(ts)));
  return rt;
}

std::unique_ptr<UniqueId> LocalModeTaskSubmitter::submitTask(
    const InvocationSpec &invocation) {
  return submit(invocation, TaskType::NORMAL_TASK);
}

std::unique_ptr<UniqueId> LocalModeTaskSubmitter::createActor(
    remote_function_ptr_holder &fptr, std::vector<::ray::blob> &&args) {
  RayRuntime &runtime = RayRuntime::getInstance();
  std::unique_ptr<UniqueId> id = runtime.getCurrentTaskId().taskComputeReturnId(0);
  typedef std::vector<::ray::blob> (*EXEC_FUNCTION)(
      uintptr_t base_addr, int32_t func_offset, const ::ray::blob &args);
  EXEC_FUNCTION exec_function = (EXEC_FUNCTION)(fptr.value[1]);
  ::ray::blob arg = blob_merge(args);
  auto data =
      (*exec_function)(dylib_base_addr, (int32_t)(fptr.value[0] - dylib_base_addr), arg);
  std::unique_ptr<::ray::blob> bb = blob_merge_to_ptr(data);
  std::unique_ptr<ActorContext> actorContext(new ActorContext());
  actorContext->currentActor = std::move(bb);
  _actorContextsMutex.lock();
  _actorContexts.emplace(*id, std::move(actorContext));
  _actorContextsMutex.unlock();
  return id;
}

std::unique_ptr<UniqueId> LocalModeTaskSubmitter::submitActorTask(
    const InvocationSpec &invocation) {
  return submit(invocation, TaskType::ACTOR_TASK);
}

}  // namespace ray