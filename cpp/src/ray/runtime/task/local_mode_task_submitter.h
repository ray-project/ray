#pragma once

#include <boost/asio/thread_pool.hpp>
#include <memory>
#include <mutex>
#include <queue>

#include <ray/core.h>
#include "invocation_spec.h"
#include "task_executer.h"
#include "task_spec.h"
#include "task_submitter.h"

namespace ray { namespace api {

class LocalModeTaskSubmitter : public TaskSubmitter {
 public:
  LocalModeTaskSubmitter();

  ObjectID SubmitTask(const InvocationSpec &invocation);

  ActorID CreateActor(remote_function_ptr_holder &fptr,
                                        std::shared_ptr<msgpack::sbuffer> args);

  ObjectID SubmitActorTask(const InvocationSpec &invocation);

 private:
  std::queue<TaskSpec> _tasks;

  std::unordered_map<ActorID, std::unique_ptr<ActorContext>> _actorContexts;

  std::mutex _actorContextsMutex;

  std::unique_ptr<boost::asio::thread_pool> _pool;

  ObjectID Submit(const InvocationSpec &invocation, TaskType type);

  ObjectID BuildReturnId(const TaskID &taskId);
};
}  }// namespace ray::api