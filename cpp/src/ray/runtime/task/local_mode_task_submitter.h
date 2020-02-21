#pragma once

#include <memory>
#include <queue>
#include <boost/asio/thread_pool.hpp>
#include <mutex>

#include "invocation_spec.h"
#include "task_spec.h"
#include "task_submitter.h"
#include "task_executer.h"

namespace ray {

class LocalModeTaskSubmitter : public TaskSubmitter {

 public:

   LocalModeTaskSubmitter();

   std::unique_ptr<UniqueId> submitTask(const InvocationSpec &invocation);

   std::unique_ptr<UniqueId> createActor(remote_function_ptr_holder &fptr, 
                                         std::vector< ::ray::blob> &&args);

   std::unique_ptr<UniqueId> submitActorTask(const InvocationSpec &invocation);

  private:
   std::queue<TaskSpec> _tasks;

   std::unordered_map<UniqueId, std::unique_ptr<ActorContext>> _actorContexts;

   std::mutex _actorContextsMutex;

   std::unique_ptr<boost::asio::thread_pool> _pool;

   std::unique_ptr<UniqueId> submit(const InvocationSpec &invocation, TaskType type);

   std::list<std::unique_ptr<UniqueId> > buildReturnIds(const UniqueId &taskId,
                                                       int returnCount);

};
}  // namespace ray