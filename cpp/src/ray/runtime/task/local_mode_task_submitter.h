#pragma once

#include <boost/asio/thread_pool.hpp>
#include <memory>
#include <queue>
#include "absl/synchronization/mutex.h"
#include "invocation_spec.h"
#include "ray/core.h"
#include "task_executor.h"
#include "task_submitter.h"

namespace ray {
namespace api {

class LocalModeTaskSubmitter : public TaskSubmitter {
 public:
  LocalModeTaskSubmitter();

  ObjectID SubmitTask(const InvocationSpec &invocation);

  ActorID CreateActor(RemoteFunctionPtrHolder &fptr,
                      std::shared_ptr<msgpack::sbuffer> args);

  ObjectID SubmitActorTask(const InvocationSpec &invocation);

 private:
  std::unordered_map<ActorID, std::unique_ptr<ActorContext>> actorContexts_;

  absl::Mutex actorContextsMutex_;

  std::unique_ptr<boost::asio::thread_pool> thread_pool_;

  ObjectID Submit(const InvocationSpec &invocation, TaskType type);
};
}  // namespace api
}  // namespace ray