#pragma once

#include <ray/api/function_manager.h>
#include <ray/api/serializer.h>
#include <boost/dll.hpp>
#include <memory>
#include "absl/synchronization/mutex.h"
#include "invocation_spec.h"
#include "ray/core.h"

namespace ray {

namespace internal {
/// Execute remote functions by networking stream.
msgpack::sbuffer TaskExecutionHandler(
    const std::string &func_name,
    const std::vector<std::shared_ptr<RayObject>> &args_buffer);

BOOST_DLL_ALIAS(internal::TaskExecutionHandler, TaskExecutionHandler);
}  // namespace internal

namespace api {

class AbstractRayRuntime;

class ActorContext {
 public:
  std::shared_ptr<msgpack::sbuffer> current_actor = nullptr;

  std::shared_ptr<absl::Mutex> actor_mutex;

  ActorContext() { actor_mutex = std::shared_ptr<absl::Mutex>(new absl::Mutex); }
};

class TaskExecutor {
 public:
  TaskExecutor(AbstractRayRuntime &abstract_ray_tuntime_);

  /// TODO(Guyang Song): support multiple tasks execution
  std::unique_ptr<ObjectID> Execute(InvocationSpec &invocation);

  static void Invoke(
      const TaskSpecification &task_spec, std::shared_ptr<msgpack::sbuffer> actor,
      AbstractRayRuntime *runtime, const uintptr_t base_addr,
      std::unordered_map<ActorID, std::unique_ptr<ActorContext>> &actor_contexts,
      absl::Mutex &actor_contexts_mutex);

  static Status ExecuteTask(
      TaskType task_type, const std::string task_name, const RayFunction &ray_function,
      const std::unordered_map<std::string, double> &required_resources,
      const std::vector<std::shared_ptr<RayObject>> &args,
      const std::vector<ObjectID> &arg_reference_ids,
      const std::vector<ObjectID> &return_ids, const std::string &debugger_breakpoint,
      std::vector<std::shared_ptr<RayObject>> *results,
      std::shared_ptr<LocalMemoryBuffer> &creation_task_exception_pb_bytes);

  virtual ~TaskExecutor(){};

 private:
  AbstractRayRuntime &abstract_ray_tuntime_;
  static std::shared_ptr<msgpack::sbuffer> current_actor_;
};
}  // namespace api
}  // namespace ray