#include "ray/core_worker/task_execution.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/transport/raylet_transport.h"

namespace ray {

CoreWorkerTaskExecutionInterface::CoreWorkerTaskExecutionInterface(
    WorkerContext &worker_context, std::unique_ptr<RayletClient> &raylet_client,
    CoreWorkerObjectInterface &object_interface, const TaskExecutor &executor)
    : worker_context_(worker_context),
      object_interface_(object_interface),
      execution_callback_(executor),
      worker_server_("Worker", 0 /* let grpc choose port */),
      main_work_(main_service_) {
  RAY_CHECK(execution_callback_ != nullptr);

  auto func = std::bind(&CoreWorkerTaskExecutionInterface::ExecuteTask, this,
                        std::placeholders::_1);
  task_receivers_.emplace(
      static_cast<int>(TaskTransportType::RAYLET),
      std::unique_ptr<CoreWorkerRayletTaskReceiver>(new CoreWorkerRayletTaskReceiver(
          raylet_client, main_service_, worker_server_, func)));

  // Start RPC server after all the task receivers are properly initialized.
  worker_server_.Run();
}

Status CoreWorkerTaskExecutionInterface::ExecuteTask(const TaskSpecification &spec) {
  worker_context_.SetCurrentTask(spec);

  RayFunction func{spec.GetLanguage(), spec.FunctionDescriptor()};

  std::vector<std::shared_ptr<RayObject>> args;
  RAY_CHECK_OK(BuildArgsForExecutor(spec, &args));

  TaskType task_type;
  if (spec.IsActorCreationTask()) {
    task_type = TaskType::ACTOR_CREATION_TASK;
  } else if (spec.IsActorTask()) {
    task_type = TaskType::ACTOR_TASK;
  } else {
    task_type = TaskType::NORMAL_TASK;
  }

  TaskInfo task_info{spec.TaskId(), spec.JobId(), task_type};

  auto num_returns = spec.NumReturns();
  if (spec.IsActorCreationTask() || spec.IsActorTask()) {
    RAY_CHECK(num_returns > 0);
    // Decrease to account for the dummy object id.
    num_returns--;
  }

  auto status = execution_callback_(func, args, task_info, num_returns);
  // TODO(zhijunfu):
  // 1. Check and handle failure.
  // 2. Save or load checkpoint.
  return status;
}

void CoreWorkerTaskExecutionInterface::Run() {
  // Run main IO service.
  main_service_.run();

  // should never reach here.
  RAY_LOG(FATAL) << "should never reach here after running main io service";
}

Status CoreWorkerTaskExecutionInterface::BuildArgsForExecutor(
    const TaskSpecification &spec, std::vector<std::shared_ptr<RayObject>> *args) {
  auto num_args = spec.NumArgs();
  (*args).resize(num_args);

  std::vector<ObjectID> object_ids_to_fetch;
  std::vector<int> indices;

  for (int i = 0; i < spec.NumArgs(); ++i) {
    int count = spec.ArgIdCount(i);
    if (count > 0) {
      // pass by reference.
      RAY_CHECK(count == 1);
      object_ids_to_fetch.push_back(spec.ArgId(i, 0));
      indices.push_back(i);
    } else {
      // pass by value.
      (*args)[i] = std::make_shared<RayObject>(
          std::make_shared<LocalMemoryBuffer>(const_cast<uint8_t *>(spec.ArgVal(i)),
                                              spec.ArgValLength(i)),
          nullptr);
    }
  }

  std::vector<std::shared_ptr<RayObject>> results;
  auto status = object_interface_.Get(object_ids_to_fetch, -1, &results);
  if (status.ok()) {
    for (size_t i = 0; i < results.size(); i++) {
      (*args)[indices[i]] = results[i];
    }
  }

  return status;
}

}  // namespace ray
