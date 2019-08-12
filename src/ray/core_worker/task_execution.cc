#include "ray/core_worker/task_execution.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/core_worker/transport/raylet_transport.h"

namespace ray {

CoreWorkerTaskExecutionInterface::CoreWorkerTaskExecutionInterface(
    WorkerContext &worker_context, std::unique_ptr<RayletClient> &raylet_client,
    CoreWorkerStoreProviderLayer &store_provider_layer, const TaskExecutor &executor)
    : worker_context_(worker_context),
      store_provider_layer_(store_provider_layer),
      execution_callback_(executor) {
  RAY_CHECK(execution_callback_ != nullptr);

  auto func = std::bind(&CoreWorkerTaskExecutionInterface::ExecuteTask, this,
                        std::placeholders::_1, std::placeholders::_2);

  task_receiver_layer_ = std::unique_ptr<CoreWorkerTaskReceiverLayer>(
      new CoreWorkerTaskReceiverLayer(raylet_client, store_provider_layer, func));
}

Status CoreWorkerTaskExecutionInterface::ExecuteTask(
    const TaskSpecification &task_spec,
    std::vector<std::shared_ptr<RayObject>> *results) {
  worker_context_.SetCurrentTask(task_spec);

  RayFunction func{task_spec.GetLanguage(), task_spec.FunctionDescriptor()};

  std::vector<std::shared_ptr<RayObject>> args;
  RAY_CHECK_OK(BuildArgsForExecutor(task_spec, &args));

  TaskType task_type;
  if (task_spec.IsActorCreationTask()) {
    task_type = TaskType::ACTOR_CREATION_TASK;
  } else if (task_spec.IsActorTask()) {
    task_type = TaskType::ACTOR_TASK;
  } else {
    task_type = TaskType::NORMAL_TASK;
  }

  TaskInfo task_info{task_spec.TaskId(), task_spec.JobId(), task_type};

  auto num_returns = task_spec.NumReturns();
  if (task_spec.IsActorCreationTask() || task_spec.IsActorTask()) {
    RAY_CHECK(num_returns > 0);
    // Decrease to account for the dummy object id.
    num_returns--;
  }

  auto status = execution_callback_(func, args, task_info, num_returns, results);
  // TODO(zhijunfu):
  // 1. Check and handle failure.
  // 2. Save or load checkpoint.
  return status;
}

void CoreWorkerTaskExecutionInterface::Run() {
  task_receiver_layer_->Run();

  // should never reach here.
  RAY_LOG(FATAL) << "should never reach here after running main io service";
}

Status CoreWorkerTaskExecutionInterface::BuildArgsForExecutor(
    const TaskSpecification &task, std::vector<std::shared_ptr<RayObject>> *args) {
  auto num_args = task.NumArgs();
  (*args).resize(num_args);

  std::vector<ObjectID> object_ids_to_fetch;
  std::vector<int> indices;

  for (size_t i = 0; i < task.NumArgs(); ++i) {
    int count = task.ArgIdCount(i);
    if (count > 0) {
      // pass by reference.
      RAY_CHECK(count == 1);
      object_ids_to_fetch.push_back(task.ArgId(i, 0));
      indices.push_back(i);
    } else {
      // pass by value.
      (*args)[i] = std::make_shared<RayObject>(
          std::make_shared<LocalMemoryBuffer>(const_cast<uint8_t *>(task.ArgVal(i)),
                                              task.ArgValLength(i)),
          nullptr);
    }
  }

  std::vector<std::shared_ptr<RayObject>> results;
  auto status = store_provider_layer_.Get(StoreProviderType::PLASMA, object_ids_to_fetch,
                                          -1, &results);
  if (status.ok()) {
    for (size_t i = 0; i < results.size(); i++) {
      (*args)[indices[i]] = results[i];
    }
  }

  return status;
}

int CoreWorkerTaskExecutionInterface::GetRpcServerPort() const {
  return task_receiver_layer_->GetRpcServerPort();
}

}  // namespace ray
