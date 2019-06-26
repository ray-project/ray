#include "ray/core_worker/task_execution.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/transport/raylet_transport.h"

namespace ray {

CoreWorkerTaskExecutionInterface::CoreWorkerTaskExecutionInterface(
    CoreWorker &core_worker)
    : core_worker_(core_worker),
      worker_server_("Worker", 0 /* let grpc choose port */),
      main_work_(main_service_) {
  task_receivers_.emplace(
      static_cast<int>(TaskTransportType::RAYLET),
      std::unique_ptr<CoreWorkerRayletTaskReceiver>(
          new CoreWorkerRayletTaskReceiver(main_service_, worker_server_)));
}

Status CoreWorkerTaskExecutionInterface::Run(const TaskExecutor &executor) {
  auto callback = [this, executor](const raylet::TaskSpecification &spec) {
    core_worker_.worker_context_.SetCurrentTask(spec);

    WorkerLanguage language = (spec.GetLanguage() == ::Language::JAVA)
                                  ? WorkerLanguage::JAVA
                                  : WorkerLanguage::PYTHON;
    RayFunction func{language, spec.FunctionDescriptor()};

    std::vector<std::shared_ptr<Buffer>> args;
    RAY_CHECK_OK(BuildArgsForExecutor(spec, &args));

    auto num_returns = spec.NumReturns();
    if (spec.IsActorCreationTask() || spec.IsActorTask()) {
      RAY_CHECK(num_returns > 0);
      // Decrease to account for the dummy object id.
      // TODO (zhijunfu): note this logic only applies to task submitted via raylet.
      num_returns--;
    }

    auto status = executor(func, args, spec.TaskId(), num_returns);
    // TODO(zhijunfu):
    // 1. Check and handle failure.
    // 2. Save or load checkpoint.
    return status;
  };

  for (auto &entry : task_receivers_) {
    entry.second->SetTaskHandler(callback);
  }

  // start rpc server after all the task receivers are properly initialized.
  worker_server_.Run();

  // TODO(zhijunfu): currently RayletClient would crash in its constructor if it cannot
  // connect to Raylet after a number of retries, this needs to be changed
  // so that the worker (java/python .etc) can retrieve and handle the error
  // instead of crashing.
  core_worker_.InitializeRayletClient(worker_server_.GetPort());

  // Run main IO service.
  main_service_.run();

  // should never reach here.
  return Status::OK();
}

Status CoreWorkerTaskExecutionInterface::BuildArgsForExecutor(
    const raylet::TaskSpecification &spec, std::vector<std::shared_ptr<Buffer>> *args) {
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
      (*args)[i] = std::make_shared<LocalMemoryBuffer>(
          const_cast<uint8_t *>(spec.ArgVal(i)), spec.ArgValLength(i));
    }
  }

  std::vector<std::shared_ptr<Buffer>> results;
  auto status = core_worker_.object_interface_.Get(object_ids_to_fetch, -1, &results);
  if (status.ok()) {
    for (size_t i = 0; i < results.size(); i++) {
      (*args)[indices[i]] = results[i];
    }
  }

  return status;
}

}  // namespace ray
