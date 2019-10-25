#include "ray/core_worker/task_execution.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/core_worker/transport/raylet_transport.h"

namespace ray {

CoreWorkerTaskExecutionInterface::CoreWorkerTaskExecutionInterface(
    CoreWorker &core_worker, WorkerContext &worker_context,
    std::unique_ptr<RayletClient> &raylet_client,
    CoreWorkerObjectInterface &object_interface,
    const std::shared_ptr<worker::Profiler> profiler,
    const TaskExecutionCallback &task_execution_callback)
    : core_worker_(core_worker),
      worker_context_(worker_context),
      object_interface_(object_interface),
      profiler_(profiler),
      task_execution_callback_(task_execution_callback),
      worker_server_("Worker", 0 /* let grpc choose port */),
      main_service_(std::make_shared<boost::asio::io_service>()),
      main_work_(*main_service_) {
  RAY_CHECK(task_execution_callback_ != nullptr);

  auto func =
      std::bind(&CoreWorkerTaskExecutionInterface::ExecuteTask, this,
                std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
  task_receivers_.emplace(
      TaskTransportType::RAYLET,
      std::unique_ptr<CoreWorkerRayletTaskReceiver>(new CoreWorkerRayletTaskReceiver(
          worker_context_, raylet_client, object_interface_, *main_service_,
          worker_server_, func)));
  task_receivers_.emplace(
      TaskTransportType::DIRECT_ACTOR,
      std::unique_ptr<CoreWorkerDirectActorTaskReceiver>(
          new CoreWorkerDirectActorTaskReceiver(worker_context_, object_interface_,
                                                *main_service_, worker_server_, func)));

  // Start RPC server after all the task receivers are properly initialized.
  worker_server_.Run();
}

Status CoreWorkerTaskExecutionInterface::ExecuteTask(
    const TaskSpecification &task_spec, const ResourceMappingType &resource_ids,
    std::vector<std::shared_ptr<RayObject>> *results) {
  idle_profile_event_.reset();
  RAY_LOG(DEBUG) << "Executing task " << task_spec.TaskId();

  resource_ids_ = resource_ids;
  worker_context_.SetCurrentTask(task_spec);
  core_worker_.SetCurrentTaskId(task_spec.TaskId());

  RayFunction func{task_spec.GetLanguage(), task_spec.FunctionDescriptor()};

  std::vector<std::shared_ptr<RayObject>> args;
  std::vector<ObjectID> arg_reference_ids;
  RAY_CHECK_OK(BuildArgsForExecutor(task_spec, &args, &arg_reference_ids));

  std::vector<ObjectID> return_ids;
  for (size_t i = 0; i < task_spec.NumReturns(); i++) {
    return_ids.push_back(task_spec.ReturnId(i));
  }

  Status status;
  TaskType task_type = TaskType::NORMAL_TASK;
  if (task_spec.IsActorCreationTask()) {
    RAY_CHECK(return_ids.size() > 0);
    return_ids.pop_back();
    task_type = TaskType::ACTOR_CREATION_TASK;
    core_worker_.SetActorId(task_spec.ActorCreationId());
  } else if (task_spec.IsActorTask()) {
    RAY_CHECK(return_ids.size() > 0);
    return_ids.pop_back();
    task_type = TaskType::ACTOR_TASK;
  }
  status = task_execution_callback_(task_type, func,
                                    task_spec.GetRequiredResources().GetResourceMap(),
                                    args, arg_reference_ids, return_ids, results);

  core_worker_.SetCurrentTaskId(TaskID::Nil());
  worker_context_.ResetCurrentTask(task_spec);

  // TODO(zhijunfu):
  // 1. Check and handle failure.
  // 2. Save or load checkpoint.
  idle_profile_event_.reset(new worker::ProfileEvent(profiler_, "worker_idle"));
  return status;
}

void CoreWorkerTaskExecutionInterface::Run() {
  idle_profile_event_.reset(new worker::ProfileEvent(profiler_, "worker_idle"));
  main_service_->run();
}

void CoreWorkerTaskExecutionInterface::Stop() {
  // Stop main IO service.
  std::shared_ptr<boost::asio::io_service> main_service = main_service_;
  // Delay the execution of io_service::stop() to avoid deadlock if
  // CoreWorkerTaskExecutionInterface::Stop is called inside a task.
  idle_profile_event_.reset();
  main_service_->post([main_service]() { main_service->stop(); });
}

Status CoreWorkerTaskExecutionInterface::BuildArgsForExecutor(
    const TaskSpecification &task, std::vector<std::shared_ptr<RayObject>> *args,
    std::vector<ObjectID> *arg_reference_ids) {
  auto num_args = task.NumArgs();
  args->resize(num_args);
  arg_reference_ids->resize(num_args);

  std::vector<ObjectID> object_ids_to_fetch;
  std::vector<int> indices;

  for (size_t i = 0; i < task.NumArgs(); ++i) {
    int count = task.ArgIdCount(i);
    if (count > 0) {
      // pass by reference.
      RAY_CHECK(count == 1);
      object_ids_to_fetch.push_back(task.ArgId(i, 0));
      indices.push_back(i);
      arg_reference_ids->at(i) = task.ArgId(i, 0);
    } else {
      // pass by value.
      std::shared_ptr<LocalMemoryBuffer> data = nullptr;
      if (task.ArgDataSize(i)) {
        data = std::make_shared<LocalMemoryBuffer>(const_cast<uint8_t *>(task.ArgData(i)),
                                                   task.ArgDataSize(i));
      }
      std::shared_ptr<LocalMemoryBuffer> metadata = nullptr;
      if (task.ArgMetadataSize(i)) {
        metadata = std::make_shared<LocalMemoryBuffer>(
            const_cast<uint8_t *>(task.ArgMetadata(i)), task.ArgMetadataSize(i));
      }
      args->at(i) = std::make_shared<RayObject>(data, metadata);
      arg_reference_ids->at(i) = ObjectID::Nil();
    }
  }

  std::vector<std::shared_ptr<RayObject>> results;
  auto status = object_interface_.Get(object_ids_to_fetch, -1, &results);
  if (status.ok()) {
    for (size_t i = 0; i < results.size(); i++) {
      args->at(indices[i]) = results[i];
    }
  }

  return status;
}

}  // namespace ray
