#include "ray/core_worker/core_worker.h"
#include "ray/common/ray_config.h"
#include "ray/common/task/task_util.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/transport/raylet_transport.h"

namespace {

void BuildCommonTaskSpec(
    ray::TaskSpecBuilder &builder, const JobID &job_id, const TaskID &task_id,
    const TaskID &current_task_id, const int task_index, const TaskID &caller_id,
    const ray::RayFunction &function, const std::vector<ray::TaskArg> &args,
    uint64_t num_returns,
    const std::unordered_map<std::string, double> &required_resources,
    const std::unordered_map<std::string, double> &required_placement_resources,
    ray::TaskTransportType transport_type, std::vector<ObjectID> *return_ids) {
  // Build common task spec.
  builder.SetCommonTaskSpec(task_id, function.GetLanguage(),
                            function.GetFunctionDescriptor(), job_id, current_task_id,
                            task_index, caller_id, num_returns, required_resources,
                            required_placement_resources);
  // Set task arguments.
  for (const auto &arg : args) {
    if (arg.IsPassedByReference()) {
      builder.AddByRefArg(arg.GetReference());
    } else {
      builder.AddByValueArg(arg.GetValue());
    }
  }

  // Compute return IDs.
  return_ids->resize(num_returns);
  for (size_t i = 0; i < num_returns; i++) {
    (*return_ids)[i] =
        ObjectID::ForTaskReturn(task_id, i + 1,
                                /*transport_type=*/static_cast<int>(transport_type));
  }
}

}  // namespace

namespace ray {

CoreWorker::CoreWorker(const WorkerType worker_type, const Language language,
                       const std::string &store_socket, const std::string &raylet_socket,
                       const JobID &job_id, const gcs::GcsClientOptions &gcs_options,
                       const std::string &log_dir, const std::string &node_ip_address,
                       const TaskExecutionCallback &task_execution_callback,
                       std::function<Status()> check_signals)
    : worker_type_(worker_type),
      language_(language),
      log_dir_(log_dir),
      worker_context_(worker_type, job_id),
      io_work_(io_service_),
      heartbeat_timer_(io_service_),
      worker_server_(WorkerTypeString(worker_type), 0 /* let grpc choose a port */),
      gcs_client_(gcs_options),
      object_interface_(worker_context_, raylet_client_, store_socket, check_signals),
      task_execution_service_work_(task_execution_service_),
      task_execution_callback_(task_execution_callback) {
  // Initialize logging if log_dir is passed. Otherwise, it must be initialized
  // and cleaned up by the caller.
  if (log_dir_ != "") {
    std::stringstream app_name;
    app_name << LanguageString(language_) << "-" << WorkerTypeString(worker_type_) << "-"
             << worker_context_.GetWorkerID();
    RayLog::StartRayLog(app_name.str(), RayLogLevel::INFO, log_dir_);
    RayLog::InstallFailureSignalHandler();
  }

  // Initialize gcs client.
  RAY_CHECK_OK(gcs_client_.Connect(io_service_));

  // Initialize profiler.
  profiler_ = std::make_shared<worker::Profiler>(worker_context_, node_ip_address,
                                                 io_service_, gcs_client_);

  // Initialize task execution.
  if (worker_type_ == WorkerType::WORKER) {
    RAY_CHECK(task_execution_callback_ != nullptr);

    // Initialize task receivers.
    auto execute_task = std::bind(&CoreWorker::ExecuteTask, this, std::placeholders::_1,
                                  std::placeholders::_2, std::placeholders::_3);
    direct_actor_task_receiver_ = std::unique_ptr<CoreWorkerDirectActorTaskReceiver>(
        new CoreWorkerDirectActorTaskReceiver(worker_context_, object_interface_,
                                              task_execution_service_, worker_server_,
                                              execute_task));
    raylet_task_receiver_ =
        std::unique_ptr<CoreWorkerRayletTaskReceiver>(new CoreWorkerRayletTaskReceiver(
            worker_context_, raylet_client_, object_interface_, task_execution_service_,
            worker_server_, execute_task,
            [this](int64_t tag) { direct_actor_task_receiver_->OnWaitComplete(tag); }));
  }

  // Start RPC server after all the task receivers are properly initialized.
  worker_server_.Run();

  // Initialize raylet client.
  // TODO(zhijunfu): currently RayletClient would crash in its constructor if it cannot
  // connect to Raylet after a number of retries, this can be changed later
  // so that the worker (java/python .etc) can retrieve and handle the error
  // instead of crashing.
  raylet_client_ = std::unique_ptr<RayletClient>(new RayletClient(
      raylet_socket, WorkerID::FromBinary(worker_context_.GetWorkerID().Binary()),
      (worker_type_ == ray::WorkerType::WORKER), worker_context_.GetCurrentJobID(),
      language_, worker_server_.GetPort()));
  if (direct_actor_task_receiver_ != nullptr) {
    direct_actor_task_receiver_->Init(*raylet_client_);
  }

  // Set timer to periodically send heartbeats containing active object IDs to the raylet.
  // If the heartbeat timeout is < 0, the heartbeats are disabled.
  if (RayConfig::instance().worker_heartbeat_timeout_milliseconds() >= 0) {
    heartbeat_timer_.expires_from_now(boost::asio::chrono::milliseconds(
        RayConfig::instance().worker_heartbeat_timeout_milliseconds()));
    heartbeat_timer_.async_wait(boost::bind(&CoreWorker::ReportActiveObjectIDs, this));
  }

  io_thread_ = std::thread(&CoreWorker::RunIOService, this);

  // Create an entry for the driver task in the task table. This task is
  // added immediately with status RUNNING. This allows us to push errors
  // related to this driver task back to the driver. For example, if the
  // driver creates an object that is later evicted, we should notify the
  // user that we're unable to reconstruct the object, since we cannot
  // rerun the driver.
  if (worker_type_ == WorkerType::DRIVER) {
    TaskSpecBuilder builder;
    std::vector<std::string> empty_descriptor;
    std::unordered_map<std::string, double> empty_resources;
    const TaskID task_id = TaskID::ForDriverTask(worker_context_.GetCurrentJobID());
    builder.SetCommonTaskSpec(task_id, language_, empty_descriptor,
                              worker_context_.GetCurrentJobID(),
                              TaskID::ComputeDriverTaskId(worker_context_.GetWorkerID()),
                              0, GetCallerId(), 0, empty_resources, empty_resources);

    std::shared_ptr<gcs::TaskTableData> data = std::make_shared<gcs::TaskTableData>();
    data->mutable_task()->mutable_task_spec()->CopyFrom(builder.Build().GetMessage());
    RAY_CHECK_OK(gcs_client_.raylet_task_table().Add(job_id, task_id, data, nullptr));
    SetCurrentTaskId(task_id);
  }

  direct_actor_submitter_ = std::unique_ptr<CoreWorkerDirectActorTaskSubmitter>(
      new CoreWorkerDirectActorTaskSubmitter(
          io_service_, object_interface_.CreateStoreProvider(StoreProviderType::MEMORY)));
}

CoreWorker::~CoreWorker() {
  io_service_.stop();
  io_thread_.join();
  if (worker_type_ == WorkerType::WORKER) {
    task_execution_service_.stop();
  }
  if (log_dir_ != "") {
    RayLog::ShutDownRayLog();
  }
}

void CoreWorker::Disconnect() {
  io_service_.stop();
  gcs_client_.Disconnect();
  if (raylet_client_) {
    RAY_IGNORE_EXPR(raylet_client_->Disconnect());
  }
}

void CoreWorker::RunIOService() {
  // Block SIGINT and SIGTERM so they will be handled by the main thread.
  sigset_t mask;
  sigemptyset(&mask);
  sigaddset(&mask, SIGINT);
  sigaddset(&mask, SIGTERM);
  pthread_sigmask(SIG_BLOCK, &mask, NULL);

  io_service_.run();
}

void CoreWorker::SetCurrentTaskId(const TaskID &task_id) {
  worker_context_.SetCurrentTaskId(task_id);
  main_thread_task_id_ = task_id;
  // Clear all actor handles at the end of each non-actor task.
  if (actor_id_.IsNil() && task_id.IsNil()) {
    for (const auto &handle : actor_handles_) {
      RAY_CHECK_OK(gcs_client_.Actors().AsyncUnsubscribe(handle.first, nullptr));
    }
    actor_handles_.clear();
  }
}

void CoreWorker::AddActiveObjectID(const ObjectID &object_id) {
  io_service_.post([this, object_id]() -> void {
    active_object_ids_.insert(object_id);
    active_object_ids_updated_ = true;
  });
}

void CoreWorker::RemoveActiveObjectID(const ObjectID &object_id) {
  io_service_.post([this, object_id]() -> void {
    if (active_object_ids_.erase(object_id)) {
      active_object_ids_updated_ = true;
    } else {
      RAY_LOG(WARNING) << "Tried to erase non-existent object ID" << object_id;
    }
  });
}

void CoreWorker::ReportActiveObjectIDs() {
  // Only send a heartbeat when the set of active object IDs has changed because the
  // raylet only modifies the set of IDs when it receives a heartbeat.
  if (active_object_ids_updated_) {
    RAY_LOG(DEBUG) << "Sending " << active_object_ids_.size() << " object IDs to raylet.";
    if (active_object_ids_.size() >
        RayConfig::instance().raylet_max_active_object_ids()) {
      RAY_LOG(WARNING) << active_object_ids_.size()
                       << "object IDs are currently in scope. "
                       << "This may lead to required objects being garbage collected.";
    }
    RAY_CHECK_OK(raylet_client_->ReportActiveObjectIDs(active_object_ids_));
  }

  // Reset the timer from the previous expiration time to avoid drift.
  heartbeat_timer_.expires_at(
      heartbeat_timer_.expiry() +
      boost::asio::chrono::milliseconds(
          RayConfig::instance().worker_heartbeat_timeout_milliseconds()));
  heartbeat_timer_.async_wait(boost::bind(&CoreWorker::ReportActiveObjectIDs, this));
  active_object_ids_updated_ = false;
}

TaskID CoreWorker::GetCallerId() const {
  TaskID caller_id;
  ActorID actor_id = GetActorId();
  if (!actor_id.IsNil()) {
    caller_id = TaskID::ForActorCreationTask(actor_id);
  } else {
    caller_id = main_thread_task_id_;
  }
  return caller_id;
}

Status CoreWorker::SubmitTask(const RayFunction &function,
                              const std::vector<TaskArg> &args,
                              const TaskOptions &task_options,
                              std::vector<ObjectID> *return_ids) {
  TaskSpecBuilder builder;
  const int next_task_index = worker_context_.GetNextTaskIndex();
  const auto task_id =
      TaskID::ForNormalTask(worker_context_.GetCurrentJobID(),
                            worker_context_.GetCurrentTaskID(), next_task_index);
  BuildCommonTaskSpec(builder, worker_context_.GetCurrentJobID(), task_id,
                      worker_context_.GetCurrentTaskID(), next_task_index, GetCallerId(),
                      function, args, task_options.num_returns, task_options.resources,
                      {}, TaskTransportType::RAYLET, return_ids);
  return raylet_client_->SubmitTask(builder.Build());
}

Status CoreWorker::CreateActor(const RayFunction &function,
                               const std::vector<TaskArg> &args,
                               const ActorCreationOptions &actor_creation_options,
                               ActorID *return_actor_id) {
  const int next_task_index = worker_context_.GetNextTaskIndex();
  const ActorID actor_id =
      ActorID::Of(worker_context_.GetCurrentJobID(), worker_context_.GetCurrentTaskID(),
                  next_task_index);
  const TaskID actor_creation_task_id = TaskID::ForActorCreationTask(actor_id);
  const JobID job_id = worker_context_.GetCurrentJobID();
  std::vector<ObjectID> return_ids;
  TaskSpecBuilder builder;
  BuildCommonTaskSpec(
      builder, job_id, actor_creation_task_id, worker_context_.GetCurrentTaskID(),
      next_task_index, GetCallerId(), function, args, 1, actor_creation_options.resources,
      actor_creation_options.placement_resources, TaskTransportType::RAYLET, &return_ids);
  builder.SetActorCreationTaskSpec(actor_id, actor_creation_options.max_reconstructions,
                                   actor_creation_options.dynamic_worker_options,
                                   actor_creation_options.is_direct_call);

  std::unique_ptr<ActorHandle> actor_handle(new ActorHandle(
      actor_id, job_id, /*actor_cursor=*/return_ids[0], function.GetLanguage(),
      actor_creation_options.is_direct_call, function.GetFunctionDescriptor()));
  RAY_CHECK(AddActorHandle(std::move(actor_handle)))
      << "Actor " << actor_id << " already exists";

  RAY_RETURN_NOT_OK(raylet_client_->SubmitTask(builder.Build()));
  *return_actor_id = actor_id;
  return Status::OK();
}

Status CoreWorker::SubmitActorTask(const ActorID &actor_id, const RayFunction &function,
                                   const std::vector<TaskArg> &args,
                                   const TaskOptions &task_options,
                                   std::vector<ObjectID> *return_ids) {
  ActorHandle *actor_handle = nullptr;
  RAY_RETURN_NOT_OK(GetActorHandle(actor_id, &actor_handle));

  // Add one for actor cursor object id for tasks.
  const int num_returns = task_options.num_returns + 1;

  const bool is_direct_call = actor_handle->IsDirectCallActor();
  const TaskTransportType transport_type =
      is_direct_call ? TaskTransportType::DIRECT_ACTOR : TaskTransportType::RAYLET;

  // Build common task spec.
  TaskSpecBuilder builder;
  const int next_task_index = worker_context_.GetNextTaskIndex();
  const TaskID actor_task_id = TaskID::ForActorTask(
      worker_context_.GetCurrentJobID(), worker_context_.GetCurrentTaskID(),
      next_task_index, actor_handle->GetActorID());
  BuildCommonTaskSpec(builder, actor_handle->CreationJobID(), actor_task_id,
                      worker_context_.GetCurrentTaskID(), next_task_index, GetCallerId(),
                      function, args, num_returns, task_options.resources, {},
                      transport_type, return_ids);

  const ObjectID new_cursor = return_ids->back();
  actor_handle->SetActorTaskSpec(builder, transport_type, new_cursor);
  // Remove cursor from return ids.
  return_ids->pop_back();

  // Submit task.
  Status status;
  if (is_direct_call) {
    status = direct_actor_submitter_->SubmitTask(builder.Build());
  } else {
    status = raylet_client_->SubmitTask(builder.Build());
  }
  return status;
}

ActorID CoreWorker::DeserializeAndRegisterActorHandle(const std::string &serialized) {
  std::unique_ptr<ActorHandle> actor_handle(new ActorHandle(serialized));
  const ActorID actor_id = actor_handle->GetActorID();
  RAY_UNUSED(AddActorHandle(std::move(actor_handle)));
  return actor_id;
}

Status CoreWorker::SerializeActorHandle(const ActorID &actor_id,
                                        std::string *output) const {
  ActorHandle *actor_handle = nullptr;
  auto status = GetActorHandle(actor_id, &actor_handle);
  if (status.ok()) {
    actor_handle->Serialize(output);
  }
  return status;
}

bool CoreWorker::AddActorHandle(std::unique_ptr<ActorHandle> actor_handle) {
  const auto &actor_id = actor_handle->GetActorID();
  auto inserted = actor_handles_.emplace(actor_id, std::move(actor_handle)).second;
  if (inserted) {
    // Register a callback to handle actor notifications.
    auto actor_notification_callback = [this](const ActorID &actor_id,
                                              const gcs::ActorTableData &actor_data) {
      if (actor_data.state() == gcs::ActorTableData::RECONSTRUCTING) {
        auto it = actor_handles_.find(actor_id);
        RAY_CHECK(it != actor_handles_.end());
        if (it->second->IsDirectCallActor()) {
          // We have to reset the actor handle since the next instance of the
          // actor will not have the last sequence number that we sent.
          // TODO: Remove the check for direct calls. We do not reset for the
          // raylet codepath because it tries to replay all tasks since the
          // last actor checkpoint.
          it->second->Reset();
        }
      } else if (actor_data.state() == gcs::ActorTableData::DEAD) {
        RAY_CHECK_OK(gcs_client_.Actors().AsyncUnsubscribe(actor_id, nullptr));
        // We cannot erase the actor handle here because clients can still
        // submit tasks to dead actors.
      }

      direct_actor_submitter_->HandleActorUpdate(actor_id, actor_data);

      RAY_LOG(INFO) << "received notification on actor, state="
                    << static_cast<int>(actor_data.state()) << ", actor_id: " << actor_id
                    << ", ip address: " << actor_data.ip_address()
                    << ", port: " << actor_data.port();
    };

    RAY_CHECK_OK(gcs_client_.Actors().AsyncSubscribe(
        actor_id, actor_notification_callback, nullptr));
  }
  return inserted;
}

Status CoreWorker::GetActorHandle(const ActorID &actor_id,
                                  ActorHandle **actor_handle) const {
  auto it = actor_handles_.find(actor_id);
  if (it == actor_handles_.end()) {
    return Status::Invalid("Handle for actor does not exist");
  }
  *actor_handle = it->second.get();
  return Status::OK();
}

std::unique_ptr<worker::ProfileEvent> CoreWorker::CreateProfileEvent(
    const std::string &event_type) {
  return std::unique_ptr<worker::ProfileEvent>(
      new worker::ProfileEvent(profiler_, event_type));
}

void CoreWorker::StartExecutingTasks() {
  idle_profile_event_.reset(new worker::ProfileEvent(profiler_, "worker_idle"));
  task_execution_service_.run();
}

Status CoreWorker::ExecuteTask(const TaskSpecification &task_spec,
                               const ResourceMappingType &resource_ids,
                               std::vector<std::shared_ptr<RayObject>> *results) {
  idle_profile_event_.reset();
  RAY_LOG(DEBUG) << "Executing task " << task_spec.TaskId();

  resource_ids_ = resource_ids;
  worker_context_.SetCurrentTask(task_spec);
  SetCurrentTaskId(task_spec.TaskId());

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
    SetActorId(task_spec.ActorCreationId());
  } else if (task_spec.IsActorTask()) {
    RAY_CHECK(return_ids.size() > 0);
    return_ids.pop_back();
    task_type = TaskType::ACTOR_TASK;
  }
  status = task_execution_callback_(task_type, func,
                                    task_spec.GetRequiredResources().GetResourceMap(),
                                    args, arg_reference_ids, return_ids,
                                    worker_context_.CurrentActorUseDirectCall(), results);

  SetCurrentTaskId(TaskID::Nil());
  worker_context_.ResetCurrentTask(task_spec);

  // TODO(zhijunfu):
  // 1. Check and handle failure.
  // 2. Save or load checkpoint.
  idle_profile_event_.reset(new worker::ProfileEvent(profiler_, "worker_idle"));
  return status;
}

Status CoreWorker::BuildArgsForExecutor(const TaskSpecification &task,
                                        std::vector<std::shared_ptr<RayObject>> *args,
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
