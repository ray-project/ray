// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/core_worker/core_worker.h"

#include "boost/fiber/all.hpp"
#include "ray/common/bundle_spec.h"
#include "ray/common/ray_config.h"
#include "ray/common/task/task_util.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/gcs/gcs_client/service_based_gcs_client.h"
#include "ray/stats/stats.h"
#include "ray/util/process.h"
#include "ray/util/util.h"

namespace {

// Duration between internal book-keeping heartbeats.
const uint64_t kInternalHeartbeatMillis = 1000;

void BuildCommonTaskSpec(
    ray::TaskSpecBuilder &builder, const JobID &job_id, const TaskID &task_id,
    const std::string name, const TaskID &current_task_id, const uint64_t task_index,
    const TaskID &caller_id, const ray::rpc::Address &address,
    const ray::RayFunction &function,
    const std::vector<std::unique_ptr<ray::TaskArg>> &args, uint64_t num_returns,
    const std::unordered_map<std::string, double> &required_resources,
    const std::unordered_map<std::string, double> &required_placement_resources,
    std::vector<ObjectID> *return_ids, const ray::BundleID &bundle_id,
    bool placement_group_capture_child_tasks, const std::string debugger_breakpoint,
    const ray::RuntimeEnv &runtime_env,
    const std::unordered_map<std::string, std::string> &override_environment_variables) {
  // Build common task spec.
  builder.SetCommonTaskSpec(
      task_id, name, function.GetLanguage(), function.GetFunctionDescriptor(), job_id,
      current_task_id, task_index, caller_id, address, num_returns, required_resources,
      required_placement_resources, bundle_id, placement_group_capture_child_tasks,
      debugger_breakpoint, runtime_env, override_environment_variables);
  // Set task arguments.
  for (const auto &arg : args) {
    builder.AddArg(*arg);
  }

  // Compute return IDs.
  return_ids->resize(num_returns);
  for (size_t i = 0; i < num_returns; i++) {
    (*return_ids)[i] = ObjectID::FromIndex(task_id, i + 1);
  }
}

ray::JobID GetProcessJobID(const ray::CoreWorkerOptions &options) {
  if (options.worker_type == ray::WorkerType::DRIVER) {
    RAY_CHECK(!options.job_id.IsNil());
  } else {
    RAY_CHECK(options.job_id.IsNil());
  }

  if (options.worker_type == ray::WorkerType::WORKER) {
    // For workers, the job ID is assigned by Raylet via an environment variable.
    const char *job_id_env = std::getenv(kEnvVarKeyJobId);
    RAY_CHECK(job_id_env);
    return ray::JobID::FromHex(job_id_env);
  }
  return options.job_id;
}

}  // namespace

namespace ray {

/// The global instance of `CoreWorkerProcess`.
std::unique_ptr<CoreWorkerProcess> core_worker_process;

thread_local std::weak_ptr<CoreWorker> CoreWorkerProcess::current_core_worker_;

void CoreWorkerProcess::Initialize(const CoreWorkerOptions &options) {
  RAY_CHECK(!core_worker_process)
      << "The process is already initialized for core worker.";
  core_worker_process.reset(new CoreWorkerProcess(options));
}

void CoreWorkerProcess::Shutdown() {
  if (!core_worker_process) {
    return;
  }
  RAY_CHECK(core_worker_process->options_.worker_type == WorkerType::DRIVER)
      << "The `Shutdown` interface is for driver only.";
  RAY_CHECK(core_worker_process->global_worker_);
  core_worker_process->global_worker_->Disconnect();
  core_worker_process->global_worker_->Shutdown();
  core_worker_process->RemoveWorker(core_worker_process->global_worker_);
  core_worker_process.reset();
}

bool CoreWorkerProcess::IsInitialized() { return core_worker_process != nullptr; }

CoreWorkerProcess::CoreWorkerProcess(const CoreWorkerOptions &options)
    : options_(options),
      global_worker_id_(
          options.worker_type == WorkerType::DRIVER
              ? ComputeDriverIdFromJob(options_.job_id)
              : (options_.num_workers == 1 ? WorkerID::FromRandom() : WorkerID::Nil())) {
  if (options_.enable_logging) {
    std::stringstream app_name;
    app_name << LanguageString(options_.language) << "-core-"
             << WorkerTypeString(options_.worker_type);
    if (!global_worker_id_.IsNil()) {
      app_name << "-" << global_worker_id_;
    }
    RayLog::StartRayLog(app_name.str(), RayLogLevel::INFO, options_.log_dir);
    if (options_.install_failure_signal_handler) {
      RayLog::InstallFailureSignalHandler();
    }
  } else {
    RAY_CHECK(options_.log_dir.empty())
        << "log_dir must be empty because ray log is disabled.";
    RAY_CHECK(!options_.install_failure_signal_handler)
        << "install_failure_signal_handler must be false because ray log is disabled.";
  }

  RAY_CHECK(options_.num_workers > 0);
  if (options_.worker_type == WorkerType::DRIVER) {
    // Driver process can only contain one worker.
    RAY_CHECK(options_.num_workers == 1);
  }

  RAY_LOG(INFO) << "Constructing CoreWorkerProcess. pid: " << getpid();

  // NOTE(kfstorm): any initialization depending on RayConfig must happen after this line.
  InitializeSystemConfig();

  if (options_.num_workers == 1) {
    // We need to create the worker instance here if:
    // 1. This is a driver process. In this case, the driver is ready to use right after
    // the CoreWorkerProcess::Initialize.
    // 2. This is a Python worker process. In this case, Python will invoke some core
    // worker APIs before `CoreWorkerProcess::RunTaskExecutionLoop` is called. So we need
    // to create the worker instance here. One example of invocations is
    // https://github.com/ray-project/ray/blob/45ce40e5d44801193220d2c546be8de0feeef988/python/ray/worker.py#L1281.
    if (options_.worker_type == WorkerType::DRIVER ||
        options_.language == Language::PYTHON) {
      CreateWorker();
    }
  }

  // Assume stats module will be initialized exactly once in once process.
  // So it must be called in CoreWorkerProcess constructor and will be reused
  // by all of core worker.
  RAY_LOG(DEBUG) << "Stats setup in core worker.";
  // Initialize stats in core worker global tags.
  const ray::stats::TagsType global_tags = {{ray::stats::ComponentKey, "core_worker"},
                                            {ray::stats::VersionKey, "2.0.0.dev0"}};

  // NOTE(lingxuan.zlx): We assume RayConfig is initialized before it's used.
  // RayConfig is generated in Java_io_ray_runtime_RayNativeRuntime_nativeInitialize
  // for java worker or in constructor of CoreWorker for python worker.
  ray::stats::Init(global_tags, options_.metrics_agent_port);

#ifndef _WIN32
  // NOTE(kfstorm): std::atexit should be put at the end of `CoreWorkerProcess`
  // constructor. We assume that spdlog has been initialized before this line. When the
  // process is exiting, `HandleAtExit` will be invoked before destructing spdlog static
  // variables. We explicitly destruct `CoreWorkerProcess` instance in the callback to
  // ensure the static `CoreWorkerProcess` instance is destructed while spdlog is still
  // usable. This prevents crashing (or hanging) when using `RAY_LOG` in
  // `CoreWorkerProcess` destructor.
  RAY_CHECK(std::atexit(CoreWorkerProcess::HandleAtExit) == 0);
#endif
}

CoreWorkerProcess::~CoreWorkerProcess() {
  RAY_LOG(INFO) << "Destructing CoreWorkerProcess. pid: " << getpid();
  RAY_LOG(DEBUG) << "Stats stop in core worker.";
  // Shutdown stats module if worker process exits.
  ray::stats::Shutdown();
  if (options_.enable_logging) {
    RayLog::ShutDownRayLog();
  }
}

void CoreWorkerProcess::EnsureInitialized() {
  RAY_CHECK(core_worker_process)
      << "The core worker process is not initialized yet or already "
      << "shutdown.";
}

void CoreWorkerProcess::HandleAtExit() { core_worker_process.reset(); }

void CoreWorkerProcess::InitializeSystemConfig() {
  // We have to create a short-time thread here because the RPC request to get the system
  // config from Raylet is asynchronous, and we need to synchronously initialize the
  // system config in the constructor of `CoreWorkerProcess`.
  std::promise<std::string> promise;
  std::thread thread([&] {
    instrumented_io_context io_service;
    boost::asio::io_service::work work(io_service);
    rpc::ClientCallManager client_call_manager(io_service);
    auto grpc_client = rpc::NodeManagerWorkerClient::make(
        options_.raylet_ip_address, options_.node_manager_port, client_call_manager);
    raylet::RayletClient raylet_client(grpc_client);

    std::function<void(int64_t)> get_once = [&get_once, &raylet_client, &promise,
                                             &io_service](int64_t num_attempts) {
      raylet_client.GetSystemConfig([num_attempts, &get_once, &promise, &io_service](
                                        const Status &status,
                                        const rpc::GetSystemConfigReply &reply) {
        RAY_LOG(DEBUG) << "Getting system config from raylet, remaining retries = "
                       << num_attempts;
        if (!status.ok()) {
          if (num_attempts <= 1) {
            RAY_LOG(FATAL) << "Failed to get the system config from Raylet: " << status;
          } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(
                RayConfig::instance().raylet_client_connect_timeout_milliseconds()));
            get_once(num_attempts - 1);
          }
        } else {
          promise.set_value(reply.system_config());
          io_service.stop();
        }
      });
    };

    get_once(RayConfig::instance().raylet_client_num_connect_attempts());
    io_service.run();
  });
  thread.join();

  RayConfig::instance().initialize(promise.get_future().get());
}

std::shared_ptr<CoreWorker> CoreWorkerProcess::TryGetWorker(const WorkerID &worker_id) {
  if (!core_worker_process) {
    return nullptr;
  }
  absl::ReaderMutexLock workers_lock(&core_worker_process->worker_map_mutex_);
  auto it = core_worker_process->workers_.find(worker_id);
  if (it != core_worker_process->workers_.end()) {
    return it->second;
  }
  return nullptr;
}

CoreWorker &CoreWorkerProcess::GetCoreWorker() {
  EnsureInitialized();
  if (core_worker_process->options_.num_workers == 1) {
    RAY_CHECK(core_worker_process->global_worker_) << "global_worker_ must not be NULL";
    return *core_worker_process->global_worker_;
  }
  auto ptr = current_core_worker_.lock();
  RAY_CHECK(ptr != nullptr)
      << "The current thread is not bound with a core worker instance.";
  return *ptr;
}

void CoreWorkerProcess::SetCurrentThreadWorkerId(const WorkerID &worker_id) {
  EnsureInitialized();
  if (core_worker_process->options_.num_workers == 1) {
    RAY_CHECK(core_worker_process->global_worker_->GetWorkerID() == worker_id);
    return;
  }
  current_core_worker_ = core_worker_process->GetWorker(worker_id);
}

std::shared_ptr<CoreWorker> CoreWorkerProcess::GetWorker(
    const WorkerID &worker_id) const {
  absl::ReaderMutexLock lock(&worker_map_mutex_);
  auto it = workers_.find(worker_id);
  RAY_CHECK(it != workers_.end()) << "Worker " << worker_id << " not found.";
  return it->second;
}

std::shared_ptr<CoreWorker> CoreWorkerProcess::CreateWorker() {
  auto worker = std::make_shared<CoreWorker>(
      options_,
      global_worker_id_ != WorkerID::Nil() ? global_worker_id_ : WorkerID::FromRandom());
  RAY_LOG(INFO) << "Worker " << worker->GetWorkerID() << " is created.";
  if (options_.num_workers == 1) {
    global_worker_ = worker;
  }
  current_core_worker_ = worker;

  absl::MutexLock lock(&worker_map_mutex_);
  workers_.emplace(worker->GetWorkerID(), worker);
  RAY_CHECK(workers_.size() <= static_cast<size_t>(options_.num_workers));
  return worker;
}

void CoreWorkerProcess::RemoveWorker(std::shared_ptr<CoreWorker> worker) {
  worker->WaitForShutdown();
  if (global_worker_) {
    RAY_CHECK(global_worker_ == worker);
  } else {
    RAY_CHECK(current_core_worker_.lock() == worker);
  }
  current_core_worker_.reset();
  {
    absl::MutexLock lock(&worker_map_mutex_);
    workers_.erase(worker->GetWorkerID());
    RAY_LOG(INFO) << "Removed worker " << worker->GetWorkerID();
  }
  if (global_worker_ == worker) {
    global_worker_ = nullptr;
  }
}

void CoreWorkerProcess::RunTaskExecutionLoop() {
  EnsureInitialized();
  RAY_CHECK(core_worker_process->options_.worker_type == WorkerType::WORKER);
  if (core_worker_process->options_.num_workers == 1) {
    // Run the task loop in the current thread only if the number of workers is 1.
    auto worker = core_worker_process->global_worker_
                      ? core_worker_process->global_worker_
                      : core_worker_process->CreateWorker();
    worker->RunTaskExecutionLoop();
    core_worker_process->RemoveWorker(worker);
  } else {
    std::vector<std::thread> worker_threads;
    for (int i = 0; i < core_worker_process->options_.num_workers; i++) {
      worker_threads.emplace_back([i] {
        SetThreadName("worker.task" + std::to_string(i));
        auto worker = core_worker_process->CreateWorker();
        worker->RunTaskExecutionLoop();
        core_worker_process->RemoveWorker(worker);
      });
    }
    for (auto &thread : worker_threads) {
      thread.join();
    }
  }

  core_worker_process.reset();
}

CoreWorker::CoreWorker(const CoreWorkerOptions &options, const WorkerID &worker_id)
    : options_(options),
      get_call_site_(RayConfig::instance().record_ref_creation_sites()
                         ? options_.get_lang_stack
                         : nullptr),
      worker_context_(options_.worker_type, worker_id, GetProcessJobID(options_)),
      io_work_(io_service_),
      client_call_manager_(new rpc::ClientCallManager(io_service_)),
      periodical_runner_(io_service_),
      task_queue_length_(0),
      num_executed_tasks_(0),
      task_execution_service_work_(task_execution_service_),
      resource_ids_(new ResourceMappingType()),
      grpc_service_(io_service_, *this) {
  RAY_LOG(INFO) << "Constructing CoreWorker, worker_id: " << worker_id;

  // Initialize task receivers.
  if (options_.worker_type == WorkerType::WORKER || options_.is_local_mode) {
    RAY_CHECK(options_.task_execution_callback != nullptr);
    auto execute_task =
        std::bind(&CoreWorker::ExecuteTask, this, std::placeholders::_1,
                  std::placeholders::_2, std::placeholders::_3, std::placeholders::_4);
    direct_task_receiver_ = std::make_unique<CoreWorkerDirectTaskReceiver>(
        worker_context_, task_execution_service_, execute_task,
        [this] { return local_raylet_client_->TaskDone(); });
  }

  // Initialize raylet client.
  // NOTE(edoakes): the core_worker_server_ must be running before registering with
  // the raylet, as the raylet will start sending some RPC messages immediately.
  // TODO(zhijunfu): currently RayletClient would crash in its constructor if it cannot
  // connect to Raylet after a number of retries, this can be changed later
  // so that the worker (java/python .etc) can retrieve and handle the error
  // instead of crashing.
  auto grpc_client = rpc::NodeManagerWorkerClient::make(
      options_.raylet_ip_address, options_.node_manager_port, *client_call_manager_);
  Status raylet_client_status;
  NodeID local_raylet_id;
  int assigned_port;
  std::string serialized_job_config = options_.serialized_job_config;
  local_raylet_client_ = std::make_shared<raylet::RayletClient>(
      io_service_, std::move(grpc_client), options_.raylet_socket, GetWorkerID(),
      options_.worker_type, worker_context_.GetCurrentJobID(), options_.language,
      options_.node_ip_address, &raylet_client_status, &local_raylet_id, &assigned_port,
      &serialized_job_config);

  if (!raylet_client_status.ok()) {
    // Avoid using FATAL log or RAY_CHECK here because they may create a core dump file.
    RAY_LOG(ERROR) << "Failed to register worker " << worker_id << " to Raylet. "
                   << raylet_client_status;
    if (options_.enable_logging) {
      RayLog::ShutDownRayLog();
    }
    // Quit the process immediately.
    _Exit(1);
  }

  connected_ = true;

  RAY_CHECK(assigned_port >= 0);

  // Parse job config from serialized string.
  job_config_.reset(new rpc::JobConfig());
  job_config_->ParseFromString(serialized_job_config);

  // Start RPC server after all the task receivers are properly initialized and we have
  // our assigned port from the raylet.
  core_worker_server_ = std::make_unique<rpc::GrpcServer>(
      WorkerTypeString(options_.worker_type), assigned_port);
  core_worker_server_->RegisterService(grpc_service_);
  core_worker_server_->Run();

  // Set our own address.
  RAY_CHECK(!local_raylet_id.IsNil());
  rpc_address_.set_ip_address(options_.node_ip_address);
  rpc_address_.set_port(core_worker_server_->GetPort());
  rpc_address_.set_raylet_id(local_raylet_id.Binary());
  rpc_address_.set_worker_id(worker_context_.GetWorkerID().Binary());
  RAY_LOG(INFO) << "Initializing worker at address: " << rpc_address_.ip_address() << ":"
                << rpc_address_.port() << ", worker ID " << worker_context_.GetWorkerID()
                << ", raylet " << local_raylet_id;

  // Initialize gcs client.
  // As asynchronous context of redis client is not used in this gcs client. We would not
  // open connection for it by setting `enable_async_conn` as false.
  gcs::GcsClientOptions gcs_options = gcs::GcsClientOptions(
      options_.gcs_options.server_ip_, options_.gcs_options.server_port_,
      options_.gcs_options.password_,
      /*enable_sync_conn=*/true, /*enable_async_conn=*/false,
      /*enable_subscribe_conn=*/true);
  gcs_client_ = std::make_shared<ray::gcs::ServiceBasedGcsClient>(gcs_options);

  RAY_CHECK_OK(gcs_client_->Connect(io_service_));
  RegisterToGcs();

  // Register a callback to monitor removed nodes.
  auto on_node_change = [this](const NodeID &node_id, const rpc::GcsNodeInfo &data) {
    if (data.state() == rpc::GcsNodeInfo::DEAD) {
      OnNodeRemoved(node_id);
    }
  };
  RAY_CHECK_OK(gcs_client_->Nodes().AsyncSubscribeToNodeChange(on_node_change, nullptr));

  // Initialize profiler.
  profiler_ = std::make_shared<worker::Profiler>(
      worker_context_, options_.node_ip_address, io_service_, gcs_client_);

  reference_counter_ = std::make_shared<ReferenceCounter>(
      rpc_address_, RayConfig::instance().distributed_ref_counting_enabled(),
      RayConfig::instance().lineage_pinning_enabled(), [this](const rpc::Address &addr) {
        return std::shared_ptr<rpc::CoreWorkerClient>(
            new rpc::CoreWorkerClient(addr, *client_call_manager_));
      });

  if (options_.worker_type == ray::WorkerType::WORKER) {
    periodical_runner_.RunFnPeriodically(
        [this] { CheckForRayletFailure(); },
        RayConfig::instance().raylet_death_check_interval_milliseconds());
  }

  plasma_store_provider_.reset(new CoreWorkerPlasmaStoreProvider(
      options_.store_socket, local_raylet_client_, reference_counter_,
      options_.check_signals,
      /*warmup=*/
      (options_.worker_type != ray::WorkerType::SPILL_WORKER &&
       options_.worker_type != ray::WorkerType::RESTORE_WORKER &&
       options_.worker_type != ray::WorkerType::UTIL_WORKER),
      /*get_current_call_site=*/boost::bind(&CoreWorker::CurrentCallSite, this)));
  memory_store_.reset(new CoreWorkerMemoryStore(
      [this](const RayObject &object, const ObjectID &object_id) {
        PutObjectIntoPlasma(object, object_id);
        return Status::OK();
      },
      options_.ref_counting_enabled ? reference_counter_ : nullptr, local_raylet_client_,
      options_.check_signals,
      [this](const RayObject &obj) {
        // Run this on the event loop to avoid calling back into the language runtime
        // from the middle of user operations.
        io_service_.post(
            [this, obj]() {
              if (options_.unhandled_exception_handler != nullptr) {
                options_.unhandled_exception_handler(obj);
              }
            },
            "CoreWorker.HandleException");
      }));

  periodical_runner_.RunFnPeriodically([this] { InternalHeartbeat(); },
                                       kInternalHeartbeatMillis);

  auto check_node_alive_fn = [this](const NodeID &node_id) {
    auto node = gcs_client_->Nodes().Get(node_id);
    return node.has_value();
  };
  auto reconstruct_object_callback = [this](const ObjectID &object_id) {
    io_service_.post(
        [this, object_id]() {
          RAY_CHECK(object_recovery_manager_->RecoverObject(object_id));
        },
        "CoreWorker.ReconstructObject");
  };
  task_manager_.reset(new TaskManager(
      memory_store_, reference_counter_,
      /* retry_task_callback= */
      [this](TaskSpecification &spec, bool delay) {
        if (delay) {
          // Retry after a delay to emulate the existing Raylet reconstruction
          // behaviour. TODO(ekl) backoff exponentially.
          uint32_t delay = RayConfig::instance().task_retry_delay_ms();
          RAY_LOG(ERROR) << "Will resubmit task after a " << delay
                         << "ms delay: " << spec.DebugString();
          absl::MutexLock lock(&mutex_);
          to_resubmit_.push_back(std::make_pair(current_time_ms() + delay, spec));
        } else {
          RAY_LOG(ERROR) << "Resubmitting task that produced lost plasma object: "
                         << spec.DebugString();
          if (spec.IsActorTask()) {
            auto actor_handle = actor_manager_->GetActorHandle(spec.ActorId());
            actor_handle->SetResubmittedActorTaskSpec(spec, spec.ActorDummyObject());
            RAY_CHECK_OK(direct_actor_submitter_->SubmitTask(spec));
          } else {
            RAY_CHECK_OK(direct_task_submitter_->SubmitTask(spec));
          }
        }
      },
      check_node_alive_fn, reconstruct_object_callback));

  // Create an entry for the driver task in the task table. This task is
  // added immediately with status RUNNING. This allows us to push errors
  // related to this driver task back to the driver. For example, if the
  // driver creates an object that is later evicted, we should notify the
  // user that we're unable to reconstruct the object, since we cannot
  // rerun the driver.
  if (options_.worker_type == WorkerType::DRIVER) {
    TaskSpecBuilder builder;
    const TaskID task_id = TaskID::ForDriverTask(worker_context_.GetCurrentJobID());
    builder.SetDriverTaskSpec(task_id, options_.language,
                              worker_context_.GetCurrentJobID(),
                              TaskID::ComputeDriverTaskId(worker_context_.GetWorkerID()),
                              GetCallerId(), rpc_address_);

    std::shared_ptr<rpc::TaskTableData> data = std::make_shared<rpc::TaskTableData>();
    data->mutable_task()->mutable_task_spec()->CopyFrom(builder.Build().GetMessage());
    SetCurrentTaskId(task_id);
  }

  core_worker_client_pool_ =
      std::make_shared<rpc::CoreWorkerClientPool>(*client_call_manager_);

  auto raylet_client_factory = [this](const std::string ip_address, int port) {
    auto grpc_client =
        rpc::NodeManagerWorkerClient::make(ip_address, port, *client_call_manager_);
    return std::shared_ptr<raylet::RayletClient>(
        new raylet::RayletClient(std::move(grpc_client)));
  };

  std::shared_ptr<ActorCreatorInterface> actor_creator =
      std::make_shared<DefaultActorCreator>(gcs_client_);

  direct_actor_submitter_ = std::shared_ptr<CoreWorkerDirectActorTaskSubmitter>(
      new CoreWorkerDirectActorTaskSubmitter(core_worker_client_pool_, memory_store_,
                                             task_manager_));

  object_status_publisher_ = std::make_shared<pubsub::Publisher>(
      /*periodical_runner=*/&periodical_runner_,
      /*get_time_ms=*/[]() { return absl::GetCurrentTimeNanos() / 1e6; },
      /*subscriber_timeout_ms=*/RayConfig::instance().subscriber_timeout_ms(),
      /*publish_batch_size_=*/RayConfig::instance().publish_batch_size());

  auto node_addr_factory = [this](const NodeID &node_id) {
    absl::optional<rpc::Address> addr;
    if (auto node_info = gcs_client_->Nodes().Get(node_id)) {
      rpc::Address address;
      address.set_raylet_id(node_info->node_id());
      address.set_ip_address(node_info->node_manager_address());
      address.set_port(node_info->node_manager_port());
      addr = address;
    }
    return addr;
  };
  auto lease_policy = RayConfig::instance().locality_aware_leasing_enabled()
                          ? std::shared_ptr<LeasePolicyInterface>(
                                std::make_shared<LocalityAwareLeasePolicy>(
                                    reference_counter_, node_addr_factory, rpc_address_))
                          : std::shared_ptr<LeasePolicyInterface>(
                                std::make_shared<LocalLeasePolicy>(rpc_address_));
  direct_task_submitter_ = std::make_unique<CoreWorkerDirectTaskSubmitter>(
      rpc_address_, local_raylet_client_, core_worker_client_pool_, raylet_client_factory,
      std::move(lease_policy), memory_store_, task_manager_, local_raylet_id,
      RayConfig::instance().worker_lease_timeout_milliseconds(), std::move(actor_creator),
      RayConfig::instance().max_tasks_in_flight_per_worker(),
      boost::asio::steady_timer(io_service_));
  auto report_locality_data_callback =
      [this](const ObjectID &object_id, const absl::flat_hash_set<NodeID> &locations,
             uint64_t object_size) {
        reference_counter_->ReportLocalityData(object_id, locations, object_size);
      };
  future_resolver_.reset(new FutureResolver(memory_store_,
                                            std::move(report_locality_data_callback),
                                            core_worker_client_pool_, rpc_address_));
  // Unfortunately the raylet client has to be constructed after the receivers.
  if (direct_task_receiver_ != nullptr) {
    task_argument_waiter_.reset(new DependencyWaiterImpl(*local_raylet_client_));
    direct_task_receiver_->Init(core_worker_client_pool_, rpc_address_,
                                task_argument_waiter_);
  }

  actor_manager_ = std::make_unique<ActorManager>(gcs_client_, direct_actor_submitter_,
                                                  reference_counter_);

  std::function<Status(const ObjectID &object_id, const ObjectLookupCallback &callback)>
      object_lookup_fn;

  if (RayConfig::instance().ownership_based_object_directory_enabled()) {
    object_lookup_fn = [this, node_addr_factory](const ObjectID &object_id,
                                                 const ObjectLookupCallback &callback) {
      std::vector<rpc::Address> locations;
      const absl::optional<absl::flat_hash_set<NodeID>> object_locations =
          reference_counter_->GetObjectLocations(object_id);
      if (object_locations.has_value()) {
        locations.reserve(object_locations.value().size());
        for (const auto &node_id : object_locations.value()) {
          absl::optional<rpc::Address> addr = node_addr_factory(node_id);
          if (addr.has_value()) {
            locations.push_back(addr.value());
          } else {
            // We're getting potentially stale locations directly from the reference
            // counter, so the location might be a dead node.
            RAY_LOG(DEBUG) << "Location " << node_id
                           << " is dead, not using it in the recovery of object "
                           << object_id;
          }
        }
      }
      callback(object_id, locations);
      return Status::OK();
    };
  } else {
    object_lookup_fn = [this](const ObjectID &object_id,
                              const ObjectLookupCallback &callback) {
      return gcs_client_->Objects().AsyncGetLocations(
          object_id, [this, object_id, callback](
                         const Status &status,
                         const boost::optional<rpc::ObjectLocationInfo> &result) {
            RAY_CHECK_OK(status);
            std::vector<rpc::Address> locations;
            for (const auto &loc : result->locations()) {
              const auto &node_id = NodeID::FromBinary(loc.manager());
              auto node = gcs_client_->Nodes().Get(node_id);
              RAY_CHECK(node.has_value());
              rpc::Address address;
              address.set_raylet_id(node->node_id());
              address.set_ip_address(node->node_manager_address());
              address.set_port(node->node_manager_port());
              locations.push_back(address);
            }
            callback(object_id, locations);
          });
    };
  }
  object_recovery_manager_ = std::make_unique<ObjectRecoveryManager>(
      rpc_address_, raylet_client_factory, local_raylet_client_, object_lookup_fn,
      task_manager_, reference_counter_, memory_store_,
      [this](const ObjectID &object_id, bool pin_object) {
        RAY_CHECK_OK(Put(RayObject(rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE),
                         /*contained_object_ids=*/{}, object_id,
                         /*pin_object=*/pin_object));
      },
      RayConfig::instance().lineage_pinning_enabled());

  // Start the IO thread after all other members have been initialized, in case
  // the thread calls back into any of our members.
  io_thread_ = std::thread([this]() { RunIOService(); });
  // Tell the raylet the port that we are listening on.
  // NOTE: This also marks the worker as available in Raylet. We do this at the
  // very end in case there is a problem during construction.
  if (options.connect_on_start) {
    RAY_CHECK_OK(
        local_raylet_client_->AnnounceWorkerPort(core_worker_server_->GetPort()));
  }
  // Used to detect if the object is in the plasma store.
  max_direct_call_object_size_ = RayConfig::instance().max_direct_call_object_size();

  /// If periodic asio stats print is enabled, it will print it.
  const auto asio_stats_print_interval_ms =
      RayConfig::instance().asio_stats_print_interval_ms();
  if (asio_stats_print_interval_ms != -1 &&
      RayConfig::instance().asio_event_loop_stats_collection_enabled()) {
    periodical_runner_.RunFnPeriodically(
        [this] {
          RAY_LOG(INFO) << "Event loop stats:\n\n" << io_service_.StatsString() << "\n\n";
        },
        asio_stats_print_interval_ms);
  }
}

void CoreWorker::Shutdown() {
  io_service_.stop();
  if (options_.worker_type == WorkerType::WORKER) {
    task_execution_service_.stop();
  }
  if (options_.on_worker_shutdown) {
    options_.on_worker_shutdown(GetWorkerID());
  }
}

void CoreWorker::ConnectToRaylet() {
  RAY_CHECK(!options_.connect_on_start);
  // Tell the raylet the port that we are listening on.
  // NOTE: This also marks the worker as available in Raylet. We do this at the
  // very end in case there is a problem during construction.
  RAY_CHECK_OK(local_raylet_client_->AnnounceWorkerPort(core_worker_server_->GetPort()));
}

void CoreWorker::Disconnect(
    rpc::WorkerExitType exit_type,
    const std::shared_ptr<LocalMemoryBuffer> &creation_task_exception_pb_bytes) {
  if (connected_) {
    connected_ = false;
    if (local_raylet_client_) {
      RAY_IGNORE_EXPR(
          local_raylet_client_->Disconnect(exit_type, creation_task_exception_pb_bytes));
    }
  }
}

void CoreWorker::Exit(
    rpc::WorkerExitType exit_type,
    const std::shared_ptr<LocalMemoryBuffer> &creation_task_exception_pb_bytes) {
  RAY_LOG(INFO) << "Exit signal received, this process will exit after all outstanding "
                   "tasks have finished"
                << ", exit_type=" << rpc::WorkerExitType_Name(exit_type);
  exiting_ = true;
  // Release the resources early in case draining takes a long time.
  RAY_CHECK_OK(
      local_raylet_client_->NotifyDirectCallTaskBlocked(/*release_resources*/ true));

  // Callback to shutdown.
  auto shutdown = [this, exit_type, creation_task_exception_pb_bytes]() {
    // To avoid problems, make sure shutdown is always called from the same
    // event loop each time.
    task_execution_service_.post(
        [this, exit_type, creation_task_exception_pb_bytes]() {
          if (exit_type == rpc::WorkerExitType::CREATION_TASK_ERROR ||
              exit_type == rpc::WorkerExitType::INTENDED_EXIT) {
            // Notify the raylet about this exit.
            // Only CREATION_TASK_ERROR and INTENDED_EXIT needs to disconnect
            // manually.
            Disconnect(exit_type, creation_task_exception_pb_bytes);
          }
          Shutdown();
        },
        "CoreWorker.Shutdown");
  };
  // Callback to drain objects once all pending tasks have been drained.
  auto drain_references_callback = [this, shutdown]() {
    // Post to the event loop to avoid a deadlock between the TaskManager and
    // the ReferenceCounter. The deadlock can occur because this callback may
    // get called by the TaskManager while the ReferenceCounter's lock is held,
    // but the callback itself must acquire the ReferenceCounter's lock to
    // drain the object references.
    task_execution_service_.post(
        [this, shutdown]() {
          bool not_actor_task = false;
          {
            absl::MutexLock lock(&mutex_);
            not_actor_task = actor_id_.IsNil();
          }
          if (not_actor_task) {
            // If we are a task, then we cannot hold any object references in the
            // heap. Therefore, any active object references are being held by other
            // processes. Wait for these processes to release their references
            // before we shutdown. NOTE(swang): This could still cause this worker
            // process to stay alive forever if another process holds a reference
            // forever.
            reference_counter_->DrainAndShutdown(shutdown);
          } else {
            // If we are an actor, then we may be holding object references in the
            // heap. Then, we should not wait to drain the object references before
            // shutdown since this could hang.
            shutdown();
          }
        },
        "CoreWorker.DrainAndShutdown");
  };

  task_manager_->DrainAndShutdown(drain_references_callback);
}
void CoreWorker::RunIOService() {
#ifndef _WIN32
  // Block SIGINT and SIGTERM so they will be handled by the main thread.
  sigset_t mask;
  sigemptyset(&mask);
  sigaddset(&mask, SIGINT);
  sigaddset(&mask, SIGTERM);
  pthread_sigmask(SIG_BLOCK, &mask, NULL);
#endif
  SetThreadName("worker.io");
  io_service_.run();
}

void CoreWorker::OnNodeRemoved(const NodeID &node_id) {
  RAY_LOG(INFO) << "Node failure " << node_id;
  const auto lost_objects = reference_counter_->ResetObjectsOnRemovedNode(node_id);
  // Delete the objects from the in-memory store to indicate that they are not
  // available. The object recovery manager will guarantee that a new value
  // will eventually be stored for the objects (either an
  // UnreconstructableError or a value reconstructed from lineage).
  memory_store_->Delete(lost_objects);
  for (const auto &object_id : lost_objects) {
    RAY_LOG(INFO) << "Object " << object_id << " lost due to node failure " << node_id;
    // NOTE(swang): There is a race condition where this can return false if
    // the reference went out of scope since the call to the ref counter to get
    // the lost objects. It's okay to not mark the object as failed or recover
    // the object since there are no reference holders.
    static_cast<void>(object_recovery_manager_->RecoverObject(object_id));
  }
}

void CoreWorker::WaitForShutdown() {
  if (io_thread_.joinable()) {
    io_thread_.join();
  }
  if (gcs_client_) {
    gcs_client_->Disconnect();
  }
  if (options_.worker_type == WorkerType::WORKER) {
    RAY_CHECK(task_execution_service_.stopped());
    // Asyncio coroutines could still run after CoreWorker is removed because it is
    // running in a different thread. This can cause segfault because coroutines try to
    // access CoreWorker methods that are already garbage collected. We should complete
    // all coroutines before shutting down in order to prevent this.
    if (worker_context_.CurrentActorIsAsync()) {
      options_.terminate_asyncio_thread();
    }
  }
}

const WorkerID &CoreWorker::GetWorkerID() const { return worker_context_.GetWorkerID(); }

void CoreWorker::SetCurrentTaskId(const TaskID &task_id) {
  worker_context_.SetCurrentTaskId(task_id);
  {
    absl::MutexLock lock(&mutex_);
    main_thread_task_id_ = task_id;
  }
}

void CoreWorker::RegisterToGcs() {
  std::unordered_map<std::string, std::string> worker_info;
  const auto &worker_id = GetWorkerID();
  worker_info.emplace("node_ip_address", options_.node_ip_address);
  worker_info.emplace("plasma_store_socket", options_.store_socket);
  worker_info.emplace("raylet_socket", options_.raylet_socket);

  if (options_.worker_type == WorkerType::DRIVER) {
    auto start_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();
    worker_info.emplace("driver_id", worker_id.Binary());
    worker_info.emplace("start_time", std::to_string(start_time));
    if (!options_.driver_name.empty()) {
      worker_info.emplace("name", options_.driver_name);
    }
  }

  if (!options_.stdout_file.empty()) {
    worker_info.emplace("stdout_file", options_.stdout_file);
  }
  if (!options_.stderr_file.empty()) {
    worker_info.emplace("stderr_file", options_.stderr_file);
  }

  auto worker_data = std::make_shared<rpc::WorkerTableData>();
  worker_data->mutable_worker_address()->set_worker_id(worker_id.Binary());
  worker_data->set_worker_type(options_.worker_type);
  worker_data->mutable_worker_info()->insert(worker_info.begin(), worker_info.end());
  worker_data->set_is_alive(true);

  RAY_CHECK_OK(gcs_client_->Workers().AsyncAdd(worker_data, nullptr));
}

void CoreWorker::CheckForRayletFailure() {
  if (!IsParentProcessAlive()) {
    RAY_LOG(ERROR) << "Raylet failed. Shutting down.";
    Shutdown();
  }
}

void CoreWorker::InternalHeartbeat() {
  absl::MutexLock lock(&mutex_);

  // Retry tasks.
  while (!to_resubmit_.empty() && current_time_ms() > to_resubmit_.front().first) {
    auto &spec = to_resubmit_.front().second;
    if (spec.IsActorTask()) {
      RAY_CHECK_OK(direct_actor_submitter_->SubmitTask(spec));
    } else {
      RAY_CHECK_OK(direct_task_submitter_->SubmitTask(spec));
    }
    to_resubmit_.pop_front();
  }

  // Check timeout tasks that are waiting for death info.
  if (direct_actor_submitter_ != nullptr) {
    direct_actor_submitter_->CheckTimeoutTasks();
  }

  // Check for unhandled exceptions to raise after a timeout on the driver.
  // Only do this for TTY, since shells like IPython sometimes save references
  // to the result and prevent normal result deletion from handling.
  // See also: https://github.com/ray-project/ray/issues/14485
  if (options_.worker_type == WorkerType::DRIVER && options_.interactive) {
    memory_store_->NotifyUnhandledErrors();
  }
}

std::unordered_map<ObjectID, std::pair<size_t, size_t>>
CoreWorker::GetAllReferenceCounts() const {
  auto counts = reference_counter_->GetAllReferenceCounts();
  std::vector<ObjectID> actor_handle_ids = actor_manager_->GetActorHandleIDsFromHandles();
  // Strip actor IDs from the ref counts since there is no associated ObjectID
  // in the language frontend.
  for (const auto &actor_handle_id : actor_handle_ids) {
    counts.erase(actor_handle_id);
  }
  return counts;
}

void CoreWorker::PutObjectIntoPlasma(const RayObject &object, const ObjectID &object_id) {
  bool object_exists;
  // This call will only be used by PromoteObjectToPlasma, which means that the
  // object will always owned by us.
  RAY_CHECK_OK(plasma_store_provider_->Put(
      object, object_id, /* owner_address = */ rpc_address_, &object_exists));
  if (!object_exists) {
    // Tell the raylet to pin the object **after** it is created.
    RAY_LOG(DEBUG) << "Pinning put object " << object_id;
    local_raylet_client_->PinObjectIDs(
        rpc_address_, {object_id},
        [this, object_id](const Status &status, const rpc::PinObjectIDsReply &reply) {
          // Only release the object once the raylet has responded to avoid the race
          // condition that the object could be evicted before the raylet pins it.
          if (!plasma_store_provider_->Release(object_id).ok()) {
            RAY_LOG(ERROR) << "Failed to release ObjectID (" << object_id
                           << "), might cause a leak in plasma.";
          }
        });
  }
  RAY_CHECK(memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA), object_id));
}

void CoreWorker::PromoteObjectToPlasma(const ObjectID &object_id) {
  auto value = memory_store_->GetOrPromoteToPlasma(object_id);
  if (value) {
    PutObjectIntoPlasma(*value, object_id);
  }
}

const rpc::Address &CoreWorker::GetRpcAddress() const { return rpc_address_; }

rpc::Address CoreWorker::GetOwnerAddress(const ObjectID &object_id) const {
  rpc::Address owner_address;
  auto has_owner = reference_counter_->GetOwner(object_id, &owner_address);
  RAY_CHECK(has_owner)
      << "Object IDs generated randomly (ObjectID.from_random()) or out-of-band "
         "(ObjectID.from_binary(...)) cannot be passed as a task argument because Ray "
         "does not know which task will create them. "
         "If this was not how your object ID was generated, please file an issue "
         "at https://github.com/ray-project/ray/issues/";
  return owner_address;
}

void CoreWorker::GetOwnershipInfo(const ObjectID &object_id,
                                  rpc::Address *owner_address) {
  auto has_owner = reference_counter_->GetOwner(object_id, owner_address);
  RAY_CHECK(has_owner)
      << "Object IDs generated randomly (ObjectID.from_random()) or out-of-band "
         "(ObjectID.from_binary(...)) cannot be serialized because Ray does not know "
         "which task will create them. "
         "If this was not how your object ID was generated, please file an issue "
         "at https://github.com/ray-project/ray/issues/";
  RAY_LOG(DEBUG) << "Promoted object to plasma " << object_id;
}

void CoreWorker::RegisterOwnershipInfoAndResolveFuture(
    const ObjectID &object_id, const ObjectID &outer_object_id,
    const rpc::Address &owner_address) {
  // Add the object's owner to the local metadata in case it gets serialized
  // again.
  reference_counter_->AddBorrowedObject(object_id, outer_object_id, owner_address);

  // We will ask the owner about the object until the object is
  // created or we can no longer reach the owner.
  future_resolver_->ResolveFutureAsync(object_id, owner_address);
}

Status CoreWorker::SetClientOptions(std::string name, int64_t limit_bytes) {
  // Currently only the Plasma store supports client options.
  return plasma_store_provider_->SetClientOptions(name, limit_bytes);
}

Status CoreWorker::Put(const RayObject &object,
                       const std::vector<ObjectID> &contained_object_ids,
                       ObjectID *object_id) {
  *object_id = ObjectID::FromIndex(worker_context_.GetCurrentTaskID(),
                                   worker_context_.GetNextPutIndex());
  reference_counter_->AddOwnedObject(
      *object_id, contained_object_ids, rpc_address_, CurrentCallSite(), object.GetSize(),
      /*is_reconstructable=*/false, NodeID::FromBinary(rpc_address_.raylet_id()));
  auto status = Put(object, contained_object_ids, *object_id, /*pin_object=*/true);
  if (!status.ok()) {
    reference_counter_->RemoveOwnedObject(*object_id);
  }
  return status;
}

Status CoreWorker::Put(const RayObject &object,
                       const std::vector<ObjectID> &contained_object_ids,
                       const ObjectID &object_id, bool pin_object) {
  bool object_exists;
  if (options_.is_local_mode ||
      (RayConfig::instance().put_small_object_in_memory_store() &&
       static_cast<int64_t>(object.GetSize()) < max_direct_call_object_size_)) {
    RAY_LOG(DEBUG) << "Put " << object_id << " in memory store";
    RAY_CHECK(memory_store_->Put(object, object_id));
    return Status::OK();
  }
  RAY_RETURN_NOT_OK(plasma_store_provider_->Put(
      object, object_id, /* owner_address = */ rpc_address_, &object_exists));
  if (!object_exists) {
    if (pin_object) {
      // Tell the raylet to pin the object **after** it is created.
      RAY_LOG(DEBUG) << "Pinning put object " << object_id;
      local_raylet_client_->PinObjectIDs(
          rpc_address_, {object_id},
          [this, object_id](const Status &status, const rpc::PinObjectIDsReply &reply) {
            // Only release the object once the raylet has responded to avoid the race
            // condition that the object could be evicted before the raylet pins it.
            if (!plasma_store_provider_->Release(object_id).ok()) {
              RAY_LOG(ERROR) << "Failed to release ObjectID (" << object_id
                             << "), might cause a leak in plasma.";
            }
          });
    } else {
      RAY_RETURN_NOT_OK(plasma_store_provider_->Release(object_id));
    }
  }
  RAY_CHECK(memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA), object_id));
  return Status::OK();
}

Status CoreWorker::CreateOwned(const std::shared_ptr<Buffer> &metadata,
                               const size_t data_size,
                               const std::vector<ObjectID> &contained_object_ids,
                               ObjectID *object_id, std::shared_ptr<Buffer> *data) {
  *object_id = ObjectID::FromIndex(worker_context_.GetCurrentTaskID(),
                                   worker_context_.GetNextPutIndex());
  reference_counter_->AddOwnedObject(*object_id, contained_object_ids, rpc_address_,
                                     CurrentCallSite(), data_size + metadata->Size(),
                                     /*is_reconstructable=*/false,
                                     NodeID::FromBinary(rpc_address_.raylet_id()));
  if (options_.is_local_mode ||
      (RayConfig::instance().put_small_object_in_memory_store() &&
       static_cast<int64_t>(data_size) < max_direct_call_object_size_)) {
    *data = std::make_shared<LocalMemoryBuffer>(data_size);
  } else {
    auto status =
        plasma_store_provider_->Create(metadata, data_size, *object_id,
                                       /* owner_address = */ rpc_address_, data);
    if (!status.ok() || !data) {
      reference_counter_->RemoveOwnedObject(*object_id);
      return status;
    }
  }
  return Status::OK();
}

Status CoreWorker::CreateExisting(const std::shared_ptr<Buffer> &metadata,
                                  const size_t data_size, const ObjectID &object_id,
                                  const rpc::Address &owner_address,
                                  std::shared_ptr<Buffer> *data) {
  if (options_.is_local_mode) {
    return Status::NotImplemented(
        "Creating an object with a pre-existing ObjectID is not supported in local "
        "mode");
  } else {
    return plasma_store_provider_->Create(metadata, data_size, object_id, owner_address,
                                          data);
  }
}

Status CoreWorker::SealOwned(const ObjectID &object_id, bool pin_object) {
  auto status = SealExisting(object_id, pin_object);
  if (!status.ok()) {
    reference_counter_->RemoveOwnedObject(object_id);
  }
  return status;
}

Status CoreWorker::SealExisting(const ObjectID &object_id, bool pin_object,
                                const absl::optional<rpc::Address> &owner_address) {
  RAY_RETURN_NOT_OK(plasma_store_provider_->Seal(object_id));
  if (pin_object) {
    // Tell the raylet to pin the object **after** it is created.
    RAY_LOG(DEBUG) << "Pinning sealed object " << object_id;
    local_raylet_client_->PinObjectIDs(
        owner_address.has_value() ? *owner_address : rpc_address_, {object_id},
        [this, object_id](const Status &status, const rpc::PinObjectIDsReply &reply) {
          // Only release the object once the raylet has responded to avoid the race
          // condition that the object could be evicted before the raylet pins it.
          if (!plasma_store_provider_->Release(object_id).ok()) {
            RAY_LOG(ERROR) << "Failed to release ObjectID (" << object_id
                           << "), might cause a leak in plasma.";
          }
        });
  } else {
    RAY_RETURN_NOT_OK(plasma_store_provider_->Release(object_id));
    reference_counter_->FreePlasmaObjects({object_id});
  }
  RAY_CHECK(memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA), object_id));
  return Status::OK();
}

Status CoreWorker::Get(const std::vector<ObjectID> &ids, const int64_t timeout_ms,
                       std::vector<std::shared_ptr<RayObject>> *results,
                       bool plasma_objects_only) {
  results->resize(ids.size(), nullptr);

  absl::flat_hash_set<ObjectID> plasma_object_ids;
  absl::flat_hash_set<ObjectID> memory_object_ids(ids.begin(), ids.end());

  bool got_exception = false;
  absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> result_map;
  auto start_time = current_time_ms();

  if (!plasma_objects_only) {
    if (!memory_object_ids.empty()) {
      RAY_RETURN_NOT_OK(memory_store_->Get(memory_object_ids, timeout_ms, worker_context_,
                                           &result_map, &got_exception));
    }

    // Erase any objects that were promoted to plasma from the results. These get
    // requests will be retried at the plasma store.
    for (auto it = result_map.begin(); it != result_map.end();) {
      auto current = it++;
      if (current->second->IsInPlasmaError()) {
        RAY_LOG(DEBUG) << current->first << " in plasma, doing fetch-and-get";
        plasma_object_ids.insert(current->first);
        result_map.erase(current);
      }
    }
  } else {
    plasma_object_ids = std::move(memory_object_ids);
  }

  if (!got_exception) {
    // If any of the objects have been promoted to plasma, then we retry their
    // gets at the provider plasma. Once we get the objects from plasma, we flip
    // the transport type again and return them for the original direct call ids.
    int64_t local_timeout_ms = timeout_ms;
    if (timeout_ms >= 0) {
      local_timeout_ms = std::max(static_cast<int64_t>(0),
                                  timeout_ms - (current_time_ms() - start_time));
    }
    RAY_LOG(DEBUG) << "Plasma GET timeout " << local_timeout_ms;
    RAY_RETURN_NOT_OK(plasma_store_provider_->Get(plasma_object_ids, local_timeout_ms,
                                                  worker_context_, &result_map,
                                                  &got_exception));
  }

  // Loop through `ids` and fill each entry for the `results` vector,
  // this ensures that entries `results` have exactly the same order as
  // they are in `ids`. When there are duplicate object ids, all the entries
  // for the same id are filled in.
  bool missing_result = false;
  bool will_throw_exception = false;
  for (size_t i = 0; i < ids.size(); i++) {
    const auto pair = result_map.find(ids[i]);
    if (pair != result_map.end()) {
      (*results)[i] = pair->second;
      RAY_CHECK(!pair->second->IsInPlasmaError());
      if (pair->second->IsException()) {
        // The language bindings should throw an exception if they see this
        // object.
        will_throw_exception = true;
      }
    } else {
      missing_result = true;
    }
  }
  // If no timeout was set and none of the results will throw an exception,
  // then check that we fetched all results before returning.
  if (timeout_ms < 0 && !will_throw_exception) {
    RAY_CHECK(!missing_result);
  }

  return Status::OK();
}

Status CoreWorker::GetIfLocal(const std::vector<ObjectID> &ids,
                              std::vector<std::shared_ptr<RayObject>> *results) {
  results->resize(ids.size(), nullptr);

  absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> result_map;
  RAY_RETURN_NOT_OK(plasma_store_provider_->GetIfLocal(ids, &result_map));
  for (size_t i = 0; i < ids.size(); i++) {
    auto pair = result_map.find(ids[i]);
    // The caller of this method should guarantee that the object exists in the plasma
    // store when this method is called.
    RAY_CHECK(pair != result_map.end());
    RAY_CHECK(pair->second != nullptr);
    (*results)[i] = pair->second;
  }
  return Status::OK();
}

Status CoreWorker::Contains(const ObjectID &object_id, bool *has_object,
                            bool *is_in_plasma) {
  bool found = false;
  bool in_plasma = false;
  found = memory_store_->Contains(object_id, &in_plasma);
  if (in_plasma) {
    RAY_RETURN_NOT_OK(plasma_store_provider_->Contains(object_id, &found));
  }
  *has_object = found;
  if (is_in_plasma != nullptr) {
    *is_in_plasma = found && in_plasma;
  }
  return Status::OK();
}

// For any objects that are ErrorType::OBJECT_IN_PLASMA, we need to move them from
// the ready set into the plasma_object_ids set to wait on them there.
void RetryObjectInPlasmaErrors(std::shared_ptr<CoreWorkerMemoryStore> &memory_store,
                               WorkerContext &worker_context,
                               absl::flat_hash_set<ObjectID> &memory_object_ids,
                               absl::flat_hash_set<ObjectID> &plasma_object_ids,
                               absl::flat_hash_set<ObjectID> &ready) {
  for (auto iter = memory_object_ids.begin(); iter != memory_object_ids.end();) {
    auto current = iter++;
    const auto &mem_id = *current;
    auto ready_iter = ready.find(mem_id);
    if (ready_iter != ready.end()) {
      std::vector<std::shared_ptr<RayObject>> found;
      RAY_CHECK_OK(memory_store->Get({mem_id}, /*num_objects=*/1, /*timeout=*/0,
                                     worker_context,
                                     /*remote_after_get=*/false, &found));
      if (found.size() == 1 && found[0]->IsInPlasmaError()) {
        plasma_object_ids.insert(mem_id);
        ready.erase(ready_iter);
        memory_object_ids.erase(current);
      }
    }
  }
}

Status CoreWorker::Wait(const std::vector<ObjectID> &ids, int num_objects,
                        int64_t timeout_ms, std::vector<bool> *results,
                        bool fetch_local) {
  results->resize(ids.size(), false);

  if (num_objects <= 0 || num_objects > static_cast<int>(ids.size())) {
    return Status::Invalid(
        "Number of objects to wait for must be between 1 and the number of ids.");
  }

  absl::flat_hash_set<ObjectID> plasma_object_ids;
  absl::flat_hash_set<ObjectID> memory_object_ids(ids.begin(), ids.end());

  if (memory_object_ids.size() != ids.size()) {
    return Status::Invalid("Duplicate object IDs not supported in wait.");
  }

  absl::flat_hash_set<ObjectID> ready;
  int64_t start_time = current_time_ms();
  RAY_RETURN_NOT_OK(memory_store_->Wait(
      memory_object_ids,
      std::min(static_cast<int>(memory_object_ids.size()), num_objects), timeout_ms,
      worker_context_, &ready));
  RAY_CHECK(static_cast<int>(ready.size()) <= num_objects);
  if (timeout_ms > 0) {
    timeout_ms =
        std::max(0, static_cast<int>(timeout_ms - (current_time_ms() - start_time)));
  }
  if (fetch_local) {
    RetryObjectInPlasmaErrors(memory_store_, worker_context_, memory_object_ids,
                              plasma_object_ids, ready);
    if (static_cast<int>(ready.size()) < num_objects && plasma_object_ids.size() > 0) {
      RAY_RETURN_NOT_OK(plasma_store_provider_->Wait(
          plasma_object_ids,
          std::min(static_cast<int>(plasma_object_ids.size()),
                   num_objects - static_cast<int>(ready.size())),
          timeout_ms, worker_context_, &ready));
    }
  }
  RAY_CHECK(static_cast<int>(ready.size()) <= num_objects);

  for (size_t i = 0; i < ids.size(); i++) {
    if (ready.find(ids[i]) != ready.end()) {
      results->at(i) = true;
    }
  }

  return Status::OK();
}

Status CoreWorker::Delete(const std::vector<ObjectID> &object_ids, bool local_only) {
  // Release the object from plasma. This does not affect the object's ref
  // count. If this was called from a non-owning worker, then a warning will be
  // logged and the object will not get released.
  reference_counter_->FreePlasmaObjects(object_ids);

  // Store an error in the in-memory store to indicate that the plasma value is
  // no longer reachable.
  memory_store_->Delete(object_ids);
  for (const auto &object_id : object_ids) {
    RAY_CHECK(memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE),
                                 object_id));
  }

  // We only delete from plasma, which avoids hangs (issue #7105). In-memory
  // objects can only be deleted once the ref count goes to 0.
  absl::flat_hash_set<ObjectID> plasma_object_ids(object_ids.begin(), object_ids.end());
  return plasma_store_provider_->Delete(plasma_object_ids, local_only);
}

void CoreWorker::TriggerGlobalGC() {
  local_raylet_client_->GlobalGC(
      [](const Status &status, const rpc::GlobalGCReply &reply) {
        if (!status.ok()) {
          RAY_LOG(ERROR) << "Failed to send global GC request: " << status.ToString();
        }
      });
}

std::string CoreWorker::MemoryUsageString() {
  // Currently only the Plasma store returns a debug string.
  return plasma_store_provider_->MemoryUsageString();
}

TaskID CoreWorker::GetCallerId() const {
  TaskID caller_id;
  ActorID actor_id = GetActorId();
  if (!actor_id.IsNil()) {
    caller_id = TaskID::ForActorCreationTask(actor_id);
  } else {
    absl::MutexLock lock(&mutex_);
    caller_id = main_thread_task_id_;
  }
  return caller_id;
}

Status CoreWorker::PushError(const JobID &job_id, const std::string &type,
                             const std::string &error_message, double timestamp) {
  if (options_.is_local_mode) {
    RAY_LOG(ERROR) << "Pushed Error with JobID: " << job_id << " of type: " << type
                   << " with message: " << error_message << " at time: " << timestamp;
    return Status::OK();
  }
  return local_raylet_client_->PushError(job_id, type, error_message, timestamp);
}

Status CoreWorker::SetResource(const std::string &resource_name, const double capacity,
                               const NodeID &node_id) {
  return local_raylet_client_->SetResource(resource_name, capacity, node_id);
}

void CoreWorker::SpillOwnedObject(const ObjectID &object_id,
                                  const std::shared_ptr<RayObject> &obj,
                                  std::function<void()> callback) {
  if (!obj->IsInPlasmaError()) {
    RAY_LOG(ERROR) << "Cannot spill inlined object " << object_id;
    callback();
    return;
  }

  // Find the raylet that hosts the primary copy of the object.
  NodeID pinned_at;
  bool spilled;
  bool owned_by_us;
  RAY_CHECK(reference_counter_->IsPlasmaObjectPinnedOrSpilled(object_id, &owned_by_us,
                                                              &pinned_at, &spilled));
  RAY_CHECK(owned_by_us);
  if (spilled) {
    // The object has already been spilled.
    return;
  }
  auto node = gcs_client_->Nodes().Get(pinned_at);
  if (pinned_at.IsNil() || !node) {
    RAY_LOG(ERROR) << "Primary raylet for object " << object_id << " unreachable";
    callback();
    return;
  }

  // Ask the raylet to spill the object.
  RAY_LOG(DEBUG) << "Sending spill request to raylet for object " << object_id;
  auto raylet_client =
      std::make_shared<raylet::RayletClient>(rpc::NodeManagerWorkerClient::make(
          node->node_manager_address(), node->node_manager_port(),
          *client_call_manager_));
  raylet_client->RequestObjectSpillage(
      object_id, [object_id, callback](const Status &status,
                                       const rpc::RequestObjectSpillageReply &reply) {
        if (!status.ok() || !reply.success()) {
          RAY_LOG(ERROR) << "Failed to spill object " << object_id
                         << ", raylet unreachable or object could not be spilled.";
        }
        // TODO(Clark): Provide spilled URL and spilled node ID to callback so it can
        // added them to the reference.
        callback();
      });
}

Status CoreWorker::SpillObjects(const std::vector<ObjectID> &object_ids) {
  auto mutex = std::make_shared<absl::Mutex>();
  auto num_remaining = std::make_shared<size_t>(object_ids.size());
  auto ready_promise = std::make_shared<std::promise<void>>(std::promise<void>());
  Status final_status;

  // TODO(Clark): Add spilled URL and spilled node ID to reference in this callback.
  auto callback = [mutex, num_remaining, ready_promise]() {
    absl::MutexLock lock(mutex.get());
    (*num_remaining)--;
    if (*num_remaining == 0) {
      ready_promise->set_value();
    }
  };

  for (const auto &object_id : object_ids) {
    RAY_LOG(DEBUG) << "Requesting spill for object " << object_id;
    // Acquire a temporary reference to make sure that the object is still in
    // scope by the time we register the callback to spill the object.
    // Otherwise, the callback may never get called.
    AddLocalReference(object_id, "<temporary (get object status)>");

    rpc::Address owner_address;
    auto has_owner = reference_counter_->GetOwner(object_id, &owner_address);
    if (!has_owner) {
      final_status =
          Status::Invalid("Cannot call spill on objects that have gone out of scope.");
      callback();
    } else if (WorkerID::FromBinary(owner_address.worker_id()) !=
               worker_context_.GetWorkerID()) {
      final_status = Status::Invalid("Cannot call spill on objects that we do not own.");
      callback();
    } else {
      memory_store_->GetAsync(
          object_id, [this, object_id, callback](std::shared_ptr<RayObject> obj) {
            SpillOwnedObject(object_id, obj, callback);
          });
    }

    // Remove the temporary reference.
    RemoveLocalReference(object_id);
  }

  ready_promise->get_future().wait();

  for (const auto &object_id : object_ids) {
    // TODO(Clark): Move this to the callback (unless we really wanted to batch it) and
    // also include the spilled URL, spilled node ID, and updated object size.
    reference_counter_->HandleObjectSpilled(object_id, "", NodeID::Nil(), -1,
                                            /*release*/ true);
  }
  return final_status;
}

std::unordered_map<std::string, double> AddPlacementGroupConstraint(
    const std::unordered_map<std::string, double> &resources,
    PlacementGroupID placement_group_id, int64_t bundle_index) {
  if (bundle_index < 0) {
    RAY_CHECK(bundle_index == -1) << "Invalid bundle index " << bundle_index;
  }
  std::unordered_map<std::string, double> new_resources;
  if (placement_group_id != PlacementGroupID::Nil()) {
    for (auto iter = resources.begin(); iter != resources.end(); iter++) {
      auto new_name = FormatPlacementGroupResource(iter->first, placement_group_id, -1);
      new_resources[new_name] = iter->second;
      if (bundle_index >= 0) {
        auto index_name =
            FormatPlacementGroupResource(iter->first, placement_group_id, bundle_index);
        new_resources[index_name] = iter->second;
      }
    }
    return new_resources;
  }
  return resources;
}

void CoreWorker::SubmitTask(const RayFunction &function,
                            const std::vector<std::unique_ptr<TaskArg>> &args,
                            const TaskOptions &task_options,
                            std::vector<ObjectID> *return_ids, int max_retries,
                            BundleID placement_options,
                            bool placement_group_capture_child_tasks,
                            const std::string &debugger_breakpoint) {
  TaskSpecBuilder builder;
  const auto next_task_index = worker_context_.GetNextTaskIndex();
  const auto task_id =
      TaskID::ForNormalTask(worker_context_.GetCurrentJobID(),
                            worker_context_.GetCurrentTaskID(), next_task_index);
  auto constrained_resources = AddPlacementGroupConstraint(
      task_options.resources, placement_options.first, placement_options.second);
  const std::unordered_map<std::string, double> required_resources;
  auto task_name = task_options.name.empty()
                       ? function.GetFunctionDescriptor()->DefaultTaskName()
                       : task_options.name;
  // Propagate existing environment variable overrides, but override them with any new
  // ones
  std::unordered_map<std::string, std::string> current_override_environment_variables =
      worker_context_.GetCurrentOverrideEnvironmentVariables();
  std::unordered_map<std::string, std::string> override_environment_variables =
      task_options.override_environment_variables;
  override_environment_variables.insert(current_override_environment_variables.begin(),
                                        current_override_environment_variables.end());
  // Update runtime env with simple dict update.
  ray::RuntimeEnv runtime_env = worker_context_.GetCurrentRuntimeEnv();
  ray::RuntimeEnv new_runtime_env = task_options.runtime_env;
  runtime_env.Update(new_runtime_env);
  if (runtime_env.IsEmpty()) {
    runtime_env = ray::RuntimeEnv::FromProto(job_config_->runtime_env());
  }
  // TODO(ekl) offload task building onto a thread pool for performance
  BuildCommonTaskSpec(builder, worker_context_.GetCurrentJobID(), task_id, task_name,
                      worker_context_.GetCurrentTaskID(), next_task_index, GetCallerId(),
                      rpc_address_, function, args, task_options.num_returns,
                      constrained_resources, required_resources, return_ids,
                      placement_options, placement_group_capture_child_tasks,
                      debugger_breakpoint, runtime_env, override_environment_variables);
  TaskSpecification task_spec = builder.Build();
  if (options_.is_local_mode) {
    ExecuteTaskLocalMode(task_spec);
  } else {
    task_manager_->AddPendingTask(task_spec.CallerAddress(), task_spec, CurrentCallSite(),
                                  max_retries);
    io_service_.post(
        [this, task_spec]() {
          RAY_UNUSED(direct_task_submitter_->SubmitTask(task_spec));
        },
        "CoreWorker.SubmitTask");
  }
}

Status CoreWorker::CreateActor(const RayFunction &function,
                               const std::vector<std::unique_ptr<TaskArg>> &args,
                               const ActorCreationOptions &actor_creation_options,
                               const std::string &extension_data,
                               ActorID *return_actor_id) {
  if (actor_creation_options.is_asyncio && options_.is_local_mode) {
    return Status::NotImplemented(
        "Async actor is currently not supported for the local mode");
  }
  const auto next_task_index = worker_context_.GetNextTaskIndex();
  const ActorID actor_id =
      ActorID::Of(worker_context_.GetCurrentJobID(), worker_context_.GetCurrentTaskID(),
                  next_task_index);
  const TaskID actor_creation_task_id = TaskID::ForActorCreationTask(actor_id);
  const JobID job_id = worker_context_.GetCurrentJobID();
  // Propagate existing environment variable overrides, but override them with any new
  // ones
  std::unordered_map<std::string, std::string> current_override_environment_variables =
      worker_context_.GetCurrentOverrideEnvironmentVariables();
  std::unordered_map<std::string, std::string> override_environment_variables =
      actor_creation_options.override_environment_variables;
  override_environment_variables.insert(current_override_environment_variables.begin(),
                                        current_override_environment_variables.end());
  // Update runtime env with simple dict update.
  ray::RuntimeEnv runtime_env = worker_context_.GetCurrentRuntimeEnv();
  ray::RuntimeEnv new_runtime_env = actor_creation_options.runtime_env;
  runtime_env.Update(new_runtime_env);
  if (runtime_env.IsEmpty()) {
    runtime_env = ray::RuntimeEnv::FromProto(job_config_->runtime_env());
  }
  std::vector<ObjectID> return_ids;
  TaskSpecBuilder builder;
  auto new_placement_resources =
      AddPlacementGroupConstraint(actor_creation_options.placement_resources,
                                  actor_creation_options.placement_options.first,
                                  actor_creation_options.placement_options.second);
  auto new_resource = AddPlacementGroupConstraint(
      actor_creation_options.resources, actor_creation_options.placement_options.first,
      actor_creation_options.placement_options.second);
  const auto actor_name = actor_creation_options.name;
  const auto task_name =
      actor_name.empty()
          ? function.GetFunctionDescriptor()->DefaultTaskName()
          : actor_name + ":" + function.GetFunctionDescriptor()->CallString();
  BuildCommonTaskSpec(builder, job_id, actor_creation_task_id, task_name,
                      worker_context_.GetCurrentTaskID(), next_task_index, GetCallerId(),
                      rpc_address_, function, args, 1, new_resource,
                      new_placement_resources, &return_ids,
                      actor_creation_options.placement_options,
                      actor_creation_options.placement_group_capture_child_tasks,
                      "", /* debugger_breakpoint */
                      runtime_env, override_environment_variables);
  builder.SetActorCreationTaskSpec(actor_id, actor_creation_options.max_restarts,
                                   actor_creation_options.max_task_retries,
                                   actor_creation_options.dynamic_worker_options,
                                   actor_creation_options.max_concurrency,
                                   actor_creation_options.is_detached, actor_name,
                                   actor_creation_options.is_asyncio, extension_data);

  // Add the actor handle before we submit the actor creation task, since the
  // actor handle must be in scope by the time the GCS sends the
  // WaitForActorOutOfScopeRequest.
  auto actor_handle = std::make_unique<ActorHandle>(
      actor_id, GetCallerId(), rpc_address_, job_id, /*actor_cursor=*/return_ids[0],
      function.GetLanguage(), function.GetFunctionDescriptor(), extension_data,
      actor_creation_options.max_task_retries);
  RAY_CHECK(actor_manager_->AddNewActorHandle(std::move(actor_handle), GetCallerId(),
                                              CurrentCallSite(), rpc_address_,
                                              actor_creation_options.is_detached))
      << "Actor " << actor_id << " already exists";

  *return_actor_id = actor_id;
  TaskSpecification task_spec = builder.Build();
  Status status;
  if (options_.is_local_mode) {
    if (task_spec.IsDetachedActor()) {
      // Since local mode doesn't pass GCS actor management code path,
      // it just register actor names in memory.
      local_mode_named_actor_registry_.emplace(actor_name, actor_id);
    }
    ExecuteTaskLocalMode(task_spec);
  } else {
    int max_retries;
    if (actor_creation_options.max_restarts == -1) {
      max_retries = -1;
    } else {
      max_retries = std::max((int64_t)RayConfig::instance().actor_creation_min_retries(),
                             actor_creation_options.max_restarts);
    }
    task_manager_->AddPendingTask(rpc_address_, task_spec, CurrentCallSite(),
                                  max_retries);
    status = direct_task_submitter_->SubmitTask(task_spec);
  }
  return status;
}

Status CoreWorker::CreatePlacementGroup(
    const PlacementGroupCreationOptions &placement_group_creation_options,
    PlacementGroupID *return_placement_group_id) {
  std::shared_ptr<std::promise<Status>> status_promise =
      std::make_shared<std::promise<Status>>();
  const PlacementGroupID placement_group_id = PlacementGroupID::FromRandom();
  PlacementGroupSpecBuilder builder;
  builder.SetPlacementGroupSpec(
      placement_group_id, placement_group_creation_options.name,
      placement_group_creation_options.bundles, placement_group_creation_options.strategy,
      placement_group_creation_options.is_detached, worker_context_.GetCurrentJobID(),
      worker_context_.GetCurrentActorID(), worker_context_.CurrentActorDetached());
  PlacementGroupSpecification placement_group_spec = builder.Build();
  *return_placement_group_id = placement_group_id;
  RAY_LOG(INFO) << "Submitting Placement Group creation to GCS: " << placement_group_id;
  RAY_UNUSED(gcs_client_->PlacementGroups().AsyncCreatePlacementGroup(
      placement_group_spec,
      [status_promise](const Status &status) { status_promise->set_value(status); }));
  auto status_future = status_promise->get_future();
  if (status_future.wait_for(std::chrono::seconds(
          RayConfig::instance().gcs_server_request_timeout_seconds())) !=
      std::future_status::ready) {
    std::ostringstream stream;
    stream << "There was timeout in creating the placement group of id "
           << placement_group_id
           << ". It is probably "
              "because GCS server is dead or there's a high load there.";
    return Status::TimedOut(stream.str());
  }
  return status_future.get();
}

Status CoreWorker::RemovePlacementGroup(const PlacementGroupID &placement_group_id) {
  std::shared_ptr<std::promise<Status>> status_promise =
      std::make_shared<std::promise<Status>>();
  // Synchronously wait for placement group removal.
  RAY_UNUSED(gcs_client_->PlacementGroups().AsyncRemovePlacementGroup(
      placement_group_id,
      [status_promise](const Status &status) { status_promise->set_value(status); }));
  auto status_future = status_promise->get_future();
  if (status_future.wait_for(std::chrono::seconds(
          RayConfig::instance().gcs_server_request_timeout_seconds())) !=
      std::future_status::ready) {
    std::ostringstream stream;
    stream << "There was timeout in removing the placement group of id "
           << placement_group_id
           << ". It is probably "
              "because GCS server is dead or there's a high load there.";
    return Status::TimedOut(stream.str());
  }
  return status_future.get();
}

Status CoreWorker::WaitPlacementGroupReady(const PlacementGroupID &placement_group_id,
                                           int timeout_seconds) {
  std::shared_ptr<std::promise<Status>> status_promise =
      std::make_shared<std::promise<Status>>();
  RAY_CHECK_OK(gcs_client_->PlacementGroups().AsyncWaitUntilReady(
      placement_group_id,
      [status_promise](const Status &status) { status_promise->set_value(status); }));
  auto status_future = status_promise->get_future();
  if (status_future.wait_for(std::chrono::seconds(timeout_seconds)) !=
      std::future_status::ready) {
    std::ostringstream stream;
    stream << "There was timeout in waiting for placement group " << placement_group_id
           << " creation.";
    return Status::TimedOut(stream.str());
  }
  return status_future.get();
}

void CoreWorker::SubmitActorTask(const ActorID &actor_id, const RayFunction &function,
                                 const std::vector<std::unique_ptr<TaskArg>> &args,
                                 const TaskOptions &task_options,
                                 std::vector<ObjectID> *return_ids) {
  auto actor_handle = actor_manager_->GetActorHandle(actor_id);

  // Add one for actor cursor object id for tasks.
  const int num_returns = task_options.num_returns + 1;

  // Build common task spec.
  TaskSpecBuilder builder;
  const auto next_task_index = worker_context_.GetNextTaskIndex();
  const TaskID actor_task_id = TaskID::ForActorTask(
      worker_context_.GetCurrentJobID(), worker_context_.GetCurrentTaskID(),
      next_task_index, actor_handle->GetActorID());
  const std::unordered_map<std::string, double> required_resources;
  const auto task_name = task_options.name.empty()
                             ? function.GetFunctionDescriptor()->DefaultTaskName()
                             : task_options.name;
  const std::unordered_map<std::string, std::string> override_environment_variables = {};
  const ray::RuntimeEnv runtime_env = RuntimeEnv();
  BuildCommonTaskSpec(builder, actor_handle->CreationJobID(), actor_task_id, task_name,
                      worker_context_.GetCurrentTaskID(), next_task_index, GetCallerId(),
                      rpc_address_, function, args, num_returns, task_options.resources,
                      required_resources, return_ids,
                      std::make_pair(PlacementGroupID::Nil(), -1),
                      true, /* placement_group_capture_child_tasks */
                      "",   /* debugger_breakpoint */
                      runtime_env, override_environment_variables);
  // NOTE: placement_group_capture_child_tasks and runtime_env will
  // be ignored in the actor because we should always follow the actor's option.

  const ObjectID new_cursor = return_ids->back();
  actor_handle->SetActorTaskSpec(builder, new_cursor);
  // Remove cursor from return ids.
  return_ids->pop_back();

  // Submit task.
  TaskSpecification task_spec = builder.Build();
  if (options_.is_local_mode) {
    ExecuteTaskLocalMode(task_spec, actor_id);
  } else {
    task_manager_->AddPendingTask(rpc_address_, task_spec, CurrentCallSite(),
                                  actor_handle->MaxTaskRetries());
    io_service_.post(
        [this, task_spec]() {
          RAY_UNUSED(direct_actor_submitter_->SubmitTask(task_spec));
        },
        "CoreWorker.SubmitActorTask");
  }
}

Status CoreWorker::CancelTask(const ObjectID &object_id, bool force_kill,
                              bool recursive) {
  if (actor_manager_->CheckActorHandleExists(object_id.TaskId().ActorId())) {
    return Status::Invalid("Actor task cancellation is not supported.");
  }
  rpc::Address obj_addr;
  if (!reference_counter_->GetOwner(object_id, &obj_addr)) {
    return Status::Invalid("No owner found for object.");
  }
  if (obj_addr.SerializeAsString() != rpc_address_.SerializeAsString()) {
    return direct_task_submitter_->CancelRemoteTask(object_id, obj_addr, force_kill,
                                                    recursive);
  }

  auto task_spec = task_manager_->GetTaskSpec(object_id.TaskId());
  if (task_spec.has_value() && !task_spec.value().IsActorCreationTask()) {
    return direct_task_submitter_->CancelTask(task_spec.value(), force_kill, recursive);
  }
  return Status::OK();
}

Status CoreWorker::CancelChildren(const TaskID &task_id, bool force_kill) {
  bool recursive_success = true;
  for (const auto &child_id : task_manager_->GetPendingChildrenTasks(task_id)) {
    auto child_spec = task_manager_->GetTaskSpec(child_id);
    if (child_spec.has_value()) {
      auto result =
          direct_task_submitter_->CancelTask(child_spec.value(), force_kill, true);
      recursive_success = recursive_success && result.ok();
    } else {
      recursive_success = false;
    }
  }
  if (recursive_success) {
    return Status::OK();
  } else {
    return Status::UnknownError("Recursive task cancelation failed--check warning logs.");
  }
}

Status CoreWorker::KillActor(const ActorID &actor_id, bool force_kill, bool no_restart) {
  if (options_.is_local_mode) {
    return KillActorLocalMode(actor_id);
  }

  if (!actor_manager_->CheckActorHandleExists(actor_id)) {
    std::stringstream stream;
    stream << "Failed to find a corresponding actor handle for " << actor_id;
    return Status::Invalid(stream.str());
  }

  RAY_CHECK_OK(
      gcs_client_->Actors().AsyncKillActor(actor_id, force_kill, no_restart, nullptr));
  return Status::OK();
}

Status CoreWorker::KillActorLocalMode(const ActorID &actor_id) {
  // KillActor doesn't do anything in local mode. We only remove named actor entry if
  // exists.
  for (auto it = local_mode_named_actor_registry_.begin();
       it != local_mode_named_actor_registry_.end();) {
    auto current = it++;
    if (current->second == actor_id) {
      local_mode_named_actor_registry_.erase(current);
    }
  }
  return Status::OK();
}

void CoreWorker::RemoveActorHandleReference(const ActorID &actor_id) {
  ObjectID actor_handle_id = ObjectID::ForActorHandle(actor_id);
  reference_counter_->RemoveLocalReference(actor_handle_id, nullptr);
}

ActorID CoreWorker::DeserializeAndRegisterActorHandle(const std::string &serialized,
                                                      const ObjectID &outer_object_id) {
  std::unique_ptr<ActorHandle> actor_handle(new ActorHandle(serialized));
  return actor_manager_->RegisterActorHandle(std::move(actor_handle), outer_object_id,
                                             GetCallerId(), CurrentCallSite(),
                                             rpc_address_);
}

Status CoreWorker::SerializeActorHandle(const ActorID &actor_id, std::string *output,
                                        ObjectID *actor_handle_id) const {
  auto actor_handle = actor_manager_->GetActorHandle(actor_id);
  actor_handle->Serialize(output);
  *actor_handle_id = ObjectID::ForActorHandle(actor_id);
  return Status::OK();
}

std::shared_ptr<const ActorHandle> CoreWorker::GetActorHandle(
    const ActorID &actor_id) const {
  return actor_manager_->GetActorHandle(actor_id);
}

std::pair<std::shared_ptr<const ActorHandle>, Status> CoreWorker::GetNamedActorHandle(
    const std::string &name) {
  RAY_CHECK(!name.empty());
  if (options_.is_local_mode) {
    return GetNamedActorHandleLocalMode(name);
  }

  // This call needs to be blocking because we can't return until the actor
  // handle is created, which requires the response from the RPC. This is
  // implemented using a promise that's captured in the RPC callback.
  // There should be no risk of deadlock because we don't hold any
  // locks during the call and the RPCs run on a separate thread.
  ActorID actor_id;
  std::shared_ptr<std::promise<void>> ready_promise =
      std::make_shared<std::promise<void>>(std::promise<void>());
  const auto &ray_namespace = job_config_->ray_namespace();
  RAY_CHECK_OK(gcs_client_->Actors().AsyncGetByName(
      name, ray_namespace,
      [this, &actor_id, name, ready_promise](
          Status status, const boost::optional<rpc::ActorTableData> &result) {
        if (status.ok() && result) {
          auto actor_handle = std::make_unique<ActorHandle>(*result);
          actor_id = actor_handle->GetActorID();
          actor_manager_->AddNewActorHandle(std::move(actor_handle), GetCallerId(),
                                            CurrentCallSite(), rpc_address_,
                                            /*is_detached*/ true);
        } else {
          // Use a NIL actor ID to signal that the actor wasn't found.
          RAY_LOG(DEBUG) << "Failed to look up actor with name: " << name;
          actor_id = ActorID::Nil();
        }
        ready_promise->set_value();
      }));
  // Block until the RPC completes. Set a timeout to avoid hangs if the
  // GCS service crashes.
  if (ready_promise->get_future().wait_for(std::chrono::seconds(
          RayConfig::instance().gcs_server_request_timeout_seconds())) !=
      std::future_status::ready) {
    std::ostringstream stream;
    stream << "There was timeout in getting the actor handle. It is probably "
              "because GCS server is dead or there's a high load there.";
    return std::make_pair(nullptr, Status::TimedOut(stream.str()));
  }

  if (actor_id.IsNil()) {
    std::ostringstream stream;
    stream << "Failed to look up actor with name '" << name << "'. You are "
           << "either trying to look up a named actor you didn't create, "
           << "the named actor died, or the actor hasn't been created "
           << "because named actor creation is asynchronous.";
    return std::make_pair(nullptr, Status::NotFound(stream.str()));
  }

  return std::make_pair(GetActorHandle(actor_id), Status::OK());
}

std::pair<std::shared_ptr<const ActorHandle>, Status>
CoreWorker::GetNamedActorHandleLocalMode(const std::string &name) {
  auto it = local_mode_named_actor_registry_.find(name);
  if (it == local_mode_named_actor_registry_.end()) {
    std::ostringstream stream;
    stream << "Failed to look up actor with name '" << name;
    return std::make_pair(nullptr, Status::NotFound(stream.str()));
  }

  return std::make_pair(GetActorHandle(it->second), Status::OK());
}

const ResourceMappingType CoreWorker::GetResourceIDs() const {
  absl::MutexLock lock(&mutex_);
  return *resource_ids_;
}

std::unique_ptr<worker::ProfileEvent> CoreWorker::CreateProfileEvent(
    const std::string &event_type) {
  return std::make_unique<worker::ProfileEvent>(profiler_, event_type);
}

void CoreWorker::RunTaskExecutionLoop() { task_execution_service_.run(); }

Status CoreWorker::AllocateReturnObjects(
    const std::vector<ObjectID> &object_ids, const std::vector<size_t> &data_sizes,
    const std::vector<std::shared_ptr<Buffer>> &metadatas,
    const std::vector<std::vector<ObjectID>> &contained_object_ids,
    std::vector<std::shared_ptr<RayObject>> *return_objects) {
  RAY_CHECK(object_ids.size() == metadatas.size());
  RAY_CHECK(object_ids.size() == data_sizes.size());
  return_objects->resize(object_ids.size(), nullptr);

  rpc::Address owner_address(options_.is_local_mode
                                 ? rpc::Address()
                                 : worker_context_.GetCurrentTask()->CallerAddress());

  for (size_t i = 0; i < object_ids.size(); i++) {
    bool object_already_exists = false;
    std::shared_ptr<Buffer> data_buffer;
    if (data_sizes[i] > 0) {
      RAY_LOG(DEBUG) << "Creating return object " << object_ids[i];
      // Mark this object as containing other object IDs. The ref counter will
      // keep the inner IDs in scope until the outer one is out of scope.
      if (!contained_object_ids[i].empty() && !options_.is_local_mode) {
        reference_counter_->AddNestedObjectIds(object_ids[i], contained_object_ids[i],
                                               owner_address);
      }

      // Allocate a buffer for the return object.
      if (options_.is_local_mode ||
          static_cast<int64_t>(data_sizes[i]) < max_direct_call_object_size_) {
        data_buffer = std::make_shared<LocalMemoryBuffer>(data_sizes[i]);
      } else {
        RAY_RETURN_NOT_OK(CreateExisting(metadatas[i], data_sizes[i], object_ids[i],
                                         owner_address, &data_buffer));
        object_already_exists = !data_buffer;
      }
    }
    // Leave the return object as a nullptr if the object already exists.
    if (!object_already_exists) {
      return_objects->at(i) =
          std::make_shared<RayObject>(data_buffer, metadatas[i], contained_object_ids[i]);
    }
  }

  return Status::OK();
}

Status CoreWorker::ExecuteTask(const TaskSpecification &task_spec,
                               const std::shared_ptr<ResourceMappingType> &resource_ids,
                               std::vector<std::shared_ptr<RayObject>> *return_objects,
                               ReferenceCounter::ReferenceTableProto *borrowed_refs) {
  RAY_LOG(DEBUG) << "Executing task, task info = " << task_spec.DebugString();
  task_queue_length_ -= 1;
  num_executed_tasks_ += 1;

  if (!options_.is_local_mode) {
    worker_context_.SetCurrentTask(task_spec);
    SetCurrentTaskId(task_spec.TaskId());
  }
  {
    absl::MutexLock lock(&mutex_);
    current_task_ = task_spec;
    if (resource_ids) {
      resource_ids_ = resource_ids;
    }
  }

  RayFunction func{task_spec.GetLanguage(), task_spec.FunctionDescriptor()};

  std::vector<std::shared_ptr<RayObject>> args;
  std::vector<ObjectID> arg_reference_ids;
  // This includes all IDs that were passed by reference and any IDs that were
  // inlined in the task spec. These references will be pinned during the task
  // execution and unpinned once the task completes. We will notify the caller
  // about any IDs that we are still borrowing by the time the task completes.
  std::vector<ObjectID> borrowed_ids;
  RAY_CHECK_OK(
      GetAndPinArgsForExecutor(task_spec, &args, &arg_reference_ids, &borrowed_ids));

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
    RAY_LOG(INFO) << "Creating actor: " << task_spec.ActorCreationId();
  } else if (task_spec.IsActorTask()) {
    RAY_CHECK(return_ids.size() > 0);
    return_ids.pop_back();
    task_type = TaskType::ACTOR_TASK;
  }

  // Because we support concurrent actor calls, we need to update the
  // worker ID for the current thread.
  CoreWorkerProcess::SetCurrentThreadWorkerId(GetWorkerID());

  std::shared_ptr<LocalMemoryBuffer> creation_task_exception_pb_bytes = nullptr;

  status = options_.task_execution_callback(
      task_type, task_spec.GetName(), func,
      task_spec.GetRequiredResources().GetResourceMap(), args, arg_reference_ids,
      return_ids, task_spec.GetDebuggerBreakpoint(), return_objects,
      creation_task_exception_pb_bytes);

  absl::optional<rpc::Address> caller_address(
      options_.is_local_mode ? absl::optional<rpc::Address>()
                             : worker_context_.GetCurrentTask()->CallerAddress());
  for (size_t i = 0; i < return_objects->size(); i++) {
    // The object is nullptr if it already existed in the object store.
    if (!return_objects->at(i)) {
      continue;
    }
    if (return_objects->at(i)->GetData() != nullptr &&
        return_objects->at(i)->GetData()->IsPlasmaBuffer()) {
      if (!SealExisting(return_ids[i], /*pin_object=*/true, caller_address).ok()) {
        RAY_LOG(FATAL) << "Task " << task_spec.TaskId() << " failed to seal object "
                       << return_ids[i] << " in store: " << status.message();
      }
    }
  }

  // Get the reference counts for any IDs that we borrowed during this task and
  // return them to the caller. This will notify the caller of any IDs that we
  // (or a nested task) are still borrowing. It will also notify the caller of
  // any new IDs that were contained in a borrowed ID that we (or a nested
  // task) are now borrowing.
  if (!borrowed_ids.empty()) {
    reference_counter_->GetAndClearLocalBorrowers(borrowed_ids, borrowed_refs);
  }
  // Unpin the borrowed IDs.
  std::vector<ObjectID> deleted;
  for (const auto &borrowed_id : borrowed_ids) {
    RAY_LOG(DEBUG) << "Decrementing ref for borrowed ID " << borrowed_id;
    reference_counter_->RemoveLocalReference(borrowed_id, &deleted);
  }
  if (options_.ref_counting_enabled) {
    memory_store_->Delete(deleted);
  }

  if (task_spec.IsNormalTask() && reference_counter_->NumObjectIDsInScope() != 0) {
    RAY_LOG(DEBUG)
        << "There were " << reference_counter_->NumObjectIDsInScope()
        << " ObjectIDs left in scope after executing task " << task_spec.TaskId()
        << ". This is either caused by keeping references to ObjectIDs in Python "
           "between "
           "tasks (e.g., in global variables) or indicates a problem with Ray's "
           "reference counting, and may cause problems in the object store.";
  }

  if (!options_.is_local_mode) {
    SetCurrentTaskId(TaskID::Nil());
    worker_context_.ResetCurrentTask();
  }
  {
    absl::MutexLock lock(&mutex_);
    current_task_ = TaskSpecification();
    if (task_spec.IsNormalTask()) {
      resource_ids_.reset(new ResourceMappingType());
    }
  }
  RAY_LOG(INFO) << "Finished executing task " << task_spec.TaskId()
                << ", status=" << status;
  if (status.IsCreationTaskError()) {
    Exit(rpc::WorkerExitType::CREATION_TASK_ERROR, creation_task_exception_pb_bytes);
  } else if (status.IsIntentionalSystemExit()) {
    Exit(rpc::WorkerExitType::INTENDED_EXIT, creation_task_exception_pb_bytes);
  } else if (status.IsUnexpectedSystemExit()) {
    Exit(rpc::WorkerExitType::SYSTEM_ERROR_EXIT, creation_task_exception_pb_bytes);
  } else if (!status.ok()) {
    RAY_LOG(FATAL) << "Unexpected task status type : " << status;
  }

  return status;
}

void CoreWorker::ExecuteTaskLocalMode(const TaskSpecification &task_spec,
                                      const ActorID &actor_id) {
  auto resource_ids = std::make_shared<ResourceMappingType>();
  auto return_objects = std::vector<std::shared_ptr<RayObject>>();
  auto borrowed_refs = ReferenceCounter::ReferenceTableProto();
  if (!task_spec.IsActorCreationTask()) {
    for (size_t i = 0; i < task_spec.NumReturns(); i++) {
      reference_counter_->AddOwnedObject(task_spec.ReturnId(i),
                                         /*inner_ids=*/{}, rpc_address_,
                                         CurrentCallSite(), -1,
                                         /*is_reconstructable=*/false);
    }
  }
  auto old_id = GetActorId();
  SetActorId(actor_id);
  RAY_UNUSED(ExecuteTask(task_spec, resource_ids, &return_objects, &borrowed_refs));
  SetActorId(old_id);
}

Status CoreWorker::GetAndPinArgsForExecutor(const TaskSpecification &task,
                                            std::vector<std::shared_ptr<RayObject>> *args,
                                            std::vector<ObjectID> *arg_reference_ids,
                                            std::vector<ObjectID> *borrowed_ids) {
  auto num_args = task.NumArgs();
  args->resize(num_args);
  arg_reference_ids->resize(num_args);

  absl::flat_hash_set<ObjectID> by_ref_ids;
  absl::flat_hash_map<ObjectID, std::vector<size_t>> by_ref_indices;

  for (size_t i = 0; i < task.NumArgs(); ++i) {
    if (task.ArgByRef(i)) {
      // We need to put an OBJECT_IN_PLASMA error here so the subsequent call to Get()
      // properly redirects to the plasma store.
      if (!options_.is_local_mode) {
        RAY_UNUSED(memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                                      task.ArgId(i)));
      }
      const auto &arg_id = task.ArgId(i);
      by_ref_ids.insert(arg_id);
      auto it = by_ref_indices.find(arg_id);
      if (it == by_ref_indices.end()) {
        by_ref_indices.emplace(arg_id, std::vector<size_t>({i}));
      } else {
        it->second.push_back(i);
      }
      arg_reference_ids->at(i) = arg_id;
      // Pin all args passed by reference for the duration of the task.  This
      // ensures that when the task completes, we can retrieve metadata about
      // any borrowed ObjectIDs that were serialized in the argument's value.
      RAY_LOG(DEBUG) << "Incrementing ref for argument ID " << arg_id;
      reference_counter_->AddLocalReference(arg_id, task.CallSiteString());
      // Attach the argument's owner's address. This is needed to retrieve the
      // value from plasma.
      reference_counter_->AddBorrowedObject(arg_id, ObjectID::Nil(),
                                            task.ArgRef(i).owner_address());
      borrowed_ids->push_back(arg_id);
    } else {
      // A pass-by-value argument.
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
      // NOTE: this is a workaround to avoid an extra copy for Java workers.
      // Python workers need this copy to pass test case
      // test_inline_arg_memory_corruption.
      bool copy_data = options_.language == Language::PYTHON;
      args->at(i) =
          std::make_shared<RayObject>(data, metadata, task.ArgInlinedIds(i), copy_data);
      arg_reference_ids->at(i) = ObjectID::Nil();
      // The task borrows all ObjectIDs that were serialized in the inlined
      // arguments. The task will receive references to these IDs, so it is
      // possible for the task to continue borrowing these arguments by the
      // time it finishes.
      for (const auto &inlined_id : task.ArgInlinedIds(i)) {
        RAY_LOG(DEBUG) << "Incrementing ref for borrowed ID " << inlined_id;
        // We do not need to add the ownership information here because it will
        // get added once the language frontend deserializes the value, before
        // the ObjectID can be used.
        reference_counter_->AddLocalReference(inlined_id, task.CallSiteString());
        borrowed_ids->push_back(inlined_id);
      }
    }
  }

  // Fetch by-reference arguments directly from the plasma store.
  bool got_exception = false;
  absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> result_map;
  if (options_.is_local_mode) {
    RAY_RETURN_NOT_OK(
        memory_store_->Get(by_ref_ids, -1, worker_context_, &result_map, &got_exception));
  } else {
    RAY_RETURN_NOT_OK(plasma_store_provider_->Get(by_ref_ids, -1, worker_context_,
                                                  &result_map, &got_exception));
  }
  for (const auto &it : result_map) {
    for (size_t idx : by_ref_indices[it.first]) {
      args->at(idx) = it.second;
    }
  }

  return Status::OK();
}

void CoreWorker::HandlePushTask(const rpc::PushTaskRequest &request,
                                rpc::PushTaskReply *reply,
                                rpc::SendReplyCallback send_reply_callback) {
  if (HandleWrongRecipient(WorkerID::FromBinary(request.intended_worker_id()),
                           send_reply_callback)) {
    return;
  }

  // Increment the task_queue_length
  task_queue_length_ += 1;

  // For actor tasks, we just need to post a HandleActorTask instance to the task
  // execution service.
  if (request.task_spec().type() == TaskType::ACTOR_TASK) {
    task_execution_service_.post(
        [this, request, reply, send_reply_callback = std::move(send_reply_callback)] {
          // We have posted an exit task onto the main event loop,
          // so shouldn't bother executing any further work.
          if (exiting_) return;
          direct_task_receiver_->HandleTask(request, reply, send_reply_callback);
        },
        "CoreWorker.HandlePushTaskActor");
  } else {
    // Normal tasks are enqueued here, and we post a RunNormalTasksFromQueue instance to
    // the task execution service.
    direct_task_receiver_->HandleTask(request, reply, send_reply_callback);
    task_execution_service_.post(
        [=] {
          // We have posted an exit task onto the main event loop,
          // so shouldn't bother executing any further work.
          if (exiting_) return;
          direct_task_receiver_->RunNormalTasksFromQueue();
        },
        "CoreWorker.HandlePushTask");
  }
}

void CoreWorker::HandleDirectActorCallArgWaitComplete(
    const rpc::DirectActorCallArgWaitCompleteRequest &request,
    rpc::DirectActorCallArgWaitCompleteReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  if (HandleWrongRecipient(WorkerID::FromBinary(request.intended_worker_id()),
                           send_reply_callback)) {
    return;
  }

  // Post on the task execution event loop since this may trigger the
  // execution of a task that is now ready to run.
  task_execution_service_.post(
      [=] {
        RAY_LOG(DEBUG) << "Arg wait complete for tag " << request.tag();
        task_argument_waiter_->OnWaitComplete(request.tag());
      },
      "CoreWorker.ArgWaitComplete");

  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void CoreWorker::HandleGetObjectStatus(const rpc::GetObjectStatusRequest &request,
                                       rpc::GetObjectStatusReply *reply,
                                       rpc::SendReplyCallback send_reply_callback) {
  if (HandleWrongRecipient(WorkerID::FromBinary(request.owner_worker_id()),
                           send_reply_callback)) {
    RAY_LOG(INFO) << "Handling GetObjectStatus for object produced by a previous worker "
                     "with the same address";
    return;
  }

  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  RAY_LOG(DEBUG) << "Received GetObjectStatus " << object_id;
  // Acquire a reference to the object. This prevents the object from being
  // evicted out from under us while we check the object status and start the
  // Get.
  AddLocalReference(object_id, "<temporary (get object status)>");

  rpc::Address owner_address;
  auto has_owner = reference_counter_->GetOwner(object_id, &owner_address);
  if (!has_owner) {
    // We owned this object, but the object has gone out of scope.
    reply->set_status(rpc::GetObjectStatusReply::OUT_OF_SCOPE);
    send_reply_callback(Status::OK(), nullptr, nullptr);
  } else {
    RAY_CHECK(owner_address.worker_id() == request.owner_worker_id());
    bool is_freed = reference_counter_->IsPlasmaObjectFreed(object_id);

    // Send the reply once the value has become available. The value is
    // guaranteed to become available eventually because we own the object and
    // its ref count is > 0.
    memory_store_->GetAsync(object_id, [this, object_id, reply, send_reply_callback,
                                        is_freed](std::shared_ptr<RayObject> obj) {
      if (is_freed) {
        reply->set_status(rpc::GetObjectStatusReply::FREED);
      } else {
        // If obj is the concrete object value, it is small, so we
        // send the object back to the caller in the GetObjectStatus
        // reply, bypassing a Plasma put and object transfer. If obj
        // is an indicator that the object is in Plasma, we set an
        // in_plasma indicator on the message, and the caller will
        // have to facilitate a Plasma object transfer to get the
        // object value.
        auto *object = reply->mutable_object();
        if (obj->HasData()) {
          const auto &data = obj->GetData();
          object->set_data(data->Data(), data->Size());
        }
        if (obj->HasMetadata()) {
          const auto &metadata = obj->GetMetadata();
          object->set_metadata(metadata->Data(), metadata->Size());
        }
        for (const auto &nested_id : obj->GetNestedIds()) {
          object->add_nested_inlined_ids(nested_id.Binary());
        }
        reply->set_status(rpc::GetObjectStatusReply::CREATED);
        // Set locality data.
        const auto &locality_data = reference_counter_->GetLocalityData(object_id);
        if (locality_data.has_value()) {
          for (const auto &node_id : locality_data.value().nodes_containing_object) {
            reply->add_node_ids(node_id.Binary());
          }
          reply->set_object_size(locality_data.value().object_size);
        }
      }
      send_reply_callback(Status::OK(), nullptr, nullptr);
    });
  }

  RemoveLocalReference(object_id);
}  // namespace ray

void CoreWorker::HandleWaitForActorOutOfScope(
    const rpc::WaitForActorOutOfScopeRequest &request,
    rpc::WaitForActorOutOfScopeReply *reply, rpc::SendReplyCallback send_reply_callback) {
  // Currently WaitForActorOutOfScope is only used when GCS actor service is enabled.
  if (HandleWrongRecipient(WorkerID::FromBinary(request.intended_worker_id()),
                           send_reply_callback)) {
    return;
  }

  // Send a response to trigger cleaning up the actor state once the handle is
  // no longer in scope.
  auto respond = [send_reply_callback](const ActorID &actor_id) {
    RAY_LOG(DEBUG) << "Replying to HandleWaitForActorOutOfScope for " << actor_id;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  };

  const auto actor_id = ActorID::FromBinary(request.actor_id());
  RAY_LOG(DEBUG) << "Received HandleWaitForActorOutOfScope for " << actor_id;
  actor_manager_->WaitForActorOutOfScope(actor_id, std::move(respond));
}

void CoreWorker::HandleSubscribeForObjectEviction(
    const rpc::SubscribeForObjectEvictionRequest &request,
    rpc::SubscribeForObjectEvictionReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  // TODO(swang): Drop requests from raylets that executed an older version of
  // the task.
  if (HandleWrongRecipient(WorkerID::FromBinary(request.intended_worker_id()),
                           send_reply_callback)) {
    return;
  }

  const auto subscriber_node_id =
      NodeID::FromBinary(request.subscriber_address().raylet_id());
  // Send a response to trigger unpinning the object when it is no longer in scope.
  auto respond = [this, subscriber_node_id](const ObjectID &object_id) {
    RAY_LOG(DEBUG) << "Object " << object_id << " is deleted. Unpinning the object.";
    object_status_publisher_->Publish(object_id);
    object_status_publisher_->UnregisterSubscription(subscriber_node_id, object_id);
  };

  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  // Returns true if the object was present and the callback was added. It might have
  // already been evicted by the time we get this request, in which case we should
  // respond immediately so the raylet unpins the object.
  if (!reference_counter_->SetDeleteCallback(object_id, respond)) {
    std::ostringstream stream;
    stream << "Reference for object " << object_id << " has already been freed.";
    RAY_LOG(DEBUG) << stream.str();
    send_reply_callback(Status::NotFound(stream.str()), nullptr, nullptr);
  } else {
    object_status_publisher_->RegisterSubscription(subscriber_node_id, object_id);
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
}

void CoreWorker::HandlePubsubLongPolling(const rpc::PubsubLongPollingRequest &request,
                                         rpc::PubsubLongPollingReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  const auto subscriber_id = NodeID::FromBinary(request.subscriber_address().raylet_id());
  auto long_polling_reply_callback =
      [send_reply_callback = std::move(send_reply_callback), reply,
       subscriber_id](const std::vector<ObjectID> &object_ids) {
        RAY_LOG(DEBUG) << "Long polling replied to " << subscriber_id;
        // TODO(sang): The max grpc message size is 100 MB, meaning this operation can
        // fail if the number of batched objects are more than 50K. Though it is very
        // rare, we should probably handle it.
        for (const auto &object_id : object_ids) {
          reply->add_object_ids(object_id.Binary());
        }
        send_reply_callback(Status::OK(), nullptr, nullptr);
      };
  RAY_LOG(DEBUG) << "Got long polling request from node " << subscriber_id;
  object_status_publisher_->ConnectToSubscriber(subscriber_id,
                                                std::move(long_polling_reply_callback));
}

void CoreWorker::HandleAddObjectLocationOwner(
    const rpc::AddObjectLocationOwnerRequest &request,
    rpc::AddObjectLocationOwnerReply *reply, rpc::SendReplyCallback send_reply_callback) {
  if (HandleWrongRecipient(WorkerID::FromBinary(request.intended_worker_id()),
                           send_reply_callback)) {
    return;
  }
  auto object_id = ObjectID::FromBinary(request.object_id());
  auto reference_exists = reference_counter_->AddObjectLocation(
      object_id, NodeID::FromBinary(request.node_id()));
  Status status =
      reference_exists
          ? Status::OK()
          : Status::ObjectNotFound("Object " + object_id.Hex() + " not found");
  send_reply_callback(status, nullptr, nullptr);
}

void CoreWorker::HandleRemoveObjectLocationOwner(
    const rpc::RemoveObjectLocationOwnerRequest &request,
    rpc::RemoveObjectLocationOwnerReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  if (HandleWrongRecipient(WorkerID::FromBinary(request.intended_worker_id()),
                           send_reply_callback)) {
    return;
  }
  auto object_id = ObjectID::FromBinary(request.object_id());
  auto reference_exists = reference_counter_->RemoveObjectLocation(
      object_id, NodeID::FromBinary(request.node_id()));
  Status status =
      reference_exists
          ? Status::OK()
          : Status::ObjectNotFound("Object " + object_id.Hex() + " not found");
  send_reply_callback(status, nullptr, nullptr);
}

void CoreWorker::HandleGetObjectLocationsOwner(
    const rpc::GetObjectLocationsOwnerRequest &request,
    rpc::GetObjectLocationsOwnerReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  if (HandleWrongRecipient(WorkerID::FromBinary(request.intended_worker_id()),
                           send_reply_callback)) {
    return;
  }
  auto object_id = ObjectID::FromBinary(request.object_id());
  const auto &callback = [object_id, reply, send_reply_callback](
                             const absl::flat_hash_set<NodeID> &locations,
                             int64_t object_size, const std::string &spilled_url,
                             const NodeID &spilled_node_id, int64_t current_version) {
    RAY_LOG(DEBUG) << "Replying to HandleGetObjectLocationsOwner for " << object_id
                   << " with location update version " << current_version << ", "
                   << locations.size() << " locations, spilled url: " << spilled_url
                   << ", spilled node ID: " << spilled_node_id
                   << ", and object size: " << object_size;
    for (const auto &node_id : locations) {
      reply->add_node_ids(node_id.Binary());
    }
    reply->set_object_size(object_size);
    reply->set_spilled_url(spilled_url);
    reply->set_spilled_node_id(spilled_node_id.Binary());
    reply->set_current_version(current_version);
    send_reply_callback(Status::OK(), nullptr, nullptr);
  };
  auto status = reference_counter_->SubscribeObjectLocations(
      object_id, request.last_version(), callback);
  if (!status.ok()) {
    send_reply_callback(status, nullptr, nullptr);
  }
}

void CoreWorker::HandleWaitForRefRemoved(const rpc::WaitForRefRemovedRequest &request,
                                         rpc::WaitForRefRemovedReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  if (HandleWrongRecipient(WorkerID::FromBinary(request.intended_worker_id()),
                           send_reply_callback)) {
    return;
  }
  const ObjectID &object_id = ObjectID::FromBinary(request.reference().object_id());
  ObjectID contained_in_id = ObjectID::FromBinary(request.contained_in_id());
  const auto owner_address = request.reference().owner_address();
  auto ref_removed_callback =
      boost::bind(&ReferenceCounter::HandleRefRemoved, reference_counter_, object_id,
                  reply, send_reply_callback);
  // Set a callback to send the reply when the requested object ID's ref count
  // goes to 0.
  reference_counter_->SetRefRemovedCallback(object_id, contained_in_id, owner_address,
                                            ref_removed_callback);
}

void CoreWorker::HandleRemoteCancelTask(const rpc::RemoteCancelTaskRequest &request,
                                        rpc::RemoteCancelTaskReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) {
  auto status = CancelTask(ObjectID::FromBinary(request.remote_object_id()),
                           request.force_kill(), request.recursive());
  send_reply_callback(status, nullptr, nullptr);
}

void CoreWorker::HandleCancelTask(const rpc::CancelTaskRequest &request,
                                  rpc::CancelTaskReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) {
  absl::MutexLock lock(&mutex_);
  TaskID task_id = TaskID::FromBinary(request.intended_task_id());
  bool requested_task_running = main_thread_task_id_ == task_id;
  bool success = requested_task_running;

  // Try non-force kill
  if (requested_task_running && !request.force_kill()) {
    RAY_LOG(INFO) << "Interrupting a running task " << main_thread_task_id_;
    success = options_.kill_main();
  } else if (!requested_task_running) {
    // If the task is not currently running, check if it is in the worker's queue of
    // normal tasks, and remove it if found.
    success = direct_task_receiver_->CancelQueuedNormalTask(task_id);
  }
  if (request.recursive()) {
    auto recursive_cancel = CancelChildren(task_id, request.force_kill());
    if (recursive_cancel.ok()) {
      RAY_LOG(INFO) << "Recursive cancel failed!";
    }
  }

  // TODO: fix race condition to avoid using this hack
  requested_task_running = main_thread_task_id_ == task_id;

  reply->set_attempt_succeeded(success);
  send_reply_callback(Status::OK(), nullptr, nullptr);

  // Do force kill after reply callback sent
  if (requested_task_running && request.force_kill()) {
    RAY_LOG(INFO) << "Force killing a worker running " << main_thread_task_id_;
    Disconnect();
    if (options_.enable_logging) {
      RayLog::ShutDownRayLog();
    }
    // NOTE(hchen): Use `_Exit()` to force-exit this process without doing cleanup.
    // `exit()` will destruct static objects in an incorrect order, which will lead to
    // core dumps.
    _Exit(1);
  }
}

void CoreWorker::HandleKillActor(const rpc::KillActorRequest &request,
                                 rpc::KillActorReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) {
  ActorID intended_actor_id = ActorID::FromBinary(request.intended_actor_id());
  if (intended_actor_id != worker_context_.GetCurrentActorID()) {
    std::ostringstream stream;
    stream << "Mismatched ActorID: ignoring KillActor for previous actor "
           << intended_actor_id
           << ", current actor ID: " << worker_context_.GetCurrentActorID();
    auto msg = stream.str();
    RAY_LOG(ERROR) << msg;
    send_reply_callback(Status::Invalid(msg), nullptr, nullptr);
    return;
  }

  if (request.force_kill()) {
    RAY_LOG(INFO) << "Got KillActor, exiting immediately...";
    if (request.no_restart()) {
      Disconnect();
    }
    if (options_.num_workers > 1) {
      // TODO (kfstorm): Should we add some kind of check before sending the killing
      // request?
      RAY_LOG(ERROR)
          << "Killing an actor which is running in a worker process with multiple "
             "workers will also kill other actors in this process. To avoid this, "
             "please create the Java actor with some dynamic options to make it being "
             "hosted in a dedicated worker process.";
    }
    if (options_.enable_logging) {
      RayLog::ShutDownRayLog();
    }
    // NOTE(hchen): Use `_Exit()` to force-exit this process without doing cleanup.
    // `exit()` will destruct static objects in an incorrect order, which will lead to
    // core dumps.
    _Exit(1);
  } else {
    Exit(rpc::WorkerExitType::INTENDED_EXIT);
  }
}

void CoreWorker::HandleGetCoreWorkerStats(const rpc::GetCoreWorkerStatsRequest &request,
                                          rpc::GetCoreWorkerStatsReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  absl::MutexLock lock(&mutex_);
  auto stats = reply->mutable_core_worker_stats();
  // TODO(swang): Differentiate between tasks that are currently pending
  // execution and tasks that have finished but may be retried.
  stats->set_num_pending_tasks(task_manager_->NumSubmissibleTasks());
  stats->set_task_queue_length(task_queue_length_);
  stats->set_num_executed_tasks(num_executed_tasks_);
  stats->set_num_object_refs_in_scope(reference_counter_->NumObjectIDsInScope());
  stats->set_current_task_name(current_task_.GetName());
  stats->set_current_task_func_desc(current_task_.FunctionDescriptor()->ToString());
  stats->set_ip_address(rpc_address_.ip_address());
  stats->set_port(rpc_address_.port());
  stats->set_pid(getpid());
  stats->set_language(options_.language);
  stats->set_job_id(worker_context_.GetCurrentJobID().Binary());
  stats->set_worker_id(worker_context_.GetWorkerID().Binary());
  stats->set_actor_id(actor_id_.Binary());
  stats->set_worker_type(worker_context_.GetWorkerType());
  auto used_resources_map = stats->mutable_used_resources();
  for (auto const &it : *resource_ids_) {
    rpc::ResourceAllocations allocations;
    for (auto const &pair : it.second) {
      auto resource_slot = allocations.add_resource_slots();
      resource_slot->set_slot(pair.first);
      resource_slot->set_allocation(pair.second);
    }
    (*used_resources_map)[it.first] = allocations;
  }
  stats->set_actor_title(actor_title_);
  google::protobuf::Map<std::string, std::string> webui_map(webui_display_.begin(),
                                                            webui_display_.end());
  (*stats->mutable_webui_display()) = webui_map;

  MemoryStoreStats memory_store_stats = memory_store_->GetMemoryStoreStatisticalData();
  stats->set_num_in_plasma(memory_store_stats.num_in_plasma);
  stats->set_num_local_objects(memory_store_stats.num_local_objects);
  stats->set_used_object_store_memory(memory_store_stats.used_object_store_memory);

  if (request.include_memory_info()) {
    reference_counter_->AddObjectRefStats(plasma_store_provider_->UsedObjectsList(),
                                          stats);
  }

  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void CoreWorker::HandleLocalGC(const rpc::LocalGCRequest &request,
                               rpc::LocalGCReply *reply,
                               rpc::SendReplyCallback send_reply_callback) {
  if (options_.gc_collect != nullptr) {
    options_.gc_collect();
    send_reply_callback(Status::OK(), nullptr, nullptr);
  } else {
    send_reply_callback(Status::NotImplemented("GC callback not defined"), nullptr,
                        nullptr);
  }
}

void CoreWorker::HandleRunOnUtilWorker(const rpc::RunOnUtilWorkerRequest &request,
                                       rpc::RunOnUtilWorkerReply *reply,
                                       rpc::SendReplyCallback send_reply_callback) {
  if (options_.run_on_util_worker_handler) {
    std::vector<std::string> args(request.args().begin(), request.args().end());

    options_.run_on_util_worker_handler(request.request(), args);
    send_reply_callback(Status::OK(), nullptr, nullptr);
  } else {
    send_reply_callback(Status::NotImplemented("RunOnUtilWorker is not supported"),
                        nullptr, nullptr);
  }
}

void CoreWorker::HandleSpillObjects(const rpc::SpillObjectsRequest &request,
                                    rpc::SpillObjectsReply *reply,
                                    rpc::SendReplyCallback send_reply_callback) {
  if (options_.spill_objects != nullptr) {
    std::vector<ObjectID> object_ids_to_spill;
    object_ids_to_spill.reserve(request.object_ids_to_spill_size());
    for (const auto &id_binary : request.object_ids_to_spill()) {
      object_ids_to_spill.push_back(ObjectID::FromBinary(id_binary));
    }
    std::vector<std::string> owner_addresses;
    owner_addresses.reserve(request.owner_addresses_size());
    for (const auto &owner_address : request.owner_addresses()) {
      owner_addresses.push_back(owner_address.SerializeAsString());
    }
    std::vector<std::string> object_urls =
        options_.spill_objects(object_ids_to_spill, owner_addresses);
    for (size_t i = 0; i < object_urls.size(); i++) {
      reply->add_spilled_objects_url(std::move(object_urls[i]));
    }
    send_reply_callback(Status::OK(), nullptr, nullptr);
  } else {
    send_reply_callback(Status::NotImplemented("Spill objects callback not defined"),
                        nullptr, nullptr);
  }
}

void CoreWorker::HandleAddSpilledUrl(const rpc::AddSpilledUrlRequest &request,
                                     rpc::AddSpilledUrlReply *reply,
                                     rpc::SendReplyCallback send_reply_callback) {
  const ObjectID object_id = ObjectID::FromBinary(request.object_id());
  const std::string &spilled_url = request.spilled_url();
  const NodeID node_id = NodeID::FromBinary(request.spilled_node_id());
  RAY_LOG(DEBUG) << "Received AddSpilledUrl request for object " << object_id
                 << ", which has been spilled to " << spilled_url << " on node "
                 << node_id;
  auto reference_exists = reference_counter_->HandleObjectSpilled(
      object_id, spilled_url, node_id, request.size(), /*release*/ false);
  Status status =
      reference_exists
          ? Status::OK()
          : Status::ObjectNotFound("Object " + object_id.Hex() + " not found");
  send_reply_callback(status, nullptr, nullptr);
}

void CoreWorker::HandleRestoreSpilledObjects(
    const rpc::RestoreSpilledObjectsRequest &request,
    rpc::RestoreSpilledObjectsReply *reply, rpc::SendReplyCallback send_reply_callback) {
  if (options_.restore_spilled_objects != nullptr) {
    // Get a list of object ids.
    std::vector<ObjectID> object_ids_to_restore;
    object_ids_to_restore.reserve(request.object_ids_to_restore_size());
    for (const auto &id_binary : request.object_ids_to_restore()) {
      object_ids_to_restore.push_back(ObjectID::FromBinary(id_binary));
    }
    // Get a list of spilled_object_urls.
    std::vector<std::string> spilled_objects_url;
    spilled_objects_url.reserve(request.spilled_objects_url_size());
    for (const auto &url : request.spilled_objects_url()) {
      spilled_objects_url.push_back(url);
    }
    auto total =
        options_.restore_spilled_objects(object_ids_to_restore, spilled_objects_url);
    reply->set_bytes_restored_total(total);
    send_reply_callback(Status::OK(), nullptr, nullptr);
  } else {
    send_reply_callback(
        Status::NotImplemented("Restore spilled objects callback not defined"), nullptr,
        nullptr);
  }
}

void CoreWorker::HandleDeleteSpilledObjects(
    const rpc::DeleteSpilledObjectsRequest &request,
    rpc::DeleteSpilledObjectsReply *reply, rpc::SendReplyCallback send_reply_callback) {
  if (options_.delete_spilled_objects != nullptr) {
    std::vector<std::string> spilled_objects_url;
    spilled_objects_url.reserve(request.spilled_objects_url_size());
    for (const auto &url : request.spilled_objects_url()) {
      spilled_objects_url.push_back(url);
    }
    options_.delete_spilled_objects(spilled_objects_url, worker_context_.GetWorkerType());
    send_reply_callback(Status::OK(), nullptr, nullptr);
  } else {
    send_reply_callback(
        Status::NotImplemented("Delete spilled objects callback not defined"), nullptr,
        nullptr);
  }
}

void CoreWorker::HandleExit(const rpc::ExitRequest &request, rpc::ExitReply *reply,
                            rpc::SendReplyCallback send_reply_callback) {
  bool own_objects = reference_counter_->OwnObjects();
  // Fail the request if it owns any object.
  reply->set_success(!own_objects);
  send_reply_callback(Status::OK(),
                      [own_objects, this]() {
                        // If it doesn't own objects, we'll exit it
                        if (!own_objects) {
                          Exit(rpc::WorkerExitType::INTENDED_EXIT);
                        }
                      },
                      // We need to kill it if grpc failed.
                      [this]() { Exit(rpc::WorkerExitType::INTENDED_EXIT); });
}

void CoreWorker::YieldCurrentFiber(FiberEvent &event) {
  RAY_CHECK(worker_context_.CurrentActorIsAsync());
  boost::this_fiber::yield();
  event.Wait();
}

void CoreWorker::GetAsync(const ObjectID &object_id, SetResultCallback success_callback,
                          void *python_future) {
  auto fallback_callback =
      std::bind(&CoreWorker::PlasmaCallback, this, success_callback,
                std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);

  memory_store_->GetAsync(object_id, [python_future, success_callback, fallback_callback,
                                      object_id](std::shared_ptr<RayObject> ray_object) {
    if (ray_object->IsInPlasmaError()) {
      fallback_callback(ray_object, object_id, python_future);
    } else {
      success_callback(ray_object, object_id, python_future);
    }
  });
}

void CoreWorker::PlasmaCallback(SetResultCallback success,
                                std::shared_ptr<RayObject> ray_object, ObjectID object_id,
                                void *py_future) {
  RAY_CHECK(ray_object->IsInPlasmaError());

  // First check if the object is available in local plasma store.
  // Note that we are using Contains instead of Get so it won't trigger pull request
  // to remote nodes.
  bool object_is_local = false;
  if (Contains(object_id, &object_is_local).ok() && object_is_local) {
    std::vector<std::shared_ptr<RayObject>> vec;
    RAY_CHECK_OK(Get(std::vector<ObjectID>{object_id}, 0, &vec));
    RAY_CHECK(vec.size() > 0)
        << "Failed to get local object but Raylet notified object is local.";
    return success(vec.front(), object_id, py_future);
  }

  // Object is not available locally. We now add the callback to listener queue.
  {
    absl::MutexLock lock(&plasma_mutex_);
    auto plasma_arrived_callback = [this, success, object_id, py_future]() {
      // This callback is invoked on the io_service_ event loop, so it cannot call
      // blocking call like Get(). We used GetAsync here, which should immediate call
      // PlasmaCallback again with object available locally.
      GetAsync(object_id, success, py_future);
    };

    async_plasma_callbacks_[object_id].push_back(plasma_arrived_callback);
  }

  // Ask raylet to subscribe to object notification. Raylet will call this core worker
  // when the object is local (and it will fire the callback immediately if the object
  // exists). CoreWorker::HandlePlasmaObjectReady handles such request.
  local_raylet_client_->SubscribeToPlasma(object_id, GetOwnerAddress(object_id));
}

void CoreWorker::HandlePlasmaObjectReady(const rpc::PlasmaObjectReadyRequest &request,
                                         rpc::PlasmaObjectReadyReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  std::vector<std::function<void(void)>> callbacks;
  {
    absl::MutexLock lock(&plasma_mutex_);
    auto it = async_plasma_callbacks_.extract(ObjectID::FromBinary(request.object_id()));
    callbacks = it.mapped();
  }
  for (auto callback : callbacks) {
    // This callback needs to be asynchronous because it runs on the io_service_, so no
    // RPCs can be processed while it's running. This can easily lead to deadlock (for
    // example if the callback calls ray.get() on an object that is dependent on an RPC
    // to be ready).
    callback();
  }
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void CoreWorker::SetActorId(const ActorID &actor_id) {
  absl::MutexLock lock(&mutex_);
  if (!options_.is_local_mode) {
    RAY_CHECK(actor_id_.IsNil());
  }
  actor_id_ = actor_id;
}

void CoreWorker::SetWebuiDisplay(const std::string &key, const std::string &message) {
  absl::MutexLock lock(&mutex_);
  webui_display_[key] = message;
}

void CoreWorker::SetActorTitle(const std::string &title) {
  absl::MutexLock lock(&mutex_);
  actor_title_ = title;
}

const rpc::JobConfig &CoreWorker::GetJobConfig() const { return *job_config_; }

std::shared_ptr<gcs::GcsClient> CoreWorker::GetGcsClient() const { return gcs_client_; }

bool CoreWorker::IsExiting() const { return exiting_; }

}  // namespace ray
