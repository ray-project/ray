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

#include "ray/core_worker/core_worker_process.h"

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "absl/cleanup/cleanup.h"
#include "absl/strings/str_format.h"
#include "ray/common/ray_config.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/core_worker_rpc_proxy.h"
#include "ray/core_worker_rpc_client/core_worker_client.h"
#include "ray/core_worker_rpc_client/core_worker_client_pool.h"
#include "ray/gcs_rpc_client/gcs_client.h"
#include "ray/object_manager/plasma/client.h"
#include "ray/raylet_ipc_client/raylet_ipc_client.h"
#include "ray/raylet_rpc_client/raylet_client.h"
#include "ray/stats/stats.h"
#include "ray/stats/tag_defs.h"
#include "ray/util/env.h"
#include "ray/util/event.h"
#include "ray/util/network_util.h"
#include "ray/util/path_utils.h"
#include "ray/util/process.h"
#include "ray/util/stream_redirection.h"
#include "ray/util/stream_redirection_options.h"
#include "ray/util/subreaper.h"

namespace ray {
namespace core {
namespace {

std::unique_ptr<CoreWorkerProcessImpl> core_worker_process;

// Get out and error filepath for worker.
// It's worth noticing that filepath format should be kept in sync with function
// `get_worker_log_file_name` under file
// "ray/python/ray/_private/ray_logging/__init__.py".
std::string GetWorkerOutputFilepath(WorkerType worker_type,
                                    const JobID &job_id,
                                    const WorkerID &worker_id,
                                    const std::string &suffix) {
  std::string parsed_job_id;
  if (job_id.IsNil()) {
    char *job_id_env = ::getenv("RAY_JOB_ID");
    if (job_id_env != nullptr) {
      parsed_job_id = job_id_env;
    }
  }
  std::string worker_name;
  if (worker_type == WorkerType::WORKER) {
    worker_name = "worker";
  } else {
    parsed_job_id = "";
    worker_name = "io_worker";
  }

  if (!parsed_job_id.empty()) {
    return absl::StrFormat(
        "%s-%s-%s-%d.%s", worker_name, worker_id.Hex(), parsed_job_id, GetPID(), suffix);
  }
  return absl::StrFormat("%s-%s-%d.%s", worker_name, worker_id.Hex(), GetPID(), suffix);
}

}  // namespace

void CoreWorkerProcess::Initialize(const CoreWorkerOptions &options) {
  RAY_CHECK(!core_worker_process)
      << "The process is already initialized for core worker.";
  core_worker_process = std::make_unique<CoreWorkerProcessImpl>(options);

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

void CoreWorkerProcess::Shutdown() {
  RAY_LOG(DEBUG) << "Shutdown. Core worker process will be deleted";
  if (!core_worker_process) {
    return;
  }
  core_worker_process->ShutdownDriver();
  core_worker_process.reset();
}

bool CoreWorkerProcess::IsInitialized() { return core_worker_process != nullptr; }

void CoreWorkerProcess::HandleAtExit() { core_worker_process.reset(); }

CoreWorker &CoreWorkerProcess::GetCoreWorker() {
  EnsureInitialized(/*quick_exit*/ true);
  return *core_worker_process->GetCoreWorker();
}

void CoreWorkerProcess::RunTaskExecutionLoop() {
  EnsureInitialized(/*quick_exit*/ false);
  core_worker_process->RunWorkerTaskExecutionLoop();
  core_worker_process.reset();
}

std::shared_ptr<CoreWorker> CoreWorkerProcess::TryGetWorker() {
  if (!core_worker_process) {
    return nullptr;
  }
  return core_worker_process->TryGetCoreWorker();
}

std::shared_ptr<CoreWorker> CoreWorkerProcessImpl::CreateCoreWorker(
    CoreWorkerOptions options, const WorkerID &worker_id) {
  /// Event loop where the IO events are handled. e.g. async GCS operations.
  auto client_call_manager = std::make_unique<rpc::ClientCallManager>(
      io_service_, /*record_stats=*/false, options.node_ip_address);
  auto periodical_runner = PeriodicalRunner::Create(io_service_);
  auto worker_context = std::make_unique<WorkerContext>(
      options.worker_type, worker_id, GetProcessJobID(options));
  auto pid = getpid();

  RAY_LOG(DEBUG) << "Creating core worker with debug source: " << options.debug_source;

  RAY_LOG(DEBUG).WithField(worker_id) << "Constructing CoreWorker";
  if (RayConfig::instance().kill_child_processes_on_worker_exit_with_raylet_subreaper()) {
#ifdef __linux__
    // Not setting sigchld = ignore: user may want to do waitpid on their own.
    // If user's bad code causes a zombie process, it will hang there in zombie status
    // until this worker exits and raylet reaps it.
    if (SetThisProcessAsSubreaper()) {
      RAY_LOG(INFO) << "Set this core_worker process as subreaper: " << pid
                    << " (deprecated; prefer per-worker process groups).";
      SetSigchldIgnore();
    } else {
      RAY_LOG(WARNING)
          << "Failed to set this core_worker process as subreaper. If Raylet is set as "
             "subreaper, user-spawn daemon processes may be killed by raylet. "
             "Subreaper is deprecated; prefer per-worker process groups.";
    }
#else
    RAY_LOG(WARNING) << "Subreaper is not supported on this platform. Raylet will not "
                        "kill unknown children.";
#endif
  }

  auto task_event_buffer = std::make_unique<worker::TaskEventBufferImpl>(
      std::make_unique<gcs::GcsClient>(options.gcs_options, options.node_ip_address),
      std::make_unique<rpc::EventAggregatorClientImpl>(options.metrics_agent_port,
                                                       *client_call_manager),
      options.session_name);

  // Start the IO thread first to make sure the checker is working.
  boost::thread::attributes io_thread_attrs;
#if defined(__APPLE__)
  // io thread will run python code through cython
  // but Mac's default stack size for non-main-thread is too small
  // for certain python libraries like numpy and will cause sigbus.
  // Here we increase the stack size to the size that python uses in
  // https://github.com/python/cpython/blob/v3.9.0/Python/thread_pthread.h#L35.
  // See https://github.com/ray-project/ray/issues/41094 for more details.
  io_thread_attrs.set_stack_size(16777216);
#endif
  io_thread_ = boost::thread(io_thread_attrs, [this]() {
#ifndef _WIN32
    // Block SIGINT and SIGTERM so they will be handled by the main thread.
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGINT);
    sigaddset(&mask, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &mask, nullptr);
#endif
    SetThreadName("worker.io");
    io_service_.run();
    RAY_LOG(INFO) << "Core worker main io service stopped.";
  });

  if (options.worker_type == WorkerType::DRIVER &&
      !options.serialized_job_config.empty()) {
    // Driver populates the job config via initialization.
    // Workers populates it when the first task is received.
    rpc::JobConfig job_config;
    job_config.ParseFromString(options.serialized_job_config);
    worker_context->MaybeInitializeJobInfo(worker_context->GetCurrentJobID(), job_config);
  }

  auto raylet_ipc_client = std::make_shared<ray::ipc::RayletIpcClient>(
      io_service_, options.raylet_socket, /*num_retries=*/-1, /*timeout=*/-1);

  NodeID local_node_id;
  int assigned_port = 0;
  Status status = raylet_ipc_client->RegisterClient(worker_context->GetWorkerID(),
                                                    options.worker_type,
                                                    worker_context->GetCurrentJobID(),
                                                    options.runtime_env_hash,
                                                    options.language,
                                                    options.node_ip_address,
                                                    options.serialized_job_config,
                                                    options.startup_token,
                                                    &local_node_id,
                                                    &assigned_port);
  if (!status.ok()) {
    // Avoid using FATAL log or RAY_CHECK here because they may create a core dump file.
    RAY_LOG(ERROR).WithField(worker_id)
        << "Failed to register worker to Raylet: " << status;
    QuickExit();
  }
  RAY_CHECK_GE(assigned_port, 0);

  // Initialize raylet client.
  // NOTE(edoakes): the core_worker_server_ must be running before registering with
  // the raylet, as the raylet will start sending some RPC messages immediately.
  // TODO(zhijunfu): currently RayletClient would crash in its constructor if it cannot
  // connect to Raylet after a number of retries, this can be changed later
  // so that the worker (java/python .etc) can retrieve and handle the error
  // instead of crashing.
  auto raylet_address = rpc::RayletClientPool::GenerateRayletAddress(
      local_node_id, options.node_ip_address, options.node_manager_port);
  auto local_raylet_rpc_client =
      std::make_shared<rpc::RayletClient>(std::move(raylet_address),
                                          *client_call_manager,
                                          /*raylet_unavailable_timeout_callback=*/[] {});
  auto core_worker_server =
      std::make_unique<rpc::GrpcServer>(WorkerTypeString(options.worker_type),
                                        assigned_port,
                                        options.node_ip_address == "127.0.0.1");
  // Start RPC server after all the task receivers are properly initialized and we have
  // our assigned port from the raylet.
  core_worker_server->RegisterService(
      std::make_unique<rpc::CoreWorkerGrpcService>(
          io_service_, *service_handler_, /*max_active_rpcs_per_handler_=*/-1),
      false /* token_auth */);
  core_worker_server->Run();

  // Set our own address.
  RAY_CHECK(!local_node_id.IsNil());
  rpc::Address rpc_address;
  rpc_address.set_ip_address(options.node_ip_address);
  rpc_address.set_port(core_worker_server->GetPort());
  rpc_address.set_node_id(local_node_id.Binary());
  rpc_address.set_worker_id(worker_context->GetWorkerID().Binary());
  RAY_LOG(INFO).WithField(worker_context->GetWorkerID()).WithField(local_node_id)
      << "Initializing worker at address: "
      << BuildAddress(rpc_address.ip_address(), rpc_address.port());

  auto gcs_client = std::make_shared<gcs::GcsClient>(
      options.gcs_options, options.node_ip_address, worker_context->GetWorkerID());
  RAY_CHECK_OK(gcs_client->Connect(io_service_));

  if (RayConfig::instance().task_events_report_interval_ms() > 0) {
    if (!task_event_buffer->Start().ok()) {
      RAY_CHECK(!task_event_buffer->Enabled()) << "TaskEventBuffer should be disabled.";
    }
  }

  auto raylet_client_pool =
      std::make_shared<rpc::RayletClientPool>([&](const rpc::Address &addr) {
        auto core_worker = GetCoreWorker();
        return std::make_shared<ray::rpc::RayletClient>(
            addr,
            *core_worker->client_call_manager_,
            rpc::RayletClientPool::GetDefaultUnavailableTimeoutCallback(
                core_worker->gcs_client_.get(),
                core_worker->raylet_client_pool_.get(),
                addr));
      });

  std::shared_ptr<rpc::CoreWorkerClientPool> core_worker_client_pool =
      std::make_shared<rpc::CoreWorkerClientPool>([this](const rpc::Address &addr) {
        auto core_worker = GetCoreWorker();
        return std::make_shared<rpc::CoreWorkerClient>(
            addr,
            *core_worker->client_call_manager_,
            rpc::CoreWorkerClientPool::GetDefaultUnavailableTimeoutCallback(
                core_worker->gcs_client_.get(),
                core_worker->core_worker_client_pool_.get(),
                core_worker->raylet_client_pool_.get(),
                addr));
      });

  auto object_info_publisher = std::make_unique<pubsub::Publisher>(
      /*channels=*/
      std::vector<rpc::ChannelType>{rpc::ChannelType::WORKER_OBJECT_EVICTION,
                                    rpc::ChannelType::WORKER_REF_REMOVED_CHANNEL,
                                    rpc::ChannelType::WORKER_OBJECT_LOCATIONS_CHANNEL},
      /*periodical_runner=*/*periodical_runner,
      /*get_time_ms=*/[]() { return absl::GetCurrentTimeNanos() / 1e6; },
      /*subscriber_timeout_ms=*/RayConfig::instance().subscriber_timeout_ms(),
      /*publish_batch_size_=*/RayConfig::instance().publish_batch_size(),
      worker_context->GetWorkerID());
  auto object_info_subscriber = std::make_unique<pubsub::Subscriber>(
      /*subscriber_id=*/worker_context->GetWorkerID(),
      /*channels=*/
      std::vector<rpc::ChannelType>{rpc::ChannelType::WORKER_OBJECT_EVICTION,
                                    rpc::ChannelType::WORKER_REF_REMOVED_CHANNEL,
                                    rpc::ChannelType::WORKER_OBJECT_LOCATIONS_CHANNEL},
      /*max_command_batch_size*/ RayConfig::instance().max_command_batch_size(),
      /*get_client=*/
      [this](const rpc::Address &address) {
        auto core_worker = GetCoreWorker();
        return core_worker->core_worker_client_pool_->GetOrConnect(address);
      },
      /*callback_service*/ &io_service_);

  auto reference_counter = std::make_shared<ReferenceCounter>(
      rpc_address,
      /*object_info_publisher=*/object_info_publisher.get(),
      /*object_info_subscriber=*/object_info_subscriber.get(),
      /*is_node_dead=*/
      [this](const NodeID &node_id) {
        return GetCoreWorker()->gcs_client_->Nodes().IsNodeDead(node_id);
      },
      RayConfig::instance().lineage_pinning_enabled());

  std::shared_ptr<LeaseRequestRateLimiter> lease_request_rate_limiter;
  if (RayConfig::instance().max_pending_lease_requests_per_scheduling_category() > 0) {
    lease_request_rate_limiter = std::make_shared<StaticLeaseRequestRateLimiter>(
        RayConfig::instance().max_pending_lease_requests_per_scheduling_category());
  } else {
    RAY_CHECK(
        RayConfig::instance().max_pending_lease_requests_per_scheduling_category() != 0)
        << "max_pending_lease_requests_per_scheduling_category can't be 0";
    lease_request_rate_limiter =
        std::make_shared<ClusterSizeBasedLeaseRequestRateLimiter>(
            /*min_concurrent_lease_cap_*/ 10);
  }

  // We can turn on exit_on_connection_failure on for the core worker plasma
  // client to early exit core worker after the raylet's death because on the
  // raylet side, we never proactively close the plasma store connection even
  // during shutdown. So any error from the raylet side should be a sign of raylet
  // death.
  auto plasma_client =
      std::make_shared<plasma::PlasmaClient>(/*exit_on_connection_failure*/ true);
  auto plasma_store_provider = std::make_shared<CoreWorkerPlasmaStoreProvider>(
      options.store_socket,
      raylet_ipc_client,
      *reference_counter,
      options.check_signals,
      /*warmup=*/
      (options.worker_type != WorkerType::SPILL_WORKER &&
       options.worker_type != WorkerType::RESTORE_WORKER),
      /*store_client=*/std::move(plasma_client),
      /*fetch_batch_size=*/RayConfig::instance().worker_fetch_request_size(),
      /*get_current_call_site=*/[this]() {
        auto core_worker = GetCoreWorker();
        return core_worker->CurrentCallSite();
      });
  auto memory_store = std::make_shared<CoreWorkerMemoryStore>(
      io_service_,
      reference_counter.get(),
      raylet_ipc_client,
      options.check_signals,
      [this](const RayObject &obj) {
        auto core_worker = GetCoreWorker();
        rpc::ErrorType error_type;
        if (obj.IsException(&error_type) &&
            error_type == rpc::ErrorType::END_OF_STREAMING_GENERATOR) {
          // End-of-stream ObjectRefs are sentinels and should never get
          // returned to the caller.
          return;
        }
        // Run this on the event loop to avoid calling back into the language runtime
        // from the middle of user operations.
        core_worker->io_service_.post(
            [this, obj]() {
              auto this_core_worker = GetCoreWorker();
              if (this_core_worker->options_.unhandled_exception_handler != nullptr) {
                this_core_worker->options_.unhandled_exception_handler(obj);
              }
            },
            "CoreWorker.HandleException");
      });

  std::shared_ptr<experimental::MutableObjectProvider>
      experimental_mutable_object_provider;

#if defined(__APPLE__) || defined(__linux__)
  auto raylet_channel_client_factory = [this](const NodeID &node_id) {
    auto core_worker = GetCoreWorker();
    auto node_info = core_worker->gcs_client_->Nodes().Get(node_id);
    RAY_CHECK(node_info) << "No GCS info for node " << node_id;
    auto addr = rpc::RayletClientPool::GenerateRayletAddress(
        node_id, node_info->node_manager_address(), node_info->node_manager_port());
    return core_worker->raylet_client_pool_->GetOrConnectByAddress(addr);
  };

  experimental_mutable_object_provider =
      std::make_shared<experimental::MutableObjectProvider>(
          plasma_store_provider->store_client(),
          raylet_channel_client_factory,
          options.check_signals);
#endif

  auto push_error_callback = [this](const JobID &job_id,
                                    const std::string &type,
                                    const std::string &error_message,
                                    double timestamp) {
    auto core_worker = GetCoreWorker();
    return core_worker->PushError(job_id, type, error_message, timestamp);
  };

  auto task_manager = std::make_shared<TaskManager>(
      *memory_store,
      *reference_counter,
      /*put_in_local_plasma_callback=*/
      [this](const RayObject &object, const ObjectID &object_id) {
        auto core_worker = GetCoreWorker();
        constexpr int max_retries = 3;
        int attempt = 0;
        int64_t backoff_ms = 10;
        Status put_status;
        while (attempt++ < max_retries) {
          put_status =
              core_worker->PutInLocalPlasmaStore(object, object_id, /*pin_object=*/true);
          if (put_status.ok()) {
            return Status::OK();
          }
          // Backoff before retrying.
          std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
          backoff_ms *= 2;
        }
        RAY_LOG(WARNING).WithField(object_id)
            << "Exhausted plasma put retries (attempts=" << attempt
            << ") with status: " << put_status;
        return put_status;
      },
      /* async_retry_task_callback=*/
      [this](TaskSpecification &spec, uint32_t delay_ms) {
        auto core_worker = GetCoreWorker();
        core_worker->AsyncRetryTask(spec, delay_ms);
      },
      /*queue_generator_resubmit=*/
      [this](const TaskSpecification &spec) {
        auto core_worker = GetCoreWorker();
        return spec.IsActorTask()
                   ? core_worker->actor_task_submitter_->QueueGeneratorForResubmit(spec)
                   : core_worker->normal_task_submitter_->QueueGeneratorForResubmit(spec);
      },
      push_error_callback,
      RayConfig::instance().max_lineage_bytes(),
      *task_event_buffer,
      /*get_actor_rpc_client_callback=*/
      [this](const ActorID &actor_id)
          -> std::optional<std::shared_ptr<rpc::CoreWorkerClientInterface>> {
        auto core_worker = GetCoreWorker();
        auto addr = core_worker->actor_task_submitter_->GetActorAddress(actor_id);
        if (!addr.has_value()) {
          return std::nullopt;
        }
        return core_worker->core_worker_client_pool_->GetOrConnect(*addr);
      },
      gcs_client,
      task_by_state_gauge_,
      /*free_actor_object_callback=*/
      [this](const ObjectID &object_id) {
        auto core_worker = GetCoreWorker();
        core_worker->free_actor_object_callback_(object_id);
      });

  auto on_excess_queueing = [this](const ActorID &actor_id,
                                   const std::string &actor_name,
                                   int64_t num_queued) {
    auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
    auto core_worker = GetCoreWorker();
    auto message = absl::StrFormat(
        "Warning: More than %d tasks are pending submission to actor %s with actor_id "
        "%s. To reduce memory usage, wait for these tasks to finish before sending more.",
        num_queued,
        actor_name,
        actor_id.Hex());
    RAY_CHECK_OK(core_worker->PushError(
        core_worker->options_.job_id, "excess_queueing_warning", message, timestamp));
  };

  auto actor_creator = std::make_shared<ActorCreator>(gcs_client->Actors());

  auto actor_task_submitter = std::make_unique<ActorTaskSubmitter>(
      *core_worker_client_pool,
      *memory_store,
      *task_manager,
      *actor_creator,
      /*tensor_transport_getter=*/
      [this](const ObjectID &object_id) {
        auto core_worker = GetCoreWorker();
        return core_worker->reference_counter_->GetTensorTransport(object_id);
      },
      on_excess_queueing,
      io_service_,
      reference_counter);

  auto node_addr_factory = [this](const NodeID &node_id) {
    auto core_worker = GetCoreWorker();
    std::optional<rpc::Address> address_opt;
    if (auto node_info = core_worker->gcs_client_->Nodes().Get(node_id)) {
      auto &address = address_opt.emplace();
      address.set_node_id(node_info->node_id());
      address.set_ip_address(node_info->node_manager_address());
      address.set_port(node_info->node_manager_port());
    }
    return address_opt;
  };

  auto lease_policy =
      RayConfig::instance().locality_aware_leasing_enabled()
          ? std::unique_ptr<LeasePolicyInterface>(
                std::make_unique<LocalityAwareLeasePolicy>(
                    *reference_counter, node_addr_factory, raylet_address))
          : std::unique_ptr<LeasePolicyInterface>(
                std::make_unique<LocalLeasePolicy>(raylet_address));

  auto normal_task_submitter = std::make_unique<NormalTaskSubmitter>(
      rpc_address,
      local_raylet_rpc_client,
      core_worker_client_pool,
      raylet_client_pool,
      std::move(lease_policy),
      memory_store,
      *task_manager,
      local_node_id,
      options.worker_type,
      RayConfig::instance().worker_lease_timeout_milliseconds(),
      actor_creator,
      worker_context->GetCurrentJobID(),
      lease_request_rate_limiter,
      /*tensor_transport_getter=*/
      [](const ObjectID &object_id) {
        // Currently, out-of-band tensor transport (i.e., GPU objects) is only
        // supported for actor tasks. Therefore, normal tasks should always use
        // OBJECT_STORE.
        return rpc::TensorTransport::OBJECT_STORE;
      },
      boost::asio::steady_timer(io_service_));

  auto report_locality_data_callback = [this](
                                           const ObjectID &object_id,
                                           const absl::flat_hash_set<NodeID> &locations,
                                           uint64_t object_size) {
    auto core_worker = GetCoreWorker();
    core_worker->reference_counter_->ReportLocalityData(
        object_id, locations, object_size);
  };

  auto future_resolver =
      std::make_unique<FutureResolver>(memory_store,
                                       reference_counter,
                                       std::move(report_locality_data_callback),
                                       core_worker_client_pool,
                                       rpc_address);

  auto actor_manager = std::make_unique<ActorManager>(
      gcs_client, *actor_task_submitter, *reference_counter);

  // For the recovery manager to lookup the addresses / ports of the nodes with secondary
  // copies.
  auto object_lookup = [this](const ObjectID &object_id,
                              const ObjectLookupCallback &callback) {
    auto core_worker = GetCoreWorker();
    std::vector<rpc::Address> locations;
    const std::optional<absl::flat_hash_set<NodeID>> object_locations =
        core_worker->reference_counter_->GetObjectLocations(object_id);
    std::vector<NodeID> nodes_to_lookup;
    if (object_locations.has_value()) {
      locations.reserve(object_locations->size());
      for (const auto &node_id : *object_locations) {
        auto *node_info =
            core_worker->gcs_client_->Nodes().Get(node_id, /*filter_dead_nodes=*/false);
        if (node_info == nullptr) {
          // Unsure if the node is dead, so we need to confirm with the GCS. This should
          // be rare, the only foreseeable reasons are:
          // 1. We filled our cache after the GCS cleared the node info due to
          //    maximum_gcs_dead_node_cached_count.
          // 2. The node is alive but we haven't received the publish yet.
          nodes_to_lookup.push_back(node_id);
          continue;
        }
        if (node_info->state() == rpc::GcsNodeInfo::DEAD) {
          continue;
        }
        rpc::Address addr;
        addr.set_node_id(node_info->node_id());
        addr.set_ip_address(node_info->node_manager_address());
        addr.set_port(node_info->node_manager_port());
        locations.push_back(std::move(addr));
      }
    }
    if (nodes_to_lookup.empty()) {
      callback(object_id, std::move(locations));
      return;
    }
    core_worker->gcs_client_->Nodes().AsyncGetAll(
        [callback, object_id, locations = std::move(locations)](
            const Status &, const std::vector<rpc::GcsNodeInfo> &node_infos) mutable {
          for (const auto &node_info : node_infos) {
            if (node_info.state() != rpc::GcsNodeInfo::DEAD) {
              rpc::Address addr;
              addr.set_node_id(node_info.node_id());
              addr.set_ip_address(node_info.node_manager_address());
              addr.set_port(node_info.node_manager_port());
              locations.push_back(std::move(addr));
            }
          }
          callback(object_id, std::move(locations));
        },
        -1,
        nodes_to_lookup);
  };

  auto object_recovery_manager = std::make_unique<ObjectRecoveryManager>(
      rpc_address,
      raylet_client_pool,
      std::move(object_lookup),
      *task_manager,
      *reference_counter,
      *memory_store,
      [this](const ObjectID &object_id, rpc::ErrorType reason, bool pin_object) {
        RAY_LOG(DEBUG).WithField(object_id)
            << "Failed to recover object due to " << rpc::ErrorType_Name(reason);
        auto core_worker = GetCoreWorker();
        // We should throw the object error to the application.
        RAY_UNUSED(core_worker->Put(RayObject(reason),
                                    /*contained_object_ids=*/{},
                                    object_id,
                                    /*pin_object=*/pin_object));
      });

  // Set event context for current core worker thread.
  RayEventContext::Instance().SetEventContext(
      ray::rpc::Event_SourceType::Event_SourceType_CORE_WORKER,
      {{"worker_id", worker_id.Hex()}});

  auto core_worker =
      std::make_shared<CoreWorker>(std::move(options),
                                   std::move(worker_context),
                                   io_service_,
                                   std::move(client_call_manager),
                                   std::move(core_worker_client_pool),
                                   std::move(raylet_client_pool),
                                   std::move(periodical_runner),
                                   std::move(core_worker_server),
                                   std::move(rpc_address),
                                   std::move(gcs_client),
                                   std::move(raylet_ipc_client),
                                   std::move(local_raylet_rpc_client),
                                   io_thread_,
                                   std::move(reference_counter),
                                   std::move(memory_store),
                                   std::move(plasma_store_provider),
                                   std::move(experimental_mutable_object_provider),
                                   std::move(future_resolver),
                                   std::move(task_manager),
                                   std::move(actor_creator),
                                   std::move(actor_task_submitter),
                                   std::move(object_info_publisher),
                                   std::move(object_info_subscriber),
                                   std::move(lease_request_rate_limiter),
                                   std::move(normal_task_submitter),
                                   std::move(object_recovery_manager),
                                   std::move(actor_manager),
                                   task_execution_service_,
                                   std::move(task_event_buffer),
                                   pid,
                                   task_by_state_gauge_,
                                   actor_by_state_gauge_);
  return core_worker;
}

CoreWorkerProcessImpl::CoreWorkerProcessImpl(const CoreWorkerOptions &options)
    : options_(options),
      worker_id_(options.worker_type == WorkerType::DRIVER
                     ? ComputeDriverIdFromJob(options_.job_id)
                     : WorkerID::FromRandom()),
      io_work_(io_service_.get_executor()),
      task_execution_service_work_(task_execution_service_.get_executor()),
      service_handler_(std::make_unique<CoreWorkerServiceHandlerProxy>()) {
  if (options_.enable_logging) {
    // Setup logging for worker system logging.
    {
      std::stringstream app_name_ss;
      app_name_ss << LanguageString(options_.language) << "-core-"
                  << WorkerTypeString(options_.worker_type);
      if (!worker_id_.IsNil()) {
        app_name_ss << "-" << worker_id_;
      }
      const std::string app_name = app_name_ss.str();
      const std::string log_filepath =
          GetLogFilepathFromDirectory(options_.log_dir, /*app_name=*/app_name);
      RayLog::StartRayLog(app_name,
                          RayLogLevel::INFO,
                          log_filepath,
                          /*err_log_filepath=*/"",
                          ray::RayLog::GetRayLogRotationMaxBytesOrDefault(),
                          ray::RayLog::GetRayLogRotationBackupCountOrDefault());
    }

    // Setup logging for worker application logging.
    if (options_.worker_type != WorkerType::DRIVER && !IsEnvTrue("RAY_LOG_TO_STDERR")) {
      // Setup redirection for stdout.
      {
        const std::string fname = GetWorkerOutputFilepath(
            options_.worker_type, options_.job_id, worker_id_, /*suffix=*/"out");
        const std::string worker_output_filepath = JoinPaths(options_.log_dir, fname);

        ray::StreamRedirectionOption stdout_redirection_options;
        stdout_redirection_options.file_path = worker_output_filepath;
        stdout_redirection_options.rotation_max_size =
            ray::RayLog::GetRayLogRotationMaxBytesOrDefault();
        stdout_redirection_options.rotation_max_file_count =
            ray::RayLog::GetRayLogRotationBackupCountOrDefault();
        ray::RedirectStdoutOncePerProcess(stdout_redirection_options);
      }

      // Setup redirection for stderr.
      {
        const std::string fname = GetWorkerOutputFilepath(
            options_.worker_type, options_.job_id, worker_id_, /*suffix=*/"err");
        const std::string worker_error_filepath = JoinPaths(options_.log_dir, fname);

        ray::StreamRedirectionOption stderr_redirection_options;
        stderr_redirection_options.file_path = worker_error_filepath;
        stderr_redirection_options.rotation_max_size =
            ray::RayLog::GetRayLogRotationMaxBytesOrDefault();
        stderr_redirection_options.rotation_max_file_count =
            ray::RayLog::GetRayLogRotationBackupCountOrDefault();
        ray::RedirectStderrOncePerProcess(stderr_redirection_options);
      }
    }

    if (options_.install_failure_signal_handler) {
      // Core worker is loaded as a dynamic library from Python or other languages.
      // We are not sure if the default argv[0] would be suitable for loading symbols
      // so leaving it unspecified as nullptr. This could make symbolization of crash
      // traces fail in some circumstances.
      //
      // Also, call the previous crash handler, e.g. the one installed by the Python
      // worker.
      RayLog::InstallFailureSignalHandler(nullptr, /*call_previous_handler=*/true);
      RayLog::InstallTerminateHandler();
    }
  } else {
    RAY_CHECK(options_.log_dir.empty())
        << "log_dir must be empty because ray log is disabled.";
    RAY_CHECK(!options_.install_failure_signal_handler)
        << "install_failure_signal_handler must be false because ray log is disabled.";
  }

  RAY_LOG(INFO) << "Constructing CoreWorkerProcess. pid: " << getpid();

  // NOTE(kfstorm): any initialization depending on RayConfig must happen after this
  // line.
  InitializeSystemConfig();

  // Assume stats module will be initialized exactly once in once process.
  // So it must be called in CoreWorkerProcess constructor and will be reused
  // by all of core worker.
  // Initialize stats in core worker global tags.
  const ray::stats::TagsType global_tags = {
      {ray::stats::ComponentKey, "core_worker"},
      {ray::stats::WorkerIdKey, worker_id_.Hex()},
      {ray::stats::VersionKey, kRayVersion},
      {ray::stats::NodeAddressKey, options_.node_ip_address},
      {ray::stats::SessionNameKey, options_.session_name}};

  // NOTE(lingxuan.zlx): We assume RayConfig is initialized before it's used.
  // RayConfig is generated in Java_io_ray_runtime_RayNativeRuntime_nativeInitialize
  // for java worker or in constructor of CoreWorker for python worker.

  // We need init stats before using it/spawning threads.
  stats::Init(global_tags, options_.metrics_agent_port, worker_id_);

  // Initialize event framework before starting up worker.
  if (RayConfig::instance().event_log_reporter_enabled() && !options_.log_dir.empty()) {
    const std::vector<SourceTypeVariant> source_types = {
        ray::rpc::Event_SourceType::Event_SourceType_CORE_WORKER,
        ray::rpc::ExportEvent_SourceType::ExportEvent_SourceType_EXPORT_TASK};
    RayEventInit(source_types,
                 /*custom_fields=*/{},
                 options_.log_dir,
                 RayConfig::instance().event_level(),
                 RayConfig::instance().emit_event_to_log_file());
  }

  {
    // Notify that core worker is initialized.
    absl::Cleanup initialzed_scope_guard = [this] {
      service_handler_->SetCoreWorker(this->GetCoreWorker().get());
    };
    // Initialize global worker instance.
    auto worker = CreateCoreWorker(options_, worker_id_);
    auto write_locked = core_worker_.LockForWrite();
    write_locked.Get() = worker;
    // Initialize metrics agent client.
    metrics_agent_client_ = std::make_unique<ray::rpc::MetricsAgentClientImpl>(
        "127.0.0.1",
        options_.metrics_agent_port,
        io_service_,
        *write_locked.Get()->client_call_manager_);
    metrics_agent_client_->WaitForServerReady([this](const Status &server_status) {
      stats::InitOpenTelemetryExporter(options_.metrics_agent_port, server_status);
    });
  }
}

CoreWorkerProcessImpl::~CoreWorkerProcessImpl() {
  RAY_LOG(INFO) << "Destructing CoreWorkerProcessImpl. pid: " << getpid();
  // Shutdown stats module if worker process exits.
  stats::Shutdown();
  if (options_.enable_logging) {
    RayLog::ShutDownRayLog();
  }
}

void CoreWorkerProcess::EnsureInitialized(bool quick_exit) {
  if (core_worker_process != nullptr) {
    return;
  }

  if (quick_exit) {
    RAY_LOG(WARNING) << "The core worker process is not initialized yet or already "
                     << "shutdown.";
    QuickExit();
  } else {
    RAY_CHECK(core_worker_process)
        << "The core worker process is not initialized yet or already "
        << "shutdown.";
  }
}

void CoreWorkerProcessImpl::InitializeSystemConfig() {
  // We have to create a short-time thread here because the RPC request to get the
  // system config from Raylet is asynchronous, and we need to synchronously initialize
  // the system config in the constructor of `CoreWorkerProcessImpl`.
  std::promise<std::string> promise;
  std::thread thread([&] {
    instrumented_io_context io_service{/*emit_metrics=*/false,
                                       /*running_on_single_thread=*/true};
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work(
        io_service.get_executor());
    rpc::ClientCallManager client_call_manager(
        io_service, /*record_stats=*/false, options_.node_ip_address);
    rpc::Address raylet_address = rpc::RayletClientPool::GenerateRayletAddress(
        NodeID::Nil(), options_.node_ip_address, options_.node_manager_port);
    // TODO(joshlee): This local raylet client has a custom retry policy below since its
    // likely the driver can start up before the raylet is ready. We want to move away
    // from this and will be fixed in https://github.com/ray-project/ray/issues/55200
    rpc::RayletClient local_raylet_rpc_client(raylet_address, client_call_manager, [] {});

    std::function<void(int64_t)> get_once = [this,
                                             &get_once,
                                             &local_raylet_rpc_client,
                                             &promise,
                                             &io_service](int64_t num_attempts) {
      local_raylet_rpc_client.GetSystemConfig(
          [this, num_attempts, &get_once, &promise, &io_service](
              const Status &status, const rpc::GetSystemConfigReply &reply) {
            RAY_LOG(DEBUG) << "Getting system config from raylet, remaining retries = "
                           << num_attempts;
            if (status.ok()) {
              promise.set_value(reply.system_config());
              io_service.stop();
              return;
            }

            if (num_attempts > 1) {
              std::this_thread::sleep_for(std::chrono::milliseconds(
                  RayConfig::instance().raylet_client_connect_timeout_milliseconds()));
              get_once(num_attempts - 1);
              return;
            }

            // If there's no more attempt to try.
            if (status.IsRpcError() &&
                status.rpc_code() == grpc::StatusCode::UNAVAILABLE) {
              std::ostringstream ss;
              ss << "Failed to get the system config from raylet because "
                 << "it is dead. Worker will terminate. Status: " << status
                 << " .Please see `raylet.out` for more details.";
              if (options_.worker_type == WorkerType::DRIVER) {
                // If it is the driver, surface the issue to the user.
                RAY_LOG(ERROR) << ss.str();
              } else {
                RAY_LOG(WARNING) << ss.str();
              }
              QuickExit();
            }

            // Unexpected.
            RAY_LOG(FATAL)
                << "Failed to get the system config from Raylet on time unexpectedly."
                << status;
          });
    };

    get_once(RayConfig::instance().raylet_client_num_connect_attempts());
    io_service.run();
  });
  thread.join();

  RayConfig::instance().initialize(promise.get_future().get());
  ray::asio::testing::Init();
  ray::rpc::testing::Init();
}

void CoreWorkerProcessImpl::RunWorkerTaskExecutionLoop() {
  RAY_CHECK(options_.worker_type == WorkerType::WORKER);
  auto core_worker = GetCoreWorker();
  RAY_CHECK(core_worker != nullptr);
  core_worker->RunTaskExecutionLoop();
  RAY_LOG(INFO) << "Task execution loop terminated. Removing the global worker.";
  {
    auto write_locked = core_worker_.LockForWrite();
    write_locked.Get().reset();
  }
}

void CoreWorkerProcessImpl::ShutdownDriver() {
  RAY_CHECK(options_.worker_type == WorkerType::DRIVER)
      << "The `Shutdown` interface is for driver only.";
  auto global_worker = GetCoreWorker();
  RAY_CHECK(global_worker);
  global_worker->Disconnect(/*exit_type*/ rpc::WorkerExitType::INTENDED_USER_EXIT,
                            /*exit_detail*/ "Shutdown by ray.shutdown().");
  global_worker->Shutdown();
  {
    auto write_locked = core_worker_.LockForWrite();
    write_locked.Get().reset();
  }
}

std::shared_ptr<CoreWorker> CoreWorkerProcessImpl::TryGetCoreWorker() const {
  const auto read_locked = core_worker_.LockForRead();
  return read_locked.Get();
}

std::shared_ptr<CoreWorker> CoreWorkerProcessImpl::GetCoreWorker() const {
  const auto read_locked = core_worker_.LockForRead();
  if (!read_locked.Get()) {
    // This could only happen when the worker has already been shutdown.
    // In this case, we should exit without crashing.
    // TODO(scv119): A better solution could be returning error code
    // and handling it at language frontend.
    if (options_.worker_type == WorkerType::DRIVER) {
      RAY_LOG(ERROR) << "The core worker has already been shutdown. This happens when "
                        "the language frontend accesses the Ray's worker after it is "
                        "shutdown. The process will exit";
    } else {
      RAY_LOG(INFO) << "The core worker has already been shutdown. This happens when "
                       "the language frontend accesses the Ray's worker after it is "
                       "shutdown. The process will exit";
    }
    QuickExit();
  }
  RAY_CHECK(read_locked.Get()) << "core_worker_ must not be NULL";
  return read_locked.Get();
}

}  // namespace core

}  // namespace ray
