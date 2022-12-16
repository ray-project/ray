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

#ifndef _WIN32
#include <unistd.h>
#endif

#include <google/protobuf/util/json_util.h>

#include "boost/fiber/all.hpp"
#include "ray/common/bundle_spec.h"
#include "ray/common/ray_config.h"
#include "ray/common/runtime_env_common.h"
#include "ray/common/task/task_util.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/gcs/gcs_client/gcs_client.h"
#include "ray/gcs/pb_util.h"
#include "ray/stats/metric_defs.h"
#include "ray/stats/stats.h"
#include "ray/util/event.h"
#include "ray/util/util.h"

namespace ray {
namespace core {

JobID GetProcessJobID(const CoreWorkerOptions &options) {
  if (options.worker_type == WorkerType::DRIVER) {
    RAY_CHECK(!options.job_id.IsNil());
  } else {
    RAY_CHECK(options.job_id.IsNil());
  }

  if (options.worker_type == WorkerType::WORKER) {
    // For workers, the job ID is assigned by Raylet via an environment variable.
    const std::string &job_id_env = RayConfig::instance().JOB_ID();
    RAY_CHECK(!job_id_env.empty());
    return JobID::FromHex(job_id_env);
  }
  return options.job_id;
}

namespace {

// Implements setting the transient RUNNING_IN_RAY_GET and RUNNING_IN_RAY_WAIT states.
// These states override the RUNNING state of a task.
class ScopedTaskMetricSetter {
 public:
  ScopedTaskMetricSetter(const WorkerContext &ctx,
                         TaskCounter &ctr,
                         rpc::TaskStatus status)
      : status_(status), ctr_(ctr) {
    auto task_spec = ctx.GetCurrentTask();
    if (task_spec != nullptr) {
      task_name_ = task_spec->GetName();
      is_retry_ = task_spec->IsRetry();
    } else {
      task_name_ = "Unknown task";
      is_retry_ = false;
    }
    ctr_.SetMetricStatus(task_name_, status, is_retry_);
  }

  ~ScopedTaskMetricSetter() { ctr_.UnsetMetricStatus(task_name_, status_, is_retry_); }

 private:
  rpc::TaskStatus status_;
  TaskCounter &ctr_;
  std::string task_name_;
  bool is_retry_;
};

using ActorLifetime = ray::rpc::JobConfig_ActorLifetime;

// Helper function converts GetObjectLocationsOwnerReply to ObjectLocation
ObjectLocation CreateObjectLocation(const rpc::GetObjectLocationsOwnerReply &reply) {
  std::vector<NodeID> node_ids;
  const auto &object_info = reply.object_location_info();
  node_ids.reserve(object_info.node_ids_size());
  for (auto i = 0; i < object_info.node_ids_size(); i++) {
    node_ids.push_back(NodeID::FromBinary(object_info.node_ids(i)));
  }
  bool is_spilled = !object_info.spilled_url().empty();
  return ObjectLocation(NodeID::FromBinary(object_info.primary_node_id()),
                        object_info.object_size(),
                        std::move(node_ids),
                        is_spilled,
                        object_info.spilled_url(),
                        NodeID::FromBinary(object_info.spilled_node_id()));
}
}  // namespace

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
      resource_ids_(new ResourceMappingType()),
      grpc_service_(io_service_, *this),
      task_execution_service_work_(task_execution_service_) {
  RAY_LOG(DEBUG) << "Constructing CoreWorker, worker_id: " << worker_id;

  // Initialize task receivers.
  if (options_.worker_type == WorkerType::WORKER || options_.is_local_mode) {
    RAY_CHECK(options_.task_execution_callback != nullptr);
    auto execute_task = std::bind(&CoreWorker::ExecuteTask,
                                  this,
                                  std::placeholders::_1,
                                  std::placeholders::_2,
                                  std::placeholders::_3,
                                  std::placeholders::_4,
                                  std::placeholders::_5,
                                  std::placeholders::_6,
                                  std::placeholders::_7);
    direct_task_receiver_ = std::make_unique<CoreWorkerDirectTaskReceiver>(
        worker_context_, task_execution_service_, execute_task, [this] {
          return local_raylet_client_->TaskDone();
        });
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

  if (options_.worker_type != WorkerType::DRIVER) {
    periodical_runner_.RunFnPeriodically(
        [this] { ExitIfParentRayletDies(); },
        RayConfig::instance().raylet_death_check_interval_milliseconds());
  }

  // Start the IO thread first to make sure the checker is working.
  io_thread_ = std::thread([this]() { RunIOService(); });

  Status raylet_client_status;
  NodeID local_raylet_id;
  int assigned_port;
  std::string serialized_job_config = options_.serialized_job_config;
  local_raylet_client_ =
      std::make_shared<raylet::RayletClient>(io_service_,
                                             std::move(grpc_client),
                                             options_.raylet_socket,
                                             GetWorkerID(),
                                             options_.worker_type,
                                             worker_context_.GetCurrentJobID(),
                                             options_.runtime_env_hash,
                                             options_.language,
                                             options_.node_ip_address,
                                             &raylet_client_status,
                                             &local_raylet_id,
                                             &assigned_port,
                                             &serialized_job_config,
                                             options_.startup_token,
                                             options_.entrypoint);

  if (!raylet_client_status.ok()) {
    // Avoid using FATAL log or RAY_CHECK here because they may create a core dump file.
    RAY_LOG(ERROR) << "Failed to register worker " << worker_id << " to Raylet. "
                   << raylet_client_status;
    QuickExit();
  }

  connected_ = true;

  RAY_CHECK(assigned_port >= 0);

  // Parse job config from serialized string.
  job_config_.reset(new rpc::JobConfig());
  job_config_->ParseFromString(serialized_job_config);
  auto job_serialized_runtime_env =
      job_config_->runtime_env_info().serialized_runtime_env();
  job_runtime_env_info_.reset(new rpc::RuntimeEnvInfo);
  *job_runtime_env_info_ = job_config_->runtime_env_info();
  if (!IsRuntimeEnvEmpty(job_serialized_runtime_env)) {
    job_runtime_env_.reset(new json());
    *job_runtime_env_ = json::parse(job_serialized_runtime_env);
  }

  // Start RPC server after all the task receivers are properly initialized and we have
  // our assigned port from the raylet.
  core_worker_server_ =
      std::make_unique<rpc::GrpcServer>(WorkerTypeString(options_.worker_type),
                                        assigned_port,
                                        options_.node_ip_address == "127.0.0.1");
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

  gcs_client_ = std::make_shared<gcs::GcsClient>(options_.gcs_options);

  RAY_CHECK_OK(gcs_client_->Connect(io_service_));
  RegisterToGcs();

  // Initialize profiler.
  profiler_ = std::make_shared<worker::Profiler>(
      worker_context_, options_.node_ip_address, io_service_, gcs_client_);

  // Initialize the task state event buffer.
  auto task_event_gcs_client = std::make_unique<gcs::GcsClient>(options_.gcs_options);
  task_event_buffer_ =
      std::make_unique<worker::TaskEventBufferImpl>(std::move(task_event_gcs_client));
  if (RayConfig::instance().task_events_report_interval_ms() > 0) {
    if (!task_event_buffer_->Start().ok()) {
      RAY_CHECK(!task_event_buffer_->Enabled()) << "TaskEventBuffer should be disabled.";
    }
  }

  core_worker_client_pool_ =
      std::make_shared<rpc::CoreWorkerClientPool>(*client_call_manager_);

  object_info_publisher_ = std::make_unique<pubsub::Publisher>(
      /*channels=*/std::vector<
          rpc::ChannelType>{rpc::ChannelType::WORKER_OBJECT_EVICTION,
                            rpc::ChannelType::WORKER_REF_REMOVED_CHANNEL,
                            rpc::ChannelType::WORKER_OBJECT_LOCATIONS_CHANNEL},
      /*periodical_runner=*/&periodical_runner_,
      /*get_time_ms=*/[]() { return absl::GetCurrentTimeNanos() / 1e6; },
      /*subscriber_timeout_ms=*/RayConfig::instance().subscriber_timeout_ms(),
      /*publish_batch_size_=*/RayConfig::instance().publish_batch_size());
  object_info_subscriber_ = std::make_unique<pubsub::Subscriber>(
      /*subscriber_id=*/GetWorkerID(),
      /*channels=*/
      std::vector<rpc::ChannelType>{rpc::ChannelType::WORKER_OBJECT_EVICTION,
                                    rpc::ChannelType::WORKER_REF_REMOVED_CHANNEL,
                                    rpc::ChannelType::WORKER_OBJECT_LOCATIONS_CHANNEL},
      /*max_command_batch_size*/ RayConfig::instance().max_command_batch_size(),
      /*get_client=*/
      [this](const rpc::Address &address) {
        return core_worker_client_pool_->GetOrConnect(address);
      },
      /*callback_service*/ &io_service_);

  auto check_node_alive_fn = [this](const NodeID &node_id) {
    auto node = gcs_client_->Nodes().Get(node_id);
    return node != nullptr;
  };
  reference_counter_ = std::make_shared<ReferenceCounter>(
      rpc_address_,
      /*object_info_publisher=*/object_info_publisher_.get(),
      /*object_info_subscriber=*/object_info_subscriber_.get(),
      check_node_alive_fn,
      RayConfig::instance().lineage_pinning_enabled(),
      [this](const rpc::Address &addr) {
        return std::shared_ptr<rpc::CoreWorkerClient>(
            new rpc::CoreWorkerClient(addr, *client_call_manager_));
      });

  // Register a callback to monitor removed nodes.
  // Note we capture a shared ownership of reference_counter_
  // here to avoid destruction order fiasco between gcs_client and reference_counter_.
  auto on_node_change = [reference_counter = this->reference_counter_](
                            const NodeID &node_id, const rpc::GcsNodeInfo &data) {
    if (data.state() == rpc::GcsNodeInfo::DEAD) {
      RAY_LOG(INFO) << "Node failure from " << node_id
                    << ". All objects pinned on that node will be lost if object "
                       "reconstruction is not enabled.";
      reference_counter->ResetObjectsOnRemovedNode(node_id);
    }
  };
  RAY_CHECK_OK(gcs_client_->Nodes().AsyncSubscribeToNodeChange(on_node_change, nullptr));

  plasma_store_provider_.reset(new CoreWorkerPlasmaStoreProvider(
      options_.store_socket,
      local_raylet_client_,
      reference_counter_,
      options_.check_signals,
      /*warmup=*/
      (options_.worker_type != WorkerType::SPILL_WORKER &&
       options_.worker_type != WorkerType::RESTORE_WORKER),
      /*get_current_call_site=*/boost::bind(&CoreWorker::CurrentCallSite, this)));
  memory_store_.reset(new CoreWorkerMemoryStore(
      reference_counter_,
      local_raylet_client_,
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

  auto push_error_callback = [this](const JobID &job_id,
                                    const std::string &type,
                                    const std::string &error_message,
                                    double timestamp) {
    return PushError(job_id, type, error_message, timestamp);
  };
  task_manager_.reset(new TaskManager(
      memory_store_,
      reference_counter_,
      /*put_in_local_plasma_callback=*/
      [this](const RayObject &object, const ObjectID &object_id) {
        RAY_CHECK_OK(PutInLocalPlasmaStore(object, object_id, /*pin_object=*/true));
      },
      /* retry_task_callback= */
      [this](TaskSpecification &spec, bool object_recovery, uint32_t delay_ms) {
        spec.GetMutableMessage().set_attempt_number(spec.AttemptNumber() + 1);
        if (!object_recovery) {
          // Retry after a delay to emulate the existing Raylet reconstruction
          // behaviour. TODO(ekl) backoff exponentially.
          RAY_LOG(INFO) << "Will resubmit task after a " << delay_ms
                        << "ms delay: " << spec.DebugString();
          absl::MutexLock lock(&mutex_);
          TaskToRetry task_to_retry{current_time_ms() + delay_ms, spec};
          to_resubmit_.push(std::move(task_to_retry));
        } else {
          if (spec.IsActorTask()) {
            auto actor_handle = actor_manager_->GetActorHandle(spec.ActorId());
            actor_handle->SetResubmittedActorTaskSpec(spec, spec.ActorDummyObject());
            RAY_CHECK_OK(direct_actor_submitter_->SubmitTask(spec));
          } else {
            RAY_CHECK_OK(direct_task_submitter_->SubmitTask(spec));
          }
        }
      },
      push_error_callback,
      RayConfig::instance().max_lineage_bytes(),
      *task_event_buffer_.get()));

  // Create an entry for the driver task in the task table. This task is
  // added immediately with status RUNNING. This allows us to push errors
  // related to this driver task back to the driver. For example, if the
  // driver creates an object that is later evicted, we should notify the
  // user that we're unable to reconstruct the object, since we cannot
  // rerun the driver.
  if (options_.worker_type == WorkerType::DRIVER) {
    TaskSpecBuilder builder;
    const TaskID task_id = TaskID::ForDriverTask(worker_context_.GetCurrentJobID());
    builder.SetDriverTaskSpec(task_id,
                              options_.language,
                              worker_context_.GetCurrentJobID(),
                              TaskID::ComputeDriverTaskId(worker_context_.GetWorkerID()),
                              GetCallerId(),
                              rpc_address_);
    // Drivers are never re-executed.
    SetCurrentTaskId(task_id, /*attempt_number=*/0, "driver");
  }

  auto raylet_client_factory = [this](const std::string ip_address, int port) {
    auto grpc_client =
        rpc::NodeManagerWorkerClient::make(ip_address, port, *client_call_manager_);
    return std::shared_ptr<raylet::RayletClient>(
        new raylet::RayletClient(std::move(grpc_client)));
  };

  auto on_excess_queueing = [this](const ActorID &actor_id, uint64_t num_queued) {
    auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
    std::ostringstream stream;
    stream << "Warning: More than " << num_queued
           << " tasks are pending submission to actor " << actor_id
           << ". To reduce memory usage, wait for these tasks to finish before sending "
              "more.";
    RAY_CHECK_OK(
        PushError(options_.job_id, "excess_queueing_warning", stream.str(), timestamp));
  };

  actor_creator_ = std::make_shared<DefaultActorCreator>(gcs_client_);

  direct_actor_submitter_ = std::shared_ptr<CoreWorkerDirectActorTaskSubmitter>(
      new CoreWorkerDirectActorTaskSubmitter(*core_worker_client_pool_,
                                             *memory_store_,
                                             *task_manager_,
                                             *actor_creator_,
                                             on_excess_queueing,
                                             io_service_));

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
      rpc_address_,
      local_raylet_client_,
      core_worker_client_pool_,
      raylet_client_factory,
      std::move(lease_policy),
      memory_store_,
      task_manager_,
      local_raylet_id,
      GetWorkerType(),
      RayConfig::instance().worker_lease_timeout_milliseconds(),
      actor_creator_,
      worker_context_.GetCurrentJobID(),
      boost::asio::steady_timer(io_service_),
      RayConfig::instance().max_pending_lease_requests_per_scheduling_category());
  auto report_locality_data_callback = [this](
                                           const ObjectID &object_id,
                                           const absl::flat_hash_set<NodeID> &locations,
                                           uint64_t object_size) {
    reference_counter_->ReportLocalityData(object_id, locations, object_size);
  };
  future_resolver_.reset(new FutureResolver(memory_store_,
                                            reference_counter_,
                                            std::move(report_locality_data_callback),
                                            core_worker_client_pool_,
                                            rpc_address_));

  // Unfortunately the raylet client has to be constructed after the receivers.
  if (direct_task_receiver_ != nullptr) {
    task_argument_waiter_.reset(new DependencyWaiterImpl(*local_raylet_client_));
    direct_task_receiver_->Init(
        core_worker_client_pool_, rpc_address_, task_argument_waiter_);
  }

  actor_manager_ = std::make_unique<ActorManager>(
      gcs_client_, direct_actor_submitter_, reference_counter_);

  std::function<Status(const ObjectID &object_id, const ObjectLookupCallback &callback)>
      object_lookup_fn;

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
  object_recovery_manager_ = std::make_unique<ObjectRecoveryManager>(
      rpc_address_,
      raylet_client_factory,
      local_raylet_client_,
      object_lookup_fn,
      task_manager_,
      reference_counter_,
      memory_store_,
      [this](const ObjectID &object_id, rpc::ErrorType reason, bool pin_object) {
        RAY_LOG(DEBUG) << "Failed to recover object " << object_id << " due to "
                       << rpc::ErrorType_Name(reason);
        // NOTE(swang): Failure here means the local raylet is probably dead.
        // We do not assert failure though, because we should throw the object
        // error to the application.
        RAY_UNUSED(Put(RayObject(reason),
                       /*contained_object_ids=*/{},
                       object_id,
                       /*pin_object=*/pin_object));
      });

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
  const auto event_stats_print_interval_ms =
      RayConfig::instance().event_stats_print_interval_ms();
  if (event_stats_print_interval_ms != -1 && RayConfig::instance().event_stats()) {
    periodical_runner_.RunFnPeriodically(
        [this] {
          RAY_LOG(INFO) << "Event stats:\n\n"
                        << io_service_.stats().StatsString() << "\n\n";
        },
        event_stats_print_interval_ms);
  }

  // Set event context for current core worker thread.
  RayEventContext::Instance().SetEventContext(
      ray::rpc::Event_SourceType::Event_SourceType_CORE_WORKER,
      {{"worker_id", worker_id.Hex()}});

  periodical_runner_.RunFnPeriodically(
      [this] {
        const auto lost_objects = reference_counter_->FlushObjectsToRecover();
        if (!lost_objects.empty()) {
          // Keep :info_message: in sync with LOG_PREFIX_INFO_MESSAGE in ray_constants.py.
          RAY_LOG(ERROR) << ":info_message: Attempting to recover " << lost_objects.size()
                         << " lost objects by resubmitting their tasks. To disable "
                         << "object reconstruction, set @ray.remote(max_retries=0).";
          // Delete the objects from the in-memory store to indicate that they are not
          // available. The object recovery manager will guarantee that a new value
          // will eventually be stored for the objects (either an
          // UnreconstructableError or a value reconstructed from lineage).
          memory_store_->Delete(lost_objects);
          for (const auto &object_id : lost_objects) {
            // NOTE(swang): There is a race condition where this can return false if
            // the reference went out of scope since the call to the ref counter to get
            // the lost objects. It's okay to not mark the object as failed or recover
            // the object since there are no reference holders.
            RAY_UNUSED(object_recovery_manager_->RecoverObject(object_id));
          }
        }
      },
      100);

  periodical_runner_.RunFnPeriodically(
      [this] { InternalHeartbeat(); },
      RayConfig::instance().core_worker_internal_heartbeat_ms());

  periodical_runner_.RunFnPeriodically(
      [this] { RecordMetrics(); },
      RayConfig::instance().metrics_report_interval_ms() / 2);

#ifndef _WIN32
  // Doing this last during CoreWorker initialization, so initialization logic like
  // registering with Raylet can finish with higher priority.
  static const bool niced = [this]() {
    if (options_.worker_type != WorkerType::DRIVER) {
      const auto niceness = nice(RayConfig::instance().worker_niceness());
      RAY_LOG(INFO) << "Adjusted worker niceness to " << niceness;
      return true;
    }
    return false;
  }();
  // Verify driver and worker are never mixed in the same process.
  RAY_CHECK_EQ(options_.worker_type != WorkerType::DRIVER, niced);
#endif
}

CoreWorker::~CoreWorker() { RAY_LOG(INFO) << "Core worker is destructed"; }

void CoreWorker::Shutdown() {
  if (is_shutdown_) {
    RAY_LOG(INFO)
        << "Shutdown request has received although the core worker is already shutdown.";
    return;
  }

  RAY_LOG(INFO) << "Shutting down a core worker.";
  is_shutdown_ = true;
  if (options_.worker_type == WorkerType::WORKER) {
    // Running in a main thread.
    // Asyncio coroutines could still run after CoreWorker is removed because it is
    // running in a different thread. This can cause segfault because coroutines try to
    // access CoreWorker methods that are already garbage collected. We should complete
    // all coroutines before shutting down in order to prevent this.
    if (worker_context_.CurrentActorIsAsync()) {
      options_.terminate_asyncio_thread();
    }
    direct_task_receiver_->Stop();
    task_execution_service_.stop();
  }
  if (options_.on_worker_shutdown) {
    // Running in a main thread.
    options_.on_worker_shutdown(GetWorkerID());
  }

  task_event_buffer_->Stop();

  if (gcs_client_) {
    // We should disconnect gcs client first otherwise because it contains
    // a blocking logic that can block the io service upon
    // gcs shutdown.
    // TODO(sang): Refactor GCS client to be more robust.
    RAY_LOG(INFO) << "Disconnecting a GCS client.";
    gcs_client_->Disconnect();
  }
  io_service_.stop();
  RAY_LOG(INFO) << "Waiting for joining a core worker io thread. If it hangs here, there "
                   "might be deadlock or a high load in the core worker io service.";
  if (io_thread_.joinable()) {
    io_thread_.join();
  }

  // Shutdown gRPC server
  core_worker_server_->Shutdown();

  // Now that gcs_client is not used within io service, we can reset the pointer and clean
  // it up.
  gcs_client_.reset();

  RAY_LOG(INFO) << "Core worker ready to be deallocated.";
}

void CoreWorker::ConnectToRaylet() {
  RAY_CHECK(!options_.connect_on_start);
  // Tell the raylet the port that we are listening on.
  // NOTE: This also marks the worker as available in Raylet. We do this at the
  // very end in case there is a problem during construction.
  RAY_CHECK_OK(local_raylet_client_->AnnounceWorkerPort(core_worker_server_->GetPort()));
}

void CoreWorker::Disconnect(
    const rpc::WorkerExitType &exit_type,
    const std::string &exit_detail,
    const std::shared_ptr<LocalMemoryBuffer> &creation_task_exception_pb_bytes) {
  // Force stats export before exiting the worker.
  RecordMetrics();

  // Force task state events push before exiting the worker.
  task_event_buffer_->FlushEvents(/* forced */ true);

  opencensus::stats::StatsExporter::ExportNow();
  if (connected_) {
    RAY_LOG(INFO) << "Disconnecting to the raylet.";
    connected_ = false;
    if (local_raylet_client_) {
      RAY_IGNORE_EXPR(local_raylet_client_->Disconnect(
          exit_type, exit_detail, creation_task_exception_pb_bytes));
    }
  }
}

void CoreWorker::Exit(
    const rpc::WorkerExitType exit_type,
    const std::string &detail,
    const std::shared_ptr<LocalMemoryBuffer> &creation_task_exception_pb_bytes) {
  RAY_LOG(INFO) << "Exit signal received, this process will exit after all outstanding "
                   "tasks have finished"
                << ", exit_type=" << rpc::WorkerExitType_Name(exit_type)
                << ", detail=" << detail;
  exiting_ = true;
  // Release the resources early in case draining takes a long time.
  RAY_CHECK_OK(
      local_raylet_client_->NotifyDirectCallTaskBlocked(/*release_resources*/ true));

  RAY_LOG(DEBUG) << "Exit signal received, remove all local references.";
  /// Since this core worker is exiting, it's necessary to release all local references,
  /// otherwise the frontend code may not release its references and this worker will be
  /// leaked. See https://github.com/ray-project/ray/issues/19639.
  reference_counter_->ReleaseAllLocalReferences();

  // Callback to shutdown.
  auto shutdown = [this, exit_type, detail, creation_task_exception_pb_bytes]() {
    // To avoid problems, make sure shutdown is always called from the same
    // event loop each time.
    task_execution_service_.post(
        [this,
         exit_type,
         detail = std::move(detail),
         creation_task_exception_pb_bytes]() {
          rpc::DrainAndResetServerCallExecutor();
          Disconnect(exit_type, detail, creation_task_exception_pb_bytes);
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

void CoreWorker::ForceExit(const rpc::WorkerExitType exit_type,
                           const std::string &detail) {
  RAY_LOG(WARNING) << "Force exit the process. "
                   << " Details: " << detail;
  Disconnect(exit_type, detail);
  // NOTE(hchen): Use `QuickExit()` to force-exit this process without doing cleanup.
  // `exit()` will destruct static objects in an incorrect order, which will lead to
  // core dumps.
  QuickExit();
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
  RAY_LOG(INFO) << "Core worker main io service stopped.";
}

const WorkerID &CoreWorker::GetWorkerID() const { return worker_context_.GetWorkerID(); }

void CoreWorker::SetCurrentTaskId(const TaskID &task_id,
                                  uint64_t attempt_number,
                                  const std::string &task_name) {
  worker_context_.SetCurrentTaskId(task_id, attempt_number);
  {
    absl::MutexLock lock(&mutex_);
    main_thread_task_id_ = task_id;
    main_thread_task_name_ = task_name;
  }
}

void CoreWorker::RegisterToGcs() {
  absl::flat_hash_map<std::string, std::string> worker_info;
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
  worker_data->set_pid(getpid());

  RAY_CHECK_OK(gcs_client_->Workers().AsyncAdd(worker_data, nullptr));
}

void CoreWorker::ExitIfParentRayletDies() {
  RAY_CHECK(!RayConfig::instance().RAYLET_PID().empty());
  static auto raylet_pid =
      static_cast<pid_t>(std::stoi(RayConfig::instance().RAYLET_PID()));
  bool should_shutdown = !IsProcessAlive(raylet_pid);
  if (should_shutdown) {
    RAY_LOG(WARNING) << "Shutting down the core worker because the local raylet failed. "
                     << "Check out the raylet.out log file. Raylet pid: " << raylet_pid;
    QuickExit();
  }
}

void CoreWorker::InternalHeartbeat() {
  // Retry tasks.
  std::vector<TaskSpecification> tasks_to_resubmit;
  {
    absl::MutexLock lock(&mutex_);
    while (!to_resubmit_.empty() &&
           current_time_ms() > to_resubmit_.top().execution_time_ms) {
      tasks_to_resubmit.push_back(std::move(to_resubmit_.top().task_spec));
      to_resubmit_.pop();
    }
  }

  for (auto &spec : tasks_to_resubmit) {
    if (spec.IsActorTask()) {
      RAY_CHECK_OK(direct_actor_submitter_->SubmitTask(spec));
    } else {
      RAY_CHECK_OK(direct_task_submitter_->SubmitTask(spec));
    }
  }

  // Check timeout tasks that are waiting for death info.
  if (direct_actor_submitter_ != nullptr) {
    direct_actor_submitter_->CheckTimeoutTasks();
  }

  // Periodically report the lastest backlog so that
  // local raylet will have the eventually consistent view of worker backlogs
  // even in cases where backlog reports from direct_task_transport
  // are lost or reordered.
  direct_task_submitter_->ReportWorkerBacklog();

  // Check for unhandled exceptions to raise after a timeout on the driver.
  // Only do this for TTY, since shells like IPython sometimes save references
  // to the result and prevent normal result deletion from handling.
  // See also: https://github.com/ray-project/ray/issues/14485
  if (options_.worker_type == WorkerType::DRIVER && options_.interactive) {
    memory_store_->NotifyUnhandledErrors();
  }
}

void CoreWorker::RecordMetrics() {
  // Record metrics for owned tasks.
  task_manager_->RecordMetrics();
  // Record metrics for executed tasks.
  task_counter_.RecordMetrics();
  // Record worker heap memory metrics.
  memory_store_->RecordMetrics();
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

const rpc::Address &CoreWorker::GetRpcAddress() const { return rpc_address_; }

rpc::Address CoreWorker::GetOwnerAddress(const ObjectID &object_id) const {
  rpc::Address owner_address;
  auto has_owner = reference_counter_->GetOwner(object_id, &owner_address);
  RAY_CHECK(has_owner)
      << "Object IDs generated randomly (ObjectID.from_random()) or out-of-band "
         "(ObjectID.from_binary(...)) cannot be passed as a task argument because Ray "
         "does not know which task created them. "
         "If this was not how your object ID was generated, please file an issue "
         "at https://github.com/ray-project/ray/issues/";
  return owner_address;
}

std::vector<rpc::ObjectReference> CoreWorker::GetObjectRefs(
    const std::vector<ObjectID> &object_ids) const {
  std::vector<rpc::ObjectReference> refs;
  for (const auto &object_id : object_ids) {
    rpc::ObjectReference ref;
    ref.set_object_id(object_id.Binary());
    rpc::Address owner_address;
    if (reference_counter_->GetOwner(object_id, &owner_address)) {
      // NOTE(swang): Detached actors do not have an owner address set.
      ref.mutable_owner_address()->CopyFrom(owner_address);
    }
    refs.push_back(std::move(ref));
  }
  return refs;
}

void CoreWorker::GetOwnershipInfo(const ObjectID &object_id,
                                  rpc::Address *owner_address,
                                  std::string *serialized_object_status) {
  auto has_owner = reference_counter_->GetOwner(object_id, owner_address);
  RAY_CHECK(has_owner)
      << "Object IDs generated randomly (ObjectID.from_random()) or out-of-band "
         "(ObjectID.from_binary(...)) cannot be serialized because Ray does not know "
         "which task will create them. "
         "If this was not how your object ID was generated, please file an issue "
         "at https://github.com/ray-project/ray/issues/: "
      << object_id;

  rpc::GetObjectStatusReply object_status;
  // Optimization: if the object exists, serialize and inline its status. This also
  // resolves some race conditions in resource release (#16025).
  if (RayConfig::instance().inline_object_status_in_refs()) {
    auto existing_object = memory_store_->GetIfExists(object_id);
    if (existing_object != nullptr) {
      PopulateObjectStatus(object_id, existing_object, &object_status);
    }
  }
  *serialized_object_status = object_status.SerializeAsString();
}

void CoreWorker::RegisterOwnershipInfoAndResolveFuture(
    const ObjectID &object_id,
    const ObjectID &outer_object_id,
    const rpc::Address &owner_address,
    const std::string &serialized_object_status) {
  // Add the object's owner to the local metadata in case it gets serialized
  // again.
  reference_counter_->AddBorrowedObject(object_id, outer_object_id, owner_address);

  rpc::GetObjectStatusReply object_status;
  object_status.ParseFromString(serialized_object_status);

  if (object_status.has_object() && !reference_counter_->OwnedByUs(object_id)) {
    // We already have the inlined object status, process it immediately.
    future_resolver_->ProcessResolvedObject(
        object_id, owner_address, Status::OK(), object_status);
  } else {
    // We will ask the owner about the object until the object is
    // created or we can no longer reach the owner.
    future_resolver_->ResolveFutureAsync(object_id, owner_address);
  }
}

Status CoreWorker::Put(const RayObject &object,
                       const std::vector<ObjectID> &contained_object_ids,
                       ObjectID *object_id) {
  *object_id = ObjectID::FromIndex(worker_context_.GetCurrentInternalTaskId(),
                                   worker_context_.GetNextPutIndex());
  reference_counter_->AddOwnedObject(*object_id,
                                     contained_object_ids,
                                     rpc_address_,
                                     CurrentCallSite(),
                                     object.GetSize(),
                                     /*is_reconstructable=*/false,
                                     /*add_local_ref=*/true,
                                     NodeID::FromBinary(rpc_address_.raylet_id()));
  auto status = Put(object, contained_object_ids, *object_id, /*pin_object=*/true);
  if (!status.ok()) {
    RemoveLocalReference(*object_id);
  }
  return status;
}

Status CoreWorker::PutInLocalPlasmaStore(const RayObject &object,
                                         const ObjectID &object_id,
                                         bool pin_object) {
  bool object_exists;
  RAY_RETURN_NOT_OK(plasma_store_provider_->Put(
      object, object_id, /* owner_address = */ rpc_address_, &object_exists));
  if (!object_exists) {
    if (pin_object) {
      // Tell the raylet to pin the object **after** it is created.
      RAY_LOG(DEBUG) << "Pinning put object " << object_id;
      local_raylet_client_->PinObjectIDs(
          rpc_address_,
          {object_id},
          /*generator_id=*/ObjectID::Nil(),
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

Status CoreWorker::Put(const RayObject &object,
                       const std::vector<ObjectID> &contained_object_ids,
                       const ObjectID &object_id,
                       bool pin_object) {
  RAY_RETURN_NOT_OK(WaitForActorRegistered(contained_object_ids));
  if (options_.is_local_mode) {
    RAY_LOG(DEBUG) << "Put " << object_id << " in memory store";
    RAY_CHECK(memory_store_->Put(object, object_id));
    return Status::OK();
  }
  return PutInLocalPlasmaStore(object, object_id, pin_object);
}

Status CoreWorker::CreateOwnedAndIncrementLocalRef(
    const std::shared_ptr<Buffer> &metadata,
    const size_t data_size,
    const std::vector<ObjectID> &contained_object_ids,
    ObjectID *object_id,
    std::shared_ptr<Buffer> *data,
    bool created_by_worker,
    const std::unique_ptr<rpc::Address> &owner_address,
    bool inline_small_object) {
  auto status = WaitForActorRegistered(contained_object_ids);
  if (!status.ok()) {
    return status;
  }
  *object_id = ObjectID::FromIndex(worker_context_.GetCurrentInternalTaskId(),
                                   worker_context_.GetNextPutIndex());
  rpc::Address real_owner_address =
      owner_address != nullptr ? *owner_address : rpc_address_;
  bool owned_by_us = real_owner_address.worker_id() == rpc_address_.worker_id();
  if (owned_by_us) {
    reference_counter_->AddOwnedObject(*object_id,
                                       contained_object_ids,
                                       rpc_address_,
                                       CurrentCallSite(),
                                       data_size + metadata->Size(),
                                       /*is_reconstructable=*/false,
                                       /*add_local_ref=*/true,
                                       NodeID::FromBinary(rpc_address_.raylet_id()));
  } else {
    // Because in the remote worker's `HandleAssignObjectOwner`,
    // a `WaitForRefRemoved` RPC request will be sent back to
    // the current worker. So we need to make sure ref count is > 0
    // by invoking `AddLocalReference` first. Note that in worker.py we set
    // skip_adding_local_ref=True to avoid double referencing the object.
    AddLocalReference(*object_id);
    RAY_UNUSED(
        reference_counter_->AddBorrowedObject(*object_id,
                                              ObjectID::Nil(),
                                              real_owner_address,
                                              /*foreign_owner_already_monitoring=*/true));

    // Remote call `AssignObjectOwner()`.
    rpc::AssignObjectOwnerRequest request;
    request.set_object_id(object_id->Binary());
    request.mutable_borrower_address()->CopyFrom(rpc_address_);
    request.set_call_site(CurrentCallSite());

    for (auto &contained_object_id : contained_object_ids) {
      request.add_contained_object_ids(contained_object_id.Binary());
    }
    request.set_object_size(data_size + metadata->Size());
    auto conn = core_worker_client_pool_->GetOrConnect(real_owner_address);
    std::promise<Status> status_promise;
    conn->AssignObjectOwner(request,
                            [&status_promise](const Status &returned_status,
                                              const rpc::AssignObjectOwnerReply &reply) {
                              status_promise.set_value(returned_status);
                            });
    // Block until the remote call `AssignObjectOwner` returns.
    status = status_promise.get_future().get();
  }

  if (options_.is_local_mode && owned_by_us && inline_small_object) {
    *data = std::make_shared<LocalMemoryBuffer>(data_size);
  } else {
    if (status.ok()) {
      status = plasma_store_provider_->Create(metadata,
                                              data_size,
                                              *object_id,
                                              /* owner_address = */ real_owner_address,
                                              data,
                                              created_by_worker);
    }
    if (!status.ok()) {
      RemoveLocalReference(*object_id);
      return status;
    } else if (*data == nullptr) {
      // Object already exists in plasma. Store the in-memory value so that the
      // client will check the plasma store.
      RAY_CHECK(
          memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA), *object_id));
    }
  }
  return Status::OK();
}

Status CoreWorker::CreateExisting(const std::shared_ptr<Buffer> &metadata,
                                  const size_t data_size,
                                  const ObjectID &object_id,
                                  const rpc::Address &owner_address,
                                  std::shared_ptr<Buffer> *data,
                                  bool created_by_worker) {
  if (options_.is_local_mode) {
    return Status::NotImplemented(
        "Creating an object with a pre-existing ObjectID is not supported in local "
        "mode");
  } else {
    return plasma_store_provider_->Create(
        metadata, data_size, object_id, owner_address, data, created_by_worker);
  }
}

Status CoreWorker::SealOwned(const ObjectID &object_id,
                             bool pin_object,
                             const std::unique_ptr<rpc::Address> &owner_address) {
  auto status =
      SealExisting(object_id, pin_object, ObjectID::Nil(), std::move(owner_address));
  if (status.ok()) return status;
  RemoveLocalReference(object_id);
  if (reference_counter_->HasReference(object_id)) {
    RAY_LOG(WARNING)
        << "Object " << object_id
        << " failed to be put but has a nonzero ref count. This object may leak.";
  }
  return status;
}

Status CoreWorker::SealExisting(const ObjectID &object_id,
                                bool pin_object,
                                const ObjectID &generator_id,
                                const std::unique_ptr<rpc::Address> &owner_address) {
  RAY_RETURN_NOT_OK(plasma_store_provider_->Seal(object_id));
  if (pin_object) {
    // Tell the raylet to pin the object **after** it is created.
    RAY_LOG(DEBUG) << "Pinning sealed object " << object_id;
    local_raylet_client_->PinObjectIDs(
        owner_address != nullptr ? *owner_address : rpc_address_,
        {object_id},
        generator_id,
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

Status CoreWorker::Get(const std::vector<ObjectID> &ids,
                       const int64_t timeout_ms,
                       std::vector<std::shared_ptr<RayObject>> *results) {
  ScopedTaskMetricSetter state(
      worker_context_, task_counter_, rpc::TaskStatus::RUNNING_IN_RAY_GET);
  results->resize(ids.size(), nullptr);

  absl::flat_hash_set<ObjectID> plasma_object_ids;
  absl::flat_hash_set<ObjectID> memory_object_ids(ids.begin(), ids.end());

  bool got_exception = false;
  absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> result_map;
  auto start_time = current_time_ms();

  if (!memory_object_ids.empty()) {
    RAY_RETURN_NOT_OK(memory_store_->Get(
        memory_object_ids, timeout_ms, worker_context_, &result_map, &got_exception));
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
    RAY_RETURN_NOT_OK(plasma_store_provider_->Get(plasma_object_ids,
                                                  local_timeout_ms,
                                                  worker_context_,
                                                  &result_map,
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

Status CoreWorker::Contains(const ObjectID &object_id,
                            bool *has_object,
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
    auto found = memory_store->GetIfExists(mem_id);
    if (found != nullptr && found->IsInPlasmaError()) {
      plasma_object_ids.insert(mem_id);
      ready.erase(mem_id);
      memory_object_ids.erase(current);
    }
  }
}

Status CoreWorker::Wait(const std::vector<ObjectID> &ids,
                        int num_objects,
                        int64_t timeout_ms,
                        std::vector<bool> *results,
                        bool fetch_local) {
  ScopedTaskMetricSetter state(
      worker_context_, task_counter_, rpc::TaskStatus::RUNNING_IN_RAY_WAIT);

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
      std::min(static_cast<int>(memory_object_ids.size()), num_objects),
      timeout_ms,
      worker_context_,
      &ready));
  RAY_CHECK(static_cast<int>(ready.size()) <= num_objects);
  if (timeout_ms > 0) {
    timeout_ms =
        std::max(0, static_cast<int>(timeout_ms - (current_time_ms() - start_time)));
  }
  if (fetch_local) {
    RetryObjectInPlasmaErrors(
        memory_store_, worker_context_, memory_object_ids, plasma_object_ids, ready);
    if (static_cast<int>(ready.size()) < num_objects && plasma_object_ids.size() > 0) {
      RAY_RETURN_NOT_OK(plasma_store_provider_->Wait(
          plasma_object_ids,
          std::min(static_cast<int>(plasma_object_ids.size()),
                   num_objects - static_cast<int>(ready.size())),
          timeout_ms,
          worker_context_,
          &ready));
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
    RAY_LOG(DEBUG) << "Freeing object " << object_id;
    RAY_CHECK(memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_FREED), object_id));
  }

  // We only delete from plasma, which avoids hangs (issue #7105). In-memory
  // objects can only be deleted once the ref count goes to 0.
  absl::flat_hash_set<ObjectID> plasma_object_ids(object_ids.begin(), object_ids.end());
  return plasma_store_provider_->Delete(plasma_object_ids, local_only);
}

Status CoreWorker::GetLocationFromOwner(
    const std::vector<ObjectID> &object_ids,
    int64_t timeout_ms,
    std::vector<std::shared_ptr<ObjectLocation>> *results) {
  results->resize(object_ids.size());
  if (object_ids.empty()) {
    return Status::OK();
  }

  auto mutex = std::make_shared<absl::Mutex>();
  auto num_remaining = std::make_shared<size_t>(object_ids.size());
  auto ready_promise = std::make_shared<std::promise<void>>();
  auto location_by_id =
      std::make_shared<absl::flat_hash_map<ObjectID, std::shared_ptr<ObjectLocation>>>();

  for (const auto &object_id : object_ids) {
    auto owner_address = GetOwnerAddress(object_id);
    auto client = core_worker_client_pool_->GetOrConnect(owner_address);
    rpc::GetObjectLocationsOwnerRequest request;
    auto object_location_request = request.mutable_object_location_request();
    object_location_request->set_intended_worker_id(owner_address.worker_id());
    object_location_request->set_object_id(object_id.Binary());
    client->GetObjectLocationsOwner(
        request,
        [object_id, mutex, num_remaining, ready_promise, location_by_id](
            const Status &status, const rpc::GetObjectLocationsOwnerReply &reply) {
          absl::MutexLock lock(mutex.get());
          if (status.ok()) {
            location_by_id->emplace(
                object_id, std::make_shared<ObjectLocation>(CreateObjectLocation(reply)));
          } else {
            RAY_LOG(WARNING) << "Failed to query location information for " << object_id
                             << " with error: " << status.ToString();
          }
          (*num_remaining)--;
          if (*num_remaining == 0) {
            ready_promise->set_value();
          }
        });
  }
  if (timeout_ms < 0) {
    ready_promise->get_future().wait();
  } else if (ready_promise->get_future().wait_for(
                 std::chrono::microseconds(timeout_ms)) != std::future_status::ready) {
    std::ostringstream stream;
    stream << "Failed querying object locations within " << timeout_ms
           << " milliseconds.";
    return Status::TimedOut(stream.str());
  }

  for (size_t i = 0; i < object_ids.size(); i++) {
    auto pair = location_by_id->find(object_ids[i]);
    if (pair == location_by_id->end()) {
      continue;
    }
    (*results)[i] = pair->second;
  }
  return Status::OK();
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

Status CoreWorker::PushError(const JobID &job_id,
                             const std::string &type,
                             const std::string &error_message,
                             double timestamp) {
  if (options_.is_local_mode) {
    RAY_LOG(ERROR) << "Pushed Error with JobID: " << job_id << " of type: " << type
                   << " with message: " << error_message << " at time: " << timestamp;
    return Status::OK();
  }
  return local_raylet_client_->PushError(job_id, type, error_message, timestamp);
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
  bool owned_by_us = false;
  NodeID pinned_at;
  bool spilled = false;
  RAY_CHECK(reference_counter_->IsPlasmaObjectPinnedOrSpilled(
      object_id, &owned_by_us, &pinned_at, &spilled));
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
  auto raylet_client = std::make_shared<raylet::RayletClient>(
      rpc::NodeManagerWorkerClient::make(node->node_manager_address(),
                                         node->node_manager_port(),
                                         *client_call_manager_));
  raylet_client->RequestObjectSpillage(
      object_id,
      [object_id, callback](const Status &status,
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

json CoreWorker::OverrideRuntimeEnv(json &child, const std::shared_ptr<json> parent) {
  // By default, the child runtime env inherits non-specified options from the
  // parent. There is one exception to this:
  //     - The env_vars dictionaries are merged, so environment variables
  //       not specified by the child are still inherited from the parent.
  json result_runtime_env = *parent;
  for (json::iterator it = child.begin(); it != child.end(); ++it) {
    if (it.key() == "env_vars" && result_runtime_env.contains("env_vars")) {
      json env_vars = it.value();
      json merged_env_vars = result_runtime_env["env_vars"];
      for (json::iterator nit = env_vars.begin(); nit != env_vars.end(); ++nit) {
        merged_env_vars[nit.key()] = nit.value();
      }
      result_runtime_env["env_vars"] = merged_env_vars;
    } else {
      result_runtime_env[it.key()] = it.value();
    }
  }
  return result_runtime_env;
}

std::shared_ptr<rpc::RuntimeEnvInfo> CoreWorker::OverrideTaskOrActorRuntimeEnvInfo(
    const std::string &serialized_runtime_env_info) {
  // TODO(Catch-Bull,SongGuyang): task runtime env not support the field eager_install
  // yet, we will overwrite the filed eager_install when it did.
  std::shared_ptr<json> parent = nullptr;
  std::shared_ptr<rpc::RuntimeEnvInfo> parent_runtime_env_info = nullptr;
  std::shared_ptr<rpc::RuntimeEnvInfo> runtime_env_info = nullptr;
  runtime_env_info.reset(new rpc::RuntimeEnvInfo());

  if (!IsRuntimeEnvInfoEmpty(serialized_runtime_env_info)) {
    RAY_CHECK(google::protobuf::util::JsonStringToMessage(serialized_runtime_env_info,
                                                          runtime_env_info.get())
                  .ok());
  }

  if (options_.worker_type == WorkerType::DRIVER) {
    if (IsRuntimeEnvEmpty(runtime_env_info->serialized_runtime_env())) {
      return job_runtime_env_info_;
    }
    parent = job_runtime_env_;
    parent_runtime_env_info = job_runtime_env_info_;
  } else {
    if (IsRuntimeEnvEmpty(runtime_env_info->serialized_runtime_env())) {
      return worker_context_.GetCurrentRuntimeEnvInfo();
    }
    parent = worker_context_.GetCurrentRuntimeEnv();
    parent_runtime_env_info = worker_context_.GetCurrentRuntimeEnvInfo();
  }
  if (parent) {
    std::string serialized_runtime_env = runtime_env_info->serialized_runtime_env();
    json child_runtime_env = json::parse(serialized_runtime_env);
    auto override_runtime_env = OverrideRuntimeEnv(child_runtime_env, parent);
    auto serialized_override_runtime_env = override_runtime_env.dump();
    runtime_env_info->set_serialized_runtime_env(serialized_override_runtime_env);
    if (runtime_env_info->uris().working_dir_uri().empty() &&
        !parent_runtime_env_info->uris().working_dir_uri().empty()) {
      runtime_env_info->mutable_uris()->set_working_dir_uri(
          parent_runtime_env_info->uris().working_dir_uri());
    }
    if (runtime_env_info->uris().py_modules_uris().size() == 0 &&
        parent_runtime_env_info->uris().py_modules_uris().size() != 0) {
      runtime_env_info->mutable_uris()->clear_py_modules_uris();
      for (const std::string &uri : parent_runtime_env_info->uris().py_modules_uris()) {
        runtime_env_info->mutable_uris()->add_py_modules_uris(uri);
      }
    }
    return runtime_env_info;
  } else {
    return runtime_env_info;
  }
}

void CoreWorker::BuildCommonTaskSpec(
    TaskSpecBuilder &builder,
    const JobID &job_id,
    const TaskID &task_id,
    const std::string &name,
    const TaskID &current_task_id,
    uint64_t task_index,
    const TaskID &caller_id,
    const rpc::Address &address,
    const RayFunction &function,
    const std::vector<std::unique_ptr<TaskArg>> &args,
    int64_t num_returns,
    const std::unordered_map<std::string, double> &required_resources,
    const std::unordered_map<std::string, double> &required_placement_resources,
    const std::string &debugger_breakpoint,
    int64_t depth,
    const std::string &serialized_runtime_env_info,
    const std::string &concurrency_group_name) {
  // Build common task spec.
  auto override_runtime_env_info =
      OverrideTaskOrActorRuntimeEnvInfo(serialized_runtime_env_info);

  bool returns_dynamic = num_returns == -1;
  if (returns_dynamic) {
    // This remote function returns 1 ObjectRef, whose value
    // is a generator of ObjectRefs.
    num_returns = 1;
  }
  RAY_CHECK(num_returns >= 0);
  builder.SetCommonTaskSpec(task_id,
                            name,
                            function.GetLanguage(),
                            function.GetFunctionDescriptor(),
                            job_id,
                            current_task_id,
                            task_index,
                            caller_id,
                            address,
                            num_returns,
                            returns_dynamic,
                            required_resources,
                            required_placement_resources,
                            debugger_breakpoint,
                            depth,
                            override_runtime_env_info,
                            concurrency_group_name);
  // Set task arguments.
  for (const auto &arg : args) {
    builder.AddArg(*arg);
  }
}

std::vector<rpc::ObjectReference> CoreWorker::SubmitTask(
    const RayFunction &function,
    const std::vector<std::unique_ptr<TaskArg>> &args,
    const TaskOptions &task_options,
    int max_retries,
    bool retry_exceptions,
    const rpc::SchedulingStrategy &scheduling_strategy,
    const std::string &debugger_breakpoint,
    const std::string &serialized_retry_exception_allowlist) {
  RAY_CHECK(scheduling_strategy.scheduling_strategy_case() !=
            rpc::SchedulingStrategy::SchedulingStrategyCase::SCHEDULING_STRATEGY_NOT_SET);

  TaskSpecBuilder builder;
  const auto next_task_index = worker_context_.GetNextTaskIndex();
  const auto task_id = TaskID::ForNormalTask(worker_context_.GetCurrentJobID(),
                                             worker_context_.GetCurrentInternalTaskId(),
                                             next_task_index);
  auto constrained_resources =
      AddPlacementGroupConstraint(task_options.resources, scheduling_strategy);

  const std::unordered_map<std::string, double> required_resources;
  auto task_name = task_options.name.empty()
                       ? function.GetFunctionDescriptor()->DefaultTaskName()
                       : task_options.name;
  int64_t depth = worker_context_.GetTaskDepth() + 1;
  // TODO(ekl) offload task building onto a thread pool for performance
  BuildCommonTaskSpec(builder,
                      worker_context_.GetCurrentJobID(),
                      task_id,
                      task_name,
                      worker_context_.GetCurrentTaskID(),
                      next_task_index,
                      GetCallerId(),
                      rpc_address_,
                      function,
                      args,
                      task_options.num_returns,
                      constrained_resources,
                      required_resources,
                      debugger_breakpoint,
                      depth,
                      task_options.serialized_runtime_env_info);
  builder.SetNormalTaskSpec(max_retries,
                            retry_exceptions,
                            serialized_retry_exception_allowlist,
                            scheduling_strategy);
  TaskSpecification task_spec = builder.Build();
  RAY_LOG(DEBUG) << "Submitting normal task " << task_spec.DebugString();
  std::vector<rpc::ObjectReference> returned_refs;
  if (options_.is_local_mode) {
    returned_refs = ExecuteTaskLocalMode(task_spec);
  } else {
    returned_refs = task_manager_->AddPendingTask(
        task_spec.CallerAddress(), task_spec, CurrentCallSite(), max_retries);
    io_service_.post(
        [this, task_spec]() {
          RAY_UNUSED(direct_task_submitter_->SubmitTask(task_spec));
        },
        "CoreWorker.SubmitTask");
  }
  return returned_refs;
}

Status CoreWorker::CreateActor(const RayFunction &function,
                               const std::vector<std::unique_ptr<TaskArg>> &args,
                               const ActorCreationOptions &actor_creation_options,
                               const std::string &extension_data,
                               ActorID *return_actor_id) {
  RAY_CHECK(actor_creation_options.scheduling_strategy.scheduling_strategy_case() !=
            rpc::SchedulingStrategy::SchedulingStrategyCase::SCHEDULING_STRATEGY_NOT_SET);

  if (actor_creation_options.is_asyncio && options_.is_local_mode) {
    return Status::NotImplemented(
        "Async actor is currently not supported for the local mode");
  }

  RAY_CHECK(job_config_ != nullptr);
  bool is_detached = false;
  if (!actor_creation_options.is_detached.has_value()) {
    /// Since this actor doesn't have a specified lifetime on creation, let's use
    /// the default value of the job.
    is_detached = job_config_->default_actor_lifetime() ==
                  ray::rpc::JobConfig_ActorLifetime_DETACHED;
  } else {
    is_detached = actor_creation_options.is_detached.value();
  }

  const auto next_task_index = worker_context_.GetNextTaskIndex();
  const ActorID actor_id = ActorID::Of(worker_context_.GetCurrentJobID(),
                                       worker_context_.GetCurrentTaskID(),
                                       next_task_index);
  const TaskID actor_creation_task_id = TaskID::ForActorCreationTask(actor_id);
  const JobID job_id = worker_context_.GetCurrentJobID();
  // Propagate existing environment variable overrides, but override them with any new
  // ones
  std::vector<ObjectID> return_ids;
  TaskSpecBuilder builder;
  auto new_placement_resources =
      AddPlacementGroupConstraint(actor_creation_options.placement_resources,
                                  actor_creation_options.scheduling_strategy);
  auto new_resource = AddPlacementGroupConstraint(
      actor_creation_options.resources, actor_creation_options.scheduling_strategy);
  const auto actor_name = actor_creation_options.name;
  const auto task_name =
      actor_name.empty()
          ? function.GetFunctionDescriptor()->DefaultTaskName()
          : actor_name + ":" + function.GetFunctionDescriptor()->CallString();
  int64_t depth = worker_context_.GetTaskDepth() + 1;
  BuildCommonTaskSpec(builder,
                      job_id,
                      actor_creation_task_id,
                      task_name,
                      worker_context_.GetCurrentTaskID(),
                      next_task_index,
                      GetCallerId(),
                      rpc_address_,
                      function,
                      args,
                      1,
                      new_resource,
                      new_placement_resources,
                      "" /* debugger_breakpoint */,
                      depth,
                      actor_creation_options.serialized_runtime_env_info);

  // If the namespace is not specified, get it from the job.
  const auto &ray_namespace = (actor_creation_options.ray_namespace.empty()
                                   ? job_config_->ray_namespace()
                                   : actor_creation_options.ray_namespace);
  auto actor_handle = std::make_unique<ActorHandle>(
      actor_id,
      GetCallerId(),
      rpc_address_,
      job_id,
      /*actor_cursor=*/ObjectID::FromIndex(actor_creation_task_id, 1),
      function.GetLanguage(),
      function.GetFunctionDescriptor(),
      extension_data,
      actor_creation_options.max_task_retries,
      actor_name,
      ray_namespace,
      actor_creation_options.max_pending_calls,
      actor_creation_options.execute_out_of_order);
  std::string serialized_actor_handle;
  actor_handle->Serialize(&serialized_actor_handle);
  builder.SetActorCreationTaskSpec(actor_id,
                                   serialized_actor_handle,
                                   actor_creation_options.scheduling_strategy,
                                   actor_creation_options.max_restarts,
                                   actor_creation_options.max_task_retries,
                                   actor_creation_options.dynamic_worker_options,
                                   actor_creation_options.max_concurrency,
                                   is_detached,
                                   actor_name,
                                   ray_namespace,
                                   actor_creation_options.is_asyncio,
                                   actor_creation_options.concurrency_groups,
                                   extension_data,
                                   actor_creation_options.execute_out_of_order);
  // Add the actor handle before we submit the actor creation task, since the
  // actor handle must be in scope by the time the GCS sends the
  // WaitForActorOutOfScopeRequest.
  RAY_CHECK(actor_manager_->AddNewActorHandle(
      std::move(actor_handle), CurrentCallSite(), rpc_address_, is_detached))
      << "Actor " << actor_id << " already exists";
  *return_actor_id = actor_id;
  TaskSpecification task_spec = builder.Build();
  RAY_LOG(DEBUG) << "Submitting actor creation task " << task_spec.DebugString();
  if (options_.is_local_mode) {
    // TODO(suquark): Should we consider namespace in local mode? Currently
    // it looks like two actors with two different namespaces become the
    // same actor in local mode. Maybe this is not an issue if we consider
    // the actor name globally unique.
    if (!actor_name.empty()) {
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
    task_manager_->AddPendingTask(
        rpc_address_, task_spec, CurrentCallSite(), max_retries);

    if (actor_name.empty()) {
      io_service_.post(
          [this, task_spec = std::move(task_spec)]() {
            RAY_UNUSED(actor_creator_->AsyncRegisterActor(
                task_spec, [this, task_spec](Status status) {
                  if (!status.ok()) {
                    RAY_LOG(ERROR)
                        << "Failed to register actor: " << task_spec.ActorCreationId()
                        << ". Error message: " << status.ToString();
                  } else {
                    RAY_UNUSED(direct_task_submitter_->SubmitTask(task_spec));
                  }
                }));
          },
          "ActorCreator.AsyncRegisterActor");
    } else {
      // For named actor, we still go through the sync way because for
      // functions like list actors these actors need to be there, especially
      // for local driver. But the current code all go through the gcs right now.
      auto status = actor_creator_->RegisterActor(task_spec);
      if (!status.ok()) {
        return status;
      }
      io_service_.post(
          [this, task_spec = std::move(task_spec)]() {
            RAY_UNUSED(direct_task_submitter_->SubmitTask(task_spec));
          },
          "CoreWorker.SubmitTask");
    }
  }
  return Status::OK();
}

Status CoreWorker::CreatePlacementGroup(
    const PlacementGroupCreationOptions &placement_group_creation_options,
    PlacementGroupID *return_placement_group_id) {
  const auto &bundles = placement_group_creation_options.bundles;
  for (const auto &bundle : bundles) {
    for (const auto &resource : bundle) {
      if (resource.first == kBundle_ResourceLabel) {
        std::ostringstream stream;
        stream << kBundle_ResourceLabel << " is a system reserved resource, which is not "
               << "allowed to be used in placement groupd ";
        return Status::Invalid(stream.str());
      }
    }
  }
  const PlacementGroupID placement_group_id = PlacementGroupID::Of(GetCurrentJobId());
  PlacementGroupSpecBuilder builder;
  builder.SetPlacementGroupSpec(
      placement_group_id,
      placement_group_creation_options.name,
      placement_group_creation_options.bundles,
      placement_group_creation_options.strategy,
      placement_group_creation_options.is_detached,
      placement_group_creation_options.max_cpu_fraction_per_node,
      worker_context_.GetCurrentJobID(),
      worker_context_.GetCurrentActorID(),
      worker_context_.CurrentActorDetached());
  PlacementGroupSpecification placement_group_spec = builder.Build();
  *return_placement_group_id = placement_group_id;
  RAY_LOG(INFO) << "Submitting Placement Group creation to GCS: " << placement_group_id;
  const auto status =
      gcs_client_->PlacementGroups().SyncCreatePlacementGroup(placement_group_spec);
  if (status.IsTimedOut()) {
    std::ostringstream stream;
    stream << "There was timeout in creating the placement group of id "
           << placement_group_id
           << ". It is probably "
              "because GCS server is dead or there's a high load there.";
    return Status::TimedOut(stream.str());
  } else {
    return status;
  }
}

Status CoreWorker::RemovePlacementGroup(const PlacementGroupID &placement_group_id) {
  // Synchronously wait for placement group removal.
  const auto status =
      gcs_client_->PlacementGroups().SyncRemovePlacementGroup(placement_group_id);
  if (status.IsTimedOut()) {
    std::ostringstream stream;
    stream << "There was timeout in removing the placement group of id "
           << placement_group_id
           << ". It is probably "
              "because GCS server is dead or there's a high load there.";
    return Status::TimedOut(stream.str());
  } else {
    return status;
  }
}

Status CoreWorker::WaitPlacementGroupReady(const PlacementGroupID &placement_group_id,
                                           int64_t timeout_seconds) {
  const auto status = gcs_client_->PlacementGroups().SyncWaitUntilReady(
      placement_group_id, timeout_seconds);
  if (status.IsTimedOut()) {
    std::ostringstream stream;
    stream << "There was timeout in waiting for placement group " << placement_group_id
           << " creation.";
    return Status::TimedOut(stream.str());
  } else {
    return status;
  }
}

std::optional<std::vector<rpc::ObjectReference>> CoreWorker::SubmitActorTask(
    const ActorID &actor_id,
    const RayFunction &function,
    const std::vector<std::unique_ptr<TaskArg>> &args,
    const TaskOptions &task_options) {
  absl::ReleasableMutexLock lock(&actor_task_mutex_);
  /// Check whether backpressure may happen at the very beginning of submitting a task.
  if (direct_actor_submitter_->PendingTasksFull(actor_id)) {
    RAY_LOG(DEBUG) << "Back pressure occurred while submitting the task to " << actor_id
                   << ". " << direct_actor_submitter_->DebugString(actor_id);
    return std::nullopt;
  }

  auto actor_handle = actor_manager_->GetActorHandle(actor_id);
  // Subscribe the actor state when we first submit the actor task. It is to reduce the
  // number of connections. The method is idempotent.
  actor_manager_->SubscribeActorState(actor_id);

  // Add one for actor cursor object id for tasks.
  const int num_returns = task_options.num_returns + 1;

  // Build common task spec.
  TaskSpecBuilder builder;
  const auto next_task_index = worker_context_.GetNextTaskIndex();
  const TaskID actor_task_id =
      TaskID::ForActorTask(worker_context_.GetCurrentJobID(),
                           worker_context_.GetCurrentInternalTaskId(),
                           next_task_index,
                           actor_handle->GetActorID());
  const std::unordered_map<std::string, double> required_resources;
  const auto task_name = task_options.name.empty()
                             ? function.GetFunctionDescriptor()->DefaultTaskName()
                             : task_options.name;

  // The depth of the actor task is depth of the caller + 1
  // The caller is not necessarily the creator of the actor.
  int64_t depth = worker_context_.GetTaskDepth() + 1;
  BuildCommonTaskSpec(builder,
                      actor_handle->CreationJobID(),
                      actor_task_id,
                      task_name,
                      worker_context_.GetCurrentTaskID(),
                      next_task_index,
                      GetCallerId(),
                      rpc_address_,
                      function,
                      args,
                      num_returns,
                      task_options.resources,
                      required_resources,
                      "",    /* debugger_breakpoint */
                      depth, /*depth*/
                      "{}",  /* serialized_runtime_env_info */
                      task_options.concurrency_group_name);
  // NOTE: placement_group_capture_child_tasks and runtime_env will
  // be ignored in the actor because we should always follow the actor's option.

  // TODO(swang): Do we actually need to set this ObjectID?
  const ObjectID new_cursor = ObjectID::FromIndex(actor_task_id, num_returns);
  actor_handle->SetActorTaskSpec(builder, new_cursor);

  // Submit task.
  TaskSpecification task_spec = builder.Build();
  RAY_LOG(DEBUG) << "Submitting actor task " << task_spec.DebugString();
  std::vector<rpc::ObjectReference> returned_refs;
  if (options_.is_local_mode) {
    /// NOTE: The lock should be released in local mode. The user code may
    /// submit another task when executing the current task locally, which
    /// cause deadlock. The code call chain is:
    /// SubmitActorTask -> python user code -> actor.xx.remote() -> SubmitActorTask
    lock.Release();
    returned_refs = ExecuteTaskLocalMode(task_spec, actor_id);
  } else {
    returned_refs = task_manager_->AddPendingTask(
        rpc_address_, task_spec, CurrentCallSite(), actor_handle->MaxTaskRetries());
    RAY_CHECK_OK(direct_actor_submitter_->SubmitTask(task_spec));
  }
  return {std::move(returned_refs)};
}

Status CoreWorker::CancelTask(const ObjectID &object_id,
                              bool force_kill,
                              bool recursive) {
  if (actor_manager_->CheckActorHandleExists(object_id.TaskId().ActorId())) {
    return Status::Invalid("Actor task cancellation is not supported.");
  }
  rpc::Address obj_addr;
  if (!reference_counter_->GetOwner(object_id, &obj_addr)) {
    return Status::Invalid("No owner found for object.");
  }
  if (obj_addr.SerializeAsString() != rpc_address_.SerializeAsString()) {
    return direct_task_submitter_->CancelRemoteTask(
        object_id, obj_addr, force_kill, recursive);
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
  std::promise<Status> p;
  auto f = p.get_future();
  io_service_.post(
      [this, p = &p, actor_id, force_kill, no_restart]() {
        auto cb = [this, p, actor_id, force_kill, no_restart](Status status) mutable {
          if (status.ok()) {
            RAY_CHECK_OK(gcs_client_->Actors().AsyncKillActor(
                actor_id, force_kill, no_restart, nullptr));
          }
          p->set_value(std::move(status));
        };
        if (actor_creator_->IsActorInRegistering(actor_id)) {
          actor_creator_->AsyncWaitForActorRegisterFinish(actor_id, std::move(cb));
        } else if (actor_manager_->CheckActorHandleExists(actor_id)) {
          cb(Status::OK());
        } else {
          std::stringstream stream;
          stream << "Failed to find a corresponding actor handle for " << actor_id;
          cb(Status::Invalid(stream.str()));
        }
      },
      "CoreWorker.KillActor");
  const auto &status = f.get();
  actor_manager_->OnActorKilled(actor_id);
  return status;
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
  return actor_manager_->RegisterActorHandle(
      std::move(actor_handle), outer_object_id, CurrentCallSite(), rpc_address_);
}

Status CoreWorker::SerializeActorHandle(const ActorID &actor_id,
                                        std::string *output,
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
    const std::string &name, const std::string &ray_namespace) {
  RAY_CHECK(!name.empty());
  if (options_.is_local_mode) {
    return GetNamedActorHandleLocalMode(name);
  }

  return actor_manager_->GetNamedActorHandle(
      name,
      ray_namespace.empty() ? job_config_->ray_namespace() : ray_namespace,
      CurrentCallSite(),
      rpc_address_);
}

std::pair<std::vector<std::pair<std::string, std::string>>, Status>
CoreWorker::ListNamedActors(bool all_namespaces) {
  if (options_.is_local_mode) {
    return ListNamedActorsLocalMode();
  }

  std::vector<std::pair<std::string, std::string>> actors;

  // This call needs to be blocking because we can't return until we get the
  // response from the RPC.
  const auto &ray_namespace = job_config_->ray_namespace();
  const auto status =
      gcs_client_->Actors().SyncListNamedActors(all_namespaces, ray_namespace, actors);
  if (status.IsTimedOut()) {
    std::ostringstream stream;
    stream << "There was timeout in getting the list of named actors, "
              "probably because the GCS server is dead or under high load .";
    return std::make_pair(std::move(actors), Status::TimedOut(stream.str()));
  } else if (!status.ok()) {
    return std::make_pair(std::move(actors), status);
  } else {
    return std::make_pair(std::move(actors), status);
  }
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

std::pair<std::vector<std::pair<std::string, std::string>>, Status>
CoreWorker::ListNamedActorsLocalMode() {
  std::vector<std::pair<std::string, std::string>> actors;
  for (auto it = local_mode_named_actor_registry_.begin();
       it != local_mode_named_actor_registry_.end();
       it++) {
    actors.push_back(std::make_pair(/*namespace=*/"", it->first));
  }
  return std::make_pair(actors, Status::OK());
}

const ResourceMappingType CoreWorker::GetResourceIDs() const {
  absl::MutexLock lock(&mutex_);
  return *resource_ids_;
}

std::unique_ptr<worker::ProfileEvent> CoreWorker::CreateProfileEvent(
    const std::string &event_type) {
  return std::make_unique<worker::ProfileEvent>(profiler_, event_type);
}

void CoreWorker::RunTaskExecutionLoop() {
  task_execution_service_.run();
  RAY_CHECK(is_shutdown_)
      << "Task execution loop was terminated without calling shutdown API.";
}

Status CoreWorker::AllocateReturnObject(const ObjectID &object_id,
                                        const size_t &data_size,
                                        const std::shared_ptr<Buffer> &metadata,
                                        const std::vector<ObjectID> &contained_object_ids,
                                        int64_t *task_output_inlined_bytes,
                                        std::shared_ptr<RayObject> *return_object) {
  rpc::Address owner_address(options_.is_local_mode
                                 ? rpc::Address()
                                 : worker_context_.GetCurrentTask()->CallerAddress());

  bool object_already_exists = false;
  std::shared_ptr<Buffer> data_buffer;
  if (data_size > 0) {
    RAY_LOG(DEBUG) << "Creating return object " << object_id;
    // Mark this object as containing other object IDs. The ref counter will
    // keep the inner IDs in scope until the outer one is out of scope.
    if (!contained_object_ids.empty() && !options_.is_local_mode) {
      reference_counter_->AddNestedObjectIds(
          object_id, contained_object_ids, owner_address);
    }

    // Allocate a buffer for the return object.
    if (options_.is_local_mode ||
        (static_cast<int64_t>(data_size) < max_direct_call_object_size_ &&
         // ensure we don't exceed the limit if we allocate this object inline.
         (*task_output_inlined_bytes + static_cast<int64_t>(data_size) <=
          RayConfig::instance().task_rpc_inlined_bytes_limit()))) {
      data_buffer = std::make_shared<LocalMemoryBuffer>(data_size);
      *task_output_inlined_bytes += static_cast<int64_t>(data_size);
    } else {
      RAY_RETURN_NOT_OK(CreateExisting(metadata,
                                       data_size,
                                       object_id,
                                       owner_address,
                                       &data_buffer,
                                       /*created_by_worker=*/true));
      object_already_exists = !data_buffer;
    }
  }
  // Leave the return object as a nullptr if the object already exists.
  if (!object_already_exists) {
    auto contained_refs = GetObjectRefs(contained_object_ids);
    *return_object =
        std::make_shared<RayObject>(data_buffer, metadata, std::move(contained_refs));
  }

  return Status::OK();
}

Status CoreWorker::ExecuteTask(
    const TaskSpecification &task_spec,
    const std::shared_ptr<ResourceMappingType> &resource_ids,
    std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> *return_objects,
    std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> *dynamic_return_objects,
    ReferenceCounter::ReferenceTableProto *borrowed_refs,
    bool *is_retryable_error,
    bool *is_application_error) {
  RAY_LOG(DEBUG) << "Executing task, task info = " << task_spec.DebugString();
  task_queue_length_ -= 1;
  num_executed_tasks_ += 1;

  // Modify the worker's per function counters.
  std::string func_name = task_spec.FunctionDescriptor()->CallString();
  if (!options_.is_local_mode) {
    task_counter_.MovePendingToRunning(func_name, task_spec.IsRetry());

    // Make task event
    if (task_event_buffer_->Enabled()) {
      rpc::TaskEvents task_event;
      task_event.set_task_id(task_spec.TaskId().Binary());
      task_event.set_attempt_number(task_spec.AttemptNumber());
      task_event.set_job_id(task_spec.JobId().Binary());

      auto state_updates = task_event.mutable_state_updates();
      state_updates->set_running_ts(absl::GetCurrentTimeNanos());
      task_event_buffer_->AddTaskEvent(std::move(task_event));
    }

    worker_context_.SetCurrentTask(task_spec);
    SetCurrentTaskId(task_spec.TaskId(), task_spec.AttemptNumber(), task_spec.GetName());
  }
  {
    absl::MutexLock lock(&mutex_);
    current_tasks_.emplace(task_spec.TaskId(), task_spec);
    if (resource_ids) {
      resource_ids_ = resource_ids;
    }
  }

  RayFunction func{task_spec.GetLanguage(), task_spec.FunctionDescriptor()};

  std::vector<std::shared_ptr<RayObject>> args;
  std::vector<rpc::ObjectReference> arg_refs;
  // This includes all IDs that were passed by reference and any IDs that were
  // inlined in the task spec. These references will be pinned during the task
  // execution and unpinned once the task completes. We will notify the caller
  // about any IDs that we are still borrowing by the time the task completes.
  std::vector<ObjectID> borrowed_ids;
  RAY_CHECK_OK(GetAndPinArgsForExecutor(task_spec, &args, &arg_refs, &borrowed_ids));

  for (size_t i = 0; i < task_spec.NumReturns(); i++) {
    return_objects->push_back(std::make_pair<>(task_spec.ReturnId(i), nullptr));
  }
  // For dynamic tasks, pass the return IDs that were dynamically generated on
  // the first execution.
  if (!task_spec.ReturnsDynamic()) {
    dynamic_return_objects = NULL;
  } else if (task_spec.AttemptNumber() > 0) {
    for (const auto &dynamic_return_id : task_spec.DynamicReturnIds()) {
      dynamic_return_objects->push_back(
          std::make_pair<>(dynamic_return_id, std::shared_ptr<RayObject>()));
      RAY_LOG(DEBUG) << "Re-executed task " << task_spec.TaskId()
                     << " should return dynamic object " << dynamic_return_id;

      AddLocalReference(dynamic_return_id, "<temporary (ObjectRefGenerator)>");
      reference_counter_->AddBorrowedObject(
          dynamic_return_id, ObjectID::Nil(), task_spec.CallerAddress());
    }
  }

  Status status;
  TaskType task_type = TaskType::NORMAL_TASK;
  if (task_spec.IsActorCreationTask()) {
    RAY_CHECK(return_objects->size() > 0);
    return_objects->pop_back();
    task_type = TaskType::ACTOR_CREATION_TASK;
    SetActorId(task_spec.ActorCreationId());
    task_counter_.BecomeActor(task_spec.FunctionDescriptor()->ClassName());
    {
      std::unique_ptr<ActorHandle> self_actor_handle(
          new ActorHandle(task_spec.GetSerializedActorHandle()));
      // Register the handle to the current actor itself.
      actor_manager_->RegisterActorHandle(std::move(self_actor_handle),
                                          ObjectID::Nil(),
                                          CurrentCallSite(),
                                          rpc_address_,
                                          /*is_self=*/true);
    }
    RAY_LOG(INFO) << "Creating actor: " << task_spec.ActorCreationId();
  } else if (task_spec.IsActorTask()) {
    RAY_CHECK(return_objects->size() > 0);
    return_objects->pop_back();
    task_type = TaskType::ACTOR_TASK;
  }

  std::shared_ptr<LocalMemoryBuffer> creation_task_exception_pb_bytes = nullptr;

  std::vector<ConcurrencyGroup> defined_concurrency_groups = {};
  std::string name_of_concurrency_group_to_execute;
  if (task_spec.IsActorCreationTask()) {
    defined_concurrency_groups = task_spec.ConcurrencyGroups();
  } else if (task_spec.IsActorTask()) {
    name_of_concurrency_group_to_execute = task_spec.ConcurrencyGroupName();
  }

  status = options_.task_execution_callback(
      task_spec.CallerAddress(),
      task_type,
      task_spec.GetName(),
      func,
      task_spec.GetRequiredResources().GetResourceUnorderedMap(),
      args,
      arg_refs,
      task_spec.GetDebuggerBreakpoint(),
      task_spec.GetSerializedRetryExceptionAllowlist(),
      return_objects,
      dynamic_return_objects,
      creation_task_exception_pb_bytes,
      is_retryable_error,
      is_application_error,
      defined_concurrency_groups,
      name_of_concurrency_group_to_execute,
      /*is_reattempt=*/task_spec.AttemptNumber() > 0);

  // Get the reference counts for any IDs that we borrowed during this task,
  // remove the local reference for these IDs, and return the ref count info to
  // the caller. This will notify the caller of any IDs that we (or a nested
  // task) are still borrowing. It will also notify the caller of any new IDs
  // that were contained in a borrowed ID that we (or a nested task) are now
  // borrowing.
  std::vector<ObjectID> deleted;
  if (!borrowed_ids.empty()) {
    reference_counter_->PopAndClearLocalBorrowers(borrowed_ids, borrowed_refs, &deleted);
  }
  if (dynamic_return_objects != NULL) {
    for (const auto &dynamic_return : *dynamic_return_objects) {
      reference_counter_->PopAndClearLocalBorrowers(
          {dynamic_return.first}, borrowed_refs, &deleted);
    }
  }
  memory_store_->Delete(deleted);

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
    SetCurrentTaskId(TaskID::Nil(), /*attempt_number=*/0, "");
    worker_context_.ResetCurrentTask();
  }
  {
    absl::MutexLock lock(&mutex_);
    auto it = current_tasks_.find(task_spec.TaskId());
    RAY_CHECK(it != current_tasks_.end());
    current_tasks_.erase(it);
    if (task_spec.IsNormalTask()) {
      resource_ids_.reset(new ResourceMappingType());
    }
  }

  if (!options_.is_local_mode) {
    task_counter_.MoveRunningToFinished(func_name, task_spec.IsRetry());
  }
  RAY_LOG(DEBUG) << "Finished executing task " << task_spec.TaskId()
                 << ", status=" << status;

  std::ostringstream stream;
  if (status.IsCreationTaskError()) {
    Exit(rpc::WorkerExitType::USER_ERROR,
         absl::StrCat(
             "Worker exits because there was an exception in the initialization method "
             "(e.g., __init__). Fix the exceptions from the initialization to resolve "
             "the issue. ",
             status.message()),
         creation_task_exception_pb_bytes);
  } else if (status.IsIntentionalSystemExit()) {
    Exit(rpc::WorkerExitType::INTENDED_USER_EXIT,
         absl::StrCat("Worker exits by an user request. ", status.message()),
         creation_task_exception_pb_bytes);
  } else if (status.IsUnexpectedSystemExit()) {
    Exit(rpc::WorkerExitType::SYSTEM_ERROR,
         absl::StrCat("Worker exits unexpectedly. ", status.message()),
         creation_task_exception_pb_bytes);
  } else if (!status.ok()) {
    RAY_LOG(FATAL) << "Unexpected task status type : " << status;
  }
  return status;
}

Status CoreWorker::SealReturnObject(const ObjectID &return_id,
                                    std::shared_ptr<RayObject> return_object,
                                    const ObjectID &generator_id) {
  RAY_LOG(DEBUG) << "Sealing return object " << return_id;
  Status status = Status::OK();
  RAY_CHECK(return_object);
  RAY_CHECK(!options_.is_local_mode);
  std::unique_ptr<rpc::Address> caller_address =
      std::make_unique<rpc::Address>(worker_context_.GetCurrentTask()->CallerAddress());
  if (return_object->GetData() != nullptr && return_object->GetData()->IsPlasmaBuffer()) {
    status = SealExisting(
        return_id, /*pin_object=*/true, generator_id, std::move(caller_address));
    if (!status.ok()) {
      RAY_LOG(FATAL) << "Failed to seal object " << return_id
                     << " in store: " << status.message();
    }
  }
  return status;
}

bool CoreWorker::PinExistingReturnObject(const ObjectID &return_id,
                                         std::shared_ptr<RayObject> *return_object,
                                         const ObjectID &generator_id) {
  // TODO(swang): If there is already an existing copy of this object, then it
  // might not have the same value as the new copy. It would be better to evict
  // the existing copy here.
  absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> result_map;
  bool got_exception;
  rpc::Address owner_address(worker_context_.GetCurrentTask()->CallerAddress());

  // Temporarily set the return object's owner's address. This is needed to retrieve the
  // value from plasma.
  reference_counter_->AddLocalReference(return_id, "<temporary (pin return object)>");
  reference_counter_->AddBorrowedObject(return_id, ObjectID::Nil(), owner_address);

  auto status = plasma_store_provider_->Get(
      {return_id}, 0, worker_context_, &result_map, &got_exception);
  // Remove the temporary ref.
  RemoveLocalReference(return_id);

  if (result_map.count(return_id)) {
    *return_object = std::move(result_map[return_id]);
    RAY_LOG(DEBUG) << "Pinning existing return object " << return_id
                   << " owned by worker "
                   << WorkerID::FromBinary(owner_address.worker_id());
    // Keep the object in scope until it's been pinned.
    std::shared_ptr<RayObject> pinned_return_object = *return_object;
    // Asynchronously ask the raylet to pin the object. Note that this can fail
    // if the raylet fails. We expect the owner of the object to handle that
    // case (e.g., by detecting the raylet failure and storing an error).
    local_raylet_client_->PinObjectIDs(
        owner_address,
        {return_id},
        generator_id,
        [return_id, pinned_return_object](const Status &status,
                                          const rpc::PinObjectIDsReply &reply) {
          if (!status.ok() || !reply.successes(0)) {
            RAY_LOG(INFO) << "Failed to pin existing copy of the task return object "
                          << return_id
                          << ". This object may get evicted while there are still "
                             "references to it.";
          }
        });
    return true;
  } else {
    // Failed to get the existing copy of the return object. It must have been
    // evicted before we could pin it.
    // TODO(swang): We should allow the owner to retry this task instead of
    // immediately returning an error to the application.
    return false;
  }
}

ObjectID CoreWorker::AllocateDynamicReturnId() {
  const auto &task_spec = worker_context_.GetCurrentTask();
  const auto return_id =
      ObjectID::FromIndex(task_spec->TaskId(), worker_context_.GetNextPutIndex());
  AddLocalReference(return_id, "<temporary (ObjectRefGenerator)>");
  reference_counter_->AddBorrowedObject(
      return_id, ObjectID::Nil(), worker_context_.GetCurrentTask()->CallerAddress());
  return return_id;
}

std::vector<rpc::ObjectReference> CoreWorker::ExecuteTaskLocalMode(
    const TaskSpecification &task_spec, const ActorID &actor_id) {
  auto resource_ids = std::make_shared<ResourceMappingType>();
  auto return_objects = std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>>();
  auto borrowed_refs = ReferenceCounter::ReferenceTableProto();

  std::vector<rpc::ObjectReference> returned_refs;
  size_t num_returns = task_spec.NumReturns();
  if (task_spec.IsActorTask()) {
    num_returns--;
  }
  for (size_t i = 0; i < num_returns; i++) {
    if (!task_spec.IsActorCreationTask()) {
      reference_counter_->AddOwnedObject(task_spec.ReturnId(i),
                                         /*inner_ids=*/{},
                                         rpc_address_,
                                         CurrentCallSite(),
                                         -1,
                                         /*is_reconstructable=*/false,
                                         /*add_local_ref=*/true);
    }
    rpc::ObjectReference ref;
    ref.set_object_id(task_spec.ReturnId(i).Binary());
    ref.mutable_owner_address()->CopyFrom(task_spec.CallerAddress());
    returned_refs.push_back(std::move(ref));
  }
  auto old_id = GetActorId();
  SetActorId(actor_id);
  bool is_retryable_error;
  bool is_application_error;
  // TODO(swang): Support ObjectRefGenerators in local mode?
  std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> dynamic_return_objects;
  RAY_UNUSED(ExecuteTask(task_spec,
                         resource_ids,
                         &return_objects,
                         &dynamic_return_objects,
                         &borrowed_refs,
                         &is_retryable_error,
                         &is_application_error));
  SetActorId(old_id);
  return returned_refs;
}

Status CoreWorker::GetAndPinArgsForExecutor(const TaskSpecification &task,
                                            std::vector<std::shared_ptr<RayObject>> *args,
                                            std::vector<rpc::ObjectReference> *arg_refs,
                                            std::vector<ObjectID> *borrowed_ids) {
  auto num_args = task.NumArgs();
  args->resize(num_args);
  arg_refs->resize(num_args);

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
      const auto &arg_ref = task.ArgRef(i);
      const auto arg_id = ObjectID::FromBinary(arg_ref.object_id());
      by_ref_ids.insert(arg_id);
      auto it = by_ref_indices.find(arg_id);
      if (it == by_ref_indices.end()) {
        by_ref_indices.emplace(arg_id, std::vector<size_t>({i}));
      } else {
        it->second.push_back(i);
      }
      arg_refs->at(i) = arg_ref;
      // Pin all args passed by reference for the duration of the task.  This
      // ensures that when the task completes, we can retrieve metadata about
      // any borrowed ObjectIDs that were serialized in the argument's value.
      RAY_LOG(DEBUG) << "Incrementing ref for argument ID " << arg_id;
      reference_counter_->AddLocalReference(arg_id, task.CallSiteString());
      // Attach the argument's owner's address. This is needed to retrieve the
      // value from plasma.
      reference_counter_->AddBorrowedObject(
          arg_id, ObjectID::Nil(), task.ArgRef(i).owner_address());
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
          std::make_shared<RayObject>(data, metadata, task.ArgInlinedRefs(i), copy_data);
      arg_refs->at(i).set_object_id(ObjectID::Nil().Binary());
      // The task borrows all ObjectIDs that were serialized in the inlined
      // arguments. The task will receive references to these IDs, so it is
      // possible for the task to continue borrowing these arguments by the
      // time it finishes.
      for (const auto &inlined_ref : task.ArgInlinedRefs(i)) {
        const auto inlined_id = ObjectID::FromBinary(inlined_ref.object_id());
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
    RAY_RETURN_NOT_OK(plasma_store_provider_->Get(
        by_ref_ids, -1, worker_context_, &result_map, &got_exception));
  }
  for (const auto &it : result_map) {
    for (size_t idx : by_ref_indices[it.first]) {
      args->at(idx) = it.second;
    }
  }

  return Status::OK();
}

void CoreWorker::HandlePushTask(rpc::PushTaskRequest request,
                                rpc::PushTaskReply *reply,
                                rpc::SendReplyCallback send_reply_callback) {
  if (HandleWrongRecipient(WorkerID::FromBinary(request.intended_worker_id()),
                           send_reply_callback)) {
    return;
  }

  // Increment the task_queue_length and per function counter.
  task_queue_length_ += 1;
  std::string func_name =
      FunctionDescriptorBuilder::FromProto(request.task_spec().function_descriptor())
          ->CallString();
  task_counter_.IncPending(func_name, request.task_spec().attempt_number() > 0);

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
    rpc::DirectActorCallArgWaitCompleteRequest request,
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

void CoreWorker::HandleRayletNotifyGCSRestart(
    rpc::RayletNotifyGCSRestartRequest request,
    rpc::RayletNotifyGCSRestartReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  gcs_client_->AsyncResubscribe();
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void CoreWorker::HandleGetObjectStatus(rpc::GetObjectStatusRequest request,
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
    memory_store_->GetAsync(object_id,
                            [this, object_id, reply, send_reply_callback, is_freed](
                                std::shared_ptr<RayObject> obj) {
                              if (is_freed) {
                                reply->set_status(rpc::GetObjectStatusReply::FREED);
                              } else {
                                PopulateObjectStatus(object_id, obj, reply);
                              }
                              send_reply_callback(Status::OK(), nullptr, nullptr);
                            });
  }

  RemoveLocalReference(object_id);
}

void CoreWorker::PopulateObjectStatus(const ObjectID &object_id,
                                      std::shared_ptr<RayObject> obj,
                                      rpc::GetObjectStatusReply *reply) {
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
  for (const auto &nested_ref : obj->GetNestedRefs()) {
    object->add_nested_inlined_refs()->CopyFrom(nested_ref);
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

void CoreWorker::HandleWaitForActorOutOfScope(
    rpc::WaitForActorOutOfScopeRequest request,
    rpc::WaitForActorOutOfScopeReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
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
  if (actor_creator_->IsActorInRegistering(actor_id)) {
    actor_creator_->AsyncWaitForActorRegisterFinish(
        actor_id, [this, actor_id, respond = std::move(respond)](auto status) {
          if (!status.ok()) {
            respond(actor_id);
          } else {
            RAY_LOG(DEBUG) << "Received HandleWaitForActorOutOfScope for " << actor_id;
            actor_manager_->WaitForActorOutOfScope(actor_id, std::move(respond));
          }
        });
  } else {
    RAY_LOG(DEBUG) << "Received HandleWaitForActorOutOfScope for " << actor_id;
    actor_manager_->WaitForActorOutOfScope(actor_id, std::move(respond));
  }
}

void CoreWorker::ProcessSubscribeForObjectEviction(
    const rpc::WorkerObjectEvictionSubMessage &message) {
  // Send a response to trigger unpinning the object when it is no longer in scope.
  auto unpin_object = [this](const ObjectID &object_id) {
    RAY_LOG(DEBUG) << "Object " << object_id << " is deleted. Unpinning the object.";

    rpc::PubMessage pub_message;
    pub_message.set_key_id(object_id.Binary());
    pub_message.set_channel_type(rpc::ChannelType::WORKER_OBJECT_EVICTION);
    pub_message.mutable_worker_object_eviction_message()->set_object_id(
        object_id.Binary());

    object_info_publisher_->Publish(pub_message);
  };

  const auto object_id = ObjectID::FromBinary(message.object_id());
  const auto intended_worker_id = WorkerID::FromBinary(message.intended_worker_id());
  if (intended_worker_id != worker_context_.GetWorkerID()) {
    RAY_LOG(INFO) << "The SubscribeForObjectEviction message for object id " << object_id
                  << " is for " << intended_worker_id << ", but the current worker id is "
                  << worker_context_.GetWorkerID() << ". The RPC will be no-op.";
    unpin_object(object_id);
    return;
  }

  if (message.has_generator_id()) {
    // For dynamically generated return values, the raylet may subscribe to
    // eviction events before we know about the object. This can happen when we
    // receive the subscription request before the reply from the task that
    // created the object. Add the dynamically created object to our ref
    // counter so that we know that it exists.
    const auto generator_id = ObjectID::FromBinary(message.generator_id());
    RAY_CHECK(!generator_id.IsNil());
    reference_counter_->AddDynamicReturn(object_id, generator_id);
  }

  // Returns true if the object was present and the callback was added. It might have
  // already been evicted by the time we get this request, in which case we should
  // respond immediately so the raylet unpins the object.
  if (!reference_counter_->SetDeleteCallback(object_id, unpin_object)) {
    // If the object is already evicted (callback cannot be set), unregister the
    // subscription & publish the message so that the subscriber knows it.
    unpin_object(object_id);
    RAY_LOG(DEBUG) << "Reference for object " << object_id << " has already been freed.";
  }
}

void CoreWorker::ProcessSubscribeMessage(const rpc::SubMessage &sub_message,
                                         rpc::ChannelType channel_type,
                                         const std::string &key_id,
                                         const NodeID &subscriber_id) {
  object_info_publisher_->RegisterSubscription(channel_type, subscriber_id, key_id);

  if (sub_message.has_worker_object_eviction_message()) {
    ProcessSubscribeForObjectEviction(sub_message.worker_object_eviction_message());
  } else if (sub_message.has_worker_ref_removed_message()) {
    ProcessSubscribeForRefRemoved(sub_message.worker_ref_removed_message());
  } else if (sub_message.has_worker_object_locations_message()) {
    ProcessSubscribeObjectLocations(sub_message.worker_object_locations_message());
  } else {
    RAY_LOG(FATAL)
        << "Invalid command has received: "
        << static_cast<int>(sub_message.sub_message_one_of_case())
        << " has received. If you see this message, please report to Ray Github.";
  }
}

void CoreWorker::ProcessPubsubCommands(const Commands &commands,
                                       const NodeID &subscriber_id) {
  for (const auto &command : commands) {
    if (command.has_unsubscribe_message()) {
      object_info_publisher_->UnregisterSubscription(
          command.channel_type(), subscriber_id, command.key_id());
    } else if (command.has_subscribe_message()) {
      ProcessSubscribeMessage(command.subscribe_message(),
                              command.channel_type(),
                              command.key_id(),
                              subscriber_id);
    } else {
      RAY_LOG(FATAL) << "Invalid command has received, "
                     << static_cast<int>(command.command_message_one_of_case())
                     << ". If you see this message, please "
                        "report to Ray "
                        "Github.";
    }
  }
}

void CoreWorker::HandlePubsubLongPolling(rpc::PubsubLongPollingRequest request,
                                         rpc::PubsubLongPollingReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  const auto subscriber_id = NodeID::FromBinary(request.subscriber_id());
  RAY_LOG(DEBUG) << "Got a long polling request from a node " << subscriber_id;
  object_info_publisher_->ConnectToSubscriber(
      request, reply, std::move(send_reply_callback));
}

void CoreWorker::HandlePubsubCommandBatch(rpc::PubsubCommandBatchRequest request,
                                          rpc::PubsubCommandBatchReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  const auto subscriber_id = NodeID::FromBinary(request.subscriber_id());
  ProcessPubsubCommands(request.commands(), subscriber_id);
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void CoreWorker::HandleUpdateObjectLocationBatch(
    rpc::UpdateObjectLocationBatchRequest request,
    rpc::UpdateObjectLocationBatchReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const auto &worker_id = request.intended_worker_id();
  if (HandleWrongRecipient(WorkerID::FromBinary(worker_id), send_reply_callback)) {
    return;
  }
  const auto &node_id = NodeID::FromBinary(request.node_id());
  const auto &object_location_updates = request.object_location_updates();

  for (const auto &object_location_update : object_location_updates) {
    const auto &object_id = ObjectID::FromBinary(object_location_update.object_id());

    if (object_location_update.has_spilled_location_update()) {
      AddSpilledObjectLocationOwner(
          object_id,
          object_location_update.spilled_location_update().spilled_url(),
          object_location_update.spilled_location_update().spilled_to_local_storage()
              ? node_id
              : NodeID::Nil(),
          object_location_update.has_generator_id()
              ? std::optional<ObjectID>(
                    ObjectID::FromBinary(object_location_update.generator_id()))
              : std::nullopt);
    }

    if (object_location_update.has_plasma_location_update()) {
      if (object_location_update.plasma_location_update() ==
          rpc::ObjectPlasmaLocationUpdate::ADDED) {
        AddObjectLocationOwner(object_id, node_id);
      } else if (object_location_update.plasma_location_update() ==
                 rpc::ObjectPlasmaLocationUpdate::REMOVED) {
        RemoveObjectLocationOwner(object_id, node_id);
      } else {
        RAY_LOG(FATAL) << "Invalid object plasma location update "
                       << object_location_update.plasma_location_update()
                       << " has been received.";
      }
    }
  }

  send_reply_callback(Status::OK(),
                      /*success_callback_on_reply*/ nullptr,
                      /*failure_callback_on_reply*/ nullptr);
}

void CoreWorker::AddSpilledObjectLocationOwner(
    const ObjectID &object_id,
    const std::string &spilled_url,
    const NodeID &spilled_node_id,
    const std::optional<ObjectID> &generator_id) {
  RAY_LOG(DEBUG) << "Received object spilled location update for object " << object_id
                 << ", which has been spilled to " << spilled_url << " on node "
                 << spilled_node_id;
  if (generator_id.has_value()) {
    // For dynamically generated return values, the raylet may spill the
    // primary copy before we know about the object. This can happen when the
    // object is spilled before the reply from the task that created the
    // object. Add the dynamically created object to our ref counter so that we
    // know that it exists.
    RAY_CHECK(!generator_id->IsNil());
    reference_counter_->AddDynamicReturn(object_id, *generator_id);
  }

  auto reference_exists =
      reference_counter_->HandleObjectSpilled(object_id, spilled_url, spilled_node_id);
  if (!reference_exists) {
    RAY_LOG(DEBUG) << "Object " << object_id << " not found";
  }
}

void CoreWorker::AddObjectLocationOwner(const ObjectID &object_id,
                                        const NodeID &node_id) {
  if (gcs_client_->Nodes().Get(node_id, /*filter_dead_nodes=*/true) == nullptr) {
    RAY_LOG(DEBUG) << "Attempting to add object location for a dead node. "
                   << "Ignoring this request. object_id: " << object_id
                   << ", node_id: " << node_id;
    return;
  }
  auto reference_exists = reference_counter_->AddObjectLocation(object_id, node_id);
  if (!reference_exists) {
    RAY_LOG(DEBUG) << "Object " + object_id.Hex() + " not found";
  }

  // For generator tasks where we haven't yet received the task reply, the
  // internal ObjectRefs may not be added yet, so we don't find out about these
  // until the task finishes.
  const auto &maybe_generator_id = task_manager_->TaskGeneratorId(object_id.TaskId());
  if (!maybe_generator_id.IsNil()) {
    // The task is a generator and may not have finished yet. Add the internal
    // ObjectID so that we can update its location.
    reference_counter_->AddDynamicReturn(object_id, maybe_generator_id);
    RAY_UNUSED(reference_counter_->AddObjectLocation(object_id, node_id));
  }
}

void CoreWorker::RemoveObjectLocationOwner(const ObjectID &object_id,
                                           const NodeID &node_id) {
  auto reference_exists = reference_counter_->RemoveObjectLocation(object_id, node_id);
  if (!reference_exists) {
    RAY_LOG(DEBUG) << "Object " + object_id.Hex() + " not found";
  }
}

void CoreWorker::ProcessSubscribeObjectLocations(
    const rpc::WorkerObjectLocationsSubMessage &message) {
  const auto intended_worker_id = WorkerID::FromBinary(message.intended_worker_id());
  const auto object_id = ObjectID::FromBinary(message.object_id());

  if (intended_worker_id != worker_context_.GetWorkerID()) {
    RAY_LOG(INFO) << "The ProcessSubscribeObjectLocations message is for "
                  << intended_worker_id << ", but the current worker id is "
                  << worker_context_.GetWorkerID() << ". The RPC will be no-op.";
    object_info_publisher_->PublishFailure(
        rpc::ChannelType::WORKER_OBJECT_LOCATIONS_CHANNEL, object_id.Binary());
    return;
  }

  // Publish the first object location snapshot when subscribed for the first time.
  reference_counter_->PublishObjectLocationSnapshot(object_id);
}

void CoreWorker::HandleGetObjectLocationsOwner(
    rpc::GetObjectLocationsOwnerRequest request,
    rpc::GetObjectLocationsOwnerReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto &object_location_request = request.object_location_request();
  if (HandleWrongRecipient(
          WorkerID::FromBinary(object_location_request.intended_worker_id()),
          send_reply_callback)) {
    return;
  }
  auto object_id = ObjectID::FromBinary(object_location_request.object_id());
  auto object_info = reply->mutable_object_location_info();
  auto status = reference_counter_->FillObjectInformation(object_id, object_info);
  send_reply_callback(status, nullptr, nullptr);
}

void CoreWorker::ProcessSubscribeForRefRemoved(
    const rpc::WorkerRefRemovedSubMessage &message) {
  const ObjectID &object_id = ObjectID::FromBinary(message.reference().object_id());

  // Set a callback to publish the message when the requested object ID's ref count
  // goes to 0.
  auto ref_removed_callback =
      boost::bind(&ReferenceCounter::HandleRefRemoved, reference_counter_, object_id);

  const auto intended_worker_id = WorkerID::FromBinary(message.intended_worker_id());
  if (intended_worker_id != worker_context_.GetWorkerID()) {
    RAY_LOG(INFO) << "The ProcessSubscribeForRefRemoved message is for "
                  << intended_worker_id << ", but the current worker id is "
                  << worker_context_.GetWorkerID() << ". The RPC will be no-op.";
    ref_removed_callback(object_id);
    return;
  }

  const auto owner_address = message.reference().owner_address();
  ObjectID contained_in_id = ObjectID::FromBinary(message.contained_in_id());
  reference_counter_->SetRefRemovedCallback(
      object_id, contained_in_id, owner_address, ref_removed_callback);
}

void CoreWorker::HandleRemoteCancelTask(rpc::RemoteCancelTaskRequest request,
                                        rpc::RemoteCancelTaskReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) {
  auto status = CancelTask(ObjectID::FromBinary(request.remote_object_id()),
                           request.force_kill(),
                           request.recursive());
  send_reply_callback(status, nullptr, nullptr);
}

void CoreWorker::HandleCancelTask(rpc::CancelTaskRequest request,
                                  rpc::CancelTaskReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) {
  TaskID task_id = TaskID::FromBinary(request.intended_task_id());
  bool requested_task_running;
  {
    absl::MutexLock lock(&mutex_);
    requested_task_running = main_thread_task_id_ == task_id;
  }
  bool success = requested_task_running;

  // Try non-force kill.
  // NOTE(swang): We do not hold the CoreWorker lock here because the kill
  // callback requires the GIL, which can cause a deadlock with the main task
  // thread. This means that the currently executing task can change by the time
  // the kill callback runs; the kill callback is responsible for also making
  // sure it cancels the right task.
  // See https://github.com/ray-project/ray/issues/29739.
  if (requested_task_running && !request.force_kill()) {
    RAY_LOG(INFO) << "Cancelling a running task with id: " << task_id;
    success = options_.kill_main(task_id);
  } else if (!requested_task_running) {
    RAY_LOG(INFO) << "Cancelling a task " << task_id
                  << " that's not running. Tasks will be removed from a queue.";
    // If the task is not currently running, check if it is in the worker's queue of
    // normal tasks, and remove it if found.
    success = direct_task_receiver_->CancelQueuedNormalTask(task_id);
  }
  if (request.recursive()) {
    auto recursive_cancel = CancelChildren(task_id, request.force_kill());
    if (!recursive_cancel.ok()) {
      RAY_LOG(ERROR) << "Recursive cancel failed for a task " << task_id
                     << " due to reason: " << recursive_cancel.ToString();
    }
  }

  reply->set_attempt_succeeded(success);
  reply->set_requested_task_running(requested_task_running);
  send_reply_callback(Status::OK(), nullptr, nullptr);

  // Do force kill after reply callback sent.
  if (request.force_kill()) {
    // We grab the lock again to make sure that we are force-killing the correct
    // task. This is guaranteed not to deadlock because ForceExit should not
    // require any other locks.
    absl::MutexLock lock(&mutex_);
    if (main_thread_task_id_ == task_id) {
      ForceExit(rpc::WorkerExitType::INTENDED_USER_EXIT,
                absl::StrCat("The worker exits because the task ",
                             main_thread_task_name_,
                             " has received a force ray.cancel request."));
    }
  }
}

void CoreWorker::HandleKillActor(rpc::KillActorRequest request,
                                 rpc::KillActorReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) {
  ActorID intended_actor_id = ActorID::FromBinary(request.intended_actor_id());
  if (intended_actor_id != worker_context_.GetCurrentActorID()) {
    std::ostringstream stream;
    stream << "Mismatched ActorID: ignoring KillActor for previous actor "
           << intended_actor_id
           << ", current actor ID: " << worker_context_.GetCurrentActorID();
    const auto &msg = stream.str();
    RAY_LOG(ERROR) << msg;
    send_reply_callback(Status::Invalid(msg), nullptr, nullptr);
    return;
  }

  const auto &kill_actor_reason =
      gcs::GenErrorMessageFromDeathCause(request.death_cause());

  if (request.force_kill()) {
    RAY_LOG(INFO) << "Force kill actor request has received. exiting immediately... "
                  << kill_actor_reason;
    // If we don't need to restart this actor, we notify raylet before force killing it.
    ForceExit(
        rpc::WorkerExitType::INTENDED_SYSTEM_EXIT,
        absl::StrCat("Worker exits because the actor is killed. ", kill_actor_reason));
  } else {
    Exit(rpc::WorkerExitType::INTENDED_SYSTEM_EXIT,
         absl::StrCat("Worker exits because the actor is killed. ", kill_actor_reason));
  }
}

void CoreWorker::HandleGetCoreWorkerStats(rpc::GetCoreWorkerStatsRequest request,
                                          rpc::GetCoreWorkerStatsReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  absl::MutexLock lock(&mutex_);
  auto limit = request.has_limit() ? request.limit() : -1;
  auto stats = reply->mutable_core_worker_stats();
  // TODO(swang): Differentiate between tasks that are currently pending
  // execution and tasks that have finished but may be retried.
  stats->set_num_pending_tasks(task_manager_->NumSubmissibleTasks());
  stats->set_task_queue_length(task_queue_length_);
  stats->set_num_executed_tasks(num_executed_tasks_);
  stats->set_num_object_refs_in_scope(reference_counter_->NumObjectIDsInScope());
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
  stats->set_used_object_store_memory(memory_store_stats.num_local_objects_bytes);

  if (request.include_memory_info()) {
    reference_counter_->AddObjectRefStats(
        plasma_store_provider_->UsedObjectsList(), stats, limit);
    task_manager_->AddTaskStatusInfo(stats);
  }

  if (request.include_task_info()) {
    task_manager_->FillTaskInfo(reply, limit);
    for (const auto &current_running_task : current_tasks_) {
      reply->add_running_task_ids(current_running_task.second.TaskId().Binary());
    }
  }

  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void CoreWorker::HandleLocalGC(rpc::LocalGCRequest request,
                               rpc::LocalGCReply *reply,
                               rpc::SendReplyCallback send_reply_callback) {
  if (options_.gc_collect != nullptr) {
    options_.gc_collect(request.triggered_by_global_gc());
    send_reply_callback(Status::OK(), nullptr, nullptr);
  } else {
    send_reply_callback(
        Status::NotImplemented("GC callback not defined"), nullptr, nullptr);
  }
}

void CoreWorker::HandleSpillObjects(rpc::SpillObjectsRequest request,
                                    rpc::SpillObjectsReply *reply,
                                    rpc::SendReplyCallback send_reply_callback) {
  if (options_.spill_objects != nullptr) {
    auto object_refs =
        VectorFromProtobuf<rpc::ObjectReference>(request.object_refs_to_spill());
    std::vector<std::string> object_urls = options_.spill_objects(object_refs);
    for (size_t i = 0; i < object_urls.size(); i++) {
      reply->add_spilled_objects_url(std::move(object_urls[i]));
    }
    send_reply_callback(Status::OK(), nullptr, nullptr);
  } else {
    send_reply_callback(
        Status::NotImplemented("Spill objects callback not defined"), nullptr, nullptr);
  }
}

void CoreWorker::HandleRestoreSpilledObjects(rpc::RestoreSpilledObjectsRequest request,
                                             rpc::RestoreSpilledObjectsReply *reply,
                                             rpc::SendReplyCallback send_reply_callback) {
  if (options_.restore_spilled_objects != nullptr) {
    // Get a list of object ids.
    std::vector<rpc::ObjectReference> object_refs_to_restore;
    object_refs_to_restore.reserve(request.object_ids_to_restore_size());
    for (const auto &id_binary : request.object_ids_to_restore()) {
      rpc::ObjectReference ref;
      ref.set_object_id(id_binary);
      object_refs_to_restore.push_back(std::move(ref));
    }
    // Get a list of spilled_object_urls.
    std::vector<std::string> spilled_objects_url;
    spilled_objects_url.reserve(request.spilled_objects_url_size());
    for (const auto &url : request.spilled_objects_url()) {
      spilled_objects_url.push_back(url);
    }
    auto total =
        options_.restore_spilled_objects(object_refs_to_restore, spilled_objects_url);
    reply->set_bytes_restored_total(total);
    send_reply_callback(Status::OK(), nullptr, nullptr);
  } else {
    send_reply_callback(
        Status::NotImplemented("Restore spilled objects callback not defined"),
        nullptr,
        nullptr);
  }
}

void CoreWorker::HandleDeleteSpilledObjects(rpc::DeleteSpilledObjectsRequest request,
                                            rpc::DeleteSpilledObjectsReply *reply,
                                            rpc::SendReplyCallback send_reply_callback) {
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
        Status::NotImplemented("Delete spilled objects callback not defined"),
        nullptr,
        nullptr);
  }
}

void CoreWorker::HandleExit(rpc::ExitRequest request,
                            rpc::ExitReply *reply,
                            rpc::SendReplyCallback send_reply_callback) {
  bool own_objects = reference_counter_->OwnObjects();
  int64_t pins_in_flight = local_raylet_client_->GetPinsInFlight();
  // We consider the worker to be idle if it doesn't own any objects and it doesn't have
  // any object pinning RPCs in flight.
  bool is_idle = !own_objects && pins_in_flight == 0;
  reply->set_success(is_idle);
  send_reply_callback(
      Status::OK(),
      [this, is_idle]() {
        // If the worker is idle, we exit.
        if (is_idle) {
          Exit(rpc::WorkerExitType::INTENDED_SYSTEM_EXIT,
               "Worker exits because it was idle (it doesn't have objects it owns while "
               "no task or actor has been scheduled) for a long time.");
        }
      },
      // We need to kill it regardless if the RPC failed.
      [this]() {
        Exit(rpc::WorkerExitType::INTENDED_SYSTEM_EXIT,
             "Worker exits because it was idle (it doesn't have objects it owns while "
             "no task or actor has been scheduled) for a long time.");
      });
}

void CoreWorker::HandleAssignObjectOwner(rpc::AssignObjectOwnerRequest request,
                                         rpc::AssignObjectOwnerReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  const auto &borrower_address = request.borrower_address();
  std::string call_site = request.call_site();
  // Get a list of contained object ids.
  std::vector<ObjectID> contained_object_ids;
  contained_object_ids.reserve(request.contained_object_ids_size());
  for (const auto &id_binary : request.contained_object_ids()) {
    contained_object_ids.push_back(ObjectID::FromBinary(id_binary));
  }
  reference_counter_->AddOwnedObject(
      object_id,
      contained_object_ids,
      rpc_address_,
      call_site,
      request.object_size(),
      /*is_reconstructable=*/false,
      /*add_local_ref=*/false,
      /*pinned_at_raylet_id=*/NodeID::FromBinary(borrower_address.raylet_id()));
  reference_counter_->AddBorrowerAddress(object_id, borrower_address);
  RAY_CHECK(memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA), object_id));
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void CoreWorker::YieldCurrentFiber(FiberEvent &event) {
  RAY_CHECK(worker_context_.CurrentActorIsAsync());
  boost::this_fiber::yield();
  event.Wait();
}

void CoreWorker::GetAsync(const ObjectID &object_id,
                          SetResultCallback success_callback,
                          void *python_future) {
  auto fallback_callback = std::bind(&CoreWorker::PlasmaCallback,
                                     this,
                                     success_callback,
                                     std::placeholders::_1,
                                     std::placeholders::_2,
                                     std::placeholders::_3);

  memory_store_->GetAsync(object_id,
                          [python_future, success_callback, fallback_callback, object_id](
                              std::shared_ptr<RayObject> ray_object) {
                            if (ray_object->IsInPlasmaError()) {
                              fallback_callback(ray_object, object_id, python_future);
                            } else {
                              success_callback(ray_object, object_id, python_future);
                            }
                          });
}

void CoreWorker::PlasmaCallback(SetResultCallback success,
                                std::shared_ptr<RayObject> ray_object,
                                ObjectID object_id,
                                void *py_future) {
  RAY_CHECK(ray_object->IsInPlasmaError());

  // First check if the object is available in local plasma store.
  // Note that we are using Contains instead of Get so it won't trigger pull request
  // to remote nodes.
  bool object_is_local = false;
  if (Contains(object_id, &object_is_local).ok() && object_is_local) {
    std::vector<std::shared_ptr<RayObject>> vec;
    if (Get(std::vector<ObjectID>{object_id}, 0, &vec).ok()) {
      RAY_CHECK(vec.size() > 0)
          << "Failed to get local object but Raylet notified object is local.";
      return success(vec.front(), object_id, py_future);
    }
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

void CoreWorker::HandlePlasmaObjectReady(rpc::PlasmaObjectReadyRequest request,
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

bool CoreWorker::IsExiting() const { return exiting_; }

std::unordered_map<std::string, std::vector<int64_t>> CoreWorker::GetActorCallStats()
    const {
  return task_counter_.AsMap();
}

Status CoreWorker::WaitForActorRegistered(const std::vector<ObjectID> &ids) {
  std::vector<ActorID> actor_ids;
  for (const auto &id : ids) {
    if (ObjectID::IsActorID(id)) {
      actor_ids.emplace_back(ObjectID::ToActorID(id));
    }
  }
  if (actor_ids.empty()) {
    return Status::OK();
  }
  std::promise<void> promise;
  auto future = promise.get_future();
  std::vector<Status> ret;
  int counter = 0;
  // Post to service pool to avoid mutex
  io_service_.post(
      [&, this]() {
        for (const auto &id : actor_ids) {
          if (actor_creator_->IsActorInRegistering(id)) {
            ++counter;
            actor_creator_->AsyncWaitForActorRegisterFinish(
                id, [&counter, &promise, &ret](Status status) {
                  ret.push_back(status);
                  --counter;
                  if (counter == 0) {
                    promise.set_value();
                  }
                });
          }
        }
        if (counter == 0) {
          promise.set_value();
        }
      },
      "CoreWorker.WaitForActorRegistered");
  future.wait();
  for (const auto &s : ret) {
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

std::vector<ObjectID> CoreWorker::GetCurrentReturnIds(int num_returns,
                                                      const ActorID &callee_actor_id) {
  std::vector<ObjectID> return_ids(num_returns);
  const auto next_task_index = worker_context_.GetTaskIndex() + 1;
  TaskID task_id;
  if (callee_actor_id.IsNil()) {
    /// Return ids for normal task call.
    task_id = TaskID::ForNormalTask(worker_context_.GetCurrentJobID(),
                                    worker_context_.GetCurrentInternalTaskId(),
                                    next_task_index);
  } else {
    /// Return ids for actor task call.
    task_id = TaskID::ForActorTask(worker_context_.GetCurrentJobID(),
                                   worker_context_.GetCurrentInternalTaskId(),
                                   next_task_index,
                                   callee_actor_id);
  }
  for (int i = 0; i < num_returns; i++) {
    return_ids[i] = ObjectID::FromIndex(task_id, i + 1);
  }
  return return_ids;
}

}  // namespace core
}  // namespace ray
