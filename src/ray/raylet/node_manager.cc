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

#include "ray/raylet/node_manager.h"

#include <cctype>
#include <fstream>
#include <memory>
#include "boost/filesystem.hpp"
#include "boost/system/error_code.hpp"
#include "ray/common/asio/asio_util.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/buffer.h"
#include "ray/common/common_protocol.h"
#include "ray/common/constants.h"
#include "ray/common/status.h"
#include "ray/gcs/pb_util.h"
#include "ray/raylet/format/node_manager_generated.h"
#include "ray/stats/stats.h"
#include "ray/util/sample.h"
#include "ray/util/util.h"

namespace {

#define RAY_CHECK_ENUM(x, y) \
  static_assert(static_cast<int>(x) == static_cast<int>(y), "protocol mismatch")

struct ActorStats {
  int live_actors = 0;
  int dead_actors = 0;
  int restarting_actors = 0;
};

inline ray::rpc::ObjectReference FlatbufferToSingleObjectReference(
    const flatbuffers::String &object_id, const ray::protocol::Address &address) {
  ray::rpc::ObjectReference ref;
  ref.set_object_id(object_id.str());
  ref.mutable_owner_address()->set_raylet_id(address.raylet_id()->str());
  ref.mutable_owner_address()->set_ip_address(address.ip_address()->str());
  ref.mutable_owner_address()->set_port(address.port());
  ref.mutable_owner_address()->set_worker_id(address.worker_id()->str());
  return ref;
}

std::vector<ray::rpc::ObjectReference> FlatbufferToObjectReference(
    const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>> &object_ids,
    const flatbuffers::Vector<flatbuffers::Offset<ray::protocol::Address>>
        &owner_addresses) {
  RAY_CHECK(object_ids.size() == owner_addresses.size());
  std::vector<ray::rpc::ObjectReference> refs;
  for (int64_t i = 0; i < object_ids.size(); i++) {
    ray::rpc::ObjectReference ref;
    ref.set_object_id(object_ids.Get(i)->str());
    const auto &addr = owner_addresses.Get(i);
    ref.mutable_owner_address()->set_raylet_id(addr->raylet_id()->str());
    ref.mutable_owner_address()->set_ip_address(addr->ip_address()->str());
    ref.mutable_owner_address()->set_port(addr->port());
    ref.mutable_owner_address()->set_worker_id(addr->worker_id()->str());
    refs.emplace_back(std::move(ref));
  }
  return refs;
}

}  // namespace

namespace ray {

namespace raylet {

// A helper function to print the leased workers.
std::string LeasedWorkersSring(
    const std::unordered_map<WorkerID, std::shared_ptr<WorkerInterface>>
        &leased_workers) {
  std::stringstream buffer;
  buffer << "  @leased_workers: (";
  for (const auto &pair : leased_workers) {
    auto &worker = pair.second;
    buffer << worker->WorkerId() << ", ";
  }
  buffer << ")";
  return buffer.str();
}

// A helper function to print the workers in worker_pool_.
std::string WorkerPoolString(
    const std::vector<std::shared_ptr<WorkerInterface>> &worker_pool) {
  std::stringstream buffer;
  buffer << "   @worker_pool: (";
  for (const auto &worker : worker_pool) {
    buffer << worker->WorkerId() << ", ";
  }
  buffer << ")";
  return buffer.str();
}

// Helper function to print the worker's owner worker and and node owner.
std::string WorkerOwnerString(std::shared_ptr<WorkerInterface> &worker) {
  std::stringstream buffer;
  const auto owner_worker_id =
      WorkerID::FromBinary(worker->GetOwnerAddress().worker_id());
  const auto owner_node_id = WorkerID::FromBinary(worker->GetOwnerAddress().raylet_id());
  buffer << "leased_worker Lease " << worker->WorkerId() << " owned by "
         << owner_worker_id << " / " << owner_node_id;
  return buffer.str();
}

HeartbeatSender::HeartbeatSender(NodeID self_node_id,
                                 std::shared_ptr<gcs::GcsClient> gcs_client)
    : self_node_id_(self_node_id), gcs_client_(gcs_client) {
  // Init heartbeat thread and run its io service.
  heartbeat_thread_.reset(new std::thread([this] {
    SetThreadName("heartbeat");
    /// The asio work to keep io_service_ alive.
    boost::asio::io_service::work io_service_work_(heartbeat_io_service_);
    heartbeat_io_service_.run();
  }));
  heartbeat_runner_.reset(new PeriodicalRunner(heartbeat_io_service_));

  // Start sending heartbeats to the GCS.
  last_heartbeat_at_ms_ = current_time_ms();
  heartbeat_runner_->RunFnPeriodically(
      [this] { Heartbeat(); },
      RayConfig::instance().raylet_heartbeat_period_milliseconds(),
      "NodeManager.deadline_timer.heartbeat");
}

HeartbeatSender::~HeartbeatSender() {
  heartbeat_runner_.reset();
  heartbeat_io_service_.stop();
  if (heartbeat_thread_->joinable()) {
    heartbeat_thread_->join();
  }
  heartbeat_thread_.reset();
}

void HeartbeatSender::Heartbeat() {
  uint64_t now_ms = current_time_ms();
  uint64_t interval = now_ms - last_heartbeat_at_ms_;
  if (interval > RayConfig::instance().num_heartbeats_warning() *
                     RayConfig::instance().raylet_heartbeat_period_milliseconds()) {
    RAY_LOG(WARNING)
        << "Last heartbeat was sent " << interval
        << " ms ago. There might be resource pressure on this node. If heartbeat keeps "
           "lagging, this node can be marked as dead mistakenly.";
  }
  last_heartbeat_at_ms_ = now_ms;
  stats::HeartbeatReportMs.Record(interval);

  auto heartbeat_data = std::make_shared<HeartbeatTableData>();
  heartbeat_data->set_node_id(self_node_id_.Binary());
  RAY_CHECK_OK(
      gcs_client_->Nodes().AsyncReportHeartbeat(heartbeat_data, [](Status status) {
        if (status.IsDisconnected()) {
          RAY_LOG(FATAL) << "This node has beem marked as dead.";
        }
      }));
}

NodeManager::NodeManager(instrumented_io_context &io_service, const NodeID &self_node_id,
                         const NodeManagerConfig &config,
                         const ObjectManagerConfig &object_manager_config,
                         std::shared_ptr<gcs::GcsClient> gcs_client,
                         std::shared_ptr<ObjectDirectoryInterface> object_directory)
    : self_node_id_(self_node_id),
      io_service_(io_service),
      object_manager_(
          io_service, self_node_id, object_manager_config, object_directory,
          [this](const ObjectID &object_id, const std::string &object_url,
                 std::function<void(const ray::Status &)> callback) {
            GetLocalObjectManager().AsyncRestoreSpilledObject(object_id, object_url,
                                                              callback);
          },
          [this](const ObjectID &object_id) {
            return GetLocalObjectManager().GetSpilledObjectURL(object_id);
          },
          [this]() {
            // This callback is called from the plasma store thread.
            // NOTE: It means the local object manager should be thread-safe.
            io_service_.post(
                [this]() { GetLocalObjectManager().SpillObjectUptoMaxThroughput(); },
                "NodeManager.SpillObjects");
            return GetLocalObjectManager().IsSpillingInProgress();
          },
          [this]() {
            // Post on the node manager's event loop since this
            // callback is called from the plasma store thread.
            // This will help keep node manager lock-less.
            io_service_.post([this]() { TriggerGlobalGC(); }, "NodeManager.GlobalGC");
          },
          /*add_object_callback=*/
          [this](const ObjectInfo &object_info) { HandleObjectLocal(object_info); },
          /*delete_object_callback=*/
          [this](const ObjectID &object_id) { HandleObjectMissing(object_id); }),
      gcs_client_(gcs_client),
      object_directory_(object_directory),
      periodical_runner_(io_service),
      report_resources_period_ms_(config.report_resources_period_ms),
      fair_queueing_enabled_(config.fair_queueing_enabled),
      temp_dir_(config.temp_dir),
      initial_config_(config),
      worker_pool_(io_service, self_node_id_, config.node_manager_address,
                   config.num_workers_soft_limit,
                   config.num_initial_python_workers_for_first_job,
                   config.maximum_startup_concurrency, config.min_worker_port,
                   config.max_worker_port, config.worker_ports, gcs_client_,
                   config.worker_commands,
                   /*starting_worker_timeout_callback=*/
                   [this] { cluster_task_manager_->ScheduleAndDispatchTasks(); },
                   /*get_time=*/[]() { return absl::GetCurrentTimeNanos() / 1e6; }),
      dependency_manager_(object_manager_),
      node_manager_server_("NodeManager", config.node_manager_port),
      node_manager_service_(io_service, *this),
      agent_manager_service_handler_(
          new DefaultAgentManagerServiceHandler(agent_manager_)),
      agent_manager_service_(io_service, *agent_manager_service_handler_),
      client_call_manager_(io_service),
      worker_rpc_pool_(client_call_manager_),
      local_object_manager_(
          self_node_id_, config.node_manager_address, config.node_manager_port,
          RayConfig::instance().free_objects_batch_size(),
          RayConfig::instance().free_objects_period_milliseconds(), worker_pool_,
          gcs_client_->Objects(), worker_rpc_pool_,
          /* automatic_object_deletion_enabled */
          config.automatic_object_deletion_enabled,
          /*max_io_workers*/ config.max_io_workers,
          /*min_spilling_size*/ config.min_spilling_size,
          /*is_external_storage_type_fs*/
          RayConfig::instance().is_external_storage_type_fs(),
          /*max_fused_object_count*/ RayConfig::instance().max_fused_object_count(),
          /*on_objects_freed*/
          [this](const std::vector<ObjectID> &object_ids) {
            object_manager_.FreeObjects(object_ids,
                                        /*local_only=*/false);
          },
          /*is_plasma_object_spillable*/
          [this](const ObjectID &object_id) {
            return object_manager_.IsPlasmaObjectSpillable(object_id);
          },
          /*core_worker_subscriber_=*/
          std::make_shared<pubsub::Subscriber>(self_node_id_, config.node_manager_address,
                                               config.node_manager_port,
                                               worker_rpc_pool_)),
      last_local_gc_ns_(absl::GetCurrentTimeNanos()),
      local_gc_interval_ns_(RayConfig::instance().local_gc_interval_s() * 1e9),
      local_gc_min_interval_ns_(RayConfig::instance().local_gc_min_interval_s() * 1e9),
      record_metrics_period_ms_(config.record_metrics_period_ms),
      runtime_env_manager_([this](const std::string &uri, std::function<void(bool)> cb) {
        return DeleteLocalURI(uri, cb);
      }) {
  RAY_LOG(INFO) << "Initializing NodeManager with ID " << self_node_id_;
  RAY_CHECK(RayConfig::instance().raylet_heartbeat_period_milliseconds() > 0);
  SchedulingResources local_resources(config.resource_config);
  cluster_resource_scheduler_ =
      std::shared_ptr<ClusterResourceScheduler>(new ClusterResourceScheduler(
          self_node_id_.Binary(), local_resources.GetTotalResources().GetResourceMap(),
          [this]() { return object_manager_.GetUsedMemory(); }));

  auto get_node_info_func = [this](const NodeID &node_id) {
    return gcs_client_->Nodes().Get(node_id);
  };
  auto is_owner_alive = [this](const WorkerID &owner_worker_id,
                               const NodeID &owner_node_id) {
    return !(failed_workers_cache_.count(owner_worker_id) > 0 ||
             failed_nodes_cache_.count(owner_node_id) > 0);
  };
  auto announce_infeasible_task = [this](const Task &task) {
    PublishInfeasibleTaskError(task);
  };
  RAY_CHECK(RayConfig::instance().max_task_args_memory_fraction() > 0 &&
            RayConfig::instance().max_task_args_memory_fraction() <= 1)
      << "max_task_args_memory_fraction must be a nonzero fraction.";
  int64_t max_task_args_memory = object_manager_.GetMemoryCapacity() *
                                 RayConfig::instance().max_task_args_memory_fraction();
  RAY_CHECK(max_task_args_memory > 0);
  cluster_task_manager_ = std::shared_ptr<ClusterTaskManager>(new ClusterTaskManager(
      self_node_id_,
      std::dynamic_pointer_cast<ClusterResourceScheduler>(cluster_resource_scheduler_),
      dependency_manager_, is_owner_alive, get_node_info_func, announce_infeasible_task,
      worker_pool_, leased_workers_,
      [this](const std::vector<ObjectID> &object_ids,
             std::vector<std::unique_ptr<RayObject>> *results) {
        return GetObjectsFromPlasma(object_ids, results);
      },
      max_task_args_memory));
  placement_group_resource_manager_ = std::make_shared<NewPlacementGroupResourceManager>(
      std::dynamic_pointer_cast<ClusterResourceScheduler>(cluster_resource_scheduler_));

  RAY_CHECK_OK(store_client_.Connect(config.store_socket_name.c_str()));
  // Run the node manger rpc server.
  node_manager_server_.RegisterService(node_manager_service_);
  node_manager_server_.RegisterService(agent_manager_service_);
  node_manager_server_.Run();

  worker_pool_.SetNodeManagerPort(GetServerPort());

  auto agent_command_line = ParseCommandLine(config.agent_command);
  for (auto &arg : agent_command_line) {
    auto node_manager_port_position = arg.find(kNodeManagerPortPlaceholder);
    if (node_manager_port_position != std::string::npos) {
      arg.replace(node_manager_port_position, strlen(kNodeManagerPortPlaceholder),
                  std::to_string(GetServerPort()));
    }
  }

  auto options = AgentManager::Options({self_node_id, agent_command_line});
  agent_manager_.reset(
      new AgentManager(std::move(options),
                       /*delay_executor=*/
                       [this](std::function<void()> task, uint32_t delay_ms) {
                         return execute_after(io_service_, task, delay_ms);
                       }));
}

ray::Status NodeManager::RegisterGcs() {
  // Start sending heartbeat here to ensure it happening after raylet being registered.
  heartbeat_sender_.reset(new HeartbeatSender(self_node_id_, gcs_client_));
  auto on_node_change = [this](const NodeID &node_id, const GcsNodeInfo &data) {
    if (data.state() == GcsNodeInfo::ALIVE) {
      NodeAdded(data);
    } else {
      RAY_CHECK(data.state() == GcsNodeInfo::DEAD);
      NodeRemoved(node_id);
    }
  };

  // If the node resource message is received first and then the node message is received,
  // ForwardTask will throw exception, because it can't get node info.
  auto on_done = [this](Status status) {
    RAY_CHECK_OK(status);
    // Subscribe to resource changes.
    const auto &resources_changed =
        [this](const rpc::NodeResourceChange &resource_notification) {
          auto id = NodeID::FromBinary(resource_notification.node_id());
          if (resource_notification.updated_resources_size() != 0) {
            ResourceSet resource_set(
                MapFromProtobuf(resource_notification.updated_resources()));
            ResourceCreateUpdated(id, resource_set);
          }

          if (resource_notification.deleted_resources_size() != 0) {
            ResourceDeleted(
                id, VectorFromProtobuf(resource_notification.deleted_resources()));
          }
        };
    RAY_CHECK_OK(gcs_client_->NodeResources().AsyncSubscribeToResources(
        /*subscribe_callback=*/resources_changed,
        /*done_callback=*/nullptr));
  };
  // Register a callback to monitor new nodes and a callback to monitor removed nodes.
  RAY_RETURN_NOT_OK(
      gcs_client_->Nodes().AsyncSubscribeToNodeChange(on_node_change, on_done));

  // Subscribe to resource usage batches from the monitor.
  const auto &resource_usage_batch_added =
      [this](const ResourceUsageBatchData &resource_usage_batch) {
        ResourceUsageBatchReceived(resource_usage_batch);
      };
  RAY_RETURN_NOT_OK(gcs_client_->NodeResources().AsyncSubscribeBatchedResourceUsage(
      resource_usage_batch_added, /*done*/ nullptr));

  // Subscribe to all unexpected failure notifications from the local and
  // remote raylets. Note that this does not include workers that failed due to
  // node failure. These workers can be identified by comparing the raylet_id
  // in their rpc::Address to the ID of a failed raylet.
  const auto &worker_failure_handler =
      [this](const rpc::WorkerDeltaData &worker_failure_data) {
        HandleUnexpectedWorkerFailure(worker_failure_data);
      };
  RAY_CHECK_OK(gcs_client_->Workers().AsyncSubscribeToWorkerFailures(
      worker_failure_handler, /*done_callback=*/nullptr));

  // Subscribe to job updates.
  const auto job_subscribe_handler = [this](const JobID &job_id,
                                            const JobTableData &job_data) {
    if (!job_data.is_dead()) {
      HandleJobStarted(job_id, job_data);
    } else {
      HandleJobFinished(job_id, job_data);
    }
  };
  RAY_RETURN_NOT_OK(
      gcs_client_->Jobs().AsyncSubscribeAll(job_subscribe_handler, nullptr));

  periodical_runner_.RunFnPeriodically(
      [this] {
        DumpDebugState();
        WarnResourceDeadlock();
      },
      RayConfig::instance().debug_dump_period_milliseconds(),
      "NodeManager.deadline_timer.debug_state_dump");
  uint64_t now_ms = current_time_ms();
  last_metrics_recorded_at_ms_ = now_ms;
  periodical_runner_.RunFnPeriodically([this] { RecordMetrics(); },
                                       record_metrics_period_ms_,
                                       "NodeManager.deadline_timer.record_metrics");
  if (RayConfig::instance().free_objects_period_milliseconds() > 0) {
    periodical_runner_.RunFnPeriodically(
        [this] { local_object_manager_.FlushFreeObjects(); },
        RayConfig::instance().free_objects_period_milliseconds(),
        "NodeManager.deadline_timer.flush_free_objects");
  }
  last_resource_report_at_ms_ = now_ms;
  periodical_runner_.RunFnPeriodically(
      [this] { ReportResourceUsage(); }, report_resources_period_ms_,
      "NodeManager.deadline_timer.report_resource_usage");
  // Start the timer that gets object manager profiling information and sends it
  // to the GCS.
  periodical_runner_.RunFnPeriodically(
      [this] { GetObjectManagerProfileInfo(); },
      RayConfig::instance().raylet_heartbeat_period_milliseconds(),
      "NodeManager.deadline_timer.object_manager_profiling");

  /// If periodic asio stats print is enabled, it will print it.
  const auto asio_stats_print_interval_ms =
      RayConfig::instance().asio_stats_print_interval_ms();
  if (asio_stats_print_interval_ms != -1 &&
      RayConfig::instance().asio_event_loop_stats_collection_enabled()) {
    periodical_runner_.RunFnPeriodically(
        [this] {
          RAY_LOG(INFO) << "Event loop stats:\n\n" << io_service_.StatsString() << "\n\n";
        },
        asio_stats_print_interval_ms,
        "NodeManager.deadline_timer.print_event_loop_stats");
  }

  return ray::Status::OK();
}

void NodeManager::KillWorker(std::shared_ptr<WorkerInterface> worker) {
#ifdef _WIN32
  // TODO(mehrdadn): implement graceful process termination mechanism
#else
  // If we're just cleaning up a single worker, allow it some time to clean
  // up its state before force killing. The client socket will be closed
  // and the worker struct will be freed after the timeout.
  kill(worker->GetProcess().GetId(), SIGTERM);
#endif

  auto retry_timer = std::make_shared<boost::asio::deadline_timer>(io_service_);
  auto retry_duration = boost::posix_time::milliseconds(
      RayConfig::instance().kill_worker_timeout_milliseconds());
  retry_timer->expires_from_now(retry_duration);
  retry_timer->async_wait([retry_timer, worker](const boost::system::error_code &error) {
    RAY_LOG(DEBUG) << "Send SIGKILL to worker, pid=" << worker->GetProcess().GetId();
    // Force kill worker
    worker->GetProcess().Kill();
  });
}

void NodeManager::DestroyWorker(std::shared_ptr<WorkerInterface> worker,
                                rpc::WorkerExitType disconnect_type) {
  // We should disconnect the client first. Otherwise, we'll remove bundle resources
  // before actual resources are returned. Subsequent disconnect request that comes
  // due to worker dead will be ignored.
  DisconnectClient(worker->Connection(), disconnect_type);
  worker->MarkDead();
  KillWorker(worker);
}

void NodeManager::HandleJobStarted(const JobID &job_id, const JobTableData &job_data) {
  RAY_LOG(DEBUG) << "HandleJobStarted " << job_id;
  RAY_CHECK(!job_data.is_dead());

  worker_pool_.HandleJobStarted(job_id, job_data.config());
  runtime_env_manager_.AddUriReference(job_id.Hex(), job_data.config().runtime_env());
  // Tasks of this job may already arrived but failed to pop a worker because the job
  // config is not local yet. So we trigger dispatching again here to try to
  // reschedule these tasks.
  cluster_task_manager_->ScheduleAndDispatchTasks();
}

void NodeManager::HandleJobFinished(const JobID &job_id, const JobTableData &job_data) {
  RAY_LOG(DEBUG) << "HandleJobFinished " << job_id;
  RAY_CHECK(job_data.is_dead());
  worker_pool_.HandleJobFinished(job_id);

  auto workers = worker_pool_.GetWorkersRunningTasksForJob(job_id);
  // Kill all the workers. The actual cleanup for these workers is done
  // later when we receive the DisconnectClient message from them.
  for (const auto &worker : workers) {
    if (!worker->IsDetachedActor()) {
      // Clean up any open ray.wait calls that the worker made.
      dependency_manager_.CancelWaitRequest(worker->WorkerId());
      // Mark the worker as dead so further messages from it are ignored
      // (except DisconnectClient).
      worker->MarkDead();
      // Then kill the worker process.
      KillWorker(worker);
    }
  }
  runtime_env_manager_.RemoveUriReference(job_id.Hex());
}

void NodeManager::FillResourceReport(rpc::ResourcesData &resources_data) {
  resources_data.set_node_id(self_node_id_.Binary());
  resources_data.set_node_manager_address(initial_config_.node_manager_address);
  // Update local cache from gcs remote cache, this is needed when gcs restart.
  // We should always keep the cache view consistent.
  cluster_resource_scheduler_->UpdateLastResourceUsage(
      gcs_client_->NodeResources().GetLastResourceUsage());
  cluster_resource_scheduler_->FillResourceUsage(resources_data);
  cluster_task_manager_->FillResourceUsage(resources_data);

  // Set the global gc bit on the outgoing heartbeat message.
  if (should_global_gc_) {
    resources_data.set_should_global_gc(true);
    resources_data.set_should_global_gc(true);
    should_global_gc_ = false;
  }

  // Trigger local GC if needed. This throttles the frequency of local GC calls
  // to at most once per heartbeat interval.
  auto now = absl::GetCurrentTimeNanos();
  if ((should_local_gc_ || now - last_local_gc_ns_ > local_gc_interval_ns_) &&
      now - last_local_gc_ns_ > local_gc_min_interval_ns_) {
    DoLocalGC();
    should_local_gc_ = false;
    last_local_gc_ns_ = now;
  }
}

void NodeManager::ReportResourceUsage() {
  if (initial_config_.pull_based_resource_reporting) {
    return;
  }
  uint64_t now_ms = current_time_ms();
  uint64_t interval = now_ms - last_resource_report_at_ms_;
  if (interval >
      RayConfig::instance().num_resource_report_periods_warning() *
          RayConfig::instance().raylet_report_resources_period_milliseconds()) {
    RAY_LOG(WARNING)
        << "Last resource report was sent " << interval
        << " ms ago. There might be resource pressure on this node. If "
           "resource reports keep lagging, scheduling decisions of other nodes "
           "may become stale";
  }
  last_resource_report_at_ms_ = now_ms;
  auto resources_data = std::make_shared<rpc::ResourcesData>();
  FillResourceReport(*resources_data);

  if (resources_data->resources_total_size() > 0 ||
      resources_data->resources_available_changed() ||
      resources_data->resource_load_changed() || resources_data->should_global_gc()) {
    RAY_CHECK_OK(gcs_client_->NodeResources().AsyncReportResourceUsage(resources_data,
                                                                       /*done*/ nullptr));
  }
}

void NodeManager::DoLocalGC() {
  auto all_workers = worker_pool_.GetAllRegisteredWorkers();
  for (const auto &driver : worker_pool_.GetAllRegisteredDrivers()) {
    all_workers.push_back(driver);
  }
  RAY_LOG(INFO) << "Sending Python GC request to " << all_workers.size()
                << " local workers to clean up Python cyclic references.";
  for (const auto &worker : all_workers) {
    rpc::LocalGCRequest request;
    worker->rpc_client()->LocalGC(
        request, [](const ray::Status &status, const rpc::LocalGCReply &r) {
          if (!status.ok()) {
            RAY_LOG(DEBUG) << "Failed to send local GC request: " << status.ToString();
          }
        });
  }
}

void NodeManager::HandleRequestObjectSpillage(
    const rpc::RequestObjectSpillageRequest &request,
    rpc::RequestObjectSpillageReply *reply, rpc::SendReplyCallback send_reply_callback) {
  const auto &object_id = ObjectID::FromBinary(request.object_id());
  RAY_LOG(DEBUG) << "Received RequestObjectSpillage for object " << object_id;
  local_object_manager_.SpillObjects(
      {object_id}, [object_id, reply, send_reply_callback](const ray::Status &status) {
        if (status.ok()) {
          RAY_LOG(DEBUG) << "Object " << object_id
                         << " has been spilled, replying to owner";
          reply->set_success(true);
          // TODO(Clark): Add spilled URLs and spilled node ID to owner RPC reply here
          // if OBOD is enabled, instead of relying on automatic raylet spilling path to
          // send an extra RPC to the owner.
        }
        send_reply_callback(Status::OK(), nullptr, nullptr);
      });
}

void NodeManager::HandleReleaseUnusedBundles(
    const rpc::ReleaseUnusedBundlesRequest &request,
    rpc::ReleaseUnusedBundlesReply *reply, rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Releasing unused bundles.";
  std::unordered_set<BundleID, pair_hash> in_use_bundles;
  for (int index = 0; index < request.bundles_in_use_size(); ++index) {
    const auto &bundle_id = request.bundles_in_use(index).bundle_id();
    in_use_bundles.emplace(
        std::make_pair(PlacementGroupID::FromBinary(bundle_id.placement_group_id()),
                       bundle_id.bundle_index()));
  }

  // Kill all workers that are currently associated with the unused bundles.
  // NOTE: We can't traverse directly with `leased_workers_`, because `DestroyWorker` will
  // delete the element of `leased_workers_`. So we need to filter out
  // `workers_associated_with_unused_bundles` separately.
  std::vector<std::shared_ptr<WorkerInterface>> workers_associated_with_unused_bundles;
  for (const auto &worker_it : leased_workers_) {
    auto &worker = worker_it.second;
    const auto &bundle_id = worker->GetBundleId();
    // We need to filter out the workers used by placement group.
    if (!bundle_id.first.IsNil() && 0 == in_use_bundles.count(bundle_id)) {
      workers_associated_with_unused_bundles.emplace_back(worker);
    }
  }

  for (const auto &worker : workers_associated_with_unused_bundles) {
    RAY_LOG(DEBUG)
        << "Destroying worker since its bundle was unused. Placement group id: "
        << worker->GetBundleId().first
        << ", bundle index: " << worker->GetBundleId().second
        << ", task id: " << worker->GetAssignedTaskId()
        << ", actor id: " << worker->GetActorId()
        << ", worker id: " << worker->WorkerId();
    DestroyWorker(worker, rpc::WorkerExitType::UNUSED_RESOURCE_RELEASED);
  }

  // Return unused bundle resources.
  placement_group_resource_manager_->ReturnUnusedBundle(in_use_bundles);

  send_reply_callback(Status::OK(), nullptr, nullptr);
}

// TODO(edoakes): this function is problematic because it both sends warnings spuriously
// under normal conditions and sometimes doesn't send a warning under actual deadlock
// conditions. The current logic is to push a warning when: all running tasks are
// blocked, there is at least one ready task, and a warning hasn't been pushed in
// debug_dump_period_ milliseconds.
// See https://github.com/ray-project/ray/issues/5790 for details.
void NodeManager::WarnResourceDeadlock() {
  ray::Task exemplar;
  bool any_pending = false;
  int pending_actor_creations = 0;
  int pending_tasks = 0;
  std::string available_resources;

  // Check if any progress is being made on this raylet.
  for (const auto &worker : worker_pool_.GetAllRegisteredWorkers()) {
    if (!worker->IsDead() && !worker->GetAssignedTaskId().IsNil() &&
        !worker->IsBlocked() && worker->GetActorId().IsNil()) {
      // Progress is being made in a task, don't warn.
      resource_deadlock_warned_ = 0;
      return;
    }
  }

  // Check if any tasks are blocked on resource acquisition.
  if (!cluster_task_manager_->AnyPendingTasks(&exemplar, &any_pending,
                                              &pending_actor_creations, &pending_tasks)) {
    // No pending tasks, no need to warn.
    resource_deadlock_warned_ = 0;
    return;
  }
  available_resources = cluster_resource_scheduler_->GetLocalResourceViewString();

  // Push an warning to the driver that a task is blocked trying to acquire resources.
  // To avoid spurious triggers, only take action starting with the second time.
  // case resource_deadlock_warned_:  0 => first time, don't do anything yet
  // case resource_deadlock_warned_:  1 => second time, print a warning
  // case resource_deadlock_warned_: >1 => global gc but don't print any warnings
  std::ostringstream error_message;
  if (any_pending && resource_deadlock_warned_++ > 0) {
    // Actor references may be caught in cycles, preventing them from being deleted.
    // Trigger global GC to hopefully free up resource slots.
    TriggerGlobalGC();

    // Suppress duplicates warning messages.
    if (resource_deadlock_warned_ > 2) {
      return;
    }

    error_message
        << "The actor or task with ID " << exemplar.GetTaskSpecification().TaskId()
        << " cannot be scheduled right now. It requires "
        << exemplar.GetTaskSpecification().GetRequiredPlacementResources().ToString()
        << " for placement, but this node only has remaining " << available_resources
        << ". In total there are " << pending_tasks << " pending tasks and "
        << pending_actor_creations << " pending actors on this node. "
        << "This is likely due to all cluster resources being claimed by actors. "
        << "To resolve the issue, consider creating fewer actors or increase the "
        << "resources available to this Ray cluster. You can ignore this message "
        << "if this Ray cluster is expected to auto-scale.";
    auto error_data_ptr = gcs::CreateErrorTableData(
        "resource_deadlock", error_message.str(), current_time_ms(),
        exemplar.GetTaskSpecification().JobId());
    RAY_CHECK_OK(gcs_client_->Errors().AsyncReportJobError(error_data_ptr, nullptr));
  }
  cluster_task_manager_->ScheduleAndDispatchTasks();
}

void NodeManager::GetObjectManagerProfileInfo() {
  int64_t start_time_ms = current_time_ms();

  auto profile_info = object_manager_.GetAndResetProfilingInfo();

  if (profile_info->profile_events_size() > 0) {
    RAY_CHECK_OK(gcs_client_->Stats().AsyncAddProfileData(profile_info, nullptr));
  }

  int64_t interval = current_time_ms() - start_time_ms;
  if (interval > RayConfig::instance().handler_warning_timeout_ms()) {
    RAY_LOG(WARNING) << "GetObjectManagerProfileInfo handler took " << interval << " ms.";
  }
}

void NodeManager::NodeAdded(const GcsNodeInfo &node_info) {
  const NodeID node_id = NodeID::FromBinary(node_info.node_id());

  RAY_LOG(DEBUG) << "[NodeAdded] Received callback from node id " << node_id;
  if (node_id == self_node_id_) {
    return;
  }

  // Store address of the new node manager for rpc requests.
  remote_node_manager_addresses_[node_id] =
      std::make_pair(node_info.node_manager_address(), node_info.node_manager_port());

  // Fetch resource info for the remote node and update cluster resource map.
  RAY_CHECK_OK(gcs_client_->NodeResources().AsyncGetResources(
      node_id,
      [this, node_id](
          Status status,
          const boost::optional<gcs::NodeResourceInfoAccessor::ResourceMap> &data) {
        if (data) {
          ResourceSet resource_set;
          for (auto &resource_entry : *data) {
            resource_set.AddOrUpdateResource(resource_entry.first,
                                             resource_entry.second->resource_capacity());
          }
          ResourceCreateUpdated(node_id, resource_set);
        }
      }));
}

void NodeManager::NodeRemoved(const NodeID &node_id) {
  // TODO(swang): If we receive a notification for our own death, clean up and
  // exit immediately.
  RAY_LOG(DEBUG) << "[NodeRemoved] Received callback from node id " << node_id;

  RAY_CHECK(node_id != self_node_id_)
      << "Exiting because this node manager has mistakenly been marked dead by the "
      << "monitor: GCS didn't receive heartbeats within timeout "
      << RayConfig::instance().num_heartbeats_timeout() *
             RayConfig::instance().raylet_heartbeat_period_milliseconds()
      << " ms. This is likely since the machine or raylet became overloaded.";

  // Below, when we remove node_id from all of these data structures, we could
  // check that it is actually removed, or log a warning otherwise, but that may
  // not be necessary.

  // Remove the node from the resource map.
  if (!cluster_resource_scheduler_->RemoveNode(node_id.Binary())) {
    RAY_LOG(DEBUG) << "Received NodeRemoved callback for an unknown node: " << node_id
                   << ".";
    return;
  }

  // Remove the node manager address.
  const auto node_entry = remote_node_manager_addresses_.find(node_id);
  if (node_entry != remote_node_manager_addresses_.end()) {
    remote_node_manager_addresses_.erase(node_entry);
  }

  // Notify the object directory that the node has been removed so that it
  // can remove it from any cached locations.
  object_directory_->HandleNodeRemoved(node_id);

  // Clean up workers that were owned by processes that were on the failed
  // node.
  rpc::WorkerDeltaData data;
  data.set_raylet_id(node_id.Binary());
  HandleUnexpectedWorkerFailure(data);
}

void NodeManager::HandleUnexpectedWorkerFailure(const rpc::WorkerDeltaData &data) {
  const WorkerID worker_id = WorkerID::FromBinary(data.worker_id());
  const NodeID node_id = NodeID::FromBinary(data.raylet_id());
  if (!worker_id.IsNil()) {
    RAY_LOG(DEBUG) << "Worker " << worker_id << " failed";
    failed_workers_cache_.insert(worker_id);
  } else {
    RAY_CHECK(!node_id.IsNil());
    failed_nodes_cache_.insert(node_id);
  }

  // TODO(swang): Also clean up any lease requests owned by the failed worker
  // from the task queues. This is only necessary for lease requests that are
  // infeasible, since requests that are fulfilled will get canceled during
  // dispatch.
  for (const auto &pair : leased_workers_) {
    auto &worker = pair.second;
    const auto owner_worker_id =
        WorkerID::FromBinary(worker->GetOwnerAddress().worker_id());
    const auto owner_node_id =
        WorkerID::FromBinary(worker->GetOwnerAddress().raylet_id());
    RAY_LOG(DEBUG) << "Lease " << worker->WorkerId() << " owned by " << owner_worker_id;
    RAY_CHECK(!owner_worker_id.IsNil() && !owner_node_id.IsNil());
    if (!worker->IsDetachedActor()) {
      // TODO (Alex): Cancel all pending child tasks of the tasks whose owners have failed
      // because the owner could've submitted lease requests before failing.
      if (!worker_id.IsNil()) {
        // If the failed worker was a leased worker's owner, then kill the leased worker.
        if (owner_worker_id == worker_id) {
          RAY_LOG(INFO) << "Owner process " << owner_worker_id
                        << " died, killing leased worker " << worker->WorkerId();
          KillWorker(worker);
        }
      } else if (owner_node_id == node_id) {
        // If the leased worker's owner was on the failed node, then kill the leased
        // worker.
        RAY_LOG(INFO) << "Owner node " << owner_node_id << " died, killing leased worker "
                      << worker->WorkerId();
        KillWorker(worker);
      }
    }
  }
}

void NodeManager::ResourceCreateUpdated(const NodeID &node_id,
                                        const ResourceSet &createUpdatedResources) {
  RAY_LOG(DEBUG) << "[ResourceCreateUpdated] received callback from node id " << node_id
                 << " with created or updated resources: "
                 << createUpdatedResources.ToString() << ". Updating resource map.";

  // Update local_available_resources_ and SchedulingResources
  for (const auto &resource_pair : createUpdatedResources.GetResourceMap()) {
    const std::string &resource_label = resource_pair.first;
    const double &new_resource_capacity = resource_pair.second;
    cluster_resource_scheduler_->UpdateResourceCapacity(node_id.Binary(), resource_label,
                                                        new_resource_capacity);
  }
  RAY_LOG(DEBUG) << "[ResourceCreateUpdated] Updated cluster_resource_map.";

  if (node_id == self_node_id_) {
    // The resource update is on the local node, check if we can reschedule tasks.
    cluster_task_manager_->ScheduleInfeasibleTasks();
  }
}

void NodeManager::ResourceDeleted(const NodeID &node_id,
                                  const std::vector<std::string> &resource_names) {
  if (RAY_LOG_ENABLED(DEBUG)) {
    std::ostringstream oss;
    for (auto &resource_name : resource_names) {
      oss << resource_name << ", ";
    }
    RAY_LOG(DEBUG) << "[ResourceDeleted] received callback from node id " << node_id
                   << " with deleted resources: " << oss.str()
                   << ". Updating resource map.";
  }

  // Update local_available_resources_ and SchedulingResources
  for (const auto &resource_label : resource_names) {
    cluster_resource_scheduler_->DeleteResource(node_id.Binary(), resource_label);
  }
  return;
}

void NodeManager::UpdateResourceUsage(const NodeID &node_id,
                                      const rpc::ResourcesData &resource_data) {
  if (!cluster_resource_scheduler_->UpdateNode(node_id.Binary(), resource_data)) {
    RAY_LOG(INFO)
        << "[UpdateResourceUsage]: received resource usage from unknown node id "
        << node_id;
    return;
  }

  // Trigger local GC at the next heartbeat interval.
  if (resource_data.should_global_gc()) {
    should_local_gc_ = true;
  }

  // If light resource usage report enabled, we update remote resources only when related
  // resources map in heartbeat is not empty.
  cluster_task_manager_->ScheduleAndDispatchTasks();
}

void NodeManager::ResourceUsageBatchReceived(
    const ResourceUsageBatchData &resource_usage_batch) {
  // Update load information provided by each message.
  for (const auto &resource_usage : resource_usage_batch.batch()) {
    const NodeID &node_id = NodeID::FromBinary(resource_usage.node_id());
    if (node_id == self_node_id_) {
      // Skip messages from self.
      continue;
    }
    UpdateResourceUsage(node_id, resource_usage);
  }
}

void NodeManager::ProcessNewClient(ClientConnection &client) {
  // The new client is a worker, so begin listening for messages.
  client.ProcessMessages();
}

void NodeManager::ProcessClientMessage(const std::shared_ptr<ClientConnection> &client,
                                       int64_t message_type,
                                       const uint8_t *message_data) {
  auto registered_worker = worker_pool_.GetRegisteredWorker(client);
  auto message_type_value = static_cast<protocol::MessageType>(message_type);
  RAY_LOG(DEBUG) << "[Worker] Message "
                 << protocol::EnumNameMessageType(message_type_value) << "("
                 << message_type << ") from worker with PID "
                 << (registered_worker
                         ? std::to_string(registered_worker->GetProcess().GetId())
                         : "nil");

  if (registered_worker && registered_worker->IsDead()) {
    // For a worker that is marked as dead (because the job has died already),
    // all the messages are ignored except DisconnectClient.
    if (message_type_value != protocol::MessageType::DisconnectClient) {
      // Listen for more messages.
      client->ProcessMessages();
      return;
    }
  }

  switch (message_type_value) {
  case protocol::MessageType::RegisterClientRequest: {
    ProcessRegisterClientRequestMessage(client, message_data);
  } break;
  case protocol::MessageType::AnnounceWorkerPort: {
    ProcessAnnounceWorkerPortMessage(client, message_data);
  } break;
  case protocol::MessageType::TaskDone: {
    HandleWorkerAvailable(client);
  } break;
  case protocol::MessageType::DisconnectClient: {
    ProcessDisconnectClientMessage(client, message_data);
    // We don't need to receive future messages from this client,
    // because it's already disconnected.
    return;
  } break;
  case protocol::MessageType::FetchOrReconstruct: {
    ProcessFetchOrReconstructMessage(client, message_data);
  } break;
  case protocol::MessageType::NotifyDirectCallTaskBlocked: {
    ProcessDirectCallTaskBlocked(client, message_data);
  } break;
  case protocol::MessageType::NotifyDirectCallTaskUnblocked: {
    std::shared_ptr<WorkerInterface> worker = worker_pool_.GetRegisteredWorker(client);
    HandleDirectCallTaskUnblocked(worker);
  } break;
  case protocol::MessageType::NotifyUnblocked: {
    // TODO(ekl) this is still used from core worker even in direct call mode to
    // finish up get requests.
    auto message = flatbuffers::GetRoot<protocol::NotifyUnblocked>(message_data);
    AsyncResolveObjectsFinish(client, from_flatbuf<TaskID>(*message->task_id()),
                              /*was_blocked*/ true);
  } break;
  case protocol::MessageType::WaitRequest: {
    ProcessWaitRequestMessage(client, message_data);
  } break;
  case protocol::MessageType::WaitForDirectActorCallArgsRequest: {
    ProcessWaitForDirectActorCallArgsRequestMessage(client, message_data);
  } break;
  case protocol::MessageType::PushErrorRequest: {
    ProcessPushErrorRequestMessage(message_data);
  } break;
  case protocol::MessageType::PushProfileEventsRequest: {
    auto fbs_message = flatbuffers::GetRoot<flatbuffers::String>(message_data);
    auto profile_table_data = std::make_shared<rpc::ProfileTableData>();
    RAY_CHECK(
        profile_table_data->ParseFromArray(fbs_message->data(), fbs_message->size()));
    RAY_CHECK_OK(gcs_client_->Stats().AsyncAddProfileData(profile_table_data, nullptr));
  } break;
  case protocol::MessageType::FreeObjectsInObjectStoreRequest: {
    auto message = flatbuffers::GetRoot<protocol::FreeObjectsRequest>(message_data);
    std::vector<ObjectID> object_ids = from_flatbuf<ObjectID>(*message->object_ids());
    // Clean up objects from the object store.
    object_manager_.FreeObjects(object_ids, message->local_only());
  } break;
  case protocol::MessageType::SubscribePlasmaReady: {
    ProcessSubscribePlasmaReady(client, message_data);
  } break;
  default:
    RAY_LOG(FATAL) << "Received unexpected message type " << message_type;
  }

  // Listen for more messages.
  client->ProcessMessages();
}

void NodeManager::ProcessRegisterClientRequestMessage(
    const std::shared_ptr<ClientConnection> &client, const uint8_t *message_data) {
  client->Register();

  auto message = flatbuffers::GetRoot<protocol::RegisterClientRequest>(message_data);
  Language language = static_cast<Language>(message->language());
  const JobID job_id = from_flatbuf<JobID>(*message->job_id());
  WorkerID worker_id = from_flatbuf<WorkerID>(*message->worker_id());
  pid_t pid = message->worker_pid();
  std::string worker_ip_address = string_from_flatbuf(*message->ip_address());
  // TODO(suquark): Use `WorkerType` in `common.proto` without type converting.
  rpc::WorkerType worker_type = static_cast<rpc::WorkerType>(message->worker_type());
  if (((worker_type != rpc::WorkerType::SPILL_WORKER &&
        worker_type != rpc::WorkerType::RESTORE_WORKER &&
        worker_type != rpc::WorkerType::UTIL_WORKER)) ||
      worker_type == rpc::WorkerType::DRIVER) {
    RAY_CHECK(!job_id.IsNil());
  } else {
    RAY_CHECK(job_id.IsNil());
  }
  auto worker = std::dynamic_pointer_cast<WorkerInterface>(
      std::make_shared<Worker>(job_id, worker_id, language, worker_type,
                               worker_ip_address, client, client_call_manager_));

  auto send_reply_callback = [this, client, job_id](Status status, int assigned_port) {
    flatbuffers::FlatBufferBuilder fbb;
    std::string serialized_job_config;
    auto job_config = worker_pool_.GetJobConfig(job_id);
    if (job_config != boost::none) {
      serialized_job_config = (*job_config).SerializeAsString();
    }
    auto reply = ray::protocol::CreateRegisterClientReply(
        fbb, status.ok(), fbb.CreateString(status.ToString()),
        to_flatbuf(fbb, self_node_id_), assigned_port,
        fbb.CreateString(serialized_job_config));
    fbb.Finish(reply);
    client->WriteMessageAsync(
        static_cast<int64_t>(protocol::MessageType::RegisterClientReply), fbb.GetSize(),
        fbb.GetBufferPointer(), [this, client](const ray::Status &status) {
          if (!status.ok()) {
            DisconnectClient(client);
          }
        });
  };
  if (worker_type == rpc::WorkerType::WORKER ||
      worker_type == rpc::WorkerType::SPILL_WORKER ||
      worker_type == rpc::WorkerType::RESTORE_WORKER ||
      worker_type == rpc::WorkerType::UTIL_WORKER) {
    // Register the new worker.
    auto status = worker_pool_.RegisterWorker(worker, pid, send_reply_callback);
    if (!status.ok()) {
      // If the worker failed to register to Raylet, trigger task dispatching here to
      // allow new worker processes to be started (if capped by
      // maximum_startup_concurrency).
      cluster_task_manager_->ScheduleAndDispatchTasks();
    }
  } else {
    // Register the new driver.
    RAY_CHECK(pid >= 0);
    worker->SetProcess(Process::FromPid(pid));
    // Compute a dummy driver task id from a given driver.
    const TaskID driver_task_id = TaskID::ComputeDriverTaskId(worker_id);
    worker->AssignTaskId(driver_task_id);
    rpc::JobConfig job_config;
    job_config.ParseFromString(message->serialized_job_config()->str());
    Status status = worker_pool_.RegisterDriver(worker, job_config, send_reply_callback);
    if (status.ok()) {
      auto job_data_ptr =
          gcs::CreateJobTableData(job_id, /*is_dead*/ false, std::time(nullptr),
                                  worker_ip_address, pid, job_config);
      RAY_CHECK_OK(gcs_client_->Jobs().AsyncAdd(job_data_ptr, nullptr));
    }
  }
}

void NodeManager::ProcessAnnounceWorkerPortMessage(
    const std::shared_ptr<ClientConnection> &client, const uint8_t *message_data) {
  bool is_worker = true;
  std::shared_ptr<WorkerInterface> worker = worker_pool_.GetRegisteredWorker(client);
  if (worker == nullptr) {
    is_worker = false;
    worker = worker_pool_.GetRegisteredDriver(client);
  }
  RAY_CHECK(worker != nullptr) << "No worker exists for CoreWorker with client: "
                               << client->DebugString();

  auto message = flatbuffers::GetRoot<protocol::AnnounceWorkerPort>(message_data);
  int port = message->port();
  worker->Connect(port);
  if (is_worker) {
    worker_pool_.OnWorkerStarted(worker);
    HandleWorkerAvailable(worker->Connection());
  }
}

void NodeManager::HandleWorkerAvailable(const std::shared_ptr<ClientConnection> &client) {
  std::shared_ptr<WorkerInterface> worker = worker_pool_.GetRegisteredWorker(client);
  HandleWorkerAvailable(worker);
}

void NodeManager::HandleWorkerAvailable(const std::shared_ptr<WorkerInterface> &worker) {
  RAY_CHECK(worker);

  if (worker->GetWorkerType() == rpc::WorkerType::SPILL_WORKER) {
    // Return the worker to the idle pool.
    worker_pool_.PushSpillWorker(worker);
    return;
  }

  if (worker->GetWorkerType() == rpc::WorkerType::RESTORE_WORKER) {
    // Return the worker to the idle pool.
    worker_pool_.PushRestoreWorker(worker);
    return;
  }

  if (worker->GetWorkerType() == rpc::WorkerType::UTIL_WORKER) {
    // Return the worker to the idle pool.
    worker_pool_.PushUtilWorker(worker);
    return;
  }

  bool worker_idle = true;

  // If the worker was assigned a task, mark it as finished.
  if (!worker->GetAssignedTaskId().IsNil()) {
    worker_idle = FinishAssignedTask(worker);
  }

  if (worker_idle) {
    // Return the worker to the idle pool.
    worker_pool_.PushWorker(worker);
  }

  cluster_task_manager_->ScheduleAndDispatchTasks();
}

void NodeManager::DisconnectClient(
    const std::shared_ptr<ClientConnection> &client, rpc::WorkerExitType disconnect_type,
    const std::shared_ptr<rpc::RayException> &creation_task_exception) {
  RAY_LOG(INFO) << "NodeManager::DisconnectClient, disconnect_type=" << disconnect_type
                << ", has creation task exception = "
                << (creation_task_exception != nullptr);
  std::shared_ptr<WorkerInterface> worker = worker_pool_.GetRegisteredWorker(client);
  bool is_worker = false, is_driver = false;
  if (worker) {
    // The client is a worker.
    is_worker = true;
  } else {
    worker = worker_pool_.GetRegisteredDriver(client);
    if (worker) {
      // The client is a driver.
      is_driver = true;
    } else {
      RAY_LOG(INFO) << "Ignoring client disconnect because the client has already "
                    << "been disconnected.";
      return;
    }
  }
  RAY_CHECK(!(is_worker && is_driver));
  // If the client has any blocked tasks, mark them as unblocked. In
  // particular, we are no longer waiting for their dependencies.
  if (is_worker && worker->IsDead()) {
    // If the worker was killed by us because the driver exited,
    // treat it as intentionally disconnected.
    // Don't need to unblock the client if it's a worker and is already dead.
    // Because in this case, its task is already cleaned up.
    RAY_LOG(DEBUG) << "Skip unblocking worker because it's already dead.";
  } else {
    // Clean up any open ray.wait calls that the worker made.
    dependency_manager_.CancelGetRequest(worker->WorkerId());
    dependency_manager_.CancelWaitRequest(worker->WorkerId());
  }

  // Erase any lease metadata.
  leased_workers_.erase(worker->WorkerId());

  if (creation_task_exception != nullptr) {
    RAY_LOG(INFO) << "Formatted creation task exception: "
                  << creation_task_exception->formatted_exception_string()
                  << ", worker_id: " << worker->WorkerId();
  }
  // Publish the worker failure.
  auto worker_failure_data_ptr = gcs::CreateWorkerFailureData(
      self_node_id_, worker->WorkerId(), worker->IpAddress(), worker->Port(),
      time(nullptr), disconnect_type, creation_task_exception);
  RAY_CHECK_OK(
      gcs_client_->Workers().AsyncReportWorkerFailure(worker_failure_data_ptr, nullptr));

  if (is_worker) {
    const ActorID &actor_id = worker->GetActorId();
    const TaskID &task_id = worker->GetAssignedTaskId();
    // If the worker was running a task or actor, clean up the task and push an
    // error to the driver, unless the worker is already dead.
    if ((!task_id.IsNil() || !actor_id.IsNil()) && !worker->IsDead()) {
      // If the worker was an actor, it'll be cleaned by GCS.
      if (actor_id.IsNil()) {
        // Return the resources that were being used by this worker.
        Task task;
        cluster_task_manager_->TaskFinished(worker, &task);
      }

      if (worker->IsDetachedActor()) {
        runtime_env_manager_.RemoveUriReference(actor_id.Hex());
      }

      if (disconnect_type == rpc::WorkerExitType::SYSTEM_ERROR_EXIT) {
        // Push the error to driver.
        const JobID &job_id = worker->GetAssignedJobId();
        // TODO(rkn): Define this constant somewhere else.
        std::string type = "worker_died";
        std::ostringstream error_message;
        error_message << "A worker died or was killed while executing task " << task_id
                      << ".";
        auto error_data_ptr = gcs::CreateErrorTableData(type, error_message.str(),
                                                        current_time_ms(), job_id);
        RAY_CHECK_OK(gcs_client_->Errors().AsyncReportJobError(error_data_ptr, nullptr));
      }
    }

    // Remove the dead client from the pool and stop listening for messages.
    worker_pool_.DisconnectWorker(worker, disconnect_type);

    // Return the resources that were being used by this worker.
    cluster_task_manager_->ReleaseWorkerResources(worker);

    // Since some resources may have been released, we can try to dispatch more tasks. YYY
    cluster_task_manager_->ScheduleAndDispatchTasks();
  } else if (is_driver) {
    // The client is a driver.
    const auto job_id = worker->GetAssignedJobId();
    RAY_CHECK(!job_id.IsNil());
    RAY_CHECK_OK(gcs_client_->Jobs().AsyncMarkFinished(job_id, nullptr));
    worker_pool_.DisconnectDriver(worker);

    RAY_LOG(INFO) << "Driver (pid=" << worker->GetProcess().GetId()
                  << ") is disconnected. "
                  << "job_id: " << worker->GetAssignedJobId();
  }

  client->Close();

  // TODO(rkn): Tell the object manager that this client has disconnected so
  // that it can clean up the wait requests for this client. Currently I think
  // these can be leaked.
}

void NodeManager::DeleteLocalURI(const std::string &uri, std::function<void(bool)> cb) {
  auto resource_path = boost::filesystem::path(initial_config_.resource_dir);
  // Format of URI must be: scheme://path
  std::string sep = "://";
  auto pos = uri.find(sep);
  if (pos == std::string::npos || pos + sep.size() == uri.size()) {
    RAY_LOG(ERROR) << "Invalid uri: " << uri;
    cb(true);
  }

  auto from_path =
      resource_path / boost::filesystem::path(uri.substr(pos + sep.size())).stem();
  if (!boost::filesystem::exists(from_path)) {
    RAY_LOG(ERROR) << uri << " doesn't exist locally: " << from_path;
    cb(true);
  }
  std::string deleting_suffix(".deleting");
  auto to_path = from_path;
  to_path += deleting_suffix;
  int suffix_num = 0;
  // This is for a rare case, where one request is under processing,
  // but get a new one here.
  while (boost::filesystem::exists(to_path)) {
    to_path = from_path;
    to_path += deleting_suffix;
    to_path += "." + std::to_string(suffix_num);
    ++suffix_num;
  }
  boost::system::error_code ec;
  boost::filesystem::rename(from_path, to_path, ec);
  if (ec.value() != 0) {
    RAY_LOG(ERROR) << "Failed to move file from " << from_path << " to " << to_path
                   << " because of error " << ec.message();
    cb(false);
  }

  std::string to_path_str = to_path.string();
  // We put actual deleting job in a worker is because deleting big file
  // will block the thread for a while.
  worker_pool_.PopUtilWorker(
      [this, to_path_str, cb](std::shared_ptr<WorkerInterface> io_worker) {
        rpc::RunOnUtilWorkerRequest req;
        // TODO(yic): Move this to another file to make it formal
        req.set_request("DEL_FILE");
        *req.add_args() = to_path_str;
        io_worker->rpc_client()->RunOnUtilWorker(
            req, [this, io_worker, cb](const ray::Status &status,
                                       const rpc::RunOnUtilWorkerReply &) {
              worker_pool_.PushUtilWorker(io_worker);
              if (!status.ok()) {
                RAY_LOG(ERROR) << "Failed to execute job in io_worker " << status;
              }
              cb(status.ok());
            });
      });
}

void NodeManager::ProcessDisconnectClientMessage(
    const std::shared_ptr<ClientConnection> &client, const uint8_t *message_data) {
  auto message = flatbuffers::GetRoot<protocol::DisconnectClient>(message_data);
  auto disconnect_type = static_cast<rpc::WorkerExitType>(message->disconnect_type());
  const flatbuffers::Vector<uint8_t> *exception_pb =
      message->creation_task_exception_pb();

  std::shared_ptr<rpc::RayException> creation_task_exception = nullptr;
  if (exception_pb != nullptr) {
    creation_task_exception = std::make_shared<rpc::RayException>();
    creation_task_exception->ParseFromString(std::string(
        reinterpret_cast<const char *>(exception_pb->data()), exception_pb->size()));
  }
  DisconnectClient(client, disconnect_type, creation_task_exception);
}

void NodeManager::ProcessFetchOrReconstructMessage(
    const std::shared_ptr<ClientConnection> &client, const uint8_t *message_data) {
  auto message = flatbuffers::GetRoot<protocol::FetchOrReconstruct>(message_data);
  const auto refs =
      FlatbufferToObjectReference(*message->object_ids(), *message->owner_addresses());
  if (message->fetch_only()) {
    std::shared_ptr<WorkerInterface> worker = worker_pool_.GetRegisteredWorker(client);
    if (!worker) {
      worker = worker_pool_.GetRegisteredDriver(client);
    }
    if (worker) {
      // This will start a fetch for the objects that gets canceled once the
      // objects are local, or if the worker dies.
      dependency_manager_.StartOrUpdateWaitRequest(worker->WorkerId(), refs);
    }
  } else {
    // The values are needed. Add all requested objects to the list to
    // subscribe to in the task dependency manager. These objects will be
    // pulled from remote node managers. If an object's owner dies, an error
    // will be stored as the object's value.
    const TaskID task_id = from_flatbuf<TaskID>(*message->task_id());
    AsyncResolveObjects(client, refs, task_id, /*ray_get=*/true,
                        /*mark_worker_blocked*/ message->mark_worker_blocked());
  }
}

void NodeManager::ProcessDirectCallTaskBlocked(
    const std::shared_ptr<ClientConnection> &client, const uint8_t *message_data) {
  auto message =
      flatbuffers::GetRoot<protocol::NotifyDirectCallTaskBlocked>(message_data);
  bool release_resources = message->release_resources();
  std::shared_ptr<WorkerInterface> worker = worker_pool_.GetRegisteredWorker(client);
  HandleDirectCallTaskBlocked(worker, release_resources);
}

void NodeManager::ProcessWaitRequestMessage(
    const std::shared_ptr<ClientConnection> &client, const uint8_t *message_data) {
  // Read the data.
  auto message = flatbuffers::GetRoot<protocol::WaitRequest>(message_data);
  std::vector<ObjectID> object_ids = from_flatbuf<ObjectID>(*message->object_ids());
  const auto refs =
      FlatbufferToObjectReference(*message->object_ids(), *message->owner_addresses());
  std::unordered_map<ObjectID, rpc::Address> owner_addresses;
  for (const auto &ref : refs) {
    owner_addresses.emplace(ObjectID::FromBinary(ref.object_id()), ref.owner_address());
  }

  bool resolve_objects = false;
  for (auto const &object_id : object_ids) {
    if (!dependency_manager_.CheckObjectLocal(object_id)) {
      // At least one object requires resolution.
      resolve_objects = true;
    }
  }

  const TaskID &current_task_id = from_flatbuf<TaskID>(*message->task_id());
  bool was_blocked = message->mark_worker_blocked();
  if (resolve_objects) {
    // Resolve any missing objects. This is a no-op for any objects that are
    // already local. Missing objects will be pulled from remote node managers.
    // If an object's owner dies, an error will be stored as the object's
    // value.
    AsyncResolveObjects(client, refs, current_task_id, /*ray_get=*/false,
                        /*mark_worker_blocked*/ was_blocked);
  }
  int64_t wait_ms = message->timeout();
  uint64_t num_required_objects = static_cast<uint64_t>(message->num_ready_objects());
  // TODO Remove in the future since it should have already be done in other place
  ray::Status status = object_manager_.Wait(
      object_ids, owner_addresses, wait_ms, num_required_objects,
      [this, resolve_objects, was_blocked, client, current_task_id](
          std::vector<ObjectID> found, std::vector<ObjectID> remaining) {
        // Write the data.
        flatbuffers::FlatBufferBuilder fbb;
        flatbuffers::Offset<protocol::WaitReply> wait_reply = protocol::CreateWaitReply(
            fbb, to_flatbuf(fbb, found), to_flatbuf(fbb, remaining));
        fbb.Finish(wait_reply);

        auto status =
            client->WriteMessage(static_cast<int64_t>(protocol::MessageType::WaitReply),
                                 fbb.GetSize(), fbb.GetBufferPointer());
        if (status.ok()) {
          // The client is unblocked now because the wait call has returned.
          if (resolve_objects) {
            AsyncResolveObjectsFinish(client, current_task_id, was_blocked);
          }
        } else {
          // We failed to write to the client, so disconnect the client.
          DisconnectClient(client);
        }
      });
  RAY_CHECK_OK(status);
}

void NodeManager::ProcessWaitForDirectActorCallArgsRequestMessage(
    const std::shared_ptr<ClientConnection> &client, const uint8_t *message_data) {
  // Read the data.
  auto message =
      flatbuffers::GetRoot<protocol::WaitForDirectActorCallArgsRequest>(message_data);
  std::vector<ObjectID> object_ids = from_flatbuf<ObjectID>(*message->object_ids());
  int64_t tag = message->tag();
  // Resolve any missing objects. This will pull the objects from remote node
  // managers or store an error if the objects have failed.
  const auto refs =
      FlatbufferToObjectReference(*message->object_ids(), *message->owner_addresses());
  std::unordered_map<ObjectID, rpc::Address> owner_addresses;
  for (const auto &ref : refs) {
    owner_addresses.emplace(ObjectID::FromBinary(ref.object_id()), ref.owner_address());
  }
  AsyncResolveObjects(client, refs, TaskID::Nil(), /*ray_get=*/false,
                      /*mark_worker_blocked*/ false);
  // Reply to the client once a location has been found for all arguments.
  // NOTE(swang): ObjectManager::Wait currently returns as soon as any location
  // has been found, so the object may still be on a remote node when the
  // client receives the reply.
  ray::Status status = object_manager_.Wait(
      object_ids, owner_addresses, -1, object_ids.size(),
      [this, client, tag](std::vector<ObjectID> found, std::vector<ObjectID> remaining) {
        RAY_CHECK(remaining.empty());
        std::shared_ptr<WorkerInterface> worker =
            worker_pool_.GetRegisteredWorker(client);
        if (!worker) {
          RAY_LOG(ERROR) << "Lost worker for wait request " << client;
        } else {
          worker->DirectActorCallArgWaitComplete(tag);
        }
      });
  RAY_CHECK_OK(status);
}

void NodeManager::ProcessPushErrorRequestMessage(const uint8_t *message_data) {
  auto message = flatbuffers::GetRoot<protocol::PushErrorRequest>(message_data);

  auto const &type = string_from_flatbuf(*message->type());
  auto const &error_message = string_from_flatbuf(*message->error_message());
  double timestamp = message->timestamp();
  JobID job_id = from_flatbuf<JobID>(*message->job_id());
  auto error_data_ptr = gcs::CreateErrorTableData(type, error_message, timestamp, job_id);
  RAY_CHECK_OK(gcs_client_->Errors().AsyncReportJobError(error_data_ptr, nullptr));
}

void NodeManager::HandleRequestResourceReport(
    const rpc::RequestResourceReportRequest &request,
    rpc::RequestResourceReportReply *reply, rpc::SendReplyCallback send_reply_callback) {
  // RAY_LOG(ERROR) << "Resource report requested";
  auto resources_data = reply->mutable_resources();
  FillResourceReport(*resources_data);

  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void NodeManager::HandleRequestWorkerLease(const rpc::RequestWorkerLeaseRequest &request,
                                           rpc::RequestWorkerLeaseReply *reply,
                                           rpc::SendReplyCallback send_reply_callback) {
  rpc::Task task_message;
  task_message.mutable_task_spec()->CopyFrom(request.resource_spec());
  auto backlog_size = -1;
  if (RayConfig::instance().report_worker_backlog()) {
    backlog_size = request.backlog_size();
  }
  Task task(task_message, backlog_size);
  bool is_actor_creation_task = task.GetTaskSpecification().IsActorCreationTask();
  ActorID actor_id = ActorID::Nil();
  metrics_num_task_scheduled_ += 1;

  if (is_actor_creation_task) {
    actor_id = task.GetTaskSpecification().ActorCreationId();

    // Save the actor creation task spec to GCS, which is needed to
    // reconstruct the actor when raylet detect it dies.
    std::shared_ptr<rpc::TaskTableData> data = std::make_shared<rpc::TaskTableData>();
    data->mutable_task()->mutable_task_spec()->CopyFrom(
        task.GetTaskSpecification().GetMessage());
    RAY_CHECK_OK(gcs_client_->Tasks().AsyncAdd(data, nullptr));
  }

  if (RayConfig::instance().enable_worker_prestart()) {
    auto task_spec = task.GetTaskSpecification();
    worker_pool_.PrestartWorkers(task_spec, request.backlog_size());
  }

  cluster_task_manager_->QueueAndScheduleTask(task, reply, send_reply_callback);
}

void NodeManager::HandlePrepareBundleResources(
    const rpc::PrepareBundleResourcesRequest &request,
    rpc::PrepareBundleResourcesReply *reply, rpc::SendReplyCallback send_reply_callback) {
  auto bundle_spec = BundleSpecification(request.bundle_spec());
  RAY_LOG(DEBUG) << "Request to prepare bundle resources is received, "
                 << bundle_spec.DebugString();

  auto prepared = placement_group_resource_manager_->PrepareBundle(bundle_spec);
  reply->set_success(prepared);
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void NodeManager::HandleCommitBundleResources(
    const rpc::CommitBundleResourcesRequest &request,
    rpc::CommitBundleResourcesReply *reply, rpc::SendReplyCallback send_reply_callback) {
  auto bundle_spec = BundleSpecification(request.bundle_spec());
  RAY_LOG(DEBUG) << "Request to commit bundle resources is received, "
                 << bundle_spec.DebugString();
  placement_group_resource_manager_->CommitBundle(bundle_spec);
  send_reply_callback(Status::OK(), nullptr, nullptr);

  cluster_task_manager_->ScheduleInfeasibleTasks();
  cluster_task_manager_->ScheduleAndDispatchTasks();
}

void NodeManager::HandleCancelResourceReserve(
    const rpc::CancelResourceReserveRequest &request,
    rpc::CancelResourceReserveReply *reply, rpc::SendReplyCallback send_reply_callback) {
  auto bundle_spec = BundleSpecification(request.bundle_spec());
  RAY_LOG(INFO) << "Request to cancel reserved resource is received, "
                << bundle_spec.DebugString();

  // Kill all workers that are currently associated with the placement group.
  // NOTE: We can't traverse directly with `leased_workers_`, because `DestroyWorker` will
  // delete the element of `leased_workers_`. So we need to filter out
  // `workers_associated_with_pg` separately.
  std::vector<std::shared_ptr<WorkerInterface>> workers_associated_with_pg;
  for (const auto &worker_it : leased_workers_) {
    auto &worker = worker_it.second;
    if (worker->GetBundleId().first == bundle_spec.PlacementGroupId()) {
      workers_associated_with_pg.emplace_back(worker);
    }
  }
  for (const auto &worker : workers_associated_with_pg) {
    RAY_LOG(DEBUG)
        << "Destroying worker since its placement group was removed. Placement group id: "
        << worker->GetBundleId().first
        << ", bundle index: " << bundle_spec.BundleId().second
        << ", task id: " << worker->GetAssignedTaskId()
        << ", actor id: " << worker->GetActorId()
        << ", worker id: " << worker->WorkerId();
    DestroyWorker(worker, rpc::WorkerExitType::PLACEMENT_GROUP_REMOVED);
  }

  // Return bundle resources.
  placement_group_resource_manager_->ReturnBundle(bundle_spec);
  cluster_task_manager_->ScheduleInfeasibleTasks();
  cluster_task_manager_->ScheduleAndDispatchTasks();
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void NodeManager::HandleReturnWorker(const rpc::ReturnWorkerRequest &request,
                                     rpc::ReturnWorkerReply *reply,
                                     rpc::SendReplyCallback send_reply_callback) {
  // Read the resource spec submitted by the client.
  auto worker_id = WorkerID::FromBinary(request.worker_id());
  std::shared_ptr<WorkerInterface> worker = leased_workers_[worker_id];

  Status status;
  leased_workers_.erase(worker_id);

  if (worker) {
    if (request.disconnect_worker()) {
      DisconnectClient(worker->Connection());
    } else {
      // Handle the edge case where the worker was returned before we got the
      // unblock RPC by unblocking it immediately (unblock is idempotent).
      if (worker->IsBlocked()) {
        HandleDirectCallTaskUnblocked(worker);
      }
      cluster_task_manager_->ReturnWorkerResources(worker);
      HandleWorkerAvailable(worker);
    }
  } else {
    status = Status::Invalid("Returned worker does not exist any more");
  }
  send_reply_callback(status, nullptr, nullptr);
}

void NodeManager::HandleReleaseUnusedWorkers(
    const rpc::ReleaseUnusedWorkersRequest &request,
    rpc::ReleaseUnusedWorkersReply *reply, rpc::SendReplyCallback send_reply_callback) {
  std::unordered_set<WorkerID> in_use_worker_ids;
  for (int index = 0; index < request.worker_ids_in_use_size(); ++index) {
    auto worker_id = WorkerID::FromBinary(request.worker_ids_in_use(index));
    in_use_worker_ids.emplace(worker_id);
  }

  std::vector<WorkerID> unused_worker_ids;
  for (auto &iter : leased_workers_) {
    // We need to exclude workers used by common tasks.
    // Because they are not used by GCS.
    if (!iter.second->GetActorId().IsNil() && !in_use_worker_ids.count(iter.first)) {
      unused_worker_ids.emplace_back(iter.first);
    }
  }

  for (auto &iter : unused_worker_ids) {
    leased_workers_.erase(iter);
  }

  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void NodeManager::HandleCancelWorkerLease(const rpc::CancelWorkerLeaseRequest &request,
                                          rpc::CancelWorkerLeaseReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  const TaskID task_id = TaskID::FromBinary(request.task_id());
  bool canceled = cluster_task_manager_->CancelTask(task_id);
  // The task cancellation failed if we did not have the task queued, since
  // this means that we may not have received the task request yet. It is
  // successful if we did have the task queued, since we have now replied to
  // the client that requested the lease.
  reply->set_success(canceled);
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void NodeManager::MarkObjectsAsFailed(
    const ErrorType &error_type, const std::vector<rpc::ObjectReference> objects_to_fail,
    const JobID &job_id) {
  const std::string meta = std::to_string(static_cast<int>(error_type));
  for (const auto &ref : objects_to_fail) {
    ObjectID object_id = ObjectID::FromBinary(ref.object_id());
    std::shared_ptr<Buffer> data;
    Status status;
    status = store_client_.TryCreateImmediately(
        object_id, ref.owner_address(), 0,
        reinterpret_cast<const uint8_t *>(meta.c_str()), meta.length(), &data);
    if (status.ok()) {
      status = store_client_.Seal(object_id);
    }
    if (!status.ok() && !status.IsObjectExists()) {
      RAY_LOG(INFO) << "Marking plasma object failed " << object_id;
      // If we failed to save the error code, log a warning and push an error message
      // to the driver.
      std::ostringstream stream;
      stream << "A plasma error (" << status.ToString() << ") occurred while saving"
             << " error code to object " << object_id << ". Anyone who's getting this"
             << " object may hang forever.";
      std::string error_message = stream.str();
      RAY_LOG(ERROR) << error_message;
      auto error_data_ptr =
          gcs::CreateErrorTableData("task", error_message, current_time_ms(), job_id);
      RAY_CHECK_OK(gcs_client_->Errors().AsyncReportJobError(error_data_ptr, nullptr));
    }
  }
}

void NodeManager::HandleDirectCallTaskBlocked(
    const std::shared_ptr<WorkerInterface> &worker, bool release_resources) {
  if (!worker || worker->IsBlocked() || worker->GetAssignedTaskId().IsNil() ||
      !release_resources) {
    return;  // The worker may have died or is no longer processing the task.
  }

  if (cluster_task_manager_->ReleaseCpuResourcesFromUnblockedWorker(worker)) {
    worker->MarkBlocked();
  }
  cluster_task_manager_->ScheduleAndDispatchTasks();
}

void NodeManager::HandleDirectCallTaskUnblocked(
    const std::shared_ptr<WorkerInterface> &worker) {
  if (!worker || worker->GetAssignedTaskId().IsNil()) {
    return;  // The worker may have died or is no longer processing the task.
  }

  // First, always release task dependencies. This ensures we don't leak resources even
  // if we don't need to unblock the worker below.
  dependency_manager_.CancelGetRequest(worker->WorkerId());

  if (worker->IsBlocked()) {
    if (cluster_task_manager_->ReturnCpuResourcesToBlockedWorker(worker)) {
      worker->MarkUnblocked();
    }
    cluster_task_manager_->ScheduleAndDispatchTasks();
  }
}

void NodeManager::AsyncResolveObjects(
    const std::shared_ptr<ClientConnection> &client,
    const std::vector<rpc::ObjectReference> &required_object_refs,
    const TaskID &current_task_id, bool ray_get, bool mark_worker_blocked) {
  std::shared_ptr<WorkerInterface> worker = worker_pool_.GetRegisteredWorker(client);
  if (!worker) {
    // The client is a driver. Drivers do not hold resources, so we simply mark
    // the task as blocked.
    worker = worker_pool_.GetRegisteredDriver(client);
  }

  RAY_CHECK(worker);
  // Subscribe to the objects required by the task. These objects will be
  // fetched and/or restarted as necessary, until the objects become local
  // or are unsubscribed.
  if (ray_get) {
    dependency_manager_.StartOrUpdateGetRequest(worker->WorkerId(), required_object_refs);
  } else {
    dependency_manager_.StartOrUpdateWaitRequest(worker->WorkerId(),
                                                 required_object_refs);
  }
}

void NodeManager::AsyncResolveObjectsFinish(
    const std::shared_ptr<ClientConnection> &client, const TaskID &current_task_id,
    bool was_blocked) {
  std::shared_ptr<WorkerInterface> worker = worker_pool_.GetRegisteredWorker(client);
  if (!worker) {
    // The client is a driver. Drivers do not hold resources, so we simply
    // mark the driver as unblocked.
    worker = worker_pool_.GetRegisteredDriver(client);
  }

  RAY_CHECK(worker);
  // Unsubscribe from any `ray.get` objects that the task was blocked on.  Any
  // fetch or reconstruction operations to make the objects local are canceled.
  // `ray.wait` calls will stay active until the objects become local, or the
  // task/actor that called `ray.wait` exits.
  dependency_manager_.CancelGetRequest(worker->WorkerId());
  // Mark the task as unblocked.
  if (was_blocked) {
    worker->RemoveBlockedTaskId(current_task_id);
  }
}

bool NodeManager::FinishAssignedTask(const std::shared_ptr<WorkerInterface> &worker_ptr) {
  // TODO (Alex): We should standardize to pass
  // std::shared_ptr<WorkerInterface> instead of refs.
  auto &worker = *worker_ptr;
  TaskID task_id = worker.GetAssignedTaskId();
  RAY_LOG(DEBUG) << "Finished task " << task_id;

  Task task;
  cluster_task_manager_->TaskFinished(worker_ptr, &task);

  const auto &spec = task.GetTaskSpecification();  //
  if ((spec.IsActorCreationTask())) {
    // If this was an actor or actor creation task, handle the actor's new
    // state.
    FinishAssignedActorCreationTask(worker, task);
  } else {
    // If this was a non-actor task, then cancel any ray.wait calls that were
    // made during the task execution.
    dependency_manager_.CancelWaitRequest(worker.WorkerId());
  }

  // Notify the task dependency manager that this task has finished execution.
  dependency_manager_.CancelGetRequest(worker.WorkerId());

  if (!spec.IsActorCreationTask()) {
    // Unset the worker's assigned task. We keep the assigned task ID for
    // direct actor creation calls because this ID is used later if the actor
    // requires objects from plasma.
    worker.AssignTaskId(TaskID::Nil());
    worker.SetOwnerAddress(rpc::Address());
  }
  // Direct actors will be assigned tasks via the core worker and therefore are
  // not idle.
  return !spec.IsActorCreationTask();
}

void NodeManager::FinishAssignedActorCreationTask(WorkerInterface &worker,
                                                  const Task &task) {
  RAY_LOG(DEBUG) << "Finishing assigned actor creation task";
  const TaskSpecification task_spec = task.GetTaskSpecification();
  ActorID actor_id = task_spec.ActorCreationId();

  // This was an actor creation task. Convert the worker to an actor.
  worker.AssignActorId(actor_id);

  if (task_spec.IsDetachedActor()) {
    worker.MarkDetachedActor();
    auto job_id = task.GetTaskSpecification().JobId();
    auto job_config = worker_pool_.GetJobConfig(job_id);
    RAY_CHECK(job_config);
    runtime_env_manager_.AddUriReference(actor_id.Hex(), job_config->runtime_env());
    ;
  }
}

void NodeManager::HandleObjectLocal(const ObjectInfo &object_info) {
  const ObjectID &object_id = object_info.object_id;
  // Notify the task dependency manager that this object is local.
  const auto ready_task_ids = dependency_manager_.HandleObjectLocal(object_id);
  RAY_LOG(DEBUG) << "Object local " << object_id << ", "
                 << " on " << self_node_id_ << ", " << ready_task_ids.size()
                 << " tasks ready";
  cluster_task_manager_->TasksUnblocked(ready_task_ids);

  auto waiting_workers = absl::flat_hash_set<std::shared_ptr<WorkerInterface>>();
  {
    absl::MutexLock guard(&plasma_object_notification_lock_);
    auto waiting = this->async_plasma_objects_notification_.extract(object_id);
    if (!waiting.empty()) {
      waiting_workers.swap(waiting.mapped());
    }
  }
  rpc::PlasmaObjectReadyRequest request;
  request.set_object_id(object_id.Binary());

  for (auto worker : waiting_workers) {
    worker->rpc_client()->PlasmaObjectReady(
        request, [](Status status, const rpc::PlasmaObjectReadyReply &reply) {
          if (!status.ok()) {
            RAY_LOG(INFO) << "Problem with telling worker that plasma object is ready"
                          << status.ToString();
          }
        });
  }
}

bool NodeManager::IsActorCreationTask(const TaskID &task_id) {
  auto actor_id = task_id.ActorId();
  if (!actor_id.IsNil() && task_id == TaskID::ForActorCreationTask(actor_id)) {
    // This task ID corresponds to an actor creation task.
    return true;
  }

  return false;
}

void NodeManager::HandleObjectMissing(const ObjectID &object_id) {
  // Notify the task dependency manager that this object is no longer local.
  const auto waiting_task_ids = dependency_manager_.HandleObjectMissing(object_id);
  std::stringstream result;
  result << "Object missing " << object_id << ", "
         << " on " << self_node_id_ << ", " << waiting_task_ids.size()
         << " tasks waiting";
  if (waiting_task_ids.size() > 0) {
    result << ", tasks: ";
    for (const auto &task_id : waiting_task_ids) {
      result << task_id << "  ";
    }
  }
  RAY_LOG(DEBUG) << result.str();
}

void NodeManager::ProcessSubscribePlasmaReady(
    const std::shared_ptr<ClientConnection> &client, const uint8_t *message_data) {
  std::shared_ptr<WorkerInterface> associated_worker =
      worker_pool_.GetRegisteredWorker(client);
  if (associated_worker == nullptr) {
    associated_worker = worker_pool_.GetRegisteredDriver(client);
  }
  RAY_CHECK(associated_worker != nullptr)
      << "No worker exists for CoreWorker with client: " << client->DebugString();

  auto message = flatbuffers::GetRoot<protocol::SubscribePlasmaReady>(message_data);
  ObjectID id = from_flatbuf<ObjectID>(*message->object_id());

  if (dependency_manager_.CheckObjectLocal(id)) {
    // Object is already local, so we directly fire the callback to tell the core worker
    // that the plasma object is ready.
    rpc::PlasmaObjectReadyRequest request;
    request.set_object_id(id.Binary());

    RAY_LOG(DEBUG) << "Object " << id << " is already local, firing callback directly.";
    associated_worker->rpc_client()->PlasmaObjectReady(
        request, [](Status status, const rpc::PlasmaObjectReadyReply &reply) {
          if (!status.ok()) {
            RAY_LOG(INFO) << "Problem with telling worker that plasma object is ready"
                          << status.ToString();
          }
        });
  } else {
    // The object is not local, so we are subscribing to pull and wait for the objects.
    std::vector<rpc::ObjectReference> refs = {FlatbufferToSingleObjectReference(
        *message->object_id(), *message->owner_address())};

    // NOTE(simon): This call will issue a pull request to remote workers and make sure
    // the object will be local.
    // 1. We currently do not allow user to cancel this call. The object will be pulled
    //    even if the `await object_ref` is cancelled.
    // 2. We currently do not handle edge cases with object eviction where the object
    //    is local at this time but when the core worker was notified, the object is
    //    is evicted. The core worker should be able to handle evicted object in this
    //    case.
    dependency_manager_.StartOrUpdateWaitRequest(associated_worker->WorkerId(), refs);

    // Add this worker to the listeners for the object ID.
    {
      absl::MutexLock guard(&plasma_object_notification_lock_);
      async_plasma_objects_notification_[id].insert(associated_worker);
    }
  }
}

void NodeManager::DumpDebugState() const {
  std::fstream fs;
  fs.open(initial_config_.session_dir + "/debug_state.txt",
          std::fstream::out | std::fstream::trunc);
  fs << DebugString();
  fs.close();
}

const NodeManagerConfig &NodeManager::GetInitialConfig() const { return initial_config_; }

std::string NodeManager::DebugString() const {
  std::stringstream result;
  uint64_t now_ms = current_time_ms();
  result << "NodeManager:";
  result << "\nInitialConfigResources: " << initial_config_.resource_config.ToString();
  if (cluster_task_manager_ != nullptr) {
    result << "\nClusterTaskManager:\n";
    result << cluster_task_manager_->DebugStr();
  }
  result << "\nClusterResources:";
  result << "\n" << local_object_manager_.DebugString();
  result << "\n" << object_manager_.DebugString();
  result << "\n" << gcs_client_->DebugString();
  result << "\n" << worker_pool_.DebugString();
  result << "\n" << dependency_manager_.DebugString();
  {
    absl::MutexLock guard(&plasma_object_notification_lock_);
    result << "\nnum async plasma notifications: "
           << async_plasma_objects_notification_.size();
  }

  result << "\nRemote node managers: ";
  for (const auto &entry : remote_node_manager_addresses_) {
    result << "\n" << entry.first;
  }

  // Event loop stats.
  result << "\nEvent loop stats:" << io_service_.StatsString();

  result << "\nDebugString() time ms: " << (current_time_ms() - now_ms);
  return result.str();
}

// Summarizes a Census view and tag values into a compact string, e.g.,
// "Tag1:Value1,Tag2:Value2,Tag3:Value3".
std::string compact_tag_string(const opencensus::stats::ViewDescriptor &view,
                               const std::vector<std::string> &values) {
  std::stringstream result;
  const auto &keys = view.columns();
  for (size_t i = 0; i < values.size(); i++) {
    result << keys[i].name() << ":" << values[i];
    if (i < values.size() - 1) {
      result << ",";
    }
  }
  return result.str();
}

bool NodeManager::GetObjectsFromPlasma(const std::vector<ObjectID> &object_ids,
                                       std::vector<std::unique_ptr<RayObject>> *results) {
  // Pin the objects in plasma by getting them and holding a reference to
  // the returned buffer.
  // NOTE: the caller must ensure that the objects already exist in plasma before
  // sending a PinObjectIDs request.
  std::vector<plasma::ObjectBuffer> plasma_results;
  // TODO(swang): This `Get` has a timeout of 0, so the plasma store will not
  // block when serving the request. However, if the plasma store is under
  // heavy load, then this request can still block the NodeManager event loop
  // since we must wait for the plasma store's reply. We should consider using
  // an `AsyncGet` instead.
  if (!store_client_
           .Get(object_ids, /*timeout_ms=*/0, &plasma_results, /*is_from_worker=*/false)
           .ok()) {
    return false;
  }

  for (const auto &plasma_result : plasma_results) {
    if (plasma_result.data == nullptr) {
      results->push_back(nullptr);
    } else {
      results->emplace_back(std::unique_ptr<RayObject>(
          new RayObject(plasma_result.data, plasma_result.metadata, {})));
    }
  }
  return true;
}

void NodeManager::HandlePinObjectIDs(const rpc::PinObjectIDsRequest &request,
                                     rpc::PinObjectIDsReply *reply,
                                     rpc::SendReplyCallback send_reply_callback) {
  std::vector<ObjectID> object_ids;
  object_ids.reserve(request.object_ids_size());
  const auto &owner_address = request.owner_address();
  for (const auto &object_id_binary : request.object_ids()) {
    object_ids.push_back(ObjectID::FromBinary(object_id_binary));
  }
  std::vector<std::unique_ptr<RayObject>> results;
  if (!GetObjectsFromPlasma(object_ids, &results)) {
    RAY_LOG(WARNING)
        << "Failed to get objects that should have been in the object store. These "
           "objects may have been evicted while there are still references in scope.";
    // TODO(suquark): Maybe "Status::ObjectNotFound" is more accurate here.
    send_reply_callback(Status::Invalid("Failed to get objects."), nullptr, nullptr);
    return;
  }
  local_object_manager_.PinObjects(object_ids, std::move(results), owner_address);
  // Wait for the object to be freed by the owner, which keeps the ref count.
  local_object_manager_.WaitForObjectFree(owner_address, object_ids);
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void NodeManager::HandleGetSystemConfig(const rpc::GetSystemConfigRequest &request,
                                        rpc::GetSystemConfigReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) {
  reply->set_system_config(initial_config_.raylet_config);
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void NodeManager::HandleGetNodeStats(const rpc::GetNodeStatsRequest &node_stats_request,
                                     rpc::GetNodeStatsReply *reply,
                                     rpc::SendReplyCallback send_reply_callback) {
  cluster_task_manager_->FillPendingActorInfo(reply);
  // Report object spilling stats.
  local_object_manager_.FillObjectSpillingStats(reply);
  // Report object store stats.
  object_manager_.FillObjectStoreStats(reply);
  // Ensure we never report an empty set of metrics.
  if (!recorded_metrics_) {
    RecordMetrics();
    RAY_CHECK(recorded_metrics_);
  }
  for (const auto &view : opencensus::stats::StatsExporter::GetViewData()) {
    auto view_data = reply->add_view_data();
    view_data->set_view_name(view.first.name());
    if (view.second.type() == opencensus::stats::ViewData::Type::kInt64) {
      for (const auto &measure : view.second.int_data()) {
        auto measure_data = view_data->add_measures();
        measure_data->set_tags(compact_tag_string(view.first, measure.first));
        measure_data->set_int_value(measure.second);
      }
    } else if (view.second.type() == opencensus::stats::ViewData::Type::kDouble) {
      for (const auto &measure : view.second.double_data()) {
        auto measure_data = view_data->add_measures();
        measure_data->set_tags(compact_tag_string(view.first, measure.first));
        measure_data->set_double_value(measure.second);
      }
    } else {
      RAY_CHECK(view.second.type() == opencensus::stats::ViewData::Type::kDistribution);
      for (const auto &measure : view.second.distribution_data()) {
        auto measure_data = view_data->add_measures();
        measure_data->set_tags(compact_tag_string(view.first, measure.first));
        measure_data->set_distribution_min(measure.second.min());
        measure_data->set_distribution_mean(measure.second.mean());
        measure_data->set_distribution_max(measure.second.max());
        measure_data->set_distribution_count(measure.second.count());
        for (const auto &bound : measure.second.bucket_boundaries().lower_boundaries()) {
          measure_data->add_distribution_bucket_boundaries(bound);
        }
        for (const auto &count : measure.second.bucket_counts()) {
          measure_data->add_distribution_bucket_counts(count);
        }
      }
    }
  }
  // As a result of the HandleGetNodeStats, we are collecting information from all
  // workers on this node. This is done by calling GetCoreWorkerStats on each worker. In
  // order to send up-to-date information back, we wait until all workers have replied,
  // and return the information from HandleNodesStatsRequest. The caller of
  // HandleGetNodeStats should set a timeout so that the rpc finishes even if not all
  // workers have replied.
  auto all_workers = worker_pool_.GetAllRegisteredWorkers(/* filter_dead_worker */ true);
  absl::flat_hash_set<WorkerID> driver_ids;
  for (auto driver :
       worker_pool_.GetAllRegisteredDrivers(/* filter_dead_driver */ true)) {
    all_workers.push_back(driver);
    driver_ids.insert(driver->WorkerId());
  }
  if (all_workers.empty()) {
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }
  for (const auto &worker : all_workers) {
    if (worker->IsDead()) {
      continue;
    }
    rpc::GetCoreWorkerStatsRequest request;
    request.set_intended_worker_id(worker->WorkerId().Binary());
    request.set_include_memory_info(node_stats_request.include_memory_info());
    worker->rpc_client()->GetCoreWorkerStats(
        request, [reply, worker, all_workers, driver_ids, send_reply_callback](
                     const ray::Status &status, const rpc::GetCoreWorkerStatsReply &r) {
          reply->add_core_workers_stats()->MergeFrom(r.core_worker_stats());
          reply->set_num_workers(reply->num_workers() + 1);
          if (reply->num_workers() == all_workers.size()) {
            send_reply_callback(Status::OK(), nullptr, nullptr);
          }
        });
  }
}

rpc::ObjectStoreStats AccumulateStoreStats(
    std::vector<rpc::GetNodeStatsReply> node_stats) {
  rpc::ObjectStoreStats store_stats;
  for (const auto &reply : node_stats) {
    auto cur_store = reply.store_stats();
    // Use max aggregation for time, since the nodes are spilling concurrently.
    store_stats.set_spill_time_total_s(
        std::max(store_stats.spill_time_total_s(), cur_store.spill_time_total_s()));
    store_stats.set_restore_time_total_s(
        std::max(store_stats.restore_time_total_s(), cur_store.restore_time_total_s()));
    // Use sum aggregation for the rest of the metrics.
    store_stats.set_spilled_bytes_total(store_stats.spilled_bytes_total() +
                                        cur_store.spilled_bytes_total());
    store_stats.set_spilled_objects_total(store_stats.spilled_objects_total() +
                                          cur_store.spilled_objects_total());
    store_stats.set_restored_bytes_total(store_stats.restored_bytes_total() +
                                         cur_store.restored_bytes_total());
    store_stats.set_restored_objects_total(store_stats.restored_objects_total() +
                                           cur_store.restored_objects_total());
    store_stats.set_object_store_bytes_used(store_stats.object_store_bytes_used() +
                                            cur_store.object_store_bytes_used());
    store_stats.set_object_store_bytes_avail(store_stats.object_store_bytes_avail() +
                                             cur_store.object_store_bytes_avail());
    store_stats.set_num_local_objects(store_stats.num_local_objects() +
                                      cur_store.num_local_objects());
    store_stats.set_consumed_bytes(store_stats.consumed_bytes() +
                                   cur_store.consumed_bytes());
  }
  return store_stats;
}

std::string FormatMemoryInfo(std::vector<rpc::GetNodeStatsReply> node_stats) {
  // First pass to compute object sizes.
  absl::flat_hash_map<ObjectID, int64_t> object_sizes;
  for (const auto &reply : node_stats) {
    for (const auto &core_worker_stats : reply.core_workers_stats()) {
      for (const auto &object_ref : core_worker_stats.object_refs()) {
        auto obj_id = ObjectID::FromBinary(object_ref.object_id());
        if (object_ref.object_size() > 0) {
          object_sizes[obj_id] = object_ref.object_size();
        }
      }
    }
  }

  std::ostringstream builder;
  builder
      << "----------------------------------------------------------------------------"
         "-----------------------------------------\n";
  builder
      << " Object ID                                                Reference Type    "
         "   Object Size  "
         " Reference Creation Site\n";
  builder
      << "============================================================================"
         "=========================================\n";

  // Second pass builds the summary string for each node.
  for (const auto &reply : node_stats) {
    for (const auto &core_worker_stats : reply.core_workers_stats()) {
      bool pid_printed = false;
      for (const auto &object_ref : core_worker_stats.object_refs()) {
        auto obj_id = ObjectID::FromBinary(object_ref.object_id());
        if (!object_ref.pinned_in_memory() && object_ref.local_ref_count() == 0 &&
            object_ref.submitted_task_ref_count() == 0 &&
            object_ref.contained_in_owned_size() == 0) {
          continue;
        }
        if (obj_id.IsNil()) {
          continue;
        }
        if (!pid_printed) {
          if (core_worker_stats.worker_type() == rpc::WorkerType::DRIVER) {
            builder << "; driver pid=" << core_worker_stats.pid() << "\n";
          } else {
            builder << "; worker pid=" << core_worker_stats.pid() << "\n";
          }
          pid_printed = true;
        }
        builder << obj_id.Hex() << "  ";
        // TODO(ekl) we could convey more information about the reference status.
        if (object_ref.pinned_in_memory()) {
          builder << "PINNED_IN_MEMORY     ";
        } else if (object_ref.submitted_task_ref_count() > 0) {
          builder << "USED_BY_PENDING_TASK ";
        } else if (object_ref.local_ref_count() > 0) {
          builder << "LOCAL_REFERENCE      ";
        } else if (object_ref.contained_in_owned_size() > 0) {
          builder << "CAPTURED_IN_OBJECT   ";
        } else {
          builder << "UNKNOWN_STATUS       ";
        }
        builder << std::right << std::setfill(' ') << std::setw(11);
        if (object_sizes.contains(obj_id)) {
          builder << object_sizes[obj_id];
        } else {
          builder << "          ?";
        }
        builder << "   " << object_ref.call_site();
        builder << "\n";
      }
    }
  }
  builder
      << "----------------------------------------------------------------------------"
         "-----------------------------------------\n";

  return builder.str();
}

void NodeManager::HandleFormatGlobalMemoryInfo(
    const rpc::FormatGlobalMemoryInfoRequest &request,
    rpc::FormatGlobalMemoryInfoReply *reply, rpc::SendReplyCallback send_reply_callback) {
  auto replies = std::make_shared<std::vector<rpc::GetNodeStatsReply>>();
  auto local_request = std::make_shared<rpc::GetNodeStatsRequest>();
  auto local_reply = std::make_shared<rpc::GetNodeStatsReply>();
  bool include_memory_info = request.include_memory_info();
  local_request->set_include_memory_info(include_memory_info);

  unsigned int num_nodes = remote_node_manager_addresses_.size() + 1;
  rpc::GetNodeStatsRequest stats_req;
  stats_req.set_include_memory_info(include_memory_info);

  auto store_reply = [replies, reply, num_nodes, send_reply_callback,
                      include_memory_info](const rpc::GetNodeStatsReply &local_reply) {
    replies->push_back(local_reply);
    if (replies->size() >= num_nodes) {
      if (include_memory_info) {
        reply->set_memory_summary(FormatMemoryInfo(*replies));
      }
      reply->mutable_store_stats()->CopyFrom(AccumulateStoreStats(*replies));
      send_reply_callback(Status::OK(), nullptr, nullptr);
    }
  };

  // Fetch from remote nodes.
  for (const auto &entry : remote_node_manager_addresses_) {
    auto client = std::make_unique<rpc::NodeManagerClient>(
        entry.second.first, entry.second.second, client_call_manager_);
    client->GetNodeStats(
        stats_req, [replies, store_reply](const ray::Status &status,
                                          const rpc::GetNodeStatsReply &r) {
          if (!status.ok()) {
            RAY_LOG(ERROR) << "Failed to get remote node stats: " << status.ToString();
          }
          store_reply(r);
        });
  }

  // Fetch from the local node.
  HandleGetNodeStats(
      stats_req, local_reply.get(),
      [local_reply, store_reply](Status status, std::function<void()> success,
                                 std::function<void()> failure) mutable {
        store_reply(*local_reply);
      });
}

void NodeManager::HandleGlobalGC(const rpc::GlobalGCRequest &request,
                                 rpc::GlobalGCReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) {
  TriggerGlobalGC();
}

void NodeManager::TriggerGlobalGC() {
  should_global_gc_ = true;
  // We won't see our own request, so trigger local GC in the next heartbeat.
  should_local_gc_ = true;
}

void NodeManager::Stop() {
  object_manager_.Stop();
  if (heartbeat_sender_) {
    heartbeat_sender_.reset();
  }
}

void NodeManager::RecordMetrics() {
  recorded_metrics_ = true;
  if (stats::StatsConfig::instance().IsStatsDisabled()) {
    return;
  }

  cluster_task_manager_->RecordMetrics();
  object_manager_.RecordMetrics();
  local_object_manager_.RecordObjectSpillingStats();

  uint64_t current_time = current_time_ms();
  uint64_t duration_ms = current_time - last_metrics_recorded_at_ms_;
  last_metrics_recorded_at_ms_ = current_time;
  object_directory_->RecordMetrics(duration_ms);
}

void NodeManager::PublishInfeasibleTaskError(const Task &task) const {
  bool suppress_warning = false;

  if (!task.GetTaskSpecification().PlacementGroupBundleId().first.IsNil()) {
    // If the task is part of a placement group, do nothing. If necessary, the infeasible
    // warning should come from the placement group scheduling, not the task scheduling.
    suppress_warning = true;
  }

  // Push a warning to the task's driver that this task is currently infeasible.
  if (!suppress_warning) {
    // TODO(rkn): Define this constant somewhere else.
    std::string type = "infeasible_task";
    std::ostringstream error_message;
    error_message
        << "The actor or task with ID " << task.GetTaskSpecification().TaskId()
        << " cannot be scheduled right now. It requires "
        << task.GetTaskSpecification().GetRequiredPlacementResources().ToString()
        << " for placement, however the cluster currently cannot provide the requested "
           "resources. The required resources may be added as autoscaling takes place "
           "or placement groups are scheduled. Otherwise, consider reducing the "
           "resource requirements of the task.";
    auto error_data_ptr =
        gcs::CreateErrorTableData(type, error_message.str(), current_time_ms(),
                                  task.GetTaskSpecification().JobId());
    RAY_CHECK_OK(gcs_client_->Errors().AsyncReportJobError(error_data_ptr, nullptr));
  }
}

}  // namespace raylet

}  // namespace ray
