#pragma once
#include "ray/gcs/gcs_server/impl/gcs_actor_manager.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace gcs {

class GcsActorManager : public rpc::ActorInfoHandler {
 public:
  GcsActorManager(
      boost::asio::executor main_executor,
      rpc::ClientFactoryFn client_factory,
      std::shared_ptr<rpc::NodeManagerClientPool> pool,
      std::shared_ptr<GcsTableStorage> gcs_table_storage,
      std::shared_ptr<GcsPublisher> gcs_publisher,
      RuntimeEnvManager &runtime_env_manager,
      GcsFunctionManager &function_manager,
      std::function<void(const ActorID &)> destroy_owned_placement_group_if_needed,
      std::function<void(std::function<void(void)>, boost::posix_time::milliseconds)>
          run_delayed,
      const rpc::ClientFactoryFn &worker_client_factory = nullptr)
      : main_executor_(main_executor) {
    for (size_t i = 0; i < ::RayConfig::instance().gcs_actor_threading_num(); ++i) {
      threads_.emplace_back([this]() { pool_.attach(); });
    }
    for (size_t i = 0; i < ::RayConfig::instance().gcs_actor_sharding_num(); ++i) {
      impls_.emplace_back(main_executor,
                          client_factory,
                          pool,
                          gcs_table_storage,
                          gcs_publisher,
                          runtime_env_manager,
                          function_manager,
                          destroy_owned_placement_group_if_needed,
                          run_delayed,
                          worker_client_factory);
    }
  }

  void HandleRegisterActor(rpc::RegisterActorRequest request,
                           rpc::RegisterActorReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;

  void HandleCreateActor(rpc::CreateActorRequest request,
                         rpc::CreateActorReply *reply,
                         rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetActorInfo(rpc::GetActorInfoRequest request,
                          rpc::GetActorInfoReply *reply,
                          rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetNamedActorInfo(rpc::GetNamedActorInfoRequest request,
                               rpc::GetNamedActorInfoReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override;

  void HandleListNamedActors(rpc::ListNamedActorsRequest request,
                             rpc::ListNamedActorsReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetAllActorInfo(rpc::GetAllActorInfoRequest request,
                             rpc::GetAllActorInfoReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override;

  void HandleKillActorViaGcs(rpc::KillActorViaGcsRequest request,
                             rpc::KillActorViaGcsReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override;

  void OnNodeDead(const NodeID &node_id, const std::string node_ip_address) {
    for (auto &impl : impls_) {
      impl.OnNodeDead(node_id, node_ip_address);
    }
  }

  void OnNodeAdded(const NodeID &node_id, std::shared_ptr<rpc::GcsNodeInfo> node) {
    for (auto &impl : impls_) {
      impl.OnNodeAdded(node_id, node);
    }
  }

  void OnWorkerDead(const NodeID &node_id,
                    const WorkerID &worker_id,
                    const std::string &worker_ip,
                    const rpc::WorkerExitType disconnect_type,
                    const std::string &disconnect_detail,
                    const rpc::RayException *creation_task_exception = nullptr) {
    for (auto &impl : impls_) {
      impl.OnWorkerDead(node_id,
                        worker_id,
                        worker_ip,
                        disconnect_type,
                        disconnect_detail,
                        creation_task_exception);
    }
  }

  /// Testing only.
  void OnWorkerDead(const NodeID &node_id, const WorkerID &worker_id) {
    for (auto &impl : impls_) {
      impl.OnWorkerDead(node_id, worker_id);
    }
  }

  void OnActorSchedulingFailed(
      std::shared_ptr<GcsActor> actor,
      const rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
      const std::string &scheduling_failure_message) {
    for (auto &impl : impls_) {
      impl.OnActorSchedulingFailed(actor, failure_type, scheduling_failure_message);
    }
  }

  void OnActorCreationSuccess(const std::shared_ptr<GcsActor> &actor,
                              const rpc::PushTaskReply &reply) {
    for (auto &impl : impls_) {
      impl.OnActorCreationSuccess(actor, reply);
    }
  }

  void Initialize(const GcsInitData &gcs_init_data) { return; }

  void OnJobFinished(const JobID &job_id) {
    for (auto &impl : impls_) {
      impl.OnJobFinished(job_id);
    }
  }

  std::string DebugString() const { return ""; }

  /// Collect stats from gcs actor manager in-memory data structures.
  void RecordMetrics() const {}

  void SetUsageStatsClient(UsageStatsClient *usage_stats_client) { return; }

 private:
  GcsActorManagerImpl &GetShard(const ActorID &actor_id) {
    return impls_[actor_id.Hash() % impls_.size()];
  }

  boost::asio::executor main_executor_;
  boost::asio::static_thread_pool pool_;
  std::vector<GcsActorManagerImpl> impls_;
  std::vector<std::thread> threads_;
  // Debug info.
  enum CountType {
    REGISTER_ACTOR_REQUEST = 0,
    CREATE_ACTOR_REQUEST = 1,
    GET_ACTOR_INFO_REQUEST = 2,
    GET_NAMED_ACTOR_INFO_REQUEST = 3,
    GET_ALL_ACTOR_INFO_REQUEST = 4,
    KILL_ACTOR_REQUEST = 5,
    LIST_NAMED_ACTORS_REQUEST = 6,
    CountType_MAX = 7,
  };
  uint64_t counts_[CountType::CountType_MAX] = {0};
};
}  // namespace gcs
}  // namespace ray
