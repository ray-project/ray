#include "ray/gcs/redis_gcs_client.h"

#include <unistd.h>
#include "ray/common/ray_config.h"
#include "ray/gcs/redis_accessor.h"
#include "ray/gcs/redis_context.h"

namespace ray {

namespace gcs {

RedisGcsClient::RedisGcsClient(const GcsClientOptions &options)
    : RedisGcsClient(options, CommandType::kRegular) {}

RedisGcsClient::RedisGcsClient(const GcsClientOptions &options, CommandType command_type)
    : GcsClient(options), command_type_(command_type) {
  RedisClientOptions redis_client_options(options.server_ip_, options.server_port_,
                                          options.password_, options.is_test_client_);
  redis_client_.reset(new RedisClient(redis_client_options));
}

Status RedisGcsClient::Connect(boost::asio::io_service &io_service) {
  RAY_CHECK(!is_connected_);

  Status status = redis_client_->Connect(io_service);
  if (!status.ok()) {
    return status;
  }

  std::shared_ptr<RedisContext> primary_context = redis_client_->GetPrimaryContext();
  std::vector<std::shared_ptr<RedisContext>> shard_contexts =
      redis_client_->GetShardContexts();

  actor_table_.reset(new ActorTable({primary_context}, this));

  // TODO(micafan) Modify ClientTable' Constructor(remove ClientID) in future.
  // We will use NodeID instead of ClientID.
  // For worker/driver, it might not have this field(NodeID).
  // For raylet, NodeID should be initialized in raylet layer(not here).
  client_table_.reset(new ClientTable({primary_context}, this));

  error_table_.reset(new ErrorTable({primary_context}, this));
  job_table_.reset(new JobTable({primary_context}, this));
  heartbeat_batch_table_.reset(new HeartbeatBatchTable({primary_context}, this));
  // Tables below would be sharded.
  object_table_.reset(new ObjectTable(shard_contexts, this));
  raylet_task_table_.reset(new raylet::TaskTable(shard_contexts, this, command_type_));
  task_reconstruction_log_.reset(new TaskReconstructionLog(shard_contexts, this));
  task_lease_table_.reset(new TaskLeaseTable(shard_contexts, this));
  heartbeat_table_.reset(new HeartbeatTable(shard_contexts, this));
  profile_table_.reset(new ProfileTable(shard_contexts, this));
  actor_checkpoint_table_.reset(new ActorCheckpointTable(shard_contexts, this));
  actor_checkpoint_id_table_.reset(new ActorCheckpointIdTable(shard_contexts, this));
  resource_table_.reset(new DynamicResourceTable({primary_context}, this));
  worker_failure_table_.reset(new WorkerFailureTable(shard_contexts, this));

  actor_accessor_.reset(new RedisActorInfoAccessor(this));
  job_accessor_.reset(new RedisJobInfoAccessor(this));
  object_accessor_.reset(new RedisObjectInfoAccessor(this));
  node_accessor_.reset(new RedisNodeInfoAccessor(this));
  task_accessor_.reset(new RedisTaskInfoAccessor(this));
  error_accessor_.reset(new RedisErrorInfoAccessor(this));
  stats_accessor_.reset(new RedisStatsInfoAccessor(this));
  worker_accessor_.reset(new RedisWorkerInfoAccessor(this));

  is_connected_ = true;

  RAY_LOG(INFO) << "RedisGcsClient Connected.";

  return Status::OK();
}

void RedisGcsClient::Disconnect() {
  RAY_CHECK(is_connected_);
  is_connected_ = false;
  redis_client_->Disconnect();
  RAY_LOG(INFO) << "RedisGcsClient Disconnected.";
}

std::string RedisGcsClient::DebugString() const {
  std::stringstream result;
  result << "RedisGcsClient:";
  result << "\n- TaskTable: " << raylet_task_table_->DebugString();
  result << "\n- ActorTable: " << actor_table_->DebugString();
  result << "\n- TaskReconstructionLog: " << task_reconstruction_log_->DebugString();
  result << "\n- TaskLeaseTable: " << task_lease_table_->DebugString();
  result << "\n- HeartbeatTable: " << heartbeat_table_->DebugString();
  result << "\n- ErrorTable: " << error_table_->DebugString();
  result << "\n- ProfileTable: " << profile_table_->DebugString();
  result << "\n- ClientTable: " << client_table_->DebugString();
  result << "\n- JobTable: " << job_table_->DebugString();
  return result.str();
}

ObjectTable &RedisGcsClient::object_table() { return *object_table_; }

raylet::TaskTable &RedisGcsClient::raylet_task_table() { return *raylet_task_table_; }

ActorTable &RedisGcsClient::actor_table() { return *actor_table_; }

WorkerFailureTable &RedisGcsClient::worker_failure_table() {
  return *worker_failure_table_;
}

TaskReconstructionLog &RedisGcsClient::task_reconstruction_log() {
  return *task_reconstruction_log_;
}

TaskLeaseTable &RedisGcsClient::task_lease_table() { return *task_lease_table_; }

ClientTable &RedisGcsClient::client_table() { return *client_table_; }

HeartbeatTable &RedisGcsClient::heartbeat_table() { return *heartbeat_table_; }

HeartbeatBatchTable &RedisGcsClient::heartbeat_batch_table() {
  return *heartbeat_batch_table_;
}

ErrorTable &RedisGcsClient::error_table() { return *error_table_; }

JobTable &RedisGcsClient::job_table() { return *job_table_; }

ProfileTable &RedisGcsClient::profile_table() { return *profile_table_; }

ActorCheckpointTable &RedisGcsClient::actor_checkpoint_table() {
  return *actor_checkpoint_table_;
}

ActorCheckpointIdTable &RedisGcsClient::actor_checkpoint_id_table() {
  return *actor_checkpoint_id_table_;
}

DynamicResourceTable &RedisGcsClient::resource_table() { return *resource_table_; }

}  // namespace gcs

}  // namespace ray
