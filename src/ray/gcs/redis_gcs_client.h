#ifndef RAY_GCS_REDIS_GCS_CLIENT_H
#define RAY_GCS_REDIS_GCS_CLIENT_H

#include <map>
#include <string>

#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/gcs/asio.h"
#include "ray/gcs/gcs_client.h"
#include "ray/gcs/tables.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

class RedisContext;

class RAY_EXPORT RedisGcsClient : public GcsClient {
  // TODO(micafan) Will remove those friend class after we replace RedisGcsClient
  // with interface class GcsClient in raylet.
  friend class RedisActorInfoAccessor;
  friend class RedisJobInfoAccessor;
  friend class SubscriptionExecutorTest;
  friend class LogSubscribeTestHelper;

 public:
  /// Constructor of RedisGcsClient.
  /// Connect() must be called(and return ok) before you call any other methods.
  /// TODO(micafan) To read and write from the GCS tables requires a further
  /// call to Connect() to the client table. Will fix this in next pr.
  ///
  /// \param options Options of this client, e.g. server address, password and so on.
  RedisGcsClient(const GcsClientOptions &options);

  /// This constructor is only used for testing.
  /// Connect() must be called(and return ok) before you call any other methods.
  ///
  /// \param options Options of this client, e.g. server address, password and so on.
  /// \param command_type The commands issued type.
  RedisGcsClient(const GcsClientOptions &options, CommandType command_type);

  /// Connect to GCS Service. Non-thread safe.
  /// Call this function before calling other functions.
  ///
  /// \param io_service The event loop for this client.
  /// Must be single-threaded io_service (get more information from RedisAsioClient).
  ///
  /// \return Status
  Status Connect(boost::asio::io_service &io_service);

  /// Disconnect with GCS Service. Non-thread safe.
  void Disconnect();

  // TODO: Some API for getting the error on the driver
  ObjectTable &object_table();
  raylet::TaskTable &raylet_task_table();
  TaskReconstructionLog &task_reconstruction_log();
  TaskLeaseTable &task_lease_table();
  ClientTable &client_table();
  HeartbeatTable &heartbeat_table();
  HeartbeatBatchTable &heartbeat_batch_table();
  ErrorTable &error_table();
  ProfileTable &profile_table();
  ActorCheckpointTable &actor_checkpoint_table();
  ActorCheckpointIdTable &actor_checkpoint_id_table();
  DynamicResourceTable &resource_table();
  /// Used only for direct calls. Tasks submitted through the raylet transport
  /// should use Actors(), which has a requirement on the order in which
  /// entries can be appended to the log.
  DirectActorTable &direct_actor_table();

  // We also need something to export generic code to run on workers from the
  // driver (to set the PYTHONPATH)

  using GetExportCallback = std::function<void(const std::string &data)>;
  Status AddExport(const std::string &job_id, std::string &export_data);
  Status GetExport(const std::string &job_id, int64_t export_index,
                   const GetExportCallback &done_callback);

  std::vector<std::shared_ptr<RedisContext>> shard_contexts() { return shard_contexts_; }
  std::shared_ptr<RedisContext> primary_context() { return primary_context_; }

  /// Returns debug string for class.
  ///
  /// \return string.
  std::string DebugString() const;

 private:
  /// Attach this client to an asio event loop. Note that only
  /// one event loop should be attached at a time.
  void Attach(boost::asio::io_service &io_service);

  /// This method will be deprecated, use method Actors() instead.
  ActorTable &actor_table();
  /// This method will be deprecated, use method Jobs() instead.
  JobTable &job_table();

  // GCS command type. If CommandType::kChain, chain-replicated versions of the tables
  // might be used, if available.
  CommandType command_type_{CommandType::kUnknown};

  std::unique_ptr<ObjectTable> object_table_;
  std::unique_ptr<raylet::TaskTable> raylet_task_table_;
  std::unique_ptr<ActorTable> actor_table_;
  std::unique_ptr<DirectActorTable> direct_actor_table_;
  std::unique_ptr<TaskReconstructionLog> task_reconstruction_log_;
  std::unique_ptr<TaskLeaseTable> task_lease_table_;
  std::unique_ptr<HeartbeatTable> heartbeat_table_;
  std::unique_ptr<HeartbeatBatchTable> heartbeat_batch_table_;
  std::unique_ptr<ErrorTable> error_table_;
  std::unique_ptr<ProfileTable> profile_table_;
  std::unique_ptr<ClientTable> client_table_;
  std::unique_ptr<ActorCheckpointTable> actor_checkpoint_table_;
  std::unique_ptr<ActorCheckpointIdTable> actor_checkpoint_id_table_;
  std::unique_ptr<DynamicResourceTable> resource_table_;
  // The following contexts write to the data shard
  std::vector<std::shared_ptr<RedisContext>> shard_contexts_;
  std::vector<std::unique_ptr<RedisAsioClient>> shard_asio_async_clients_;
  std::vector<std::unique_ptr<RedisAsioClient>> shard_asio_subscribe_clients_;
  // The following context writes everything to the primary shard
  std::shared_ptr<RedisContext> primary_context_;
  std::unique_ptr<JobTable> job_table_;
  std::unique_ptr<RedisAsioClient> asio_async_auxiliary_client_;
  std::unique_ptr<RedisAsioClient> asio_subscribe_auxiliary_client_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_REDIS_GCS_CLIENT_H
