#ifndef RAY_GCS_CLIENT_H
#define RAY_GCS_CLIENT_H

#include <map>
#include <string>

#include "plasma/events.h"
#include "ray/gcs/asio.h"
#include "ray/gcs/tables.h"
#include "ray/id.h"
#include "ray/status.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

class RedisContext;

class RAY_EXPORT AsyncGcsClient {
 public:
  /// Start a GCS client with the given client ID and command type (regular or
  /// chain-replicated). To read from the GCS tables, Connect() and then
  /// Attach() must be called. To read and write from the GCS tables requires a
  /// further call to Connect() to the client table.
  ///
  /// \param address The GCS IP address.
  /// \param port The GCS port.
  /// \param sharding If true, use sharded redis for the GCS.
  /// \param client_id The ID to assign to the client.
  /// \param command_type GCS command type.  If CommandType::kChain, chain-replicated
  /// versions of the tables might be used, if available.
  AsyncGcsClient(const std::string &address, int port, const ClientID &client_id,
                 CommandType command_type, bool is_test_client);
  AsyncGcsClient(const std::string &address, int port, const ClientID &client_id,
                 bool is_test_client);
  AsyncGcsClient(const std::string &address, int port, CommandType command_type);
  AsyncGcsClient(const std::string &address, int port, CommandType command_type,
                 bool is_test_client);
  AsyncGcsClient(const std::string &address, int port);
  AsyncGcsClient(const std::string &address, int port, bool is_test_client);

  /// Attach this client to a plasma event loop. Note that only
  /// one event loop should be attached at a time.
  Status Attach(plasma::EventLoop &event_loop);
  /// Attach this client to an asio event loop. Note that only
  /// one event loop should be attached at a time.
  Status Attach(boost::asio::io_service &io_service);

  inline FunctionTable &function_table();
  // TODO: Some API for getting the error on the driver
  inline ClassTable &class_table();
  inline CustomSerializerTable &custom_serializer_table();
  inline ConfigTable &config_table();
  ObjectTable &object_table();
  TaskTable &task_table();
  raylet::TaskTable &raylet_task_table();
  ActorTable &actor_table();
  TaskReconstructionLog &task_reconstruction_log();
  TaskLeaseTable &task_lease_table();
  ClientTable &client_table();
  HeartbeatTable &heartbeat_table();
  ErrorTable &error_table();
  DriverTable &driver_table();
  ProfileTable &profile_table();

  // We also need something to export generic code to run on workers from the
  // driver (to set the PYTHONPATH)

  using GetExportCallback = std::function<void(const std::string &data)>;
  Status AddExport(const std::string &driver_id, std::string &export_data);
  Status GetExport(const std::string &driver_id, int64_t export_index,
                   const GetExportCallback &done_callback);

  std::vector<std::shared_ptr<RedisContext>> shard_contexts() { return shard_contexts_; }
  std::shared_ptr<RedisContext> primary_context() { return primary_context_; }

 private:
  std::unique_ptr<FunctionTable> function_table_;
  std::unique_ptr<ClassTable> class_table_;
  std::unique_ptr<ObjectTable> object_table_;
  std::unique_ptr<TaskTable> task_table_;
  std::unique_ptr<raylet::TaskTable> raylet_task_table_;
  std::unique_ptr<ActorTable> actor_table_;
  std::unique_ptr<TaskReconstructionLog> task_reconstruction_log_;
  std::unique_ptr<TaskLeaseTable> task_lease_table_;
  std::unique_ptr<HeartbeatTable> heartbeat_table_;
  std::unique_ptr<ErrorTable> error_table_;
  std::unique_ptr<ProfileTable> profile_table_;
  std::unique_ptr<ClientTable> client_table_;
  // The following contexts write to the data shard
  std::vector<std::shared_ptr<RedisContext>> shard_contexts_;
  std::vector<std::unique_ptr<RedisAsioClient>> shard_asio_async_clients_;
  std::vector<std::unique_ptr<RedisAsioClient>> shard_asio_subscribe_clients_;
  // The following context writes everything to the primary shard
  std::shared_ptr<RedisContext> primary_context_;
  std::unique_ptr<DriverTable> driver_table_;
  std::unique_ptr<RedisAsioClient> asio_async_auxiliary_client_;
  std::unique_ptr<RedisAsioClient> asio_subscribe_auxiliary_client_;
  CommandType command_type_;
};

class SyncGcsClient {
  Status LogEvent(const std::string &key, const std::string &value, double timestamp);
  Status NotifyError(const std::map<std::string, std::string> &error_info);
  Status RegisterFunction(const JobID &job_id, const FunctionID &function_id,
                          const std::string &language, const std::string &name,
                          const std::string &data);
  Status RetrieveFunction(const JobID &job_id, const FunctionID &function_id,
                          std::string *name, std::string *data);

  Status AddExport(const std::string &driver_id, std::string &export_data);
  Status GetExport(const std::string &driver_id, int64_t export_index, std::string *data);
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_CLIENT_H
