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
  AsyncGcsClient();
  ~AsyncGcsClient();

  /// Connect to the GCS.
  ///
  /// \param address The GCS IP address.
  /// \param port The GCS port.
  /// \param client_info Information about the local client to connect.
  /// \return Status.
  Status Connect(const std::string &address, int port,
                 const ClientTableDataT &client_info);
  /// Attach this client to a plasma event loop. Note that only
  /// one event loop should be attached at a time.
  Status Attach(plasma::EventLoop &event_loop);
  /// Attach this client to an asio event loop. Note that only
  /// one event loop should be attached at a time.
  Status Attach(boost::asio::io_service &io_service);

  inline FunctionTable &function_table();
  // TODO: Some API for getting the error on the driver
  inline ClassTable &class_table();
  inline ActorTable &actor_table();
  inline CustomSerializerTable &custom_serializer_table();
  inline ConfigTable &config_table();
  ObjectTable &object_table();
  TaskTable &task_table();
  raylet::TaskTable &raylet_task_table();
  TaskReconstructionLog &task_reconstruction_log();
  ClientTable &client_table();
  inline ErrorTable &error_table();

  // We also need something to export generic code to run on workers from the
  // driver (to set the PYTHONPATH)

  using GetExportCallback = std::function<void(const std::string &data)>;
  Status AddExport(const std::string &driver_id, std::string &export_data);
  Status GetExport(const std::string &driver_id, int64_t export_index,
                   const GetExportCallback &done_callback);

  std::shared_ptr<RedisContext> context() { return context_; }

 private:
  std::unique_ptr<FunctionTable> function_table_;
  std::unique_ptr<ClassTable> class_table_;
  std::unique_ptr<ObjectTable> object_table_;
  std::unique_ptr<TaskTable> task_table_;
  std::unique_ptr<raylet::TaskTable> raylet_task_table_;
  std::unique_ptr<TaskReconstructionLog> task_reconstruction_log_;
  std::unique_ptr<ClientTable> client_table_;
  std::shared_ptr<RedisContext> context_;
  std::unique_ptr<RedisAsioClient> asio_async_client_;
  std::unique_ptr<RedisAsioClient> asio_subscribe_client_;
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
