#ifndef RAY_GCS_CLIENT_H
#define RAY_GCS_CLIENT_H

#include <map>
#include <string>

#include "plasma/events.h"
#include "ray/id.h"
#include "ray/status.h"
#include "ray/gcs/tables.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

class RedisContext;

class RAY_EXPORT AsyncGcsClient {
 public:
  AsyncGcsClient();
  ~AsyncGcsClient();

  Status Connect(const std::string &address, int port);
  Status Attach(plasma::EventLoop &event_loop);

  inline FunctionTable &function_table();
  // TODO: Some API for getting the error on the driver
  inline ClassTable &class_table();
  inline ActorTable &actor_table();
  inline CustomSerializerTable &custom_serializer_table();
  inline ConfigTable &config_table();
  ObjectTable &object_table();
  TaskTable &task_table();
  inline ErrorTable &error_table();

  // We also need something to export generic code to run on workers from the
  // driver (to set the PYTHONPATH)

  using GetExportCallback = std::function<void(const std::string &data)>;
  Status AddExport(const std::string &driver_id, std::string &export_data);
  Status GetExport(const std::string &driver_id,
                   int64_t export_index,
                   const GetExportCallback &done_callback);

  std::shared_ptr<RedisContext> context() { return context_; }

 private:
  std::unique_ptr<FunctionTable> function_table_;
  std::unique_ptr<ClassTable> class_table_;
  std::unique_ptr<ObjectTable> object_table_;
  std::unique_ptr<TaskTable> task_table_;
  std::shared_ptr<RedisContext> context_;
};

class SyncGcsClient {
  Status LogEvent(const std::string &key,
                  const std::string &value,
                  double timestamp);
  Status NotifyError(const std::map<std::string, std::string> &error_info);
  Status RegisterFunction(const JobID &job_id,
                          const FunctionID &function_id,
                          const std::string &language,
                          const std::string &name,
                          const std::string &data);
  Status RetrieveFunction(const JobID &job_id,
                          const FunctionID &function_id,
                          std::string *name,
                          std::string *data);

  Status AddExport(const std::string &driver_id, std::string &export_data);
  Status GetExport(const std::string &driver_id,
                   int64_t export_index,
                   std::string *data);
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_CLIENT_H
