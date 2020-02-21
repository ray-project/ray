
#pragma once

#include <mutex>

#include <ray/api/blob.h>
#include <ray/api/ray_api.h>
#include <ray/api/ray_config.h>
#include <ray/util/type_util.h>
#include "./context/worker_context.h"
#include "./object/object_store.h"
#include "./task/task_executer.h"
#include "./task/task_submitter.h"

namespace ray {

class RayRuntime : public RayApi {
  friend class Ray;

 private:
 protected:
  static std::unique_ptr<RayRuntime> _ins;
  static std::once_flag isInited;

  std::shared_ptr<RayConfig> _config;
  std::unique_ptr<Worker> _worker;
  std::unique_ptr<TaskSubmitter> _taskSubmitter;
  std::unique_ptr<TaskExcuter> _taskExcuter;
  std::unique_ptr<ObjectStore> _objectStore;

 public:
  static RayRuntime &init(std::shared_ptr<RayConfig> config);

  static RayRuntime &getInstance();

  void put(std::vector< ::ray::blob> &&data, const UniqueId &objectId,
           const UniqueId &taskId);

  std::unique_ptr<UniqueId> put(std::vector< ::ray::blob> &&data);

  del_unique_ptr< ::ray::blob> get(const UniqueId &objectId);

  std::unique_ptr<UniqueId> call(remote_function_ptr_holder &fptr,
                                 std::vector< ::ray::blob> &&args);

  std::unique_ptr<UniqueId> create(remote_function_ptr_holder &fptr,
                                   std::vector< ::ray::blob> &&args);

  std::unique_ptr<UniqueId> call(const remote_function_ptr_holder &fptr,
                                 const UniqueId &actor, std::vector< ::ray::blob> &&args);

  TaskSpec *getCurrentTask();

  void setCurrentTask(TaskSpec &task);

  int getNextPutIndex();

  const UniqueId &getCurrentTaskId();

  virtual ~RayRuntime(){};

 private:
  static RayRuntime &doInit(std::shared_ptr<RayConfig> config);

  virtual char *get_actor_ptr(const UniqueId &id);

  void execute(const TaskSpec &taskSpec);
};
}  // namespace ray