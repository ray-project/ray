
#pragma once

#include <mutex>

#include <ray/api/ray_config.h>
#include <ray/api/ray_runtime.h>
#include <msgpack.hpp>
#include "./context/worker_context.h"
#include "./object/object_store.h"
#include "./task/task_executer.h"
#include "./task/task_submitter.h"

namespace ray {

class AbstractRayRuntime : public RayRuntime {
  friend class Ray;

 private:
 protected:
  static std::unique_ptr<AbstractRayRuntime> _ins;
  static std::once_flag isInited;

  std::shared_ptr<RayConfig> _config;
  std::unique_ptr<Worker> _worker;
  std::unique_ptr<TaskSubmitter> _taskSubmitter;
  std::unique_ptr<TaskExcuter> _taskExcuter;
  std::unique_ptr<ObjectStore> _objectStore;

 public:
  static AbstractRayRuntime &init(std::shared_ptr<RayConfig> config);

  static AbstractRayRuntime &getInstance();

  void put(std::shared_ptr<msgpack::sbuffer> data, const UniqueId &objectId,
           const UniqueId &taskId);

  UniqueId put(std::shared_ptr<msgpack::sbuffer> data);

  std::shared_ptr<msgpack::sbuffer> get(const UniqueId &id);

  std::vector<std::shared_ptr<msgpack::sbuffer>> get(
      const std::vector<UniqueId> &objects);

  WaitResultInternal wait(const std::vector<UniqueId> &objects, int num_objects,
                          int64_t timeout_ms);

  std::unique_ptr<UniqueId> call(remote_function_ptr_holder &fptr,
                                 std::shared_ptr<msgpack::sbuffer> args);

  std::unique_ptr<UniqueId> create(remote_function_ptr_holder &fptr,
                                   std::shared_ptr<msgpack::sbuffer> args);

  std::unique_ptr<UniqueId> call(const remote_function_ptr_holder &fptr,
                                 const UniqueId &actor,
                                 std::shared_ptr<msgpack::sbuffer> args);

  TaskSpec *getCurrentTask();

  void setCurrentTask(TaskSpec &task);

  int getNextPutIndex();

  const UniqueId &getCurrentTaskId();

  virtual ~AbstractRayRuntime(){};

 private:
  static AbstractRayRuntime &doInit(std::shared_ptr<RayConfig> config);

  virtual char *get_actor_ptr(const UniqueId &id);

  void execute(const TaskSpec &taskSpec);
};
}  // namespace ray