
#pragma once

#include <mutex>

#include <ray/api/ray_config.h>
#include <ray/api/ray_runtime.h>
#include <msgpack.hpp>
#include <ray/core.h>
#include "./object/object_store.h"
#include "./task/task_executer.h"
#include "./task/task_submitter.h"
#include "./task/task_spec.h"

namespace ray { namespace api {

class AbstractRayRuntime : public RayRuntime {
  friend class Ray;

 private:
 protected:
  static std::unique_ptr<AbstractRayRuntime> _ins;
  static std::once_flag isInited;

  std::shared_ptr<RayConfig> _config;
  std::unique_ptr<WorkerContext> _worker;
  std::unique_ptr<TaskSubmitter> _taskSubmitter;
  std::unique_ptr<TaskExcuter> _taskExcuter;
  std::unique_ptr<ObjectStore> _objectStore;

 public:
  static AbstractRayRuntime &Init(std::shared_ptr<RayConfig> config);

  static AbstractRayRuntime &GetInstance();

  void Put(std::shared_ptr<msgpack::sbuffer> data, const ObjectID &objectId,
           const TaskID &taskId);

  ObjectID Put(std::shared_ptr<msgpack::sbuffer> data);

  std::shared_ptr<msgpack::sbuffer> Get(const ObjectID &id);

  std::vector<std::shared_ptr<msgpack::sbuffer>> Get(
      const std::vector<ObjectID> &objects);

  WaitResult Wait(const std::vector<ObjectID> &objects, int num_objects,
                          int64_t timeout_ms);

  ObjectID Call(remote_function_ptr_holder &fptr,
                                 std::shared_ptr<msgpack::sbuffer> args);

  ActorID Create(remote_function_ptr_holder &fptr,
                                   std::shared_ptr<msgpack::sbuffer> args);

  ObjectID Call(const remote_function_ptr_holder &fptr,
                                 const ActorID &actor,
                                 std::shared_ptr<msgpack::sbuffer> args);

  ActorID GetNextActorID();

  const TaskID &GetCurrentTaskId();

  virtual ~AbstractRayRuntime(){};

 private:
  static AbstractRayRuntime &DoInit(std::shared_ptr<RayConfig> config);

  void Execute(const TaskSpec &taskSpec);
};
}  }// namespace ray::api