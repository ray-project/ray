
#pragma once

#include <mutex>

#include <ray/api/ray_config.h>
#include <ray/api/ray_runtime.h>
#include <msgpack.hpp>
#include "./object/object_store.h"
#include "./task/task_executor.h"
#include "./task/task_submitter.h"
#include "ray/core.h"

namespace ray {
namespace api {

class AbstractRayRuntime : public RayRuntime {
 public:
  virtual ~AbstractRayRuntime(){};

  static AbstractRayRuntime &GetInstance();

  void Put(std::shared_ptr<msgpack::sbuffer> data, const ObjectID &object_id);

  ObjectID Put(std::shared_ptr<msgpack::sbuffer> data);

  std::shared_ptr<msgpack::sbuffer> Get(const ObjectID &id);

  std::vector<std::shared_ptr<msgpack::sbuffer>> Get(
      const std::vector<ObjectID> &objects);

  WaitResult Wait(const std::vector<ObjectID> &objects, int num_objects,
                  int64_t timeout_ms);

  ObjectID Call(RemoteFunctionPtrHolder &fptr, std::shared_ptr<msgpack::sbuffer> args);

  ActorID CreateActor(RemoteFunctionPtrHolder &fptr,
                      std::shared_ptr<msgpack::sbuffer> args);

  ObjectID CallActor(const RemoteFunctionPtrHolder &fptr, const ActorID &actor,
                     std::shared_ptr<msgpack::sbuffer> args);

  ActorID GetNextActorID();

  const TaskID &GetCurrentTaskId();

  const JobID &GetCurrentJobID();

 protected:
  static std::unique_ptr<AbstractRayRuntime> ins_;
  static std::once_flag isInited_;
  std::shared_ptr<RayConfig> config_;
  std::unique_ptr<WorkerContext> worker_;
  std::unique_ptr<TaskSubmitter> taskSubmitter_;
  std::unique_ptr<TaskExecutor> TaskExecutor_;
  std::unique_ptr<ObjectStore> objectStore_;

 private:
  static AbstractRayRuntime &DoInit(std::shared_ptr<RayConfig> config);

  void Execute(const TaskSpecification &taskSpec);

  friend class Ray;
};
}  // namespace api
}  // namespace ray