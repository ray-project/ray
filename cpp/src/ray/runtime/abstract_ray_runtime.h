
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

  void Put(std::shared_ptr<msgpack::sbuffer> data, const ObjectID &object_id);

  ObjectID Put(std::shared_ptr<msgpack::sbuffer> data);

  std::shared_ptr<msgpack::sbuffer> Get(const ObjectID &id);

  std::vector<std::shared_ptr<msgpack::sbuffer>> Get(const std::vector<ObjectID> &ids);

  WaitResult Wait(const std::vector<ObjectID> &ids, int num_objects, int timeout_ms);

  ObjectID Call(RemoteFunctionPtrHolder &fptr, std::shared_ptr<msgpack::sbuffer> args);

  ActorID CreateActor(RemoteFunctionPtrHolder &fptr,
                      std::shared_ptr<msgpack::sbuffer> args);

  ObjectID CallActor(const RemoteFunctionPtrHolder &fptr, const ActorID &actor,
                     std::shared_ptr<msgpack::sbuffer> args);

  ActorID GetNextActorID();

  const TaskID &GetCurrentTaskId();

  const JobID &GetCurrentJobID();

  const std::unique_ptr<WorkerContext> &GetWorkerContext();

 protected:
  std::shared_ptr<RayConfig> config_;
  std::unique_ptr<WorkerContext> worker_;
  std::unique_ptr<TaskSubmitter> task_submitter_;
  std::unique_ptr<TaskExecutor> task_executor_;
  std::unique_ptr<ObjectStore> object_store_;

 private:
  static AbstractRayRuntime *DoInit(std::shared_ptr<RayConfig> config);

  void Execute(const TaskSpecification &task_spec);

  friend class Ray;
};
}  // namespace api
}  // namespace ray