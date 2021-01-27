
#pragma once

#include <ray/api/ray_config.h>
#include <ray/api/ray_runtime.h>

#include <msgpack.hpp>
#include <mutex>

#include "./object/object_store.h"
#include "./task/task_executor.h"
#include "./task/task_submitter.h"
#include "ray/core.h"

namespace ray {
namespace api {

class AbstractRayRuntime : public RayRuntime {
 public:
  virtual ~AbstractRayRuntime(){};

  void Put(std::shared_ptr<msgpack::sbuffer> data, ObjectID *object_id);

  void Put(std::shared_ptr<msgpack::sbuffer> data, const ObjectID &object_id);

  ObjectID Put(std::shared_ptr<msgpack::sbuffer> data);

  std::shared_ptr<msgpack::sbuffer> Get(const ObjectID &id);

  std::vector<std::shared_ptr<msgpack::sbuffer>> Get(const std::vector<ObjectID> &ids);

  WaitResult Wait(const std::vector<ObjectID> &ids, int num_objects, int timeout_ms);

  ObjectID Call(const RemoteFunctionPtrHolder &fptr,
                std::vector<std::unique_ptr<::ray::TaskArg>> &args);

  ActorID CreateActor(const RemoteFunctionPtrHolder &fptr,
                      std::vector<std::unique_ptr<::ray::TaskArg>> &args);

  ObjectID CallActor(const RemoteFunctionPtrHolder &fptr, const ActorID &actor,
                     std::vector<std::unique_ptr<::ray::TaskArg>> &args);

  const TaskID &GetCurrentTaskId();

  const JobID &GetCurrentJobID();

  const std::unique_ptr<WorkerContext> &GetWorkerContext();

  static std::shared_ptr<AbstractRayRuntime> GetInstance();

 protected:
  std::shared_ptr<RayConfig> config_;
  std::unique_ptr<WorkerContext> worker_;
  std::unique_ptr<TaskSubmitter> task_submitter_;
  std::unique_ptr<TaskExecutor> task_executor_;
  std::unique_ptr<ObjectStore> object_store_;

 private:
  static std::shared_ptr<AbstractRayRuntime> abstract_ray_runtime_;
  static std::shared_ptr<AbstractRayRuntime> DoInit(std::shared_ptr<RayConfig> config);

  static void DoShutdown(std::shared_ptr<RayConfig> config);

  void Execute(const TaskSpecification &task_spec);

  friend class Ray;
};
}  // namespace api
}  // namespace ray