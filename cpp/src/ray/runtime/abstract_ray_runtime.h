
#pragma once

#include <ray/api/ray_config.h>
#include <ray/api/ray_runtime.h>

#include <msgpack.hpp>
#include <mutex>

#include "./object/object_store.h"
#include "./task/task_executor.h"
#include "./task/task_submitter.h"
#include "ray/common/id.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"

namespace ray {
namespace api {

class AbstractRayRuntime : public RayRuntime {
 public:
  virtual ~AbstractRayRuntime(){};

  void Put(std::shared_ptr<msgpack::sbuffer> data, ObjectID *object_id);

  void Put(std::shared_ptr<msgpack::sbuffer> data, const ObjectID &object_id);

  std::string Put(std::shared_ptr<msgpack::sbuffer> data);

  std::shared_ptr<msgpack::sbuffer> Get(const std::string &id);

  std::vector<std::shared_ptr<msgpack::sbuffer>> Get(const std::vector<std::string> &ids);

  std::vector<bool> Wait(const std::vector<std::string> &ids, int num_objects,
                         int timeout_ms);

  std::string Call(const RemoteFunctionHolder &remote_function_holder,
                   std::vector<ray::api::TaskArg> &args);

  std::string CreateActor(const RemoteFunctionHolder &remote_function_holder,
                          std::vector<ray::api::TaskArg> &args);

  std::string CallActor(const RemoteFunctionHolder &remote_function_holder,
                        const std::string &actor, std::vector<ray::api::TaskArg> &args);

  void AddLocalReference(const std::string &id);

  void RemoveLocalReference(const std::string &id);

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