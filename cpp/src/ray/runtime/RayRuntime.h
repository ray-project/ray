
#pragma once

#include <mutex>

#include <ray/api/RayApi.h>
#include <ray/api/RayConfig.h>
#include <ray/api/Blob.h>
#include <ray/util/type-util.h>
#include "../spi/TaskProxy.h"
#include "../spi/ObjectProxy.h"
#include "../spi/Worker.h"

namespace ray {

class RayRuntime : public RayApi {
  friend class Ray;

 private:
  // RayRuntime() = delete;
  // RayRuntime(RayRuntime&) = delete;
  // RayRuntime& operator=(RayRuntime const&) = delete;

 protected:
  static std::unique_ptr<RayRuntime> _ins;
  static std::once_flag isInited;

  std::shared_ptr<RayConfig> _params;
  std::unique_ptr<Worker> _worker;
  std::unique_ptr<TaskProxy> _taskProxy;
  std::unique_ptr<ObjectProxy> _objectProxy;

 public:
  static RayRuntime &init(std::shared_ptr<RayConfig> params);

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
  static RayRuntime &doInit(std::shared_ptr<RayConfig> params);

  virtual char *get_actor_ptr(const UniqueId &id);

  void execute(const TaskSpec &taskSpec);
};
}