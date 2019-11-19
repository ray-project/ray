
#pragma once

#include <ray/api/ray_config.h>
#include <ray/api/uniqueId.h>
#include "task_spec.h"

namespace ray {

class Worker {
 private:
  UniqueId _workerId;
  UniqueId _actorId;
  int _taskCounter;
  bool _connected;
  int _currentTaskPutCounter;
  TaskSpec *_currentTask;
  RayConfig _params;

 public:
  Worker(std::shared_ptr<RayConfig> params);

  void onSubmitTask();

  void setCurrentTask(TaskSpec &task);

  TaskSpec *getCurrentTask();

  int getNextPutIndex();

  const UniqueId &getWorkerId();

  const UniqueId &getActorId();

  const UniqueId &getCurrentTaskId();

  std::unique_ptr<UniqueId> getCurrentTaskNextPutId();

  RunMode getCurrentWorkerRunMode();
};

}  // namespace ray