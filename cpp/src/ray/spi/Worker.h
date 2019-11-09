
#pragma once

#include <ray/api/RayConfig.h>
#include <ray/api/UniqueId.h>
#include "TaskSpec.h"

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