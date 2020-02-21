
#include "worker_context.h"

namespace ray {

static TaskSpec dummyTaskSpec;

Worker::Worker(std::shared_ptr<RayConfig> config) {
  // TODO: workerId =
  // TODO: actorId =
  _taskCounter = 0;
  _connected = false;
  _currentTaskPutCounter = 0;
  _currentTask = &dummyTaskSpec;
  // TODO: _currentTask->actorId =

  UniqueId uid;
  uid.random();                  // TODO: make it deterministic
  _currentTask->driverId = uid;  // TODO: parse for config
  //_currentTask->driverId = &UniqueId.from_random(); //???
  //_currentTask->parentTaskId = nilUniqueId;
  uid.random();  // TODO: make it deterministic
  _currentTask->taskId = uid;
  _config = *config;
}

void Worker::onSubmitTask() { _taskCounter++; }

void Worker::setCurrentTask(TaskSpec &task) { _currentTask = &task; }

TaskSpec *Worker::getCurrentTask() { return _currentTask; }

int Worker::getNextPutIndex() { return _currentTaskPutCounter++; }

const UniqueId &Worker::getWorkerId() { return _workerId; }

const UniqueId &Worker::getActorId() { return _actorId; }

const UniqueId &Worker::getCurrentTaskId() { return _currentTask->taskId; }

std::unique_ptr<UniqueId> Worker::getCurrentTaskNextPutId() {
  return _currentTask->taskId.taskComputePutId(getNextPutIndex());
}

RunMode Worker::getCurrentWorkerRunMode() { return _config.runMode; }

}  // namespace ray