
#pragma once

#include <memory>
#include <queue>

#include "TaskInterface.h"

namespace ray {

class TaskInterfaceMock : public TaskInterface {
 private:
  std::queue<TaskSpec> tasks;

 public:
  void submitTask(TaskSpec &&task);

  std::unique_ptr<TaskSpec> getTask();

  void markTaskPutDependency(const UniqueId &taskId, const UniqueId &objectId);

  void reconstructObject(const UniqueId &objectId);

  void notifyUnblocked();

  const UniqueId &getSchedulerId();
};

}  // namespace ray