
#pragma once

#include <ray/api/Blob.h>
#include <ray/api/UniqueId.h>
#include <vector>

#include "TaskSpec.h"

namespace ray {

class TaskInterface {
 public:
  virtual void submitTask(TaskSpec &&task) = 0;

  virtual std::unique_ptr<TaskSpec> getTask() = 0;

  virtual void markTaskPutDependency(const UniqueId &taskId,
                                     const UniqueId &objectId) = 0;

  virtual void reconstructObject(const UniqueId &objectId) = 0;

  virtual void notifyUnblocked() = 0;

  virtual const UniqueId &getSchedulerId() = 0;

  virtual ~TaskInterface(){};
};
}