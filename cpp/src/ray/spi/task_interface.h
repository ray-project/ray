
#pragma once

#include <ray/api/blob.h>
#include <ray/api/uniqueId.h>
#include <vector>

#include "task_spec.h"

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