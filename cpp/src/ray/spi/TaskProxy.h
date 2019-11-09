
#pragma once

#include <memory>

#include "InvocationSpec.h"
#include "TaskInterface.h"

namespace ray {

class TaskProxy {
 private:
  std::unique_ptr<TaskInterface> _taskInterface;

 public:
  TaskProxy(std::unique_ptr<TaskInterface> scheduler);

  std::unique_ptr<UniqueId> submit(const InvocationSpec &invocation);

  std::unique_ptr<TaskSpec> getTask();

  void markTaskPutDependency(const UniqueId &taskId, const UniqueId &objectId);

  void reconstructObject(const UniqueId &objectId);

  void notifyUnblocked();

  const UniqueId &getSchedulerId();

  std::unique_ptr<UniqueId> buildReturnId(const UniqueId &taskId, int index);

  std::list<std::unique_ptr<UniqueId> > buildReturnIds(const UniqueId &taskId,
                                                       int returnCount);
};
}