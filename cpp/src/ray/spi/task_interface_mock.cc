
#include "task_interface_mock.h"

#include <utility>

#include <ray/api/uniqueId.h>

namespace ray {

void TaskInterfaceMock::submitTask(TaskSpec &&task) {
  tasks.push(std::forward<TaskSpec>(task));
}

std::unique_ptr<TaskSpec> TaskInterfaceMock::getTask() {
  std::unique_ptr<TaskSpec> ret(new TaskSpec(std::move(tasks.front())));
  tasks.pop();
  return ret;
}

void TaskInterfaceMock::markTaskPutDependency(const UniqueId &taskId,
                                              const UniqueId &objectId) {}

void TaskInterfaceMock::reconstructObject(const UniqueId &objectId) {}

void TaskInterfaceMock::notifyUnblocked() {}

const UniqueId &TaskInterfaceMock::getSchedulerId() { return nilUniqueId; }

}  // namespace ray