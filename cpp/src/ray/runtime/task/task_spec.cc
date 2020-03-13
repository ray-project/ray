#include "task_spec.h"

namespace ray {
namespace api {

TaskSpec::TaskSpec() {
  driverId = JobID::Nil();
  taskId = TaskID::Nil();
  parentTaskId = TaskID::Nil();
  parentCounter = 0;
  actorId = ActorID::Nil();
  actorCounter = 0;
}

int32_t TaskSpec::GetFuncOffset() const { return funcOffset; }

int32_t TaskSpec::GetexecFuncOffset() const { return execFuncOffset; }

void TaskSpec::SetFuncOffset(int32_t offset) {
  funcOffset = offset;
  return;
}

void TaskSpec::SetexecFuncOffset(int32_t offset) {
  execFuncOffset = offset;
  return;
}
}  // namespace api
}  // namespace ray