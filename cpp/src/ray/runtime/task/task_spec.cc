#include "task_spec.h"

namespace ray { namespace api {

LocalTaskSpec::LocalTaskSpec() {
  driverId = JobID::Nil();
  taskId = TaskID::Nil();
  parentTaskId = TaskID::Nil();
  parentCounter = 0;
  actorId = ActorID::Nil();
  actorCounter = 0;
}

int32_t LocalTaskSpec::get_func_offset() const {
  return func_offset;
}

int32_t LocalTaskSpec::get_exec_func_offset() const {
  return exec_func_offset;
}

void LocalTaskSpec::set_func_offset(int32_t offset) {
  func_offset = offset;
  return;
}

void LocalTaskSpec::set_exec_func_offset(int32_t offset) {
  exec_func_offset = offset;
  return;
}
}  }// namespace ray::api