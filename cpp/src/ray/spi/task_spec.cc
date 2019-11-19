#include "task_spec.h"

namespace ray {

TaskSpec::TaskSpec() {
  driverId = nilUniqueId;
  taskId = nilUniqueId;
  parentTaskId = nilUniqueId;
  parentCounter = 0;
  actorId = nilUniqueId;
  actorCounter = 0;
}

int32_t TaskSpec::get_func_offset() const {
  int32_t func_offset;
  memcpy(&func_offset, functionId.data(), sizeof(int32_t));
  return func_offset;
}

int32_t TaskSpec::get_exec_func_offset() const {
  int32_t exec_func_offset;
  memcpy(&exec_func_offset, functionId.data() + sizeof(int32_t), sizeof(int32_t));
  return exec_func_offset;
}

void TaskSpec::set_func_offset(int32_t offset) {
  memcpy((void *)functionId.data(), (void *)&offset, sizeof(int32_t));
  return;
}

void TaskSpec::set_exec_func_offset(int32_t offset) {
  memcpy((void *)(functionId.data() + sizeof(int32_t)), (void *)&offset, sizeof(int32_t));
  return;
}
}