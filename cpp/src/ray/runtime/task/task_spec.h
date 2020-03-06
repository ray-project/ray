
#pragma once

#include <ray/api/task_type.h>
#include <ray/api/uniqueId.h>
#include <list>
#include <msgpack.hpp>
#include <vector>

namespace ray {

class TaskSpec {
 public:
  TaskType type;
  UniqueId driverId;
  UniqueId taskId;
  UniqueId parentTaskId;
  int parentCounter;
  UniqueId actorId;
  int actorCounter;
  UniqueId functionId;
  std::shared_ptr<msgpack::sbuffer> args;
  std::list<std::unique_ptr<UniqueId> > returnIds;

  TaskSpec();

  int32_t get_func_offset() const;
  int32_t get_exec_func_offset() const;
  void set_func_offset(int32_t offset);
  void set_exec_func_offset(int32_t offset);
};

}  // namespace ray