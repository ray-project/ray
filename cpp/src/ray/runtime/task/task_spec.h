
#pragma once

#include <list>
#include <vector>

#include <ray/api/blob.h>
#include <ray/api/uniqueId.h>

namespace ray {

enum TaskType { 
  NORMAL_TASK, 
  ACTOR_CREATION_TASK, 
  ACTOR_TASK,
  UNRECOGNIZED
  };

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
  std::vector< ::ray::blob> args;
  std::list<std::unique_ptr<UniqueId> > returnIds;

  TaskSpec();

  int32_t get_func_offset() const;
  int32_t get_exec_func_offset() const;
  void set_func_offset(int32_t offset);
  void set_exec_func_offset(int32_t offset);
};

}  // namespace ray