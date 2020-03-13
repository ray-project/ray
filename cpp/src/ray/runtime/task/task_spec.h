
#pragma once

#include <ray/core.h>
#include <msgpack.hpp>
#include <vector>
#include <ray/core.h>

namespace ray { namespace api {

class LocalTaskSpec {
 public:
  TaskType type;
  JobID driverId;
  TaskID taskId;
  TaskID parentTaskId;
  int parentCounter;
  ActorID actorId;
  int actorCounter;
  int32_t func_offset;
  int32_t exec_func_offset;
  std::shared_ptr<msgpack::sbuffer> args;
  ObjectID returnId;

  LocalTaskSpec();

  int32_t get_func_offset() const;
  int32_t get_exec_func_offset() const;
  void set_func_offset(int32_t offset);
  void set_exec_func_offset(int32_t offset);
};

}  }// namespace ray::api