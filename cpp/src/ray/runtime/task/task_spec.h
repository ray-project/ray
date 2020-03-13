
#pragma once

#include <ray/core.h>
#include <msgpack.hpp>
#include <vector>
#include <ray/core.h>

namespace ray { namespace api {

class TaskSpec {
 public:
  TaskType type;
  JobID driverId;
  TaskID taskId;
  TaskID parentTaskId;
  int parentCounter;
  ActorID actorId;
  int actorCounter;
  int32_t funcOffset;
  int32_t execFuncOffset;
  std::shared_ptr<msgpack::sbuffer> args;
  ObjectID returnId;

  TaskSpec();

  int32_t GetFuncOffset() const;
  int32_t GetexecFuncOffset() const;
  void SetFuncOffset(int32_t offset);
  void SetexecFuncOffset(int32_t offset);
};

}  }// namespace ray::api