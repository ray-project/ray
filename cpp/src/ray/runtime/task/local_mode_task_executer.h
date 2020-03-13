#pragma once

#include <memory>

#include "task_executer.h"

namespace ray { namespace api {

class LocalModeTaskExcuter : public TaskExcuter {
 public:
  std::unique_ptr<ObjectID> Execute(const InvocationSpec &invocation);
};
}  }// namespace ray::api