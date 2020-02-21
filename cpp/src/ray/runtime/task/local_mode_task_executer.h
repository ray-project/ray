#pragma once

#include <memory>

#include "task_executer.h"

namespace ray {

class LocalModeTaskExcuter : public TaskExcuter {

 public:
   std::unique_ptr<UniqueId> execute(const InvocationSpec &invocation);

};
}  // namespace ray