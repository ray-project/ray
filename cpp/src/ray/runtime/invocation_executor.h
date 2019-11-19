#pragma once

#include "../spi/task_spec.h"

namespace ray {

class InvocationExecutor {
 public:
  static void execute(const TaskSpec &taskSpec, uintptr_t dylib_base_addr, char *actor);
};
}