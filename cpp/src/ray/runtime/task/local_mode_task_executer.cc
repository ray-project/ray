
#include <memory>

#include "local_mode_task_executer.h"

namespace ray {

std::unique_ptr<UniqueId> LocalModeTaskExcuter::execute(
    const InvocationSpec &invocation) {
  std::unique_ptr<UniqueId> dummy(new UniqueId());
  return dummy;
};
}  // namespace ray