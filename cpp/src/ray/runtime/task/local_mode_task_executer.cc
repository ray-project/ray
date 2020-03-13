
#include <memory>

#include "local_mode_task_executer.h"

namespace ray { namespace api {

std::unique_ptr<ObjectID> LocalModeTaskExcuter::execute(
    const InvocationSpec &invocation) {
  std::unique_ptr<ObjectID> dummy(new ObjectID());
  return dummy;
};
}  }// namespace ray::api