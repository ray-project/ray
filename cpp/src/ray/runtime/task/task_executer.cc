
#include <memory>

#include "task_executer.h"

namespace ray { namespace api {

std::unique_ptr<ObjectID> TaskExcuter::execute(const InvocationSpec &invocation) {
  std::unique_ptr<ObjectID> dummy(new ObjectID());
  return dummy;
};
}  }// namespace ray::api