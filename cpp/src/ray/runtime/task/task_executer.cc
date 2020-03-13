
#include <memory>

#include "task_executer.h"

namespace ray {
namespace api {

std::unique_ptr<ObjectID> TaskExcuter::Execute(const InvocationSpec &invocation) {
  std::unique_ptr<ObjectID> dummy(new ObjectID());
  return dummy;
};
}  // namespace api
}  // namespace ray