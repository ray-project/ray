#pragma once

#include <memory>

#include <ray/api/ray_api.h>
#include "invocation_spec.h"

namespace ray {

class TaskSubmitter {

 public:
 TaskSubmitter();

  virtual std::unique_ptr<UniqueId> submitTask(const InvocationSpec &invocation) = 0;

  virtual std::unique_ptr<UniqueId> createActor(remote_function_ptr_holder &fptr, 
                                                std::vector< ::ray::blob> &&args) = 0;

  virtual std::unique_ptr<UniqueId> submitActorTask(const InvocationSpec &invocation) = 0;

  virtual ~TaskSubmitter(){};
};
}  // namespace ray