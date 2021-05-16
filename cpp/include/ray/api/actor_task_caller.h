
#pragma once

#include <ray/api/arguments.h>
#include <ray/api/exec_funcs.h>
#include <ray/api/object_ref.h>
#include <ray/api/static_check.h>
#include "ray/core.h"

namespace ray {
namespace api {

template <typename F>
class ActorTaskCaller {
 public:
  ActorTaskCaller() = default;

  ActorTaskCaller(RayRuntime *runtime, ActorID id, RemoteFunctionHolder ptr,
                  std::vector<std::unique_ptr<::ray::TaskArg>> &&args)
      : runtime_(runtime), id_(id), ptr_(std::move(ptr)), args_(std::move(args)) {}

  ActorTaskCaller(RayRuntime *runtime, ActorID id, RemoteFunctionHolder ptr)
      : runtime_(runtime), id_(id), ptr_(std::move(ptr)) {}

  template <typename... Args>
  ObjectRef<boost::callable_traits::return_type_t<F>> Remote(Args &&... args);

 private:
  RayRuntime *runtime_;
  ActorID id_;
  RemoteFunctionHolder ptr_;
  std::vector<std::unique_ptr<::ray::TaskArg>> args_;
};

// ---------- implementation ----------

template <typename F>
template <typename... Args>
ObjectRef<boost::callable_traits::return_type_t<F>> ActorTaskCaller<F>::Remote(
    Args &&... args) {
  using ReturnType = boost::callable_traits::return_type_t<F>;
  StaticCheck<F, Args...>();

  Arguments::WrapArgs(&args_, std::forward<Args>(args)...);
  auto returned_object_id = runtime_->CallActor(ptr_, id_, args_);
  return ObjectRef<ReturnType>(returned_object_id);
}

}  // namespace api
}  // namespace ray
