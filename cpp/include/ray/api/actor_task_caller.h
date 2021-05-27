
#pragma once

#include <ray/api/arguments.h>
#include <ray/api/object_ref.h>
#include <ray/api/static_check.h>

namespace ray {
namespace api {

template <typename F>
class ActorTaskCaller {
 public:
  ActorTaskCaller() = default;

  ActorTaskCaller(RayRuntime *runtime, std::string id,
                  RemoteFunctionHolder remote_function_holder)
      : runtime_(runtime),
        id_(id),
        remote_function_holder_(std::move(remote_function_holder)) {}

  template <typename... Args>
  ObjectRef<boost::callable_traits::return_type_t<F>> Remote(Args &&... args);

 private:
  RayRuntime *runtime_;
  std::string id_;
  RemoteFunctionHolder remote_function_holder_;
  std::vector<ray::api::TaskArg> args_;
};

// ---------- implementation ----------

template <typename F>
template <typename... Args>
ObjectRef<boost::callable_traits::return_type_t<F>> ActorTaskCaller<F>::Remote(
    Args &&... args) {
  using ReturnType = boost::callable_traits::return_type_t<F>;
  StaticCheck<F, Args...>();

  Arguments::WrapArgs(&args_, std::forward<Args>(args)...);
  auto returned_object_id = runtime_->CallActor(remote_function_holder_, id_, args_);
  return ObjectRef<ReturnType>(returned_object_id);
}

}  // namespace api
}  // namespace ray
