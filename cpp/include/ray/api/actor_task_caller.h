
#pragma once

#include <ray/api/arguments.h>
#include <ray/api/object_ref.h>
#include <ray/api/static_check.h>

namespace ray {
namespace internal {

template <typename F>
class ActorTaskCaller {
 public:
  ActorTaskCaller() = default;

  ActorTaskCaller(ray::internal::RayRuntime *runtime, std::string id,
                  ray::internal::RemoteFunctionHolder remote_function_holder)
      : runtime_(runtime),
        id_(id),
        remote_function_holder_(std::move(remote_function_holder)) {}

  template <typename... Args>
  ObjectRef<boost::callable_traits::return_type_t<F>> Remote(Args &&... args);

 private:
  ray::internal::RayRuntime *runtime_;
  std::string id_;
  ray::internal::RemoteFunctionHolder remote_function_holder_;
  std::vector<ray::internal::TaskArg> args_;
};

// ---------- implementation ----------

template <typename F>
template <typename... Args>
ObjectRef<boost::callable_traits::return_type_t<F>> ActorTaskCaller<F>::Remote(
    Args &&... args) {
  using ReturnType = boost::callable_traits::return_type_t<F>;
  ray::internal::StaticCheck<F, Args...>();

  ray::internal::Arguments::WrapArgs(&args_, std::forward<Args>(args)...);
  auto returned_object_id = runtime_->CallActor(remote_function_holder_, id_, args_);
  return ObjectRef<ReturnType>(returned_object_id);
}

}  // namespace internal
}  // namespace ray
