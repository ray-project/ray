
#pragma once

#include <ray/api/static_check.h>

namespace ray {
namespace call {

template <typename F>
class TaskCaller {
 public:
  TaskCaller();

  TaskCaller(ray::api::RayRuntime *runtime,
             ray::api::RemoteFunctionHolder remote_function_holder);

  template <typename... Args>
  ObjectRef<boost::callable_traits::return_type_t<F>> Remote(Args &&... args);

 private:
  ray::api::RayRuntime *runtime_;
  ray::api::RemoteFunctionHolder remote_function_holder_{};
  std::string function_name_;
  std::vector<ray::api::TaskArg> args_;
};

// ---------- implementation ----------

template <typename F>
TaskCaller<F>::TaskCaller() {}

template <typename F>
TaskCaller<F>::TaskCaller(ray::api::RayRuntime *runtime,
                          ray::api::RemoteFunctionHolder remote_function_holder)
    : runtime_(runtime), remote_function_holder_(std::move(remote_function_holder)) {}

template <typename F>
template <typename... Args>
ObjectRef<boost::callable_traits::return_type_t<F>> TaskCaller<F>::Remote(
    Args &&... args) {
  ray::api::StaticCheck<F, Args...>();
  using ReturnType = boost::callable_traits::return_type_t<F>;
  ray::api::Arguments::WrapArgs(&args_, std::forward<Args>(args)...);
  auto returned_object_id = runtime_->Call(remote_function_holder_, args_);
  return ObjectRef<ReturnType>(returned_object_id);
}
}  // namespace call
}  // namespace ray
