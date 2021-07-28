
#pragma once

#include <ray/api/static_check.h>

namespace ray {
namespace api {

template <typename F>
class TaskCaller {
 public:
  TaskCaller();

  TaskCaller(RayRuntime *runtime, RemoteFunctionHolder remote_function_holder);

  template <typename... Args>
  ObjectRef<boost::callable_traits::return_type_t<F>> Remote(Args &&... args);

 private:
  RayRuntime *runtime_;
  RemoteFunctionHolder remote_function_holder_{};
  std::string function_name_;
  std::vector<ray::api::TaskArg> args_;
};

// ---------- implementation ----------

template <typename F>
TaskCaller<F>::TaskCaller() {}

template <typename F>
TaskCaller<F>::TaskCaller(RayRuntime *runtime,
                          RemoteFunctionHolder remote_function_holder)
    : runtime_(runtime), remote_function_holder_(std::move(remote_function_holder)) {}

template <typename F>
template <typename... Args>
ObjectRef<boost::callable_traits::return_type_t<F>> TaskCaller<F>::Remote(
    Args &&... args) {
  StaticCheck<F, Args...>();
  using ReturnType = boost::callable_traits::return_type_t<F>;
  Arguments::WrapArgs(&args_, std::forward<Args>(args)...);
  auto returned_object_id = runtime_->Call(remote_function_holder_, args_);
  return ObjectRef<ReturnType>(returned_object_id);
}
}  // namespace api
}  // namespace ray
