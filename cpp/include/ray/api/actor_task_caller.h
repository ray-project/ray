
#pragma once

#include <ray/api/arguments.h>
#include <ray/api/object_ref.h>
#include <ray/api/static_check.h>
#include <ray/api/task_options.h>
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

  ActorTaskCaller &SetName(std::string name) {
    task_options_.name = std::move(name);
    return *this;
  }

  ActorTaskCaller &SetResources(std::unordered_map<std::string, double> resources) {
    task_options_.resources = std::move(resources);
    return *this;
  }

  ActorTaskCaller &SetResources(std::string name, double value) {
    task_options_.resources.emplace(std::move(name), value);
    return *this;
  }

 private:
  RayRuntime *runtime_;
  std::string id_;
  RemoteFunctionHolder remote_function_holder_;
  std::vector<ray::api::TaskArg> args_;
  CallOptions task_options_;
};

// ---------- implementation ----------

template <typename F>
template <typename... Args>
ObjectRef<boost::callable_traits::return_type_t<F>> ActorTaskCaller<F>::Remote(
    Args &&... args) {
  using ReturnType = boost::callable_traits::return_type_t<F>;
  StaticCheck<F, Args...>();
  CheckTaskOptions(task_options_.resources);

  Arguments::WrapArgs(&args_, std::forward<Args>(args)...);
  auto returned_object_id =
      runtime_->CallActor(remote_function_holder_, id_, args_, task_options_);
  return ObjectRef<ReturnType>(returned_object_id);
}

}  // namespace api
}  // namespace ray
