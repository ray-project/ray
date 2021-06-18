
#pragma once

#include <ray/api/actor_handle.h>
#include <ray/api/task_options.h>

namespace ray {
namespace api {

template <typename F>
using GetActorType = std::remove_pointer_t<boost::callable_traits::return_type_t<F>>;

template <typename F>
class ActorCreator {
 public:
  ActorCreator() {}

  ActorCreator(RayRuntime *runtime, RemoteFunctionHolder remote_function_holder)
      : runtime_(runtime), remote_function_holder_(std::move(remote_function_holder)) {}

  template <typename... Args>
  ActorHandle<GetActorType<F>> Remote(Args &&... args);

  ActorCreator &SetName(std::string name) {
    create_options_.name = std::move(name);
    return *this;
  }

  ActorCreator &SetResources(std::unordered_map<std::string, double> resources) {
    create_options_.resources = std::move(resources);
    return *this;
  }

  ActorCreator &SetResources(std::string name, double value) {
    create_options_.resources.emplace(std::move(name), value);
    return *this;
  }

  ActorCreator &SetMaxRestarts(int max_restarts) {
    create_options_.max_restarts = max_restarts;
    return *this;
  }

  ActorCreator &SetMaxConcurrency(int max_concurrency) {
    create_options_.max_concurrency = max_concurrency;
    return *this;
  }

 private:
  RayRuntime *runtime_;
  RemoteFunctionHolder remote_function_holder_;
  std::vector<ray::api::TaskArg> args_;
  ActorCreationOptions create_options_{};
};

// ---------- implementation ----------
template <typename F>
template <typename... Args>
ActorHandle<GetActorType<F>> ActorCreator<F>::Remote(Args &&... args) {
  StaticCheck<F, Args...>();
  CheckTaskOptions(create_options_.resources);
  Arguments::WrapArgs(&args_, std::forward<Args>(args)...);
  auto returned_actor_id =
      runtime_->CreateActor(remote_function_holder_, args_, create_options_);
  return ActorHandle<GetActorType<F>>(returned_actor_id);
}
}  // namespace api
}  // namespace ray
