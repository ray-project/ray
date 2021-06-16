
#pragma once

#include <ray/api/actor_handle.h>

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

 private:
  RayRuntime *runtime_;
  RemoteFunctionHolder remote_function_holder_;
  std::vector<ray::api::TaskArg> args_;
};

// ---------- implementation ----------
template <typename F>
template <typename... Args>
ActorHandle<GetActorType<F>> ActorCreator<F>::Remote(Args &&... args) {
  StaticCheck<F, Args...>();
  Arguments::WrapArgs(&args_, std::forward<Args>(args)...);
  auto returned_actor_id = runtime_->CreateActor(remote_function_holder_, args_);
  return ActorHandle<GetActorType<F>>(returned_actor_id);
}
}  // namespace api
}  // namespace ray
