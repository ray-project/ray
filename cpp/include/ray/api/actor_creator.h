
#pragma once

#include <ray/api/actor_handle.h>

namespace ray {
namespace internal {

template <typename F>
using GetActorType = std::remove_pointer_t<boost::callable_traits::return_type_t<F>>;

template <typename F>
class ActorCreator {
 public:
  ActorCreator() {}

  ActorCreator(ray::internal::RayRuntime *runtime,
               ray::internal::RemoteFunctionHolder remote_function_holder)
      : runtime_(runtime), remote_function_holder_(std::move(remote_function_holder)) {}

  template <typename... Args>
  ray::ActorHandle<GetActorType<F>> Remote(Args &&... args);

 private:
  ray::internal::RayRuntime *runtime_;
  ray::internal::RemoteFunctionHolder remote_function_holder_;
  std::vector<ray::internal::TaskArg> args_;
};

// ---------- implementation ----------
template <typename F>
template <typename... Args>
ray::ActorHandle<GetActorType<F>> ActorCreator<F>::Remote(Args &&... args) {
  ray::internal::StaticCheck<F, Args...>();
  ray::internal::Arguments::WrapArgs(&args_, std::forward<Args>(args)...);
  auto returned_actor_id = runtime_->CreateActor(remote_function_holder_, args_);
  return ray::ActorHandle<GetActorType<F>>(returned_actor_id);
}
}  // namespace internal
}  // namespace ray
