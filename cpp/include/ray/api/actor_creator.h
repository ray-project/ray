
#pragma once

#include <ray/api/actor_handle.h>

namespace ray {
namespace call {

template <typename F>
using GetActorType = std::remove_pointer_t<boost::callable_traits::return_type_t<F>>;

template <typename F>
class ActorCreator {
 public:
  ActorCreator() {}

  ActorCreator(ray::runtime::RayRuntime *runtime,
               ray::runtime::RemoteFunctionHolder remote_function_holder)
      : runtime_(runtime), remote_function_holder_(std::move(remote_function_holder)) {}

  template <typename... Args>
  ray::ActorHandle<GetActorType<F>> Remote(Args &&... args);

 private:
  ray::runtime::RayRuntime *runtime_;
  ray::runtime::RemoteFunctionHolder remote_function_holder_;
  std::vector<ray::serializer::TaskArg> args_;
};

// ---------- implementation ----------
template <typename F>
template <typename... Args>
ray::ActorHandle<GetActorType<F>> ActorCreator<F>::Remote(Args &&... args) {
  ray::internal::StaticCheck<F, Args...>();
  ray::serializer::Arguments::WrapArgs(&args_, std::forward<Args>(args)...);
  auto returned_actor_id = runtime_->CreateActor(remote_function_holder_, args_);
  return ray::ActorHandle<GetActorType<F>>(returned_actor_id);
}
}  // namespace call
}  // namespace ray
