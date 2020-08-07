#include "native_task_submitter.h"
#include <ray/api/ray_exception.h>
#include "../../util/address_helper.h"
#include "../abstract_ray_runtime.h"

namespace ray {
namespace api {

NativeTaskSubmitter::NativeTaskSubmitter(NativeRayRuntime &native_ray_tuntime_)
    : native_ray_tuntime_(native_ray_tuntime_) {}

ObjectID NativeTaskSubmitter::Submit(const InvocationSpec &invocation) {

  auto base_addr = GetBaseAddressOfLibraryFromAddr((void *)invocation.fptr.function_pointer);

  auto func_offset = (size_t)(invocation.fptr.function_pointer - base_addr);
  auto exec_func_offset = (size_t)(invocation.fptr.exec_function_pointer - base_addr);
  auto function_descriptor = FunctionDescriptorBuilder::BuildCpp(
      invocation.lib_name, std::to_string(func_offset),
      std::to_string(exec_func_offset));
  auto ray_function = RayFunction(Language::CPP, function_descriptor);

  auto buffer = std::make_shared<::ray::LocalMemoryBuffer>(
      reinterpret_cast<uint8_t *>(invocation.args->data()), invocation.args->size(),
      true);
  std::vector<std::unique_ptr<ray::TaskArg>> args;
  auto task_arg = new TaskArgByValue(
      std::make_shared<::ray::RayObject>(buffer, nullptr, std::vector<ObjectID>()));
  args.emplace_back(task_arg);
  
  auto &core_worker = CoreWorkerProcess::GetCoreWorker();
  std::vector<ObjectID> return_ids;
  core_worker.SubmitTask(ray_function, args, TaskOptions(), &return_ids, 1, std::make_pair(PlacementGroupID::Nil(), -1));
  return return_ids[0];
}

ObjectID NativeTaskSubmitter::SubmitTask(const InvocationSpec &invocation) {
  return Submit(invocation);
}

ActorID NativeTaskSubmitter::CreateActor(const InvocationSpec &invocation) {
  return native_ray_tuntime_.GetNextActorID();
}

ObjectID NativeTaskSubmitter::SubmitActorTask(const InvocationSpec &invocation) {
  return Submit(invocation);
}

}  // namespace api
}  // namespace ray
