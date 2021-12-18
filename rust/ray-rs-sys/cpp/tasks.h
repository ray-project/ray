#include "ray/common/id.h"
#include "ray/common/task/task_util.h"

namespace ray {

using ray::core::CoreWorkerProcess;
using ray::core::TaskOptions;
using ray::core::RayFunction;

ObjectID Submit(std::string name, std::vector<std::unique_ptr<::ray::TaskArg>> args) {
  auto &core_worker = CoreWorkerProcess::GetCoreWorker();
  TaskOptions options{};
  std::vector<rpc::ObjectReference> return_refs;
  BundleID bundle_id = std::make_pair(PlacementGroupID::Nil(), -1);
  auto function_descriptor = FunctionDescriptorBuilder::BuildRust(name);
  return_refs =
      core_worker.SubmitTask(RayFunction(ray::Language::RUST, function_descriptor), args, options, 1,
                             false, std::move(bundle_id), true, "");
  std::vector<ObjectID> return_ids;
  for (const auto &ref : return_refs) {
    return_ids.push_back(ObjectID::FromBinary(ref.object_id()));
  }
  return return_ids[0];
}

std::vector<std::unique_ptr<::ray::TaskArg>> TransformArgs(
    std::vector<ray::internal::TaskArg> &args) {
  std::vector<std::unique_ptr<::ray::TaskArg>> ray_args;
  for (auto &arg : args) {
    std::unique_ptr<::ray::TaskArg> ray_arg = nullptr;
    if (arg.buf) {
      auto &buffer = *arg.buf;
      auto memory_buffer = std::make_shared<ray::LocalMemoryBuffer>(
          reinterpret_cast<uint8_t *>(buffer.data()), buffer.size(), true);
      ray_arg = std::make_unique<ray::TaskArgByValue>(std::make_shared<ray::RayObject>(
          memory_buffer, nullptr, std::vector<rpc::ObjectReference>()));
    } else {
      RAY_CHECK(arg.id);
      ray_arg = std::make_unique<ray::TaskArgByReference>(ObjectID::FromBinary(*arg.id),
                                                           ray::rpc::Address{},
                                                           /*call_site=*/"");
    }
    ray_args.push_back(std::move(ray_arg));
  }

  return ray_args;
}
}
