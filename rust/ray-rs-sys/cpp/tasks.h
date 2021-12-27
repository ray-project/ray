#pragma once

#include "ray/common/id.h"
#include "ray/common/task/task_util.h"
#include "ray/core_worker/core_worker_process.h"
#include "ray/core_worker/core_worker_options.h"
#include "ray/util/event.h"
#include "ray/util/event_label.h"

#include "rust/cxx.h"
#include "rust/ray-rs-sys/src/lib.rs.h"

#include ""

#include "ray/api.h"
#include <ray/api/common_types.h>

namespace ray {

using ray::core::CoreWorkerProcess;
using ray::core::TaskOptions;
using ray::core::RayFunction;

using TaskArgs = std::unique_ptr<::ray::TaskArg>;

std::vector<std::unique_ptr<::ray::TaskArg>> TransformArgs(
    const rust::Vec<RustTaskArg>& args) {
  std::vector<std::unique_ptr<::ray::TaskArg>> ray_args;
  for (auto& arg : args) {
    std::unique_ptr<::ray::TaskArg> ray_arg = nullptr;
    if (arg.is_value()) {
      rust::Vec<uint8_t> buffer = arg.value();
      auto memory_buffer = std::make_shared<ray::LocalMemoryBuffer>(
          buffer.data(), buffer.size(), true);
      ray_arg = std::make_unique<ray::TaskArgByValue>(std::make_shared<ray::RayObject>(
          memory_buffer, nullptr, std::vector<rpc::ObjectReference>()));
    } else {
      auto id = ObjectID::FromBinary(static_cast<std::string>(arg.object_ref()));
      auto owner_address = ray::rpc::Address{};
      if (CoreWorkerProcess::IsInitialized()) {
        auto &core_worker = CoreWorkerProcess::GetCoreWorker();
        owner_address = core_worker.GetOwnerAddress(id);
      }
      ray_arg = std::make_unique<ray::TaskArgByReference>(id, owner_address,
                                                           /*call_site=*/"");
    }
    ray_args.push_back(std::move(ray_arg));
  }
  return ray_args;
}

/// The purpose of this interface is to
std::unique_ptr<ObjectID> Submit(rust::Str name, const rust::Vec<RustTaskArg>& args) {
  auto &core_worker = CoreWorkerProcess::GetCoreWorker();
  TaskOptions options{};
  std::vector<rpc::ObjectReference> return_refs;
  BundleID bundle_id = std::make_pair(PlacementGroupID::Nil(), -1);
  auto function_descriptor = FunctionDescriptorBuilder::BuildCpp(static_cast<std::string>(name));
  auto ray_args = TransformArgs(args);
  return_refs =
      core_worker.SubmitTask(RayFunction(ray::Language::CPP, function_descriptor), ray_args, options, 1,
                             false, std::move(bundle_id), true, "");
  std::vector<ObjectID> return_ids;
  for (const auto &ref : return_refs) {
    return_ids.push_back(ObjectID::FromBinary(ref.object_id()));
  }
  return std::make_unique<ObjectID>(return_ids[0]);
}

void InitRust() {
  ray::RayConfig config;
  ray::Init(config, internal::ExecuteTask, 0, nullptr);
}

} // namespace ray
