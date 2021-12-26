#pragma once

#include "ray/common/id.h"
#include "ray/common/task/task_util.h"
#include "rust/cxx.h"
#include "rust/ray-rs-sys/src/lib.rs.h"

#include "ray/api.h"
#include "ray/core_worker/core_worker_process.h"
#include "ray/core_worker/core_worker_options.h"

#include "ray/util/event.h"
#include "ray/util/event_label.h"

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

namespace internal {

// todos:
Status ExecuteTask(
    ray::TaskType task_type, const std::string task_name, const RayFunction &ray_function,
    const std::unordered_map<std::string, double> &required_resources,
    const std::vector<std::shared_ptr<ray::RayObject>> &args_buffer,
    const std::vector<rpc::ObjectReference> &arg_refs,
    const std::vector<ObjectID> &return_ids, const std::string &debugger_breakpoint,
    std::vector<std::shared_ptr<ray::RayObject>> *results,
    std::shared_ptr<ray::LocalMemoryBuffer> &creation_task_exception_pb_bytes,
    bool *is_application_level_error,
    const std::vector<ConcurrencyGroup> &defined_concurrency_groups,
    const std::string name_of_concurrency_group_to_execute) {
  RAY_LOG(INFO) << "Execute task: " << TaskType_Name(task_type);
  RAY_CHECK(ray_function.GetLanguage() == ray::Language::RUST);
  auto function_descriptor = ray_function.GetFunctionDescriptor();
  RAY_CHECK(function_descriptor->Type() ==
            ray::FunctionDescriptorType::kRustFunctionDescriptor);
  auto typed_descriptor = function_descriptor->As<ray::RustFunctionDescriptor>();
  std::string func_name = typed_descriptor->FunctionName();

  Status status{};
  std::shared_ptr<msgpack::sbuffer> data = nullptr;
  // ArgsBufferList ray_args_buffer;
  for (size_t i = 0; i < args_buffer.size(); i++) {
    auto &arg = args_buffer.at(i);

    // Since `SharedPtr<RayObject>` "ensures" the lifetime of underlying
    // Buffer for the duration of task execution,
    // we can pass the raw pointers safely.

    // The user-side code will deserialize these buffers into their
    // respective arg types.

    // (surely the answer is yes or zero-copy shared memory wouldn't be possible?)

    // The lifetime of the slice
    rust::Vec<uint8_t> buf = ray::allocate_vec_and_copy_from_raw_parts(
      arg->GetData()->Data(), arg->GetData()->Size()
    );
    // sbuf.write((const char *)(arg->GetData()->Data()), arg->GetData()->Size());

    // ray_args_buffer.push_back(std::move(sbuf));
  }
  // if (task_type == ray::TaskType::ACTOR_CREATION_TASK) {
  //   std::tie(status, data) = GetExecuteResult(func_name, ray_args_buffer, nullptr);
  //   current_actor_ = data;
  // } else if (task_type == ray::TaskType::ACTOR_TASK) {
  //   RAY_CHECK(current_actor_ != nullptr);
  //   std::tie(status, data) =
  //       GetExecuteResult(func_name, ray_args_buffer, current_actor_.get());
  // } else {  // NORMAL_TASK
    // std::tie(status, data) = GetExecuteResult(func_name, ray_args_buffer, nullptr);
  // }

  // std::shared_ptr<ray::LocalMemoryBuffer> meta_buffer = nullptr;
  // if (!status.ok()) {
  //   if (status.IsIntentionalSystemExit()) {
  //     return status;
  //   } else {
  //     RAY_EVENT(ERROR, EL_RAY_CPP_TASK_FAILED)
  //             .WithField("task_type", TaskType_Name(task_type))
  //             .WithField("function_name", func_name)
  //         << "C++ task failed: " << status.ToString();
  //   }
  //
  //   std::string meta_str = std::to_string(ray::rpc::ErrorType::TASK_EXECUTION_EXCEPTION);
  //   meta_buffer = std::make_shared<ray::LocalMemoryBuffer>(
  //       reinterpret_cast<uint8_t *>(&meta_str[0]), meta_str.size(), true);
  //
  //   msgpack::sbuffer buf;
  //   std::string msg = status.ToString();
  //   buf.write(msg.data(), msg.size());
  //   data = std::make_shared<msgpack::sbuffer>(std::move(buf));
  // }
  //
  // results->resize(return_ids.size(), nullptr);
  // if (task_type != ray::TaskType::ACTOR_CREATION_TASK) {
  //   size_t data_size = data->size();
  //   auto &result_id = return_ids[0];
  //   auto result_ptr = &(*results)[0];
  //   int64_t task_output_inlined_bytes = 0;
  //   RAY_CHECK_OK(CoreWorkerProcess::GetCoreWorker().AllocateReturnObject(
  //       result_id, data_size, meta_buffer, std::vector<ray::ObjectID>(),
  //       task_output_inlined_bytes, result_ptr));
  //
  //   auto result = *result_ptr;
  //   if (result != nullptr) {
  //     if (result->HasData()) {
  //       memcpy(result->GetData()->Data(), data->data(), data_size);
  //     }
  //   }
  //
  //   RAY_CHECK_OK(CoreWorkerProcess::GetCoreWorker().SealReturnObject(result_id, result));
  // } else {
  //   if (!status.ok()) {
  //     return ray::Status::CreationTaskError();
  //   }
  // }
  return ray::Status::OK();
}
} // namespace internal

void InitRust() {
  ray::RayConfig config;
  ray::Init(config, internal::ExecuteTask, 0, nullptr);
}

} // namespace ray
