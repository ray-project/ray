#pragma once

#include "ray/common/id.h"
#include "ray/common/task/task_util.h"
#include "ray/core_worker/core_worker_process.h"
#include "ray/core_worker/core_worker_options.h"
#include "ray/util/event.h"
#include "ray/util/event_label.h"

#include "rust/cxx.h"
#include "rust/ray-rs-sys/src/lib.rs.h"

#include "ray/api.h"
#include <ray/api/common_types.h>

namespace ray {

using ray::core::CoreWorkerProcess;
using ray::core::TaskOptions;
using ray::core::RayFunction;

struct RustTaskArg;

using TaskArgs = std::unique_ptr<::ray::TaskArg>;

std::vector<std::unique_ptr<::ray::TaskArg>> TransformArgs(const rust::Vec<RustTaskArg>& args);

std::unique_ptr<ObjectID> Submit(rust::Str name, const rust::Vec<RustTaskArg>& args);

void InitRust();

namespace internal {
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
  const std::string name_of_concurrency_group_to_execute
);
}


} // namespace ray
