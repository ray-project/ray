#ifndef RAY_COMMON_TASK_TASK_COMMON_H
#define RAY_COMMON_TASK_TASK_COMMON_H

#include "ray/protobuf/common.pb.h"

namespace ray {

// NOTE(hchen): Below we alias `ray::rpc::Language|TaskType)` in  `ray` namespace.
// The reason is because other code should use them as if they were defined in this
// `task_common.h` file, shouldn't care about the implementation detail that they
// are defined in protobuf.

/// See `common.proto` for definition of `Language` enum.
using Language = rpc::Language;
/// See `common.proto` for definition of `TaskType` enum.
using TaskType = rpc::TaskType;

}  // namespace ray

#endif
