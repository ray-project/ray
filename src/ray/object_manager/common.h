
#pragma once

#include <functional>

namespace ray {

/// A callback to asynchronously spill objects when space is needed.
/// It spills objects as much as the max throughput and return bytes of spilled objects.
using SpillObjectsCallback = std::function<int64_t()>;

/// A callback to call when space has been released.
using SpaceReleasedCallback = std::function<void()>;

}  // namespace ray
