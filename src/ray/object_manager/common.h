
#pragma once

#include <functional>

namespace ray {

/// A callback to asynchronously spill objects when space is needed. The
/// callback returns the amount of space still needed after the spilling is
/// complete.
using SpillObjectsCallback =
    std::function<int64_t(int64_t num_bytes_required, int64_t min_bytes_required)>;

/// A callback to call when space has been released.
using SpaceReleasedCallback = std::function<void()>;

}  // namespace ray
