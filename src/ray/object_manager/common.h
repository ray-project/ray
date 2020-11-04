
#pragma once

#include <functional>

namespace ray {

/// A callback to asynchronously spill objects when space is needed. The
/// callback returns the amount of space still needed after the spilling is
/// complete.
using SpillObjectsCallback = std::function<int64_t(int64_t num_bytes_required)>;

}  // namespace ray
