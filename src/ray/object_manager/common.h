
#pragma once

#include <boost/asio.hpp>
#include <functional>

#include "ray/common/id.h"
#include "ray/object_manager/format/object_manager_generated.h"

namespace ray {

/// A callback to asynchronously spill objects when space is needed.
/// The callback tries to spill objects as much as num_bytes_to_spill and returns
/// the amount of space needed after the spilling is complete.
/// The returned value is calculated based off of min_bytes_to_spill. That says,
/// although it fails to spill num_bytes_to_spill, as long as it spills more than
/// min_bytes_to_spill, it will return the value that is less than 0 (meaning we
/// don't need any more additional space).
using SpillObjectsCallback =
    std::function<int64_t(int64_t num_bytes_to_spill, int64_t min_bytes_to_spill)>;

/// A callback to call when space has been released.
using SpaceReleasedCallback = std::function<void()>;

/// A callback to call when a spilled object needs to be returned to the object store.
using RestoreSpilledObjectCallback = std::function<void(
    const ObjectID &, const std::string &, std::function<void(const ray::Status &)>)>;

}  // namespace ray
