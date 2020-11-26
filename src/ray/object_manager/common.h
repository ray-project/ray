
#pragma once

#include <boost/asio.hpp>
#include <functional>

#include "ray/common/id.h"
#include "ray/object_manager/format/object_manager_generated.h"

namespace ray {

/// A callback to asynchronously spill objects when space is needed. The
/// callback returns the amount of space still needed after the spilling is
/// complete.
using SpillObjectsCallback = std::function<int64_t(int64_t num_bytes_required)>;

/// A callback to call when space has been released.
using SpaceReleasedCallback = std::function<void()>;

/// A callback to call when a spilled object needs to be returned to the object store.
using RestoreSpilledObjectCallback = std::function<void(
    const ObjectID &, const std::string &, std::function<void(const ray::Status &)>)>;

}  // namespace ray
