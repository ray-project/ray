#ifndef RAY_GCS_CALLBACK_H
#define RAY_GCS_CALLBACK_H

#include <boost/optional/optional.hpp>
#include <vector>
#include "ray/common/status.h"

namespace ray {

namespace gcs {

using StatusCallback = std::function<void(Status status)>;

template <typename Data>
using OptionalItemCallback =
    std::function<void(Status status, boost::optional<Data> result)>;

template <typename Data>
using MultiItemCallback =
    std::function<void(Status status, const std::vector<Data> &result)>;

template <typename ID, typename Data>
using SubscribeCallback =
    std::function<void(const ID &id, const std::vector<Data> &result)>;

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_CALLBACK_H
