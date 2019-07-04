#ifndef RAY_GCS_CALL_BACK_H
#define RAY_GCS_CALL_BACK_H

#include <boost/optional/optional.hpp>
#include <vector>
#include "ray/common/status.h"

namespace ray {

namespace gcs {

using StatusCallback = std::function<void(Status status)>;

template <typename Data>
using OptionalItemCallback =
    std::function<void(Status status, boost::optional<Data> datum)>;

template <typename Data>
using MultiItemCallback =
    std::function<void(Status status, const std::vector<Data> &datums)>;

template <typename ID, typename Data>
using SubscribeCallback =
    std::function<void(const ID &id, const std::vector<Data> &datums)>;

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_CALL_BACK_H
